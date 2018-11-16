# Author: Dustin Doubet
# Description: 
# Pandas SQL extention lib for writing dataframes in bulk vs single insert.
# A few other utility type methods as well

import re
import warnings
import traceback
from datetime import datetime, date

import numpy as np
import pandas.lib as lib
import pandas.core.common as com
from pandas.compat import map, zip, string_types
from pandas.core.api import DataFrame, Series
from pandas.core.common import isnull
from pandas.core.base import PandasObject
from pandas.tseries.tools import to_datetime

from sqlalchemy import exc
from sqlalchemy.schema import MetaData


#------------------------------------------------------------------------------

def _convert_params(sql, params):
    """convert sql and params args to DBAPI2.0 compliant format"""
    args = [sql]
    if params is not None:
        if hasattr(params, 'keys'):  # test if params is a mapping
            args += [params]
        else:
            args += [list(params)]
    return args


def _handle_date_column(col, format=None):
    if isinstance(format, dict):
        return to_datetime(col, **format)
    else:
        if format in ['D', 's', 'ms', 'us', 'ns']:
            return to_datetime(col, coerce=True, unit=format)
        elif (issubclass(col.dtype.type, np.floating)
                or issubclass(col.dtype.type, np.integer)):
            # parse dates as timestamp
            format = 's' if format is None else format
            return to_datetime(col, coerce=True, unit=format)
        else:
            return to_datetime(col, coerce=True, format=format)

def _parse_date_columns(data_frame, parse_dates):
    """
    Force non-datetime columns to be read as such.
    Supports both string formatted and integer timestamp columns
    """
    # handle non-list entries for parse_dates gracefully
    if parse_dates is True or parse_dates is None or parse_dates is False:
        parse_dates = []

    if not hasattr(parse_dates, '__iter__'):
        parse_dates = [parse_dates]

    for col_name in parse_dates:
        df_col = data_frame[col_name]
        try:
            fmt = parse_dates[col_name]
        except TypeError:
            fmt = None
        data_frame[col_name] = _handle_date_column(df_col, format=fmt)

    return data_frame


def _wrap_result(data, columns, index_col=None, coerce_float=True,
                 parse_dates=None):
    """Wrap result set of query in a DataFrame """

    frame = DataFrame.from_records(data, columns=columns,
                                   coerce_float=coerce_float)

    _parse_date_columns(frame, parse_dates)

    if index_col is not None:
        frame.set_index(index_col, inplace=True)

    return frame



def execute(sql, engine, params=None):
    """
    Execute the given SQL query using the provided connection object.

    Parameters
    ----------
    sql : string
        Query to be executed
    engine : SQLAlchemy engine or sqlite3 DBAPI2 connection
        Using SQLAlchemy makes it possible to use any DB supported by that
        library.
    params : list or tuple, optional
        List of parameters to pass to execute method.

    Returns
    -------
    Results Iterable
    """
   
    pandas_sql = SQLDatabase(engine, schema=None, meta=None)
    args = _convert_params(sql, params)
    
    return pandas_sql.execute(*args)


def read_sql_table(table_name, engine, schema=None, meta=None, index_col=None,
                            coerce_float=True, parse_dates=None, columns=None,
                            chunksize=None):
    """Read SQL database table into a DataFrame.

    Given a table name and an SQLAlchemy engine, returns a DataFrame.
    This function does not support DBAPI connections.

    Parameters
    ----------
    table_name : string
        Name of SQL table in database
    engine : SQLAlchemy engine
        Sqlite DBAPI connection mode not supported
    schema : string, default None
        Name of SQL schema in database to query (if database flavor
        supports this). If None, use default schema (default).
    meta : SQLAlchemy MetaData object, default None
        If provided, this MetaData object is used instead of using
        meta.reflect() to provide the table description. This allows to 
        specify database flavor specific arguments in the MetaData object.
    index_col : string, optional
        Column to set as index
    coerce_float : boolean, default True
        Attempt to convert values to non-string, non-numeric objects (like
        decimal.Decimal) to floating point. Can result in loss of Precision.
    parse_dates : list or dict
        - List of column names to parse as dates
        - Dict of ``{column_name: format string}`` where format string is
          strftime compatible in case of parsing string times or is one of
          (D, s, ns, ms, us) in case of parsing integer timestamps
        - Dict of ``{column_name: arg dict}``, where the arg dict corresponds
          to the keyword arguments of :func:`pandas.to_datetime`
          Especially useful with databases without native Datetime support,
          such as SQLite
    columns : list
        List of column names to select from sql table
    chunksize : int, default None
        If specified, return an iterator where `chunksize` is the number of
        rows to include in each chunk.

    Returns
    -------
    DataFrame

    See also
    --------
    read_sql_query : Read SQL query into a DataFrame.
    read_sql

    """
    if meta is None:
        meta = MetaData(engine, schema=schema)
 
    try:
        meta.reflect(only=[table_name])
        
    except exc.InvalidRequestError:
        raise ValueError("Table %s not found" % table_name)

    pandas_sql = SQLDatabase(engine, meta=meta)
    
    table = pandas_sql.read_table(table_name, index_col=index_col, coerce_float=coerce_float,
                                parse_dates=parse_dates, columns=columns, chunksize=chunksize)

    if table is not None:
        return table
    else:
        raise ValueError("Table %s not found" % table_name, con)


def read_sql_query(sql, engine, index_col=None, coerce_float=True, params=None,
                   parse_dates=None, chunksize=None):
    """Read SQL query into a DataFrame.

    Returns a DataFrame corresponding to the result set of the query
    string. Optionally provide an `index_col` parameter to use one of the
    columns as the index, otherwise default integer index will be used.

    Parameters
    ----------
    sql : string
        SQL query to be executed
    engine : SQLAlchemy engine or sqlite3 DBAPI2 connection
        Using SQLAlchemy makes it possible to use any DB supported by that
        library.
        If a DBAPI2 object, only sqlite3 is supported.
    index_col : string, optional
        Column name to use as index for the returned DataFrame object.
    coerce_float : boolean, default True
        Attempt to convert values to non-string, non-numeric objects (like
        decimal.Decimal) to floating point, useful for SQL result sets
    params : list, tuple or dict, optional
        List of parameters to pass to execute method.  The syntax used
        to pass parameters is database driver dependent. Check your
        database driver documentation for which of the five syntax styles,
        described in PEP 249's paramstyle, is supported.
        Eg. for psycopg2, uses %(name)s so use params={'name' : 'value'}
    parse_dates : list or dict
        - List of column names to parse as dates
        - Dict of ``{column_name: format string}`` where format string is
          strftime compatible in case of parsing string times or is one of
          (D, s, ns, ms, us) in case of parsing integer timestamps
        - Dict of ``{column_name: arg dict}``, where the arg dict corresponds
          to the keyword arguments of :func:`pandas.to_datetime`
          Especially useful with databases without native Datetime support,
          such as SQLite
    chunksize : int, default None
        If specified, return an iterator where `chunksize` is the number of
        rows to include in each chunk.

    Returns
    -------
    DataFrame

    See also
    --------
    read_sql_table : Read SQL database table into a DataFrame
    read_sql

    """
    pandas_sql = SQLDatabase(engine, schema=None, meta=None)
    
    return pandas_sql.read_query(sql, index_col=index_col, params=params, 
                                  coerce_float=coerce_float, parse_dates=parse_dates, 
                                  chunksize=chunksize)
                                  
                                  
def has_table(table_name, engine, schema=None):
    """
    Check if DataBase has named table.

    Parameters
    ----------
    table_name: string
        Name of SQL table
    engine: SQLAlchemy engine or sqlite3 DBAPI2 connection
        Using SQLAlchemy makes it possible to use any DB supported by that
        library.
        If a DBAPI2 object, only sqlite3 is supported.
    schema : string, default None
        Name of SQL schema in database to write to (if database flavor supports
        this). If None, use default schema (default).

    Returns
    -------
    boolean
    """
    pandas_sql = SQLDatabase(engine, schema=schema, meta=None)
    return pandas_sql.has_table(table_name)
    
    
def add_metadata(table_names, meta, not_found='ignore'):
    """
    Parameters
    ----------
    table_name: string
        Name of SQL table
    meta : SQLAlchemy MetaData object, used to reflect database 
        table descriptions. This allows to specify database flavor 
        specific arguments in the MetaData object.
    """
                
    if not isinstance(table_names,list):
        table_names = [table_names]
        
    for table_name in table_names:   
        try:
            meta.reflect(only=[table_name])
            
        except exc.InvalidRequestError:
            if not_found == 'ignore':
                pass
            elif not_found == 'fail':
                raise ValueError("Table %s not found" % table_name)
                
                
def get_schema(self, engine, frame, name, keys=None):
    """
    Get the SQL db table schema for the given frame.

    Parameters
    ----------
    frame : DataFrame
    name : string
        name of SQL table
    keys : string or sequence
        columns to use a primary key
    engine: an open SQL database connection object or an SQLAlchemy engine
        Using SQLAlchemy makes it possible to use any DB supported by that
        library.
        If a DBAPI2 object, only sqlite3 is supported.

    """
    pandas_sql = SQLDatabase(engine, schema=None, meta=None)
    return pandas_sql._create_sql_schema(frame, name, keys=keys)
    
          

class SQLTable(PandasObject):
    """
    For mapping Pandas tables to SQL tables.
    Uses fact that table is reflected by SQLAlchemy to
    do better type convertions.
    Also holds various flags needed to avoid having to
    pass them between functions all the time.
    """
    # TODO: support for multiIndex
    def __init__(self, name, class_method, frame=None, table_setup=False, 
                index=False, index_label=None, if_exists='fail', prefix='pandas', 
                                schema=None, table_keys=None, sql_para_max=2100):
                  
        self.name = name
        self.pd_sql = class_method
        self.prefix = prefix
        self.frame = frame
        self.index = self._index_name(index, index_label)
        self.schema = schema
        self.if_exists = if_exists
        self.keys = table_keys
        self.sql_para_max = sql_para_max

  
        if table_setup:
            self.table = self._create_table_setup()
        else:
            self.table = self.pd_sql.get_table(self.name, self.schema)

        if self.table is None:
            if table_setup is True:
                raise ValueError("Could not init table '%s'" % name)
            else:
                # Quick check before doing anything
                if not self.exists():
                    raise exc.NoSuchTableError("The provided table name '{0}'"
                                               " was not found in {1}".format(
                                                 self.name,self.pd_sql.name))

    def exists(self):
        return self.pd_sql.has_table(self.name, self.schema)

    def sql_schema(self):
        from sqlalchemy.schema import CreateTable
        return str(CreateTable(self.table))

    def _execute_create(self):
        # Inserting table into database, add to MetaData object
        self.table = self.table.tometadata(self.pd_sql.meta)
        self.table.create()

    def create(self):
        if self.exists():
            if self.if_exists == 'fail':
                raise ValueError("Table '{0}' already exists.".format(self.name))
            elif self.if_exists == 'replace':
                self.pd_sql.drop_table(self.name, self.schema)
                self._execute_create()
            elif self.if_exists == 'append':
                pass
            else:
                raise ValueError(
                    "'{0}' is not valid for if_exists".format(self.if_exists))
        else:
            self._execute_create()


    def insert_data(self, copy=True):
        """doc string"""
    
        if copy:
            temp = self.frame.copy()
        else:
            temp = self.frame
            
        if self.index is not None: 
            temp_index = self.frame.index
            temp.index.names = self.index
            try:
                temp.reset_index(inplace=True)
            except ValueError as err:
                raise ValueError(
                    "duplicate name in index/columns: {0}".format(err))
     

        column_names = list(map(str, temp.columns))
        ncols = len(column_names)
        data_list = [None] * ncols
        blocks = temp._data.blocks

        for i in xrange(len(blocks)):
            b = blocks[i]
            if b.is_datetime:
                # convert to microsecond resolution so this yields
                # datetime.datetime
                d = b.values.astype('M8[us]').astype(object)
            else:
                d = np.array(b.values, dtype=object)

            # replace NaN with None
            if b._can_hold_na:
                mask = isnull(d)
                d[mask] = None

            for col_loc, col in zip(b.mgr_locs, d):
                data_list[col_loc] = col

        return column_names, ncols, data_list

    def _execute_many_insert(self, conn, keys, data_iter):
        """doc string"""
        
        data = [dict((k, v) for k, v in zip(keys, row)) for row in data_iter]
        result = conn.execute(self.table.insert(), data)
        result.close()
        
    def _execute_bulk_insert(self, conn, keys, data_iter):
        """doc string"""
    
        #data = [item for item in data_iter]
        data = [dict((k, v) for k, v in zip(keys, row)) for row in data_iter]
        result = conn.execute(self.table.insert().values(data))
        result.close()
        
    def insert(self, conn, bulk, chunksize=None, auto_adjust=True, copy=True):
        """doc string"""
        
        #Sub Function#
        def sub_insert(conn, bulk, chunks, columns_names, chunksize, nrows, data_list):
            """doc string"""
            for i in xrange(chunks):
                start_i = i * chunksize
                end_i = min((i + 1) * chunksize, nrows)
                if start_i >= end_i:
                    break

                chunk_iter = zip(*[arr[start_i:end_i] for arr in data_list])
             
    
                if bulk:
                    self._execute_bulk_insert(conn, columns_names, chunk_iter)
                else:
                    self._execute_many_insert(conn, columns_names, chunk_iter)
                    
        #Start Method#
        
        columns_names, ncols, data_list = self.insert_data(copy)

        nrows = len(self.frame)
        
        if nrows == 0:
            return

        if chunksize is None:
            if ncols*nrows >= self.sql_para_max:
                chunksize = int(self.sql_para_max/ncols)
                if chunksize*ncols == self.sql_para_max:
                    chunksize = int(self.sql_para_max/(ncols + 1))
            else:
                chunksize = nrows
                        
        elif chunksize == 0:
            raise ValueError("chunksize argument should be non-zero")
         
        elif ncols*nrows >= self.sql_para_max:
            if auto_adjust:
                chunksize = int(self.sql_para_max/ncols)
                if chunksize*ncols == self.sql_para_max:
                    chunksize = int(self.sql_para_max/(ncols + 1))
                warnings.warn("chunksize was adjusted to %s to avoid the SQL \
                               %s (nrows*ncols) parameter limit"%(chunksize,self.sql_para_max))
            elif not auto_adjust:
                raise ValueError("chunksize argument should be adjusted to allow \
                                  for the SQL %s (nrows*ncols) parameter limit"%self.sql_para_max)
            else:
                raise ValueError("%s is not a valid for if_adjust")

        chunks = int(nrows / chunksize) + 1

        #If conn is passed in, use it but no commit or closure, which gives
        #transaction control and connection closure back to the creator
        if conn != None:
            if not conn.closed:
                sub_insert(conn, bulk, chunks, columns_names, chunksize, nrows, data_list)
            else:
                raise exc.SQLAlchemyError("Connection closed")
                
        else:
            #No conn passed in so execute insert with connectionless option
            #and context manager, which will close the connection and commit
            #on success or rollback upon failure.
            with self.pd_sql.engine.begin() as conn:
                sub_insert(conn, bulk, chunks, columns_names, chunksize, nrows, data_list)
                      
                    
    def _query_iterator(self, result, chunksize, columns, coerce_float=True, parse_dates=None):
        """Return generator through chunked result set"""

        while True:
            data = result.fetchmany(chunksize)
            if not data:
                break
            else:
                self.frame = DataFrame.from_records(
                    data, columns=columns, coerce_float=coerce_float)

                self._harmonize_columns(parse_dates=parse_dates)

                if self.index is not None:
                    self.frame.set_index(self.index, inplace=True)

                yield self.frame

    def read(self, coerce_float=True, parse_dates=None, columns=None, chunksize=None):
        """doc string"""

        if columns is not None and len(columns) > 0:
            from sqlalchemy import select
            cols = [self.table.c[n] for n in columns]
            if self.index is not None:
                [cols.insert(0, self.table.c[idx]) for idx in self.index[::-1]]
            sql_select = select(cols)
        else:
            sql_select = self.table.select()

        result = self.pd_sql.execute(sql_select)
        column_names = result.keys()

        if chunksize is not None:
            return self._query_iterator(result, chunksize, column_names,
                                        coerce_float=coerce_float,
                                        parse_dates=parse_dates)
        else:
            data = result.fetchall()
            self.frame = DataFrame.from_records(
                data, columns=column_names, coerce_float=coerce_float)

            self._harmonize_columns(parse_dates=parse_dates)

            if self.index is not None:
                self.frame.set_index(self.index, inplace=True)

            return self.frame

    def _index_name(self, index, index_label):
        """doc string"""
        # for writing: index=True to include index in sql table
        if index is True:
            nlevels = self.frame.index.nlevels
            # if index_label is specified, set this as index name(s)
            if index_label is not None:
                if not isinstance(index_label, list):
                    index_label = [index_label]
                if len(index_label) != nlevels:
                    raise ValueError(
                        "Length of 'index_label' should match number of "
                        "levels, which is {0}".format(nlevels))
                else:
                    return index_label
            # return the used column labels for the index columns
            if (nlevels == 1 and 'index' not in self.frame.columns
                    and self.frame.index.name is None):
                return ['index']
            else:
                return [l if l is not None else "level_{0}".format(i)
                        for i, l in enumerate(self.frame.index.names)]

        # for reading: index=(list of) string to specify column to set as index
        elif isinstance(index, string_types):
            return [index]
        elif isinstance(index, list):
            return index
        else:
            return None

    def _get_column_names_and_types(self, dtype_mapper):
        """doc string"""
        
        column_names_and_types = []
        if self.index is not None:
            for i, idx_label in enumerate(self.index):
                idx_type = dtype_mapper(
                    self.frame.index.get_level_values(i))
                column_names_and_types.append((idx_label, idx_type, True))

        column_names_and_types += [
            (str(self.frame.columns[i]),
             dtype_mapper(self.frame.iloc[:, i]),
             False)
            for i in range(len(self.frame.columns))
            ]

        return column_names_and_types

    def _create_table_setup(self):
        """doc string"""
        
        from sqlalchemy import Table, Column, PrimaryKeyConstraint

        column_names_and_types = \
            self._get_column_names_and_types(self._sqlalchemy_type)

        columns = [Column(name, typ, index=is_index)
                   for name, typ, is_index in column_names_and_types]

        if self.keys is not None:
            pkc = PrimaryKeyConstraint(self.keys, name=self.name + '_pk')
            columns.append(pkc)

        schema = self.schema or self.pd_sql.meta.schema

        # At this point, attach to new metadata, only attach to self.meta
        # once table is created.
        
        meta = MetaData(self.pd_sql, schema=schema)

        return Table(self.name, meta, *columns, schema=schema)

    def _harmonize_columns(self, parse_dates=None):
        """
        Make the DataFrame's column types align with the SQL table
        column types.
        Need to work around limited NA value support. Floats are always
        fine, ints must always be floats if there are Null values.
        Booleans are hard because converting bool column with None replaces
        all Nones with false. Therefore only convert bool if there are no
        NA values.
        Datetimes should already be converted to np.datetime64 if supported,
        but here we also force conversion if required
        """
        # handle non-list entries for parse_dates gracefully
        if parse_dates is True or parse_dates is None or parse_dates is False:
            parse_dates = []

        if not hasattr(parse_dates, '__iter__'):
            parse_dates = [parse_dates]

        for sql_col in self.table.columns:
            col_name = sql_col.name
            try:
                df_col = self.frame[col_name]
                # the type the dataframe column should have
                col_type = self._numpy_type(sql_col.type)

                if col_type is datetime or col_type is date:
                    if not issubclass(df_col.dtype.type, np.datetime64):
                        self.frame[col_name] = _handle_date_column(df_col)

                elif col_type is float:
                    # floats support NA, can always convert!
                    self.frame[col_name] = df_col.astype(col_type, copy=False)

                elif len(df_col) == df_col.count():
                    # No NA values, can convert ints and bools
                    if col_type is np.dtype('int64') or col_type is bool:
                        self.frame[col_name] = df_col.astype(col_type, copy=False)

                # Handle date parsing
                if col_name in parse_dates:
                    try:
                        fmt = parse_dates[col_name]
                    except TypeError:
                        fmt = None
                    self.frame[col_name] = _handle_date_column(
                        df_col, format=fmt)

            except KeyError:
                pass  # this column not in results

    def _sqlalchemy_type(self, col):
        """doc string"""
      
        from sqlalchemy.types import (BigInteger, Float, Text, Boolean, DateTime, Date, Time)

        if com.is_datetime64_dtype(col):
            try:
                tz = col.tzinfo
                return DateTime(timezone=True)
            except:
                return DateTime
        if com.is_timedelta64_dtype(col):
            warnings.warn("the 'timedelta' type is not supported, and will be "
                          "written as integer values (ns frequency) to the "
                          "database.", UserWarning)
            return BigInteger
        elif com.is_float_dtype(col):
            return Float
        elif com.is_integer_dtype(col):
            # TODO: Refine integer size.
            return BigInteger
        elif com.is_bool_dtype(col):
            return Boolean
        inferred = lib.infer_dtype(com._ensure_object(col))
        if inferred == 'date':
            return Date
        if inferred == 'time':
            return Time
        return Text

    def _numpy_type(self, sqltype):
        """doc string"""
    
        from sqlalchemy.types import Integer, Float, Boolean, DateTime, Date

        if isinstance(sqltype, Float):
            return float
        if isinstance(sqltype, Integer):
            # TODO: Refine integer size.
            return np.dtype('int64')
        if isinstance(sqltype, DateTime):
            # Caution: np.datetime64 is also a subclass of np.number.
            return datetime
        if isinstance(sqltype, Date):
            return date
        if isinstance(sqltype, Boolean):
            return bool
        return object


class SQLDatabase:
    """
    This class enables convertion between DataFrame and SQL databases
    using SQLAlchemy to handle DataBase abstraction

    Parameters
    ----------
    engine : SQLAlchemy engine
        Engine to connect with the database. Using SQLAlchemy makes it
        possible to use any DB supported by that library.
    schema : string, default None
        Name of SQL schema in database to write to (if database flavor
        supports this). If None, use default schema (default).
    meta : SQLAlchemy MetaData object, default None
        If provided, this MetaData object is used instead of a newly
        created. This allows to specify database flavor specific
        arguments in the MetaData object.

    """

    def __init__(self, engine, meta=None, schema=None):
        self.engine = engine
        if meta is None:
            meta = MetaData(self.engine, schema=schema)
          
        self.meta = meta

    def run_transaction(self):
        return self.engine.begin()

    def execute(self, *args, **kwargs):
        """Simple passthrough to SQLAlchemy engine"""
        return self.engine.execute(*args, **kwargs)

    def read_table(self, table_name, index_col=None, coerce_float=True, parse_dates=None, 
                                                columns=None, schema=None, chunksize=None):
        """Read SQL database table into a DataFrame.

        Parameters
        ----------
        table_name : string
            Name of SQL table in database
        index_col : string, optional
            Column to set as index
        coerce_float : boolean, default True
            Attempt to convert values to non-string, non-numeric objects
            (like decimal.Decimal) to floating point. This can result in
            loss of precision.
        parse_dates : list or dict
            - List of column names to parse as dates
            - Dict of ``{column_name: format string}`` where format string is
              strftime compatible in case of parsing string times or is one of
              (D, s, ns, ms, us) in case of parsing integer timestamps
            - Dict of ``{column_name: arg}``, where the arg corresponds
              to the keyword arguments of :func:`pandas.to_datetime`.
              Especially useful with databases without native Datetime support,
              such as SQLite
        columns : list
            List of column names to select from sql table
        schema : string, default None
            Name of SQL schema in database to query (if database flavor
            supports this).  If specified, this overwrites the default
            schema of the SQLDatabase object.
        chunksize : int, default None
            If specified, return an iterator where `chunksize` is the number
            of rows to include in each chunk.

        Returns
        -------
        DataFrame

        See also
        --------
        pandas.read_sql_table
        SQLDatabase.read_query

        """
        table = SQLTable(table_name, self, index=index_col, schema=schema)
        return table.read(coerce_float=coerce_float, parse_dates=parse_dates, columns=columns, chunksize=chunksize)

    @staticmethod
    def _query_iterator(result, chunksize, columns, index_col=None, coerce_float=True, parse_dates=None):
        """Return generator through chunked result set"""

        while True:
            data = result.fetchmany(chunksize)
            if not data:
                break
            else:
                yield _wrap_result(data, columns, index_col=index_col,
                                   coerce_float=coerce_float,
                                   parse_dates=parse_dates)

    def read_query(self, sql, index_col=None, coerce_float=True, parse_dates=None, 
                                                      params=None, chunksize=None):
        """Read SQL query into a DataFrame.

        Parameters
        ----------
        sql : string
            SQL query to be executed
        index_col : string, optional
            Column name to use as index for the returned DataFrame object.
        coerce_float : boolean, default True
            Attempt to convert values to non-string, non-numeric objects (like
            decimal.Decimal) to floating point, useful for SQL result sets
        params : list, tuple or dict, optional
            List of parameters to pass to execute method.  The syntax used
            to pass parameters is database driver dependent. Check your
            database driver documentation for which of the five syntax styles,
            described in PEP 249's paramstyle, is supported.
            Eg. for psycopg2, uses %(name)s so use params={'name' : 'value'}
        parse_dates : list or dict
            - List of column names to parse as dates
            - Dict of ``{column_name: format string}`` where format string is
              strftime compatible in case of parsing string times or is one of
              (D, s, ns, ms, us) in case of parsing integer timestamps
            - Dict of ``{column_name: arg dict}``, where the arg dict corresponds
              to the keyword arguments of :func:`pandas.to_datetime`
              Especially useful with databases without native Datetime support,
              such as SQLite

        Returns
        -------
        DataFrame

        See also
        --------
        read_sql_table : Read SQL database table into a DataFrame
        read_sql

        """
        args = _convert_params(sql, params)

        result = self.execute(*args)
        columns = result.keys()

        if chunksize is not None:
            return self._query_iterator(result, chunksize, columns,
                                        index_col=index_col,
                                        coerce_float=coerce_float,
                                        parse_dates=parse_dates)
        else:
            data = result.fetchall()
            frame = _wrap_result(data, columns, index_col=index_col,
                                 coerce_float=coerce_float,
                                 parse_dates=parse_dates)
            return frame

    

    def insert_bulk(self, frame, table_name, conn=None, index=False, index_label=None, 
                          schema=None, chunksize=None, copy=True, auto_adjust=True):
        """
        Write records stored in a DataFrame to a SQL database.

        Parameters
        ----------
        frame : DataFrame
        table_name : string
            Name of SQL table
        conn : SQLAlchemy connection e.i. engine.connect()
            The allows control of the transaction. If conn is None the 
            connectionless option will be used with a context manager. 
        index : boolean, default True
            Write DataFrame index as a column
        index_label : string or sequence, default None
            Column label for index column(s). If None is given (default) and
            `index` is True, then the index names are used.
            A sequence should be given if the DataFrame uses MultiIndex.
        schema : string, default None
            Name of SQL schema in database to write to (if database flavor
            supports this). If specified, this overwrites the default
            schema of the SQLDatabase object.
        chunksize : int, default None
            If not None, then rows will be written in batches of this size at a
            time.  If None, all rows will be written at once.
         copy : boolean, default False
            Whether to copy the frame when resetting the index, if index=True.
            There is a possibility that the index will not reset correctly and
            it could affect the current in frame.
    
        """

        table = SQLTable(table_name, self, frame=frame, table_setup=False, index=index,
                         if_exists='append', index_label=index_label, schema=schema)
                         
        table.insert(conn=conn, bulk=True, chunksize=chunksize, copy=copy, 
                                                            auto_adjust=auto_adjust)
                                                                              

                          
    def insert_many(self, frame, table_name, conn=None, index=False, index_label=None, 
                              schema=None, chunksize=None, copy=True, auto_adjust=True):
        """
        Write records stored in a DataFrame to a SQL database.

        Parameters
        ----------
        frame : DataFrame
        table_name : string
            Name of SQL table
        conn : SQLAlchemy connection e.i. engine.connect()
            The allows control of the transaction. If conn is None the 
            connectionless option will be used with a context manager.
        index : boolean, default True
            Write DataFrame index as a column
        index_label : string or sequence, default None
            Column label for index column(s). If None is given (default) and
            `index` is True, then the index names are used.
            A sequence should be given if the DataFrame uses MultiIndex.
        schema : string, default None
            Name of SQL schema in database to write to (if database flavor
            supports this). If specified, this overwrites the default
            schema of the SQLDatabase object.
        chunksize : int, default None
            If not None, then rows will be written in batches of this size at a
            time.  If None, all rows will be written at once.
        copy : boolean, default False
            Whether to copy the frame when resetting the index, if index=True.
            There is a possibility that the index will not reset correctly and
            it could affect the current in frame.
             
        """
                                        
        table = SQLTable(table_name, self, frame=frame, table_setup=False, index=index,
                         if_exists='append', index_label=index_label, schema=schema)
            
        table.insert(conn=conn, bulk=False, chunksize=chunksize, copy=copy, 
                                                            auto_adjust=auto_adjust)
        

    def table_from_frame(self, frame, table_name, conn=None, if_exists='fail', index=False,
                                  index_label=None, schema=None, chunksize=None, copy=True):
        """
        Create SQL database table from DataFrame structure and insert records from
        DataFrame into a SQL table.

        Parameters
        ----------
        frame : DataFrame
        table_name : string
            Name of SQL table
        conn : SQLAlchemy connection e.i. engine.connect()
            The allows control of the transaction. If conn is None the 
            connectionless option will be used with a context manager.
        if_exists : {'fail', 'replace', 'append'}, default 'fail'
            - fail: If table exists, do nothing.
            - replace: If table exists, drop it, recreate it, and insert data.
            - append: If table exists, insert data. If create_table is False
                      then if does not exist.
        index : boolean, default True
            Write DataFrame index as a column
        index_label : string or sequence, default None
            Column label for index column(s). If None is given (default) and
            `index` is True, then the index names are used.
            A sequence should be given if the DataFrame uses MultiIndex.
        schema : string, default None
            Name of SQL schema in database to write to (if database flavor
            supports this). If specified, this overwrites the default
            schema of the SQLDatabase object.
        chunksize : int, default None
            If not None, then rows will be written in batches of this size at a
            time.  If None, all rows will be written at once.
        copy : boolean, default False
            Whether to copy the frame when resetting the index, if index=True.
            There is a possibility that the index will not reset correctly and
            it could affect the current in frame.
             
        """
             
        table = SQLTable(table_name, self, frame=frame, table_setup=True, index=index,
                     if_exists=if_exists, index_label=index_label, schema=schema)
                     
        table.create()
        
        # check for potentially case sensitivity issues (GH7815)
        if table_name not in self.engine.table_names(schema=schema or self.meta.schema):
            warnings.warn("The provided table name '{0}' is not found exactly "
                          "as such in the database after writing the table, "
                          "possibly due to case sensitivity issues. Consider "
                          "using lower case table names.".format(name), UserWarning)
  
            
        table.insert(conn=conn, bulk=True, chunksize=chunksize, copy=copy)

    @property
    def tables(self):
        return self.meta.tables

    def has_table(self, name, schema=None):
        return self.engine.has_table(name, schema or self.meta.schema)

    def get_table(self, table_name, schema=None):
        schema = schema or self.meta.schema
        if schema:
            return self.meta.tables.get('.'.join([schema, table_name]))
        else:
            return self.meta.tables.get(table_name)

    def drop_table(self, table_name, schema=None):
        schema = schema or self.meta.schema
        if self.engine.has_table(table_name, schema):
            self.meta.reflect(only=[table_name], schema=schema)
            self.get_table(table_name, schema).drop()
            self.meta.clear()

    def _create_sql_schema(self, frame, table_name, keys=None):
        table = SQLTable(table_name, self, frame=frame, index=False, keys=keys)
        return str(table.sql_schema())







