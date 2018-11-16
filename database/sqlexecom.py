# Author: Dustin Doubet
# Description: 
# Python SQL extention lib for using SQLAlchemy and wraps some common operations

#Import Python standard libraries
import os
import sys
import time
import logging
import traceback
#
from sqlalchemy import exc
#
import pandas_sql
from shared_exc import ResourceError    
    
class SQLCommonClass:
    
    
    def __init__(self, engine, meta, wf_logger=None, log_to_file=True, log_level=None, 
                                                              hdlr_path=None, **kwargs):
    
    
        if wf_logger == None:
            self.loggerName = 'SQLCommonClass'
            if log_to_file:
                self.logger = logging.getLogger(self.loggerName)
                if hdlr_path is None:
                    hdlr = logging.FileHandler(self.loggerName + '.log')
                else:
                    hdlr = logging.FileHandler(hdlr_path)
                formatter = logging.Formatter(
                        "%(asctime)s    [%(name)s]   %(levelname)s    %(message)s")
                hdlr.setFormatter(formatter)
                self.logger.addHandler(hdlr)
                if log_level is None:
                    self.logger.setLevel(logging.INFO)
                else:
                    self.logger.setLevel(log_level.upper())
            else:
                self.logger = logging.getLogger(self.loggerName)
                hdlr = logging.StreamHandler()
                formatter = logging.Formatter(
                        '%(asctime)s    %(name)s    %(levelname)s    %(message)s')
                hdlr.setFormatter(formatter)
                self.logger.addHandler(hdlr)
                if log_level is None:
                    self.logger.setLevel(logging.INFO)
                else:
                    self.logger.setLevel(log_level.upper())
        else:
            self.loggerName = wf_logger +'.SQLCommonClass'
            self.logger = logging.getLogger(self.loggerName)
         
         
        #///////////////////////////////////////////////////////////////
           
        self.Meta   = meta    
        self.Engine = engine
        self.SqlDB  = pandas_sql.SQLDatabase(engine=self.Engine, meta=self.Meta)    
        
        self.RETRIES    = 3
        self.SLEEP_TIME = 3
        
        #///////////////////////////////////////////////////////////////
    
    
    def db_conn(self, instance, transaction=True, queue_routine=None):
        """doc string"""
        
        self.logger.debug("In method, db_conn")
        
        conn = None
        instance = instance.lower()
            
        for i in range(self.RETRIES):
            try:
                self.logger.info("Connecting to %s database"%instance)
                
                conn = self.Engine.connect()
                
                if transaction:
                    self.logger.info("Starting %s database transaction"%instance)
                    trans = conn.begin()
                
            except exc.SQLAlchemyError as e:
                excInfo = sys.exc_info()  
                traceback.print_exc(file=sys.stderr)
                msg = ("Received '%s' while connecting to %s database"
                       %(e,instance))
                self.logger.error(msg, exc_info=True)
                
                if type(e) in (exc.InvalidRequestError, exc.DisconnectionError):
                    if i+1 != self.RETRIES:
                        time.sleep(3)
                        continue
                    elif queue_routine is not None:
                        queue_routine(instance=instance, logMsg=msg, excInfo=excInfo)
                        self.logger.debug("In method, db_conn")
                        break
                    else:
                        raise ResourceError("%s database not available."%instance,errors=e)

                elif type(e) is exc.ProgrammingError:
                    errorMsg = e.message.lower()
                    if errorMsg.find('shutdown is in progress') != -1 and errorMsg.find('6005') != -1:
                        self.logger.warning("Received SQL Server Shutdown ")
                        if queue_routine is not None:
                            queue_routine(instance=instance,logMsg=msg, excInfo=excInfo)
                            self.logger.debug("In method, db_conn")
                            break
                        else:
                            raise ResourceError("%s database not available."%instance,errors=e)
                    else:
                        raise e
                        
                elif i+1 != self.RETRIES:
                    self.logger.info("Resubmitting request to connect to %s database"%instance)
                    time.sleep(5)
                else:
                    self.logger.warning("Number of resubmission requests exceeded")
                    if conn is not None and not conn.closed:
                        #This only applies if a connection is open
                        #but there is an issue starting a transaction. 
                        self.logger.info("Closing log database connection...")
                        conn.close()
                   
                    raise e
            else:
                self.logger.debug("Successful connection to %s database"%instance)
                break
                
        if transaction:
            return trans, conn
        else:
            return conn
            
            
    def _execute_with_exc(self, conn, instance, table_name, exec_stmt, frame=None, insert=True, 
                                   update=False, open_trans=False, queue_routine=None, **kwargs):
        """doc string"""
        
        self.logger.debug("In method, _execute_with_exc")
        
        result = None
        closeConn = False
        
        for i in range(self.RETRIES):
            try:
                
                if frame is not None and insert:
                    self.logger.debug("Executing bulk insert for DataFrame object")
                    if conn is None and not open_trans:
                        #The insert_bulk method will use a context manager 
                        #to open and close a connection
                        self.SqlDB.insert_bulk(frame=frame, table_name=table_name, conn=None)
                    else:
                        #Use passed in connection
                        self.SqlDB.insert_bulk(frame=frame, table_name=table_name, conn=conn)
                else:
                    if (conn is None and not open_trans) and (insert or update):
                        #Only allow for a connection to be created if executing a insert or update
                        #otherwise a connection object must be passed in. This is so query objects
                        #can be executed and returned and the connection objects are not left checked
                        #out or not returned to the connection pool.
                        self.logger.debug("Connecting to %s database"%instance)
                        conn = self.Engine.connect()
                        closeConn = True
                        
                    self.logger.debug("Executing statement %s..."%exec_stmt)
                    result = conn.execute(exec_stmt, **kwargs)
                
            except exc.SQLAlchemyError as e:
                self.logger.debug("In method, _execute_with_exc")
                excInfo = sys.exc_info()
                traceback.print_exc(file=sys.stderr)
                if insert:
                    msg = ("Received '%s' while inserting record(s) into %s table %s."
                           %(e,instance,table_name))
                elif update:
                    msg = ("Received '%s' while updating record(s) in %s table %s."
                           %(e,instance,table_name))
                else:
                    msg = ("Received '%s' while executing %s in %s on table %s."
                           %(e,exec_stmt,instance,table_name))
                                      
                self.logger.error(msg, exc_info=True)
                
                if type(e) in (exc.InvalidRequestError, exc.DisconnectionError):
                    if i+1 != self.RETRIES:
                        if queue_routine is not None:
                            queue_routine(instance=instance, logMsg=msg, excInfo=excInfo)
                            self.logger.debug("In method, _execute_with_exc")
                            break
                        elif open_trans:
                            #There is a tranaction associated with
                            #this connection so dont retry
                            raise e
                        else:
                            conn = None
                            time.sleep(self.SLEEP_TIME)
                            continue
                            
                    elif queue_routine is not None:
                        queue_routine(instance=instance, logMsg=msg, excInfo=excInfo)
                        self.logger.debug("In method, _execute_with_exc")
                        break
                    else:
                        raise ResourceError('%s database not available'%instance,errors=e)

                elif type(e) is exc.ProgrammingError:
                    errorMsg = e.message.lower()
                    if errorMsg.find('shutdown is in progress') != -1 and errorMsg.find('6005') != -1:
                        self.logger.warning("Received SQL Server Shutdown ")
                        if queue_routine is not None:
                            queue_routine(logMsg=msg, excInfo=excInfo)
                            self.logger.debug("In method, _execute_with_exc")
                            break
                        else:
                            raise ResourceError('%s database not available'%instance,errors=e)
                    else:
                        raise e

                elif i+1 != self.RETRIES:
                    if conn.invalidated:
                        if not open_trans:
                            conn = None
                        else:
                            raise e
                    self.logger.info("Resubmitting request to insert record(s) into '%s' table"
                                      %table_name)
                    time.sleep(self.SLEEP_TIME)
                else:
                    self.logger.warning("Number of resubmission requests exceeded")
                    raise e
            else:
                self.logger.debug("In method, _execute_with_exc")
                if insert:
                    self.logger.info("Record(s) successfully inserted to table %s"%table_name)
                elif update:
                    self.logger.info("Record(s) successfully updated in table %s"%table_name)
                else:
                    self.logger.info("Statement successully executed on table %s"%table_name)
                #Break the RETRIES for loop
                break
            finally:
                if closeConn:
                    #Close the connection because, it was created and 
                    #there is no transaction associated with it.
                    if conn is not None and not conn.closed:
                        self.logger.info("Closing %s database connection..."%instance)
                        conn.close()
                    else:
                        self.logger.info("%s database connection closed"%instance)
   
        return result
        
            
    def insert(self, conn, instance, table_name, insert_stmt, open_trans=False, queue_routine=None, **kwargs):
        """doc string"""
        
        self._execute_with_exc(conn=conn, instance=instance, table_name=table_name, 
                                       exec_stmt=insert_stmt, open_trans=open_trans, 
                                              queue_routine=queue_routine, **kwargs)
        return
        
        
    def update(self, conn, instance, table_name, update_stmt, open_trans=False, queue_routine=None, **kwargs):
        """doc string"""
        
        self._execute_with_exc(conn=conn, instance=instance, table_name=table_name, 
                               exec_stmt=update_stmt, insert=False, update=True, 
                               open_trans=open_trans, queue_routine=queue_routine, **kwargs)
        return
                        
            
    def insert_bulk(self, frame, conn, instance, table_name, open_trans=False, queue_routine=None, **kwargs):
        """doc string"""
        
        
        self._execute_with_exc(conn=conn, instance=instance, table_name=table_name, 
                                 frame=frame, exec_stmt=None, open_trans=open_trans, 
                                               queue_routine=queue_routine, **kwargs)              
        return
        
   
    def query(self, conn, instance, table_name, query_stmt, open_trans=False, queue_routine=None, **kwargs):
        """doc string"""
    
    
        result = self._execute_with_exc(conn=conn, instance=instance, table_name=table_name, 
                                exec_stmt=query_stmt, insert=False, update=False, 
                                open_trans=open_trans, queue_routine=queue_routine, **kwargs)
        return result
    
    