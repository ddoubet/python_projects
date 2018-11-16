
import os, sys, time
import collections

#-------------------------------------Configuration Begin--------------------------------------------#
#
oldDBName        = '<old db name>'
#
newDBName        = '<db name>'
#
exportDDLFile    = 'export_ddl.sql'
#
exportDataFile   = 'export_data_final.sql'
#
exportDir        = '/Users/<path>'
#
convertDDLFile   = ''
#
importDDLFile    = convertDDLFile.rstrip('.sql') + '_replace.sql'
#
importFile       = exportDataFile.rstrip('.sql') + '_replace.sql'
#
importDir        = exportDir
#
dtypeCvrt        = None
#
# Example of a dtypeCvrt (Data type convert mapping dictionary) 
# Replace dtypeCvrt from None to a dictionary similar to below to give more 
# specific data type converting.
#
# Note: 
# The 'all' key can be set to None and no table keys have to be specified.
# You must be specific on the 
#
# Format           = {'all' : {'postgres_dtype' : 'mysql_dtype'}}
#
#
# dtypeCvrt        = {'all' : {'BOOLEAN' : 'BIT'},
#                    'tableName1' : {'colName1' : {'TIMESTAMP'   : 'DATETIME'},
#                                    'colName2' : {'CHAR'        : 'VARCHAR'}
#                                   },
#                    'tableName2' : {'colName1' : {'TEXT'        : 'MEDIUMTEXT'},
#                                    'colName2' : {'CHAR'        : 'VARCHAR'}
#                                   },
#                    'tableName3' : {'colName1' : {'FLOAT4'      : 'DOUBLE'},
#                                    'colName2' : {'VARCHAR(20)' : 'VARCHAR(30)'}
#                                   }
#                    }

#-------------------------------------Configuration End----------------------------------------------#



def pg_mysql_dtype_map():
    """doc string"""
    
    #                       PostgreSQL                        MySQL
    pg_mysql_mapping = {    'array':                          'LONGTEXT',
                            'bigint':                         'BIGINT',                     
                            'bigserial':                      'BIGINT NOT NULL AUTO_INCREMENT UNIQUE',
                            'bit':                            'BIT',
                            'bit varying':                    'BIT',
                            'boolean':                        'BIT',
                            'box':                            'POLYGON',
                            'bytea':                          'LONGBLOB',
                            'character varying':              'VARCHAR',
                            'character':                      'CHAR',
                            'cidr':                           'VARCHAR(43)',
                            'circle':                         'POLYGON',
                            'date':                           'DATE',
                            'decimal':                        'DECIMAL',
                            'double precision':               'DOUBLE',
                            'inet':                           'VARCHAR(43)',
                            'integer':                        'INT',
                            'interval':                       'TIME',
                            'json':                           'LONGTEXT',
                            'line':                           'LINESTRING',
                            'lseg':                           'LINESTRING',
                            'macaddr':                        'VARCHAR(17)',
                            'money':                          'DECIMAL(19,2)',
                            'numeric':                        'FLOAT',
                            'path':                           'LINESTRING',
                            'point':                          'POINT',
                            'polygon':                        'POLYGON',
                            'real':                           'FLOAT',
                            'smallint':                       'SMALLINT',
                            'serial':                         'INT NOT NULL AUTO_INCREMENT UNIQUE',
                            'smallserieal':                   'SMALLINT NOT NULL AUTO_INCREMENT UNIQUE',
                            'text':                           'LONGTEXT',
                            'time':                           'TIME',
                            'timestamp ':                     'DATETIME',
                            'tsquery':                        'LONGTEXT',
                            'tsvector':                       'LONGTEXT',
                            'txid_snapshot':                  'VARCHAR',
                            'uuid':                           'VARCHAR(36)',
                            'xml':                            'LONGTEXT',
                            'int8':                           'BIGINT',
                            'serial8':                        'BIGINT NOT NULL AUTO_INCREMENT UNIQUE',
                            'varbit':                         'BIT',
                            'bool':                           'BIT',
                            'varchar':                        'VARCHAR',
                            'char':                           'CHAR',
                            'float8':                         'DOUBLE',
                            'int' :                           'INT',
                            'int4':                           'INT',
                            'float4':                         'FLOAT',
                            'int2':                           'SMALLINT',
                            'serial2':                        'SMALLINT NOT NULL AUTO_INCREMENT UNIQUE',
                            'serial4':                        'INT NOT NULL AUTO_INCREMENT UNIQUE',
                            'timetz':                         'TIME',
                            'timestamptz':                    'DATETIME'
                       }
        
    return pg_mysql_mapping


def _set_fk_checks(ck_fk, outFile):
    """doc string"""

    if ck_fk not in [True, False]:
        raise TypeError("Set ck_val with type bool") 

    if ck_fk:
        outFile.write("\nSET FOREIGN_KEY_CHECKS=%s;\n"%1)
    else:
        outFile.write("\nSET FOREIGN_KEY_CHECKS=%s;\n"%0)
        
    return outFile


def _strip_cast_stmts(line):
    """doc string"""
    
    # In Work:
    newLine    = line.replace(", cast(",", ")
    newLine    = newLine.replace(" as BLOB)","")
    newLine    = newLine.replace(" as CLOB)","")
    return newLine


def _verify_values(from_db, col_index, dtype, dtype_map, values):
    """doc string"""
    
    # IN WORK:
    if from_db == 'pg':
        if dtype in ['bool','boolean']:
            pass
            
    return


def x_form_insert(inputFile, outFile, ddlDict, oldDBName, newDBName, dtype_map,
                              strip_cast_stmts=False, fk_cks_off=True, multiVal=False, upCaseTN=False):
    """doc string"""
    
    # TODO: Add a database name old to new mapping dictionary
    
    count = 0
    castWarn = False
    newLineStmt = False
    INS = 'INSERT INTO '
    print "\nParsing export file to generate new insert statements..."
    print "Old Database Name: %s will be replaced with %s"%(oldDBName, newDBName) 
    
    for line in inputFile:
        count += 1
        
        if not multiVal and not line.startswith(INS + oldDBName.strip()):
            outFile.write(line)
            continue
            
        if count == 1:
            if fk_cks_off:
                outFile = _set_fk_checks(ck_fk=False, outFile=outFile)
                # Make sure there is a space between alter and insert statements
                outFile.write("\n\n")
                           
        #try:
        newLine    = line.replace(INS + oldDBName.strip(), INS + newDBName.strip())
        insertList = newLine.split('VALUES (')
            
        bStmtList  = insertList[0].split('(')
        tableName  = bStmtList[0].split('.')[1].strip().strip('"')

        # IN WORK:
        """
        for i, dtype in enumerate(ddlDict[oldDBName]['tables'][tableName]['dtypes']):
            if dtype_map == pg_mysql_dtype_map():
                values = _verify_values(from_db='pg', dtype, dtype_map, insertList[1])

            elif dtype_map == derby_mysql_dtype_map():
                values = _verify_values(from_db='derby', dtype, dtype_map, insertList[1])
        """

        try:
            colsLine   = bStmtList[1].strip().rstrip(')')
        except:
            print "\nLine: " + newLine
            print "\nInsert List: " + str(insertList)
            print "\nbStmtList: " + str(bStmtList)
            print "Count: " + str(count)
            
        columns    = ['`'+col.strip().strip('"')+'`' for col in colsLine.split(',')]
            
        #except Exception as e:
            #print e
            #print "\nPlease check file for format inconsistencies"
            #raise e
            
        bStmtList[0] = bStmtList[0].replace('"','')
        
        if upCaseTN:
            bStmtList[0] = bStmtList[0].replace(tableName, tableName.upper())
        
        insertLine = bStmtList[0] + '(' + ','.join(columns) + ')' + ' VALUES(' + insertList[1]
        
        if strip_cast_stmts:
            castStmtLoc = insertLine.find(', cast(')
            if castStmtLoc != -1:
                if not castWarn:
                    castWarn = True
                    print "Nested SQL Syntax 'CAST' found in insert statement"
                    
                if count == 1:
                    print "Stripping CAST statements from file..."
                insertLine = _strip_cast_stmts(insertLine)
            
        outFile.write(insertLine)
        
        if count <= 2:
            print "Sample: %s"%insertLine[:150]


    if fk_cks_off:
        outFile = _set_fk_checks(ck_fk=True, outFile=outFile)
    
    print "All insert statements successfully generated"
    
    return outFile


def pg_ddl_parser(ddlFile):
    """doc string"""
    
    # TODO: Add support for non sequence/serial default values
    db        = ""
    table     = "" 
    CT        = 'CREATE TABLE'
    CTSEQ     = 'CREATE SEQUENCE'
    COLSEQ    = 'NEXTVAL'
    DEFAULT   = 'DEFAULT'
    SERIAL    = 'SERIAL'
    END_DELIM = ');'
    KEYWORDS  = ['PRIMARY KEY','ALT TABLE', 'INSERT INTO', 'FOREIGN KEY', 
                 'REFERENCES', 'SELECT', 'GO', "SET", 'USE']
    fndTable  = False
    ddlDict   = collections.OrderedDict()
    
    for line in ddlFile:
        #
        if line[:14].upper().find(CT) != -1:
            fndTable = True
            dbtLine = line.strip().lstrip(CT).strip().rstrip('(').strip()
            
            db, dbLoc, table = dbtLine.split('.')
            #print "Found new create table statement for %s"%(dbtLine)
            
            if db in ddlDict.keys():
                ddlDict[db]['tables'][table] = {}
                ddlDict[db]['tables'][table]['columns']   = []
                ddlDict[db]['tables'][table]['dtypes']    = []
                ddlDict[db]['tables'][table]['auto_incr'] = []
                ddlDict[db]['tables'][table]['nullable']  = []
                ddlDict[db]['tables'][table]['pk']        = []
                ddlDict[db]['tables'][table]['fk']        = []
                ddlDict[db]['tables'][table]['default']   = []
            else:
                ddlDict[db] = {}
                ddlDict[db]['tables'] = collections.OrderedDict()
                ddlDict[db]['tables'][table] = {}
                ddlDict[db]['tables'][table]['columns']   = []
                ddlDict[db]['tables'][table]['dtypes']    = []
                ddlDict[db]['tables'][table]['auto_incr'] = []
                ddlDict[db]['tables'][table]['nullable']  = []
                ddlDict[db]['tables'][table]['pk']        = []
                ddlDict[db]['tables'][table]['fk']        = []
                ddlDict[db]['tables'][table]['default']   = []
            
        elif fndTable:
            # Assume a column line until proven wrong
            colLine = True
            line = line.strip()
            
            if line == END_DELIM:
                # End of table definition
                fndTable = False
                colLine = False
            
            elif not line.isspace():
                # Last line ends with );
                if line.endswith(END_DELIM):
                    # Table definition end
                    line = line.rstrip(END_DELIM)
                    fndTable = False
                #
                for kwd in KEYWORDS:
                    # Line starts with keyword is not a column definition line
                    if line[:len(kwd)].upper() == kwd:
                        colLine = False
                        # Primary key keyword found at beginning of line, so 
                        # primary key definition is at the end of the table definition 
                        if kwd != 'PRIMARY KEY':
                            break
                        else:
                            pkCols = line.split('(')[1].rstrip(')').split(',')
                            # Extract columns defined as primary keys
                            for pkCol in pkCols:
                                pkCol = pkCol.strip('"')
                                #print "Primary Key: %s\n"%pkCol
                                # Loop through columns already found and defined and 
                                # if a match, update the primary key indicator.
                                for i, col in enumerate(ddlDict[db]['tables'][table]['columns']):
                                    if col == pkCol:
                                        #print "Primary Key = True"
                                        ddlDict[db]['tables'][table]['pk'][i] = True
                    
            if colLine:
                #print "Column Line: %s"%line
                splitLine = line.rstrip(',').split(" ")

                if len(splitLine) >= 2:
                    # Extract the column name and strip any column identifiers 
                    col = splitLine[0].strip('"').strip("'").strip("`")
                    # Extract the column data type
                    dtype = splitLine[1].strip()
                    ddlDict[db]['tables'][table]['columns'].append(col)
                    ddlDict[db]['tables'][table]['dtypes'].append(dtype)
                    #print "Column %s found of type %s"%(col, dtype)
                    # Join all characters after column and data type 
                    line = "".join(splitLine[2:]).upper()
                    # Look for alias of a sequence
                    if SERIAL in dtype.upper():
                        ddlDict[db]['tables'][table]['auto_incr'].append(True)
                    # Look for indication of a sequence 
                    elif line.find(COLSEQ) != -1:
                        ddlDict[db]['tables'][table]['auto_incr'].append(True)
                    else:
                        ddlDict[db]['tables'][table]['auto_incr'].append(False)
                    # Whether column is nullable
                    if line.find('NOTNULL') != -1:
                        ddlDict[db]['tables'][table]['nullable'].append(False)
                    else:
                        ddlDict[db]['tables'][table]['nullable'].append(True)
                    # Check for primary key flag for this column
                    if line.find('PRIMARYKEY') != -1:
                        #print "\nPrimary Key Line: %s"%line
                        ddlDict[db]['tables'][table]['pk'].append(True)
                    else:                
                        ddlDict[db]['tables'][table]['pk'].append(False)
                    # Check for foreign key flag for this column
                    if line.find('FOREIGNKEY') != -1:
                        #print "\nForeign Key Line: %s"%line
                        ddlDict[db]['tables'][table]['fk'].append(True)
                    else:                
                        ddlDict[db]['tables'][table]['fk'].append(False)
                        
                    # TODO: Finish this logic for default value detection
                    if line.find(DEFAULT) != -1:
                        if not ddlDict[db]['tables'][table]['auto_incr'][-1]:
                            if ddlDict[db]['tables'][table]['nullable'][-1]:
                                defVal = line.lstrip(DEFAULT).rstrip('NULL').strip()
                            else:
                                defVal = line.lstrip(DEFAULT).rstrip('NOTNULL').strip()

                            ddlDict[db]['tables'][table]['default'].append(defVal)
                    
    return ddlDict


def _format_dtype_cvrt(dtypeCvrt):
    """doc string"""
    
    print "Reformatting input conversion"
    _dtypeCvrt = {}
    if dtypeCvrt['all'] is not None:
        _dtypeCvrt['all'] = {}
        for key in dtypeCvrt['all'].keys():
            _dtypeCvrt['all'][key.lower()] = dtypeCvrt['all'][key].upper()
    else:
        _dtypeCvrt = dtypeCvrt
    
    if len(dtypeCvrt.keys()) > 1:
        for tableKey in dtypeCvrt.keys():
            if tableKey.lower() != 'all':
                _dtypeCvrt[tableKey] = {}
                for key in dtypeCvrt[tableKey].keys():
                    _dtypeCvrt[tableKey][key.lower()] = dtypeCvrt[tableKey][key].upper()
                    
    return _dtypeCvrt


def _build_alt_table_stmts(db, table, col, dtype, tableDict, col_pos, altStmt=None):
    """doc string"""
    
    #print "Generating alter table statements"
    #
    global newDBName
    #
    # Add MySQL compatible column identifier backticks
    col       = '`' + col + '`'
    is_pk     = tableDict['pk'][col_pos]
    is_null   = tableDict['nullable'][col_pos]
    auto_incr = tableDict['auto_incr'][col_pos]
    
    if altStmt is None:
        altStmt = 'CHANGE %s %s %s'%(col, col, dtype)

        loc = altStmt.find('AUTO_INCREMENT')
        
        if loc != -1:
            if not is_pk:
                altStmt = altStmt[:loc] + 'PRIMARY KEY ' + altStmt[loc:]
        elif auto_incr:
            if not is_pk:
                altStmt = altStmt + ' PRIMARY KEY'
            if is_null:
                altStmt = altStmt + ' NOT NULL'
                
            altStmt = altStmt + ' AUTO_INCREMENT'

    return '\nALTER TABLE %s.%s %s;'%(newDBName, table, altStmt)
     
    
def _get_type_alt_stmts(db, table, tableDict, dtype_map, use_default=False):
    """doc string"""
    
    #print "Checking type conversion..."
    global dtypeCvrt
    altStmtList = []
    
    if dtypeCvrt is None:
        if not use_default:
            for i, from_dtype in enumerate(tableDict['dtypes']):
                if not tableDict['auto_incr'][i]:
                    col = tableDict['columns'][i]
                    if from_dtype in dtype_map.keys():
                        to_dtype = dtype_map[from_dtype]
                        altStmtList.append(_build_alt_table_stmts(db, table, col, to_dtype, 
                                                                  tableDict, col_pos=i))
        else:
            return []
    else:
        #print "Retreiving PostgreSQL to MySQL type mapping"
        scdtype = _format_dtype_cvrt(dtypeCvrt)

        for i, from_dtype in enumerate(tableDict['dtypes']):
            col = tableDict['columns'][i]
            if scdtype['all'] is not None:
                print "\nGlobal data type conversion configuration detected."
                for dtype in scdtype['all'].keys():
                    if from_dtype == dtype:
                        to_dtype = scdtype[tableKey][from_dtype]
                        if not to_dtype in dtype_map.values():
                            print ("\nWarning: The following MySQL type: %s is not "
                                  "supported for %s"%(to_dtype, from_dtype))
                            to_dtype = dtype_map[from_dtype]

                    elif from_dtype.split('(')[0] == dtype:
                        to_dtype = scdtype[tableKey][dtype]
                        if not to_dtype in dtype_map.values():
                            print ("\nWarning: The following MySQL type: %s is not "
                                  "supported for %s"%(to_dtype, from_dtype))
                            to_dtype = dtype_map[from_dtype]
                    else:
                        print "\nData type %s not found, will use default mapping"%dtype
                        from_dtype = from_dtype.split('(')
                        to_dtype = dtype_map[from_dtype[0]]

                    if (type(from_dtype) == list) and (len(from_dtype) > 1):
                        to_dtype = to_dtype + '(' + from_dtype[1]
                    
                    print ("Creating alter table statement for data type conversion "
                          "from %s to %s"%(dtype, to_dtype))
                    altStmtList.append(_build_alt_table_stmts(db, table, col, to_dtype, 
                                                              tableDict, col_pos=i))
                            
            elif len(scdtype.keys()) > 1:
                for tableKey in scdtype.keys():
                    if table == tableKey:
                        print "\nTable specific data type conversion configuration detected."
                        for colKey in scdtype[tableKey].keys():
                            if col == colKey:
                                dtype = scdtype[tableKey][colKey]
                                if from_dtype == dtype:
                                    to_dtype = scdtype[tableKey][from_dtype]
                                    if not to_dtype in dtype_map.values():
                                        print ("\nWarning: The following MySQL type: %s is not "
                                              "supported for %s"%(to_dtype, from_dtype))
                                        to_dtype = dtype_map[from_dtype]

                                elif from_dtype.split('(')[0] == dtype:
                                    to_dtype = scdtype[tableKey][dtype]
                                    if not to_dtype in dtype_map.values():
                                        print ("\nWarning: The following MySQL type: %s is not "
                                              "supported for %s"%(to_dtype, from_dtype))
                                        to_dtype = dtype_map[from_dtype]
                                else:
                                    print "\nData type %s not found, will use default mapping"%dtype
                                    from_dtype = from_dtype.split('(')
                                    to_dtype = dtype_map[from_dtype[0]]

                                if (type(from_dtype) == list) and (len(from_dtype) > 1):
                                    to_dtype = to_dtype + '(' + from_dtype[1]

                                print ("Creating alter table statement for data type conversion "
                                      "from %s to %s"%(dtype, to_dtype))
                                altStmtList.append(_build_alt_table_stmts(db, table, col, to_dtype, 
                                                                          tableDict, col_pos=i))
            else:
                from_dtype = from_dtype.split('(')
                to_dtype = dtype_map[from_dtype[0]]
                if len(from_dtype) > 1:
                    to_dtype = to_dtype + '(' + from_dtype[1]
                print ("Creating alter table statement for data type conversion "
                      "from %s to %s"%(dtype, to_dtype))
                altStmtList.append(_build_alt_table_stmts(db, table, col, to_dtype, 
                                                          tableDict, col_pos=i))
                               
    return altStmtList


def _get_auto_incr_alt_stmts(db, table, tableDict, dtype_map):
    """doc string"""
    
    altStmtList = []
    #print "Retreiving PostgreSQL to MySQL type mapping"
    for i, item in enumerate(tableDict['auto_incr']):
        if item:
            col = tableDict['columns'][i]
            from_dtype = tableDict['dtypes'][i].split('(')
            to_dtype = dtype_map[from_dtype[0].lower()]
            
            if len(from_dtype) > 1:
                to_dtype = to_dtype + '(' + from_dtype[1]
            
            altStmtList.append(_build_alt_table_stmts(db, table, col, to_dtype, 
                                                      tableDict, col_pos=i))
                               
    return altStmtList
        
    
def alt_table_import(altFile, ddlDict, dtype_map):
    """doc string"""
    
    print "\n\nBuilding alter table statements..."
    # Should only be one db key per file for this use case
    for db in ddlDict.keys():
        for tableName in ddlDict[db]['tables'].keys():
            tableDict   = ddlDict[db]['tables'][tableName]
            altStmtList = _get_auto_incr_alt_stmts(db, tableName, 
                                                   tableDict, dtype_map)
            altStmtList = altStmtList + _get_type_alt_stmts(db, tableName, 
                                                            tableDict, dtype_map)
            altStmtList.append('\n')
            for altStmt in altStmtList:
                altFile.write(altStmt)
        else:
            altFile.write('\n\n\n')
    
    return altFile
    

#
def pg_db_migration():
    
    global importDir
    global importFile
    global exportDir
    global exportDataFile
    
    with open(os.path.join(exportDir, exportDDLFile), 'r') as ddlFile:
        ddlDict = pg_ddl_parser(ddlFile)

    #
    with open(os.path.join(importDir, importFile), 'w') as altFile:
        altFile = _set_fk_checks(ck_fk=False, outFile=altFile)
        altFile = alt_table_import(altFile=altFile, ddlDict=ddlDict, 
                                   dtype_map=pg_mysql_dtype_map())

        with open(os.path.join(exportDir, exportDataFile), 'r') as inputFile:
            altFile = x_form_insert(inputFile=inputFile, outFile=altFile, ddlDict=ddlDict, oldDBName=oldDBName,
                                    newDBName=newDBName, dtype_map=pg_mysql_dtype_map(), strip_cast_stmts=False, 
                                    fk_cks_off=False)
        
        altFile = _set_fk_checks(ck_fk=True, outFile=altFile)
    
    return


def dirby_db_migration():
    
    global importDir
    global importFile
    global exportDir
    global exportDataFile
    #
    importFiles = ['WF_JOBS.sql']

    
    for _file in importFiles:
        inFile = _file.replace('.sql', '_replace.sql')
        with open(os.path.join(importDir, inFile), 'w') as altFile:
            altFile = _set_fk_checks(ck_fk=False, outFile=altFile)
            #altFile = alt_table_import(altFile=altFile, ddlDict=ddlDict, 
                                       #dtype_map=pg_mysql_dtype_map())
            
           
            with open(os.path.join(exportDir, _file), 'r') as inputFile:
                altFile = x_form_insert(inputFile=inputFile, outFile=altFile, ddlDict=None, oldDBName=oldDBName,
                                        newDBName=newDBName, dtype_map=None, strip_cast_stmts=True, 
                                        fk_cks_off=False)

            altFile = _set_fk_checks(ck_fk=True, outFile=altFile)
        
    return

        
def pg_mysql_db_migration():
    
    global importDir
    global importFile
    global exportDir
    global exportDataFile
    #
    importFiles = ['WF_JOBS.sql']

    
    for _file in importFiles:
        inFile = _file.replace('.sql', '_replace.sql')
        with open(os.path.join(importDir, inFile), 'w') as altFile:
            altFile = _set_fk_checks(ck_fk=False, outFile=altFile)
            #altFile = alt_table_import(altFile=altFile, ddlDict=ddlDict, 
                                       #dtype_map=pg_mysql_dtype_map())

            with open(os.path.join(exportDir, exportDataFile), 'r') as inputFile:
                altFile = x_form_insert(inputFile=inputFile, outFile=altFile, ddlDict=None, oldDBName=oldDBName,
                                        newDBName=newDBName, dtype_map=None, strip_cast_stmts=False, 
                                        fk_cks_off=False, upCaseTN=True)

            altFile = _set_fk_checks(ck_fk=True, outFile=altFile)
    
    return

    
beginTime = time.time()     
dirby_db_migration1()
endTime = time.time()

#print "\nOutput file written to: " + outputPath
#print "\nNumber of lines transformed: " + str(count)
print "\nThis operation took "+str(float(endTime - beginTime))+" seconds to complete"
