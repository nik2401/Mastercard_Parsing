try:
    import sys
    import pyodbc
    import urllib
    import inspect
    import datetime
    import sqlalchemy as sa
    from Logger import Logger
    import IPM_Select_And_Updates
    # package should be install, if not [Command: pip install modulename]
except ModuleNotFoundError as err:
    print(str({err.name.strip()})," module is missing, Command to Install - {"" pip install ", str(err.name.strip()), "}")
    sys.exit(1)
    
######################################################################################################################################################

logger = Logger()

######################################################################################################################################################

def GetObjectName():
    return inspect.stack()[1][3]

######################################################################################################################################################

def udf_GetConnectionString(SqlOdbcDriver, DB_Server_NAME, DBName_CI):
    Connection_String = 'Driver={' + str(SqlOdbcDriver) + '};Server=' + DB_Server_NAME + ';Database=' + DBName_CI + ';Trusted_Connection=yes;MultiSubnetFailover=Yes;'
    logger.debug(f"Connection_String = {Connection_String}")
    return Connection_String

######################################################################################################################################################

def udf_InsSingleRecIntoDB(Connection_String, InsQuery):
    try:
        logger.debug(f"Query Executing = {InsQuery}")
        connection_uri = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(Connection_String)}"
        engine = sa.create_engine(connection_uri, fast_executemany=True)
        with engine.begin() as conn:
            try:
                conn.execute(sa.text(InsQuery).execution_options(autocommit = True))
            except Exception as e:
                logger.debug(f"Error Raised udf_InsSingleRecIntoDB : {e}")
                logger.log_exception(*sys.exc_info())

    except Exception as e:
        logger.debug(f"Error Raised udf_InsSingleRecIntoDB : {e}",True)
        logger.log_exception(*sys.exc_info())

######################################################################################################################################################

def udf_SPCall(Connection_String, SPExec, Inp_Jobid = 0):
    try:
        Inp_Jobid = int(Inp_Jobid)
        logger.debug(f"Executing SP is = {SPExec}",True)
        objCon = pyodbc.connect(Connection_String)
        objCursor = objCon.cursor()
        objCursor.execute(SPExec)
        qry_result = objCursor.fetchall()
        objCursor.commit()
        objCursor.close()
        
        if qry_result is None or not qry_result :
            logger.error("SP_Result : SP Exec Doesnot Return Expected Result")
            print("SP_Result : SP Exec Doesnot Return Expected Result")
            sys.exit()
            
        return qry_result
    except Exception as e:
        ErrorReason = f"Error Raised For {SPExec} : {e}"
        ErrorReason = ErrorReason.replace("'","")
        if Inp_Jobid > 0:
            IPM_Select_And_Updates.IPM_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        logger.debug(ErrorReason, True)
        logger.log_exception(*sys.exc_info())
        sys.exit()

######################################################################################################################################################
    
def execute_select_query(Connection_String, query):
    try:
        connection = pyodbc.connect(Connection_String)
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        connection.close()
        processed_result = process_result(result)
        return processed_result
    except pyodbc.Error as e:
        logger.debug(f"Error While Executing Query: {query}", True)
        logger.debug(f"Exception execute_select_query: {e}", True)
        logger.log_exception(*sys.exc_info())
        sys.exit()

######################################################################################################################################################

def process_result(result):
    try:
        processed_result = []
        for row in result:
            processed_row = []
            for col in row:
                if isinstance(col, int):
                    processed_row.append(col)
                elif isinstance(col, str):
                    processed_row.append(col.strip())
                elif isinstance(col, datetime.datetime):
                    processed_row.append(col)
                else:
                    processed_row.append(str(col))  # Convert other types to string
            processed_result.append(tuple(processed_row))
        return processed_result
    except pyodbc.Error as e:
        logger.debug(f"Error While Processing Result: {result}", True)
        logger.debug(f"Exception process_result: {e}", True)
        logger.log_exception(*sys.exc_info())
        sys.exit()

######################################################################################################################################################