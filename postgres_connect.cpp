
#include "postgres_connect.h"


PostgresConnect::PostgresConnect(void)
{
	dbmsErrorCode_[5] = NULL;		// for debug
}


PostgresConnect::~PostgresConnect(void)
{
}


/*******************************************************************************************************//**
*! \brief Connect to DBMS by parameters provided
* \param[in] host - Name of host to connect to.
* \param[in] port - Port number to connect to at the server host.
* \param[in] dbname - database name
* \param[in] userName - PostgreSQL user name to connect as.
* \param[in] password
* \return A ptr to the database connection or NULL in case of a failure
*******************************************************************************************************/
PGconn *PostgresConnect::init(char *host, char *port, char *database, char *userName, char *password){

	int retValue;

	 //connect to database
 	PGconn *conn = PQsetdbLogin(host, port, NULL, NULL, database, userName, password);

	if (PQstatus(conn) == CONNECTION_OK) {
		//if connected
		retValue = 0;
	}else{  
		saveError(conn);
		retValue = FAILED_DBMS_CONNECTION;    
	}

    return conn;

}


/*******************************************************************************************************//**
*! \brief Create Statements - this method includes predefined create statements to send to the database.
* \param[in] conn - DBMS Connection
* \param[in] ignoreTableExists - is set with 'true' to ignore table exists message
* \return 0 for success or an error code
*******************************************************************************************************/
int PostgresConnect::runCreateStatements(PGconn *conn, bool ignoreDuplicateTable){

	int retValue;
	PGresult *res;


	res = PQexec(conn, "DROP TABLE IF EXISTS readings");
	if (PQresultStatus(res) != PGRES_COMMAND_OK){
		saveError(res, conn);
		return DROP_TABLE_FAILED;
	}

	
	//create a table
	
	res = PQexec(conn, "CREATE TABLE readings (id bigint, asset_code character varying(50), read_key   uuid, reading jsonb, user_ts timestamp(6), ts timestamp(6))");

	if (PQresultStatus(res) != PGRES_COMMAND_OK){
		
		saveError(res, conn);

		if (ignoreDuplicateTable && isDuplicateTable()){
			retValue = 0;	// Ignore duplicate tables
		}else{
			retValue = CREATE_TABLE_FAILED;
		}

	}else{
		retValue = 0;
	}

	PQclear(res);

	return retValue;

}

/*******************************************************************************************************//**
*! \brief Insert Data
* \param[in] conn - DBMS Connection
* \param[in] sqlStmt - the sql string
* \return 0 for success or an error code
*******************************************************************************************************/
int PostgresConnect::insert(PGconn *conn, char *sqlStmt){

	int retValue;
	PGresult *res;

	//insert data
	res = PQexec(conn, sqlStmt);

	if (PQresultStatus(res) != PGRES_COMMAND_OK){
		saveError(res, conn);
		retValue = INSERT_FAILED;
	}else{
		retValue = 0;
	}
    
	PQclear(res);

	return retValue;
}

/*******************************************************************************************************//**
*! \brief Update Data
* \param[in] conn - DBMS Connection
* \param[in] sqlStmt - the sql string
* \return 0 for success or an error code
*******************************************************************************************************/
int PostgresConnect::update(PGconn *conn, char *sqlStmt){

	int retValue;
	PGresult *res;

	//insert data
	res = PQexec(conn, sqlStmt);

	if (PQresultStatus(res) != PGRES_COMMAND_OK){
		saveError(res, conn);
		retValue = INSERT_FAILED;
	}else{
		retValue = 0;
	}
    
	PQclear(res);

	return retValue;
}


/*******************************************************************************************************//**
*! \brief Select Data
* \param[in] conn - DBMS Connection
* \param[in] sqlStmt - the sql string
* \return 0 for success or an error code
*******************************************************************************************************/
int PostgresConnect::select(PGconn *conn, char *sqlStmt){


	int retValue;
	PGresult *res;


	//query the db
	res = PQexec(conn, sqlStmt);

	if (PQresultStatus(res) != PGRES_TUPLES_OK){
		
		saveError(res, conn);
		retValue = INSERT_FAILED;
	}else{

		//id bigint, asset_code character varying(50), read_key   uuid, reading jsonb, user_ts timestamp(6), ts timestamp(6))

		createJson( res );
		retValue = 0;
	
	}

	PQclear(res);


  return retValue;
}

/*******************************************************************************************************//**
*! \brief Delete Data
* \param[in] conn - DBMS Connection
* \param[in] sqlStmt - the sql string
* \return 0 for success or an error code
*******************************************************************************************************/
int PostgresConnect::deleteData(PGconn *conn, char *sqlStmt){

	int retValue;
	PGresult *res;

	//insert data
	res = PQexec(conn, sqlStmt);

	if (PQresultStatus(res) != PGRES_COMMAND_OK){
		saveError(res, conn);
		retValue = INSERT_FAILED;
	}else{
		retValue = 0;
	}
    
	PQclear(res);

	return retValue;
}

/*******************************************************************************************************//**
*! \brief Create JSON doc from a result set
*******************************************************************************************************/
void PostgresConnect::createJson( PGresult *res ){

	int fieldType;
	int nfields, ntuples, i, j;
	int retValue;
	char *fieldName;
	char *fieldValue;
	
	Document document;
	document.SetObject();	// create a JSON document

	Document::AllocatorType& allocator = document.GetAllocator();	//allocator for reallocating memory

	ntuples = PQntuples(res);
	nfields = PQnfields(res);

	document.AddMember("count", ntuples, allocator);

	Value value_obj(kObjectType);

	for(i = 0; i < ntuples; i++){

		Value valueObj(kObjectType);		// this is an object organized for each tuple.

		for(j = 0; j < nfields; j++){
			fieldType = PQftype(res, j);
			fieldName = PQfname(res, j);
			fieldValue = PQgetvalue(res, i, j);

			switch (fieldType){	// Need to include pg_type.h - The list is available here - https://doxygen.postgresql.org/include_2catalog_2pg__type_8h_source.html 

				case 20:		// dataType bigint (replace 20 with INT8OID)
					{
						string strName(fieldName);
						int intValue = atoi(fieldValue);
						Value attrName(strName.c_str(), allocator);
//						document.AddMember(attrName, intValue, allocator);
						valueObj.AddMember(attrName, intValue, allocator);
					}
					break;

				case 1043:		// dataType character varying (replace 1043 with VARCHAROID)
				case 2950:		// dataType uuid (replace 2950 with UUIDOID)
				case 3802:		// dataType jsonb (replace 3802 with JSONBOID)
				case 1114:		// dataType timestamp (replace 3802 with TIMESTAMPOID)
					{
						string strName(fieldName);
						string strValue(fieldValue);
						Value attrName(strName.c_str(), allocator);
						Value attrValue(strValue.c_str(), allocator);
//						document.AddMember(attrName, attrValue, allocator);
						valueObj.AddMember(attrName, attrValue, allocator);
					}

					break;
				case 710:		// dataType float (replace 701 with FLOAT8OID)
					{
						string strName(fieldName);
						double floatValue = atof(fieldValue);
						Value attrName(strName.c_str(), allocator);
//						document.AddMember(attrName, floatValue, allocator);
						valueObj.AddMember(attrName, floatValue, allocator);
					}
					break;
				default:
					retValue = NON_SUPPORTED_DATA_TYPE;
					break;
			}
		}
		document.AddMember("row", valueObj, allocator);
	}

	// Convert JSON document to string
	GenericStringBuffer< UTF8<> > buffer;
	Writer< GenericStringBuffer< UTF8<> > > writer(buffer);
	document.Accept(writer);

	const char* str = buffer.GetString();

#ifdef DEBUG_QUERY
	printf("\nJSON: %s\n", str);
#endif
}
/*******************************************************************************************************//**
*! \brief Disconnect from postgres
*******************************************************************************************************/
void PostgresConnect::disconnect(PGconn *conn){

 //disconnect
  PQfinish(conn);

}

