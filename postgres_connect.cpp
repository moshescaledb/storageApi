
#include "postgres_connect.h"


PostgresConnect::PostgresConnect(void)
{
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
	
	//create a table
	res = PQexec(conn, "CREATE TABLE hello (message VARCHAR(32))");
	
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
*! \brief Select Data
* \param[in] conn - DBMS Connection
* \param[in] sqlStmt - the sql string
* \return 0 for success or an error code
*******************************************************************************************************/
int PostgresConnect::select(PGconn *conn, char *sqlStmt){

	int retValue;

	int nfields, ntuples, i, j;
	PGresult *res;

	//query the db
	res = PQexec(conn, sqlStmt);

	if (PQresultStatus(res) != PGRES_TUPLES_OK){
		
		saveError(res, conn);
		retValue = INSERT_FAILED;
	}else{

		nfields = PQnfields(res);
		ntuples = PQntuples(res);

		for(i = 0; i < ntuples; i++)
			for(j = 0; j < nfields; j++)
				printf("[%d,%d] %s\n", i, j, PQgetvalue(res, i, j));

	}

	PQclear(res);


  return retValue;
}

/*******************************************************************************************************//**
*! \brief Disconnect from postgres
*******************************************************************************************************/
void PostgresConnect::disconnect(PGconn *conn){

 //disconnect
  PQfinish(conn);

}

