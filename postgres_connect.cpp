
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
*******************************************************************************************************/

int PostgresConnect::init(char *host, char *port, char *database, char *userName, char *password){

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


 int nfields, ntuples, i, j;
  PGresult *res;

  //create a table
  res = PQexec(conn, "CREATE TABLE hello (message VARCHAR(32))");
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
    saveError(conn);
  PQclear(res);

  //insert data
  res = PQexec(conn, "INSERT INTO hello VALUES ('Hello World!')");
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
    saveError(conn);
  PQclear(res);

  //query the db
  res = PQexec(conn, "SELECT * FROM hello");
  if (PQresultStatus(res) != PGRES_TUPLES_OK)
    saveError(conn);
  nfields = PQnfields(res);
  ntuples = PQntuples(res);

  for(i = 0; i < ntuples; i++)
    for(j = 0; j < nfields; j++)
      printf("[%d,%d] %s\n", i, j, PQgetvalue(res, i, j));
  PQclear(res);

  //disconnect
  PQfinish(conn);

    return retValue;
 

}

	