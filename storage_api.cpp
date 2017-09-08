// storage_api.cpp : Defines the entry point for the console application.
//

#include <stdio.h>
#include "storage_mapper.h"
#include "postgres_connect.h"

/*******************************************************************************************************//**
*! \brief Call the storage API with JSON structures representing insert / update / delete / select
*******************************************************************************************************/
void demoStorageCalls(){

	int retValue;
	char sqlStmt[MAX_SQL_STMT_LENGTH];

	StorageMapper *pMapper = new StorageMapper();			// init the mapper

	PostgresConnect *pPstgresConnect = new PostgresConnect();	// init connection to PostgreSQL

	// connect 
	PGconn *conn = pPstgresConnect->init("localhost", "5432", "postgres", "postgres", "password");		// Initialize postgres, return dbms connection
	if (!conn){
		// connection failed - print error and exit
		printf("\nFailed to connect to Postgres - %s", pPstgresConnect->getErrorMessage());
		exit(-1);
	}


	// Create some tables
	if (pPstgresConnect->runCreateStatements(conn, true)){
		// got an error
	}

	// 1. Insert

	char jsonInsert[] = " { \"id\" : 12345, \"asset_code\" : \"ABCDE\" , \"read_key\" : 1009, \"user_ts\" : \"10152017\", \"read_key\" : \"072291\" } ";;

	//char jsonInsert[] = " { \"name\" : \"Moshe\", \"city\" : \"Redwood City\" , \"pi\" : \"3.1416\"  } ";

	pMapper->insert( "aa\\myTable" , jsonInsert, sqlStmt);


	char jasonQuery[] = "{ \"where\": { \"column\": \"c1\", \"condition\": \"=\" , \"value\" : \"mine\", \"and\" : { \"column\" : \"c2\", \"condition\" : \"<\", \"value\" : \"20\" } } }";

	pMapper->select( "aa\\myTable" , jasonQuery, sqlStmt);

	pPstgresConnect->disconnect(conn);

	delete pMapper;
	delete pPstgresConnect;
}



int main(int argc, char* argv[])
{

	demoStorageCalls();
	return 0;
}

