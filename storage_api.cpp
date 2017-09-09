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
		printf("\nFailed to connect to PostgreSQL - %s", pPstgresConnect->getErrorMessage());
		exit(-1);
	}


	// Create some tables
	if (pPstgresConnect->runCreateStatements(conn, true)){
		// got an error
		printf("\nFailed to create tables - %s", pPstgresConnect->getErrorMessage());
		exit(-1);
	}

	// 1. Insert

	char jsonInsert[] = " { \"id\" : 12345, \"asset_code\" : \"ABCDE\" , \"read_key\" : \"c7c41c31-fb4d-4720-8abd-80b65e55f24b\", \"user_ts\" : \"1994-11-29\" } ";;

	//char jsonInsert[] = " { \"name\" : \"Moshe\", \"city\" : \"Redwood City\" , \"pi\" : \"3.1416\"  } ";

	retValue = pMapper->insert( "aa\\readings" , jsonInsert, sqlStmt);	// get the sql from the json
	if (retValue){
		printf("\nFailed to parse JSON insert with error %i", retValue);
		exit(-1);
	}

	retValue = pPstgresConnect->insert(conn, sqlStmt);	// insert the sql to postgres
	if (retValue){
		printf("\nFailed to insert data to the dbms - %s", pPstgresConnect->getErrorMessage());
		exit(-1);
	}

	// test select
	memset(sqlStmt, ' ', MAX_SQL_STMT_LENGTH);	// for debug

	char jasonQuery2[] = "{ \"where\": { \"column\": \"id\", \"condition\": \"=\" , \"value\" : 12345, \"and\" : { \"column\" : \"asset_code\", \"condition\" : \"<=\", \"value\" : \"ABCDE\" } } }";

	retValue = pMapper->select( "aa\\readings" , jasonQuery2, sqlStmt);
	if (retValue){
		printf("\nFailed to parse JSON select with error - %i", retValue);
		exit(-1);
	}


	retValue = pPstgresConnect->select(conn, sqlStmt);	
	if (retValue){
		printf("\nFailed to select data - %s", pPstgresConnect->getErrorMessage());
		exit(-1);
	}

	pPstgresConnect->disconnect(conn);

	delete pMapper;
	delete pPstgresConnect;
}


int main(int argc, char* argv[])
{

	demoStorageCalls();
	return 0;
}

