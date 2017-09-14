// Copyright (c) 2017 OSIsoft, LLC

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

	char host[] = "localhost";
	char port[] = "5432";
	char database[] = "postgres";
	char user[] = "postgres";
	char password[] = "password";


	// connect 
	PGconn *conn = pPstgresConnect->init(host, port, database, user, password);		// Initialize postgres, return dbms connection
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

	// 1. Insert - 1

	char jsonInsert1[] = " { \"id\" : 12345, \"asset_code\" : \"ABCDE\" , \"read_key\" : \"c7c41c31-fb4d-4720-8abd-80b65e55f24b\", \"user_ts\" : \"1994-11-29\" } ";;

	retValue = pMapper->insert( "aa/readings" , jsonInsert1, sqlStmt);	// get the sql from the json
	if (retValue){
		printf("\nFailed to parse JSON insert with error %i", retValue);
		exit(-1);
	}

	retValue = pPstgresConnect->insert(conn, sqlStmt);	// insert the sql to postgres
	if (retValue){
		printf("\nFailed to insert data to the dbms - %s", pPstgresConnect->getErrorMessage());
		exit(-1);
	}

	// 2. Insert - 2

	char jsonInsert2[] = " { \"id\" : 98765, \"asset_code\" : \"QRSTUV\" , \"read_key\" : \"c7c41c31-fb4d-4720-8abd-80b65e55f24b\", \"user_ts\" : \"1994-11-24\" } ";;

	retValue = pMapper->insert( "aa/readings" , jsonInsert2, sqlStmt);	// get the sql from the json
	if (retValue){
		printf("\nFailed to parse JSON insert with error %i", retValue);
		exit(-1);
	}

	retValue = pPstgresConnect->insert(conn, sqlStmt);	// insert the sql to postgres
	if (retValue){
		printf("\nFailed to insert data to the dbms - %s", pPstgresConnect->getErrorMessage());
		exit(-1);
	}

	// 3. Test update
	memset(sqlStmt, ' ', MAX_SQL_STMT_LENGTH);	// for debug

	char jsonUpdate[] = "{ \"set\" : {\"column\": \"id\", \"condition\": \"=\" , \"value\" : 22222, \"column\" : \"asset_code\", \"condition\" : \"=\", \"value\" : \"EDCBA\" } , \"where\": { \"column\": \"id\", \"condition\": \"=\" , \"value\" : 12345, \"and\" : { \"column\" : \"asset_code\", \"condition\" : \"=\", \"value\" : \"ABCDE\" } } }";

	retValue = pMapper->update( "aa/readings" , jsonUpdate, sqlStmt);		
	if (retValue){
		printf("\nFailed to parse JSON select with error - %i", retValue);
		exit(-1);
	}

	retValue = pPstgresConnect->update(conn, sqlStmt);	// returns 1 row
	if (retValue){
		printf("\nFailed to update data - %s", pPstgresConnect->getErrorMessage());
		exit(-1);
	}

	// 4. test select
	memset(sqlStmt, ' ', MAX_SQL_STMT_LENGTH);	// for debug

	char jsonQuery1[] = "{ \"where\": { \"column\": \"id\", \"condition\": \">=\" , \"value\" : 12345, \"and\" : { \"column\" : \"asset_code\", \"condition\" : \">=\", \"value\" : \"ABCDE\" } } }";

	retValue = pMapper->select( "aa/readings" , jsonQuery1, sqlStmt);		
	if (retValue){
		printf("\nFailed to parse JSON select with error - %i", retValue);
		exit(-1);
	}

	retValue = pPstgresConnect->select(conn, sqlStmt);	// returns 1 row
	if (retValue){
		printf("\nFailed to select data - %s", pPstgresConnect->getErrorMessage());
		exit(-1);
	}


	// 5. test delete

	char jsondelete[] = "{ \"where\": { \"column\": \"id\", \"condition\": \"=\" , \"value\" : 12345, \"and\" : { \"column\" : \"asset_code\", \"condition\" : \"=\", \"value\" : \"ABCDE\" } } }";

	retValue = pMapper->deleteData( "aa/readings" , jsonQuery1, sqlStmt);		
	if (retValue){
		printf("\nFailed to parse JSON select with error - %i", retValue);
		exit(-1);
	}

	retValue = pPstgresConnect->deleteData(conn, sqlStmt);	// delete from the dbms
	if (retValue){
		printf("\nFailed to delete data - %s", pPstgresConnect->getErrorMessage());
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

