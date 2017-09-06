// storage_api.cpp : Defines the entry point for the console application.
//

#include <stdio.h>
#include "storage_mapper.h"
#include "postgres_connect.h"

/*******************************************************************************************************//**
*! \brief Call the storage API with JSON structures representing insert / update / delete / select
*******************************************************************************************************/
void demoStorageCalls(){

	StorageMapper *pMapper = new StorageMapper();			// init the mapper

	PostgresConnect *pPstgresConnect = new PostgresConnect();	// init connection to PostgreSQL


	pPstgresConnect->init("localhost", "5432", "postgres", "postgres", "password");

	// 1. Insert

	char jsonInsert[] = " { \"name\" : \"moshe\", \"city\" : \"Redwood City\" , \"pi\" : \"3.1416\"  } ";

	pMapper->insert( "aa\\myTable" , jsonInsert);


	char jasonQuery[] = "{ \"where\": { \"column\": \"c1\", \"condition\": \"=\" , \"value\" : \"mine\", \"and\" : { \"column\" : \"c2\", \"condition\" : \"<\", \"value\" : \"20\" } } }";

	pMapper->select( "aa\\myTable" , jasonQuery);

	delete pMapper;
	delete pPstgresConnect;
}



int main(int argc, char* argv[])
{

	demoStorageCalls();
	return 0;
}

