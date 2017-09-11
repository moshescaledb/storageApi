#ifndef _POSTGRE_CONNECT_H
#define  _POSTGRE_CONNECT_H

#include <iostream>
#include <libpq-fe.h>
#include <stdio.h>
#include <string>
#include <stdlib.h>
#include <errno.h>
#include <time.h>
#include <string>

#include "mapper_constants.h"

#include "include\rapidjson\document.h"
#include "include\rapidjson\writer.h"
#include "include\rapidjson\stringbuffer.h"
#include "include\rapidjson\prettywriter.h" // for stringify JSON


using namespace rapidjson;
using namespace std;

class PostgresConnect
{
public:
	PostgresConnect(void);
	~PostgresConnect(void);

	PGconn *init(char *host, char *port, char *database, char *userName, char *password);
	int runCreateStatements(PGconn *conn, bool ignoreDuplicateTable);
	int insert(PGconn *conn, char *sqlStmt);
	int select(PGconn *conn, char *sqlStmt);
	int deleteData(PGconn *conn, char *sqlStmt);
	int update(PGconn *conn, char *sqlStmt);

	void disconnect(PGconn *conn);
	char *getErrorMessage() { return dbmsErrorTxt_; }

private:
	void createJson( PGresult *res );

/*******************************************************************************************************//**
*! \brief Save the error Code and Error Text returned from the database to a buffer
* All messages emitted by the PostgreSQL server are assigned five-character error codes that follow the SQL standard's conventions for "SQLSTATE" codes. 
*******************************************************************************************************/
	void saveError(PGresult *res, PGconn *conn){

		char* errCode = PQresultErrorField(res, PG_DIAG_SQLSTATE);
		
		memcpy(dbmsErrorCode_, errCode, 5);	// save code

		saveError(conn);	// save text
	}


/*******************************************************************************************************//**
*! \brief Copy the error returned from the database to a buffer
*******************************************************************************************************/
	void saveError(PGconn *conn){

		char *errorTxrt = PQerrorMessage(conn);
		
		int errLength = (int)strlen(errorTxrt);

		if (errLength < MAX_DBMS_ERR_MSG_LENGTH){
			memcpy(dbmsErrorTxt_, errorTxrt, errLength + 1);
		}else{
			// copy as much as possible
			memcpy(dbmsErrorTxt_, errorTxrt, MAX_DBMS_ERR_MSG_LENGTH - 1);
			dbmsErrorTxt_[MAX_DBMS_ERR_MSG_LENGTH - 1] = NULL;
		}

	}

/*******************************************************************************************************//**
*! \brief Test the postgres error code for duplicate table
*******************************************************************************************************/
	bool isDuplicateTable(){ return memcmp(dbmsErrorCode_, "42P07", 5) == 0 ? true : false; }

	char dbmsErrorTxt_[MAX_DBMS_ERR_MSG_LENGTH];
	char dbmsErrorCode_[6];

};

#endif