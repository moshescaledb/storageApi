#ifndef _POSTGRE_CONNECT_H
#define  _POSTGRE_CONNECT_H

#include <iostream>
#include <libpq-fe.h>
#include <stdio.h>
#include <string>
#include <stdlib.h>
#include <errno.h>
#include <time.h>

#include "mapper_constants.h"

using namespace std;

class PostgresConnect
{
public:
	PostgresConnect(void);
	~PostgresConnect(void);

	int init(char *host, char *port, char *database, char *userName, char *password);

private:


/*******************************************************************************************************//**
*! \brief Copy the error returned from the database to a buffer
*******************************************************************************************************/
	void saveError(PGconn *conn){

		char *errorTxrt = PQerrorMessage(conn);
		int errLength = (int)strlen(errorTxrt);

		if (errLength < MAX_DBMS_ERR_MSG_LENGTH){
			memcpy(dbmsError_, errorTxrt, errLength + 1);
		}else{
			// copy as much as possible
			memcpy(dbmsError_, errorTxrt, MAX_DBMS_ERR_MSG_LENGTH - 1);
			dbmsError_[MAX_DBMS_ERR_MSG_LENGTH - 1] = NULL;
		}

	}


	char dbmsError_[MAX_DBMS_ERR_MSG_LENGTH];

};

#endif