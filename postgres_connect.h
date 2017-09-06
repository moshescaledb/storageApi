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

	void error(char *mess){
		fprintf(stderr, "### %s\n", mess);
		exit(1);
	}

};

#endif