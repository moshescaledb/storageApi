#ifndef _STORAGE_SERVER_H
#define  _STORAGE_SERVER_H

#include "mapper_constants.h"
#include<sys/types.h>
#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<unistd.h>
#include<errno.h>

#ifdef OS_LINUX
#include<sock.h>
#endif

#ifdef OS_WINDOWS
#include<winsock2.h>
#pragma comment(lib, "Ws2_32.lib")
#endif


class StorageServer
{
public:
	StorageServer(void);
	~StorageServer(void);

	void runServer();
private:
	void server();
	int init();
	void clean();
};
#endif
