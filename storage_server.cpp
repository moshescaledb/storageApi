// Copyright (c) 2017 OSIsoft, LLC

#include "storage_server.h"


StorageServer::StorageServer(void){
}
StorageServer::~StorageServer(void){
}

void StorageServer::runServer(){

	while(1){
		init();
		server();
		clean();
	}
}

/*******************************************************************************************************//**
*! \brief HTTP Web Server -- reply to http://127.0.0.1:8080 
*******************************************************************************************************/
void StorageServer::server()
{
	int sock;
	int connected;
	int bytes_recieved;
	
	char send_data [1024];
	char recv_data[1024];

	struct sockaddr_in server_addr;
	struct sockaddr_in client_addr;
	int sin_size;
	int reuse = 1;


	if ((sock = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("Socket");
		exit(-1);
	}

	if (setsockopt(sock,SOL_SOCKET,SO_REUSEADDR, (const char*)&reuse, sizeof(reuse)) == -1) {
		perror("Setsockopt");
		exit(-1);
	}

	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(8080);
	server_addr.sin_addr.s_addr = INADDR_ANY;

	//bzero(&(server_addr.sin_zero),8); --This is for POSIX based systems
	memset(&(server_addr.sin_zero),0,8);
	if (bind(sock, (struct sockaddr *)&server_addr, sizeof(struct sockaddr))== -1)
	{
		perror("Unable to bind");
		exit(-1);
	}

	if (listen(sock, 5) == -1){
		perror("Listen");
		exit(1);
	}

	printf("\n\nMyHTTPServer waiting on port 8080");
	fflush(stdout);

	sin_size = sizeof(struct sockaddr_in);

	connected = accept(sock, (struct sockaddr *)&client_addr,&sin_size);

	// printf("\n I got a connection from (%s , %d)",
	//   inet_ntoa(client_addr.sin_addr),ntohs(client_addr.sin_port));

	char kk[9999];
	recv(connected,kk,sizeof(kk),0);

	printf("\n Received:%s",kk);

	char xx[9999];
	strcpy(xx,"HTTP/1.1 200 OK\nContent-length: 46\nContent-Type: text/html\n\n<html><body><H1>Hello World</H1></body></html>");

	int c= send(connected, (const char *)&xx, sizeof(xx), 0);
	printf("\nSTATUS:%d",c);

	printf("\nSent : %s\n",xx);

//	close(sock);
	WSACleanup();
}
/*******************************************************************************************************//**
*! \brief function specific for Windows-to be called before invoking call to socket()
* \return -1 for Winsock error
*******************************************************************************************************/
int StorageServer::init(){
#ifdef OS_WINDOWS
	WSADATA wsaData;
	int retValue;

	// Initialize Winsock
	retValue = WSAStartup(MAKEWORD(2,2), &wsaData);
	if (retValue != 0) 
	{
		printf("WSAStartup failed: %d\n", retValue);
		return -1;
	}
#endif
	return 0;
}


/*******************************************************************************************************//**
*! \brief function specific for windows-to be called after all socket communication is complete
*******************************************************************************************************/
void StorageServer::clean(){
#ifdef OS_WINDOWS
	WSACleanup();
#endif
}
