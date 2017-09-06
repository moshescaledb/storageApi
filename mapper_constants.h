


#define DEBUG_INSERT
#define DEBUG_SELECT

#define MAX_SQL_STMT_LENGTH 1000		// max size of a sql statement
#define MAX_DBMS_ERR_MSG_LENGTH 1000	// max size of an error message of the database

// Internal Mapper Errors (stating at 8000)

#define MISSING_TABLE_NAME				8000
#define TABLE_NAME_LARGER_THAN_ALLOWED	8001
#define PARSER_FAILURE_ON_JSON_DOCUMENT	8002
#define NON_SUPPORTED_JSON_FORMAT		8003
#define JSON_SIZE_ERROR					8004


// Errors from DBMS calls
#define FAILED_DBMS_CONNECTION			9000
#define CREATE_TABLE_FAILED				9001
#define INSERT_FAILED					9002


// internal numbers representing the sql part to be considered
#define WHERE_ERROR -1
#define WHERE_COLUMN_NAME 1		// column name
#define WHERE_COLUMN_VALUE 2
#define WHERE_CONDITION 3
#define WHERE_AND		4
