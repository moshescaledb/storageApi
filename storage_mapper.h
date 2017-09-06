
#ifndef _STORAGE_MAPPER_H
#define  _STORAGE_MAPPER_H

#include "include\rapidjson\document.h"
#include "include\rapidjson\writer.h"
#include "include\rapidjson\stringbuffer.h"
#include "include\rapidjson\prettywriter.h" // for stringify JSON
#include <cstdio>
#include <iostream>

#include "mapper_constants.h"

using namespace rapidjson;
using namespace std;

#define RETURN_IF_NO_SQL_BUFF_SPACE(offset,bytesToAdd) if ((offset + bytesToAdd) > MAX_SQL_STMT_LENGTH) return JSON_SIZE_ERROR

class StorageMapper{

public:

	StorageMapper();
	~StorageMapper();


	int select(char *url, char *jsonDoc);
	int insert(char *tableName, char *jsonDoc);


private:

	int getTableNameFromUrl(char *url, int maxLength, char *dest, int *nameSize);
	int moveStringToDest(char *dest, char *src, int stringLength, int maxLength, bool addComma);
	bool moveStringToSqlBuff(char *sqlStmt, int offsetSql, char *src, int stringLength);
	int setSqlPrefix(char *sqlPrefix, int sqlPrefixSize, char *url, char *dest );
	int getNestedObject( Value *nestedObject, char *sqlStmt, int *offsetSql  );
	int getWhereInfoType(char *name, int nameLength);
	int addColumnName(char *sqlStmt, int *destOffset, Value::ConstMemberIterator *nestedItr);
	int addCondition(char *sqlStmt, int *destOffset, Value::ConstMemberIterator *nestedItr);
	int addValue(char *sqlStmt, int *destOffset, Value::ConstMemberIterator *nestedItr);
/*******************************************************************************************************//**
*! \brief Test that the destination SQL buffer has sufficient space
* \return false if no sufficient space
*******************************************************************************************************/
	inline bool isSqlBuffWithSpace(int offset, int bytesToAdd){
		return (offset + bytesToAdd) <= MAX_SQL_STMT_LENGTH ? true : false;
	}
/*******************************************************************************************************//**
*! \brief Limit the characters that can be used in the SQL syntax part
* \return true for valid chars
*******************************************************************************************************/
	inline bool isValidSqlChar(char ch){
		
		if (ch >= 'a' && ch <='z'){
			return true;
		}
		if (ch >= 'A' && ch <='Z'){
			return true;
		}
		if (ch >= '0' && ch <='9'){
			return true;
		}
		if (ch == '-' || ch == '_'){
			return true;
		}

		return false;
	}


/*******************************************************************************************************//**
*! \brief set the end of the sql string
* \return false if no space in the sql buffer
*******************************************************************************************************/
	inline bool copyToSqlStmt(char *sqlStmt, int offsetSql, char *subString, int subStringLength){

		if (offsetSql + 3 > MAX_SQL_STMT_LENGTH){
			return false;
		}
		
		memcpy(sqlStmt + offsetSql, subString, subStringLength);
		return true;
	}


};

#endif
