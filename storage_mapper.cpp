#include "storage_mapper.h"


StorageMapper::StorageMapper(){
}


StorageMapper::~StorageMapper(){
}


/*******************************************************************************************************//**
*! \brief Execute a query
* \param[in] url - represents the table name in the database (table name is the last section of the url)
* \param[in] jsonDoc - the data in JSON format
* \param[out] sqlStmt - the sql statement to execute
*******************************************************************************************************/
int StorageMapper::select(char *url, char *jsonDoc, char *sqlStmt){
	
	char *name;
	int retValue;
	int nameSize;
	Document document;  // Default template parameter
	int offsetSql = 14;		// the offset in the sql stmt updated
	int stringLength;
	Value nestedObject;


	memcpy(sqlStmt, "select * from ", offsetSql);

	// add table name
	retValue = getTableNameFromUrl(url, MAX_SQL_STMT_LENGTH - offsetSql, sqlStmt + offsetSql, &nameSize);
	if (retValue){
		return retValue;
	}

	offsetSql += nameSize;

	*(sqlStmt + offsetSql) = ' ';
	++offsetSql;


	// parse the JSON
    if (document.Parse(jsonDoc).HasParseError()){
        return PARSER_FAILURE_ON_JSON_DOCUMENT;
	}

	retValue = mapWhereConditionToSql(&document, sqlStmt, &offsetSql);
	if (retValue){
		return retValue;
	}

	if (!copyToSqlStmt(sqlStmt, offsetSql, ";\0", 2)){		
		return JSON_SIZE_ERROR;
	}

#ifdef DEBUG_INSERT
	printf("\n%s", sqlStmt);
#endif

	return 0;
}
/*******************************************************************************************************//**
*! \brief Execute a delete
* \param[in] url - represents the table name in the database (table name is the last section of the url)
* \param[in] jsonDoc - the data in JSON format
* \param[out] sqlStmt - the sql statement to execute
*******************************************************************************************************/
int StorageMapper::deleteData(char *url, char *jsonDoc, char *sqlStmt){
	

	int retValue;
	int nameSize;
	Document document;  // Default template parameter
	int offsetSql = 12;		// the offset in the sql stmt updated

	Value nestedObject;


	memcpy(sqlStmt, "delete from ", offsetSql);

	// add table name
	retValue = getTableNameFromUrl(url, MAX_SQL_STMT_LENGTH - offsetSql, sqlStmt + offsetSql, &nameSize);
	if (retValue){
		return retValue;
	}

	offsetSql += nameSize;

	*(sqlStmt + offsetSql) = ' ';
	++offsetSql;


	// parse the JSON
    if (document.Parse(jsonDoc).HasParseError()){
        return PARSER_FAILURE_ON_JSON_DOCUMENT;
	}


	retValue = mapWhereConditionToSql(&document, sqlStmt, &offsetSql);
	if (retValue){
		return retValue;
	}


	if (!copyToSqlStmt(sqlStmt, offsetSql, ";\0", 2)){		
		return JSON_SIZE_ERROR;
	}

#ifdef DEBUG_INSERT
	printf("\n%s", sqlStmt);
#endif

	return 0;
}


/*******************************************************************************************************//**
*! \brief Map a JSON where condition to SQL
* \param[in] document - a pointer to the JSON document.
* \param[out] sqlStmt - a pointer to the sql statement
* \param[in/out] offsetSql - a pointer to the current offset in the sql statement
*******************************************************************************************************/
int StorageMapper::mapWhereConditionToSql(Document *document, char *sqlStmt, int *offsetSql){

	char *name;
	int stringLength;
	int retValue = NON_SUPPORTED_JSON_FORMAT;

	for (Value::ConstMemberIterator itr = (*document).MemberBegin(); itr != (*document).MemberEnd(); ++itr){
		if (itr->name.IsString()){
			name = (char *)itr->name.GetString();		// needs to be one of the following: 1) where 2) ...
		}else{
			return PARSER_FAILURE_ON_JSON_DOCUMENT;		// missing expected sql text
		}

		stringLength = itr->name.GetStringLength();

		RETURN_IF_NO_SQL_BUFF_SPACE(*offsetSql, (stringLength + 1));

		memcpy(sqlStmt + *offsetSql, name , stringLength);
		*offsetSql += stringLength;

		*(sqlStmt + *offsetSql) = ' ';
		++*offsetSql;

	
		if (stringLength == 5 && memcmp(name, "where", stringLength) == 0){
			if (!(*document)[ name ].IsObject()){
				return PARSER_FAILURE_ON_JSON_DOCUMENT;		// document[ name ] needs to show an Object representing where condition
			}

			retValue = getNestedObject( &(*document)[ name ], sqlStmt, offsetSql);
			
			if (retValue){
				return retValue;
			}
		}

	}

	return retValue;
}

/*******************************************************************************************************//**
*! \brief Parse a nested object of the json
* \param[in] nestedObject - represents the nested object (i.e. where condition object)
* \param[in] sqlStmt - the destination buffer
* \param[in/out] destOffset - the offset for the sql text derived from the object - this value is shared with the caller
*******************************************************************************************************/
int StorageMapper::getNestedObject( Value *nestedObject, char *sqlStmt, int *offsetSql){

	int infoType;
	int retValue;
	Value newObject;

	Document document;

	for (Value::ConstMemberIterator nestedItr = (*nestedObject).MemberBegin(); nestedItr != (*nestedObject).MemberEnd(); ++nestedItr) { 

		if (nestedItr->name.IsString()){
			infoType = getWhereInfoType((char *)nestedItr->name.GetString(), nestedItr->name.GetStringLength());	// return a number representing the type of info on the where condition

			switch (infoType){
				case WHERE_COLUMN_NAME:
					retValue = addColumnName(sqlStmt, offsetSql, &nestedItr);
					if (retValue){
						return retValue;
					}
					break;
				case WHERE_COLUMN_VALUE:
					retValue = addValue(sqlStmt, offsetSql, &nestedItr);
					if (retValue){
						return retValue;
					}
					break;
				case WHERE_CONDITION:
					retValue = addCondition(sqlStmt, offsetSql, &nestedItr);
					if (retValue){
						return retValue;
					}
					break;
				case WHERE_AND:		// parse another and condition

					RETURN_IF_NO_SQL_BUFF_SPACE(*offsetSql, 4);
					memcpy(sqlStmt + *offsetSql, " and ", 5);
					*offsetSql += 5;

					if (nestedItr->value.IsObject()){
						// must be an object with the AND condition
						
						retValue = getNestedObject(( Value *)&(nestedItr->value), sqlStmt, offsetSql);
						if (retValue){
							return retValue;
						}
					}else{
						return NON_SUPPORTED_JSON_FORMAT;
					}
					break;
				default:

					return NON_SUPPORTED_JSON_FORMAT;
			}

		}else{
			return NON_SUPPORTED_JSON_FORMAT;
		}
	}


	return 0;
}

/*******************************************************************************************************//**
*! \brief return a number representing the part of the where condition being considered or - 1 for an error
*******************************************************************************************************/
int StorageMapper::getWhereInfoType(char *name, int nameLength){

	switch(nameLength){

		case 3:
			if (memcmp("and", name, 3) == 0){
				return WHERE_AND;
			}
			return WHERE_ERROR;
		case 5:
			if (memcmp("value", name, 5) == 0){
				return WHERE_COLUMN_VALUE;
			}
			return WHERE_ERROR;
		case 6:
			if (memcmp("column", name, 6) == 0){
				return WHERE_COLUMN_NAME;
			}
			return WHERE_ERROR;
		case 9:
			if (memcmp("condition", name, 9) == 0){
				return WHERE_CONDITION;
			}
			return WHERE_ERROR;

		default:
			return WHERE_ERROR;
	}
}

/*******************************************************************************************************//**
*! \brief Inserts the JSON data to the database
* \param[in] url - represents the table name in the database (table name is the last section of the url)
* \param[in] jsonDoc - the data in JSON format
* \param[out] sqlStmt - the sql statement to execute
*******************************************************************************************************/
int StorageMapper::insert(char *url, char *jsonDoc, char *sqlStmt){

	int retValue;
	int nameSize;
	Document document;  // Default template parameter
	int offsetSql = 12;		// the offset in the sql stmt updated
	int stringLength;

	memcpy(sqlStmt, "insert into ", offsetSql);

	// add table name
	retValue = getTableNameFromUrl(url, MAX_SQL_STMT_LENGTH - offsetSql, sqlStmt + offsetSql, &nameSize);
	if (retValue){
		return retValue;
	}

	offsetSql += nameSize;

	// parse the JSON
    if (document.Parse(jsonDoc).HasParseError()){
        return PARSER_FAILURE_ON_JSON_DOCUMENT;
	}

	// copy the field names

	sqlStmt[offsetSql++] = ' ';
	sqlStmt[offsetSql++] = '(';
	
	for (Value::ConstMemberIterator itr = document.MemberBegin(); itr != document.MemberEnd(); ++itr){
		if (itr->name.IsString()){
			stringLength = itr->name.GetStringLength();
			retValue = moveStringToDest( sqlStmt + offsetSql, (char *)itr->name.GetString(), stringLength, MAX_SQL_STMT_LENGTH - offsetSql, false, true, &offsetSql);
			if (retValue){
				return retValue;
			}
		}else{
			return NON_SUPPORTED_JSON_FORMAT;
		}
		
	}
	
	RETURN_IF_NO_SQL_BUFF_SPACE(offsetSql,9);

	memcpy(sqlStmt + offsetSql - 1, ") values (", 10);
	offsetSql += 9;


	// copy the field values
	for (Value::ConstMemberIterator itr = document.MemberBegin(); itr != document.MemberEnd(); ++itr){
		if (itr->value.IsString()){
			stringLength = itr->value.GetStringLength();
			retValue = moveStringToDest( sqlStmt + offsetSql, (char *)itr->value.GetString(), stringLength, MAX_SQL_STMT_LENGTH - offsetSql, true, true, &offsetSql);
			if (retValue){
				return retValue;
			}
		}else{
			if (itr->value.IsInt()){
				retValue = moveIntToDest( sqlStmt + offsetSql, itr->value.GetInt(), MAX_SQL_STMT_LENGTH - offsetSql, true, &offsetSql);
			}else{
				return NON_SUPPORTED_JSON_FORMAT;
			}
		}
		
	}

	if (!copyToSqlStmt(sqlStmt, offsetSql - 1, ");\0", 3)){		// the -1 to overwrite the comma
		return JSON_SIZE_ERROR;
	}

#ifdef DEBUG_INSERT
	printf("\n%s", sqlStmt);
#endif

	return 0;
}

/*******************************************************************************************************//**
*! \brief Add a column name to the sql string
* \param[in] sqlStmt - the destination buffer
* \param[in/out] destOffset - the offset for the sql text derived from the object - this value is shared with the caller
* \param[in] nestedItr - the iterator showing the attribute value pair
* \return 0 or error value
*******************************************************************************************************/
int StorageMapper::addColumnName(char *sqlStmt, int *destOffset, Value::ConstMemberIterator *nestedItr){

	char *value;
	int stringLength;
	int offsetSql = *destOffset;

	if ((*nestedItr)->value.IsString()){
		value = (char *)(*nestedItr)->value.GetString();
		stringLength = (*nestedItr)->value.GetStringLength();
		if (!moveStringToSqlBuff(sqlStmt, offsetSql, value, stringLength)){
			return JSON_SIZE_ERROR;
		}
		offsetSql += stringLength;
	}else{
		return PARSER_FAILURE_ON_JSON_DOCUMENT;	// column name must be a string
	}

	*destOffset = offsetSql;
	return 0;
}
/*******************************************************************************************************//**
*! \brief Add a column condition to the sql string
* \param[in] sqlStmt - the destination buffer
* \param[in/out] destOffset - the offset for the sql text derived from the object - this value is shared with the caller
* \param[in] nestedItr - the iterator showing the attribute value pair
* \return 0 or error value
*******************************************************************************************************/
int StorageMapper::addCondition(char *sqlStmt, int *destOffset, Value::ConstMemberIterator *nestedItr){

	char *value;
	int stringLength;
	int offsetSql = *destOffset;

	if ((*nestedItr)->value.IsString()){
		value = (char *)(*nestedItr)->value.GetString();
		stringLength = (*nestedItr)->value.GetStringLength();
		if (!moveStringToSqlBuff(sqlStmt, offsetSql, value, stringLength)){
			return JSON_SIZE_ERROR;
		}
		offsetSql += stringLength;
	}else{
		return PARSER_FAILURE_ON_JSON_DOCUMENT;	// column name must be a string
	}

	*destOffset = offsetSql;
	return 0;
}
/*******************************************************************************************************//**
*! \brief Add a value to the sql string
* \param[in] sqlStmt - the destination buffer
* \param[in/out] destOffset - the offset for the sql text derived from the object - this value is shared with the caller
* \param[in] nestedItr - the iterator showing the attribute value pair
* \return 0 or error value
*******************************************************************************************************/
int StorageMapper::addValue(char *sqlStmt, int *destOffset, Value::ConstMemberIterator *nestedItr){

	char *charValue;
	int intValue;
	int stringLength;
	int offsetSql = *destOffset;
	int retValue;

	if ((*nestedItr)->value.IsString()){
		charValue = (char *)(*nestedItr)->value.GetString();
		stringLength = (*nestedItr)->value.GetStringLength();
		retValue = moveStringToDest(sqlStmt + offsetSql, charValue, stringLength, MAX_SQL_STMT_LENGTH - offsetSql, true, false, &offsetSql);
		if (retValue){
			return retValue;
		}
	}else{
		if ((*nestedItr)->value.IsInt()){
			intValue = (*nestedItr)->value.GetInt();
			retValue = moveIntToDest(sqlStmt + offsetSql, intValue,  MAX_SQL_STMT_LENGTH - offsetSql, false, &offsetSql);
			if (retValue){
				return retValue;
			}
		}else{
			return PARSER_FAILURE_ON_JSON_DOCUMENT;	// value needs to be a string or an int
		}
	}

	RETURN_IF_NO_SQL_BUFF_SPACE(offsetSql, 1);

	sqlStmt[offsetSql] = ' ';		// space after value
	*destOffset = offsetSql + 1;

	return 0;
}
/*******************************************************************************************************//**
*! \brief Copy a string to the SQL Buffer
* \return false if no space
*******************************************************************************************************/
bool StorageMapper::moveStringToSqlBuff(char *sqlStmt, int offsetSql, char *src, int stringLength){

	if (offsetSql + stringLength > MAX_SQL_STMT_LENGTH){
		return false;
	}

	memcpy(sqlStmt + offsetSql, src, stringLength);

	return true;
}

/*******************************************************************************************************//**
*! \brief Copy a string to a destination buffer, consider the space available in the dest buffer.
* \param[in] dest - ptr to destination string
* \param[in] src - ptr to source string
* \param[in] stringLength - length of string to copy without quotation and comma
* \param[in] addQuotation - true to rap quotation
* \param[in] addComma - true to add comma
* \param[out] offsetSql - increase this value for the caller by the length including quotation and comma.
* \return 0 or an error value
*******************************************************************************************************/
int StorageMapper::moveStringToDest(char *dest, char *src, int stringLength, int maxLength, bool addQuotation, bool addComma, int *offsetSql){

	int offset;

	if ((stringLength + (addQuotation ? 2 : 0) + (addComma ? 1 : 0)) > maxLength){	// determine the size including Quotation and comma
		return JSON_SIZE_ERROR;
	}

	if (addQuotation){
		dest[0] = '\'';
		offset = 1;
	}else{
		offset = 0;
	}

	memcpy(dest + offset, src, stringLength);

	if (addQuotation){
		dest[stringLength + offset] = '\'';
		++offset;
	}

	if (addComma){
		*(dest + stringLength + offset) = ',';
		++offset;
	}

	*offsetSql += (stringLength + offset);

	return 0;
}


/*******************************************************************************************************//**
*! \brief Copy a string to a destination buffer, consider the space available in the dest buffer.
* \param[in] dest - ptr to destination string
* \param[in] value - value to place on dest
* \param[in] stringLength - length of string to copy without quotation and comma
* \param[in] addQuotation - true to rap quotation
* \param[in] addComma - true to add comma
* \param[out] offsetSql - increase this value for the caller by the length including quotation and comma.
* \return 0 or an error value
*******************************************************************************************************/
int StorageMapper::moveIntToDest(char *dest, int value, int maxLength, bool addComma, int *offsetSql){

	int offset;
	char numberBuffer[20];
	int stringLength;

	itoa(value, numberBuffer, 10);

	stringLength = (int)strlen(numberBuffer);

	if (stringLength > maxLength){	// test the size against the max size.
		return JSON_SIZE_ERROR;
	}

	memcpy(dest, numberBuffer, stringLength);

	if (addComma){
		*(dest + stringLength) = ',';
		offset = 1;
	}else{
		offset = 0;
	}

	*offsetSql += (stringLength + offset);

	return 0;
}

/*******************************************************************************************************//**
*! \brief Get the table name from the suffix of the URL and place the name at the location specified by dest.
* \param[in] url - represents the table name in the database (table name is the last section of the url)
* \param[in] maxLength - the max size allowed for the table name
* \param[out] dest - location to place the table name
* \param[out] nameSize - the size of the name of the table
* \return 0 or an error value
*******************************************************************************************************/
int StorageMapper::getTableNameFromUrl(char *url, int maxLength, char *dest, int *nameSize){

	int tableNameSize = 0;
	int length;
	int urlLength;
	char ch;

	if (!url){
		return MISSING_TABLE_NAME;	// URL was not provided
	}

	urlLength = (int)strlen(url);
	length = urlLength;

	if (!length){
		return MISSING_TABLE_NAME;	// URL stats with NULL value
	}

	while (length--){
		ch = url[length];
		if (!isValidSqlChar(ch)){
			if (ch == '\\' || ch == '/'){
				break;
			}
			return MISSING_TABLE_NAME;	// Invalid chars in the table name
		}
		++tableNameSize;
	}
	
	if (!tableNameSize){
		return MISSING_TABLE_NAME;	// Invalid chars in the table name
	}

	if (tableNameSize > maxLength){
		return TABLE_NAME_LARGER_THAN_ALLOWED;			// table name size larger than allowed
	}

	// copy table name to dest
	memcpy(dest,  url + urlLength - tableNameSize, tableNameSize);

	*nameSize = tableNameSize;	// update the caller with the size

	return 0;
}