#ifndef PARSER_H
#define PARSER_H

#include <string>
#include <vector>

struct Query {
    std::string type;  // e.g. CREATE, INSERT, SELECT, UPDATE, DELETE, DROP, ALTER, DESCRIBE, BEGIN, COMMIT, ROLLBACK, TRUNCATE, RENAME, CREATEINDEX, DROPINDEX, MERGE, REPLACE
    std::string tableName;
    // For CREATE TABLE: list of (column name, type)
    std::vector<std::pair<std::string, std::string>> columns;
    // For INSERT and REPLACE: list of rows (each row is a list of values)
    std::vector<std::vector<std::string>> values;
    // For UPDATE: list of (column, new value)
    std::vector<std::pair<std::string, std::string>> updates;
    // For SELECT: list of columns to display
    std::vector<std::string> selectColumns;
    // WHERE clause (and HAVING for GROUP BY)
    std::string condition;
    std::string havingCondition;
    // ORDER BY and GROUP BY
    std::vector<std::string> orderByColumns;
    std::vector<std::string> groupByColumns;
    // For ALTER TABLE: action and column info
    std::string alterAction; // "ADD" or "DROP" or "RENAME"
    std::pair<std::string, std::string> alterColumn; // column name and type (for ADD)
    // For JOIN in SELECT:
    bool isJoin = false;
    std::string joinTable;
    std::string joinCondition;

    // New functionalities:
    // For TRUNCATE TABLE and RENAME TABLE
    std::string newTableName; // used for RENAME
    // For INDEX operations
    std::string indexName;
    std::string columnName; // used in CREATE INDEX
    // For MERGE
    std::string mergeCommand;
};

class Parser {
public:
    Query parseQuery(const std::string& query);
    std::vector<std::vector<std::string>> extractValues(const std::string& query);
    std::vector<std::pair<std::string, std::string>> extractColumns(const std::string& query);
    std::vector<std::pair<std::string, std::string>> extractUpdates(const std::string& query);
    std::vector<std::string> extractSelectColumns(const std::string& query);
    std::string extractCondition(const std::string& query);
};

#endif // PARSER_H
