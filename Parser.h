#ifndef PARSER_H
#define PARSER_H

#include <string>
#include <vector>
#include <utility>

// Structure to hold a parsed query.
struct Query {
    std::string type;         // e.g. "CREATE", "INSERT", "SELECT", "DELETE", "UPDATE", "MERGE", "UPSERT", "ALTER", "DROP", "TRUNCATE", "RENAME", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "SET_TRANSACTION", "GRANT", "REVOKE", "DENY", "SHOW_TABLES"
    std::string tableName;
    std::vector<std::string> columns; // For CREATE (column names) or SELECT (selected columns)
    std::vector<std::vector<std::string>> values; // For INSERT
    std::string condition;    // WHERE clause (for SELECT, DELETE, UPDATE)
    std::string joinTable;    // For JOIN (other table name)
    std::string joinCondition;// For JOIN (e.g. "leftcol = rightcol")
    std::string groupBy;      // GROUP BY column (only one supported here)
    std::string having;       // HAVING clause (not deeply processed)
    std::string orderBy;      // ORDER BY column (only one supported here)
    int limit = -1;           // LIMIT number (-1 means no limit)
    int offset = 0;           // OFFSET number

    // For UPDATE command: list of assignments: column = value
    std::vector<std::pair<std::string, std::string>> updateAssignments;
    
    // For RENAME command: new table name.
    std::string newTableName;
};

class Parser {
public:
    Query parseQuery(const std::string& query);
    std::vector<std::vector<std::string>> extractValues(const std::string& query);
};

#endif // PARSER_H
