#ifndef PARSER_H
#define PARSER_H

#include <string>
#include <vector>
#include <memory>
#include "Table.h"

// Enhanced Query structure to handle all the new SQL features
struct Query {
    std::string type;  // SQL command type
    
    // Table information
    std::string tableName;
    std::string newTableName; // For RENAME
    
    // Columns and data
    std::vector<std::pair<std::string, std::string>> columns;
    std::vector<std::vector<std::string>> values;
    std::vector<std::pair<std::string, std::string>> updates;
    
    // SELECT specific
    std::vector<std::string> selectColumns;
    bool distinct = false;
    bool all = false;
    
    // Filtering and sorting
    std::string condition;
    std::string havingCondition;
    std::vector<std::string> orderByColumns;
    std::vector<std::string> groupByColumns;
    
    // ALTER TABLE
    std::string alterAction; // ADD, DROP, RENAME, ADD CONSTRAINT, DROP CONSTRAINT, etc.
    std::pair<std::string, std::string> alterColumn;
    
    // JOIN operations
    bool isJoin = false;
    std::string joinType; // INNER, LEFT OUTER, RIGHT OUTER, FULL OUTER, NATURAL
    std::string joinTable;
    std::string joinCondition;
    std::vector<std::string> usingColumns; // For USING clause
    
    // Constraints
    std::vector<Constraint> constraints;
    
    // Indexes
    std::string indexName;
    std::string columnName;
    
    // Merge operation
    std::string mergeCommand;
    
    // Subqueries
    std::vector<std::string> subqueries;
    
    // WITH clause
    std::vector<std::pair<std::string, std::string>> withClauses;
    
    // Set operations
    std::string setOperation; // UNION, INTERSECT, EXCEPT
    std::string rightQuery;
    
    // View
    std::string viewName;
    std::string viewDefinition;
    
    // User-defined types
    std::string typeName;
    
    // Authorization
    std::string username;
    std::string privilege;
    bool multiplePrivileges = false;
    std::vector<std::string> privileges;
    // Assertions
    std::string assertionName;
    std::string assertionCondition;
    
    // Recursive queries
    bool isRecursive = false;
    std::string recursiveQuery;
};

class Parser {
public:
    Query parseQuery(const std::string& query);
    
    // Extract different parts of SQL statements
    std::vector<std::vector<std::string>> extractValues(const std::string& query);
    std::vector<std::pair<std::string, std::string>> extractColumns(const std::string& query);
    std::vector<std::pair<std::string, std::string>> extractUpdates(const std::string& query);
    std::vector<std::string> extractSelectColumns(const std::string& query);
    std::string extractCondition(const std::string& query);
    std::vector<Constraint> extractConstraints(const std::string& query);
    std::vector<std::pair<std::string, std::string>> extractWithClauses(const std::string& query);
    
    // Parse specific statement types
    Query parseCreateTable(const std::string& query);
    Query parseAlterTable(const std::string& query);
    Query parseSelect(const std::string& query);
    Query parseInsert(const std::string& query);
    Query parseUpdate(const std::string& query);
    Query parseDelete(const std::string& query);
    Query parseCreateIndex(const std::string& query);
    Query parseCreateView(const std::string& query);
    Query parseCreateType(const std::string& query);
    Query parseGrant(const std::string& query);
    Query parseRevoke(const std::string& query);
    Query parseCreateAssertion(const std::string& query);
    
    // Parse JOIN conditions
    std::pair<std::string, std::string> parseJoinCondition(const std::string& query);
    
    // Parse WITH clauses
    std::vector<std::pair<std::string, std::string>> parseWithClause(const std::string& query);
    
    // Parse set operations
    std::pair<std::string, std::string> parseSetOperation(const std::string& query);
    
    // Parse subqueries
    std::vector<std::string> extractSubqueries(const std::string& query);
};

#endif // PARSER_H