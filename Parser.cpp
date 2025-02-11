#include "Parser.h"
#include "Utils.h"
#include <regex>
#include <sstream>
#include <algorithm>

Query Parser::parseQuery(const std::string& query) {
    Query q;
    std::string lowerQuery = toLowerCase(query);
    std::smatch match;
    
    // BEGIN command (for starting transactions)
    if(lowerQuery.find("begin") == 0) {
        q.type = "BEGIN";
    }
    // SELECT command
    else if(lowerQuery.find("select") == 0) {
        q.type = "SELECT";
        // Pattern: select <columns> from <table> [join <table> on <condition>] [where <condition>] [group by <groupBy>] [having <having>] [order by <orderBy>] [limit <limit>] [offset <offset>]
        std::regex selectRegex(R"(select\s+(.*?)\s+from\s+(\w+)(.*))");
        if(std::regex_match(lowerQuery, match, selectRegex)) {
            std::string colStr = match[1];
            q.tableName = match[2];
            std::string remainder = match[3];
            std::istringstream colStream(colStr);
            std::string col;
            while(getline(colStream, col, ',')) {
                col = trim(col);
                if(!col.empty())
                    q.columns.push_back(col);
            }
            // JOIN clause
            std::regex joinRegex(R"(join\s+(\w+)\s+on\s+(.*?)(\s+where|\s+group by|\s+having|\s+order by|\s+limit|\s+offset|$))");
            if(std::regex_search(remainder, match, joinRegex)) {
                q.joinTable = match[1];
                q.joinCondition = trim(match[2]);
            }
            // WHERE clause
            std::regex whereRegex(R"(where\s+(.*?)(\s+group by|\s+having|\s+order by|\s+limit|\s+offset|$))");
            if(std::regex_search(remainder, match, whereRegex)) {
                q.condition = trim(match[1]);
            }
            // GROUP BY clause
            std::regex groupByRegex(R"(group by\s+(.*?)(\s+having|\s+order by|\s+limit|\s+offset|$))");
            if(std::regex_search(remainder, match, groupByRegex)) {
                q.groupBy = trim(match[1]);
            }
            // HAVING clause
            std::regex havingRegex(R"(having\s+(.*?)(\s+order by|\s+limit|\s+offset|$))");
            if(std::regex_search(remainder, match, havingRegex)) {
                q.having = trim(match[1]);
            }
            // ORDER BY clause
            std::regex orderByRegex(R"(order by\s+(.*?)(\s+limit|\s+offset|$))");
            if(std::regex_search(remainder, match, orderByRegex)) {
                q.orderBy = trim(match[1]);
            }
            // LIMIT clause
            std::regex limitRegex(R"(limit\s+(\d+))");
            if(std::regex_search(remainder, match, limitRegex)) {
                q.limit = std::stoi(match[1]);
            }
            // OFFSET clause
            std::regex offsetRegex(R"(offset\s+(\d+))");
            if(std::regex_search(remainder, match, offsetRegex)) {
                q.offset = std::stoi(match[1]);
            }
        }
    }
    // CREATE TABLE command
    else if(lowerQuery.find("create") == 0) {
        q.type = "CREATE";
        std::regex createRegex(R"(create\s+table\s+(\w+)\s+(.*))");
        if(std::regex_match(lowerQuery, match, createRegex)) {
            q.tableName = match[1];
            std::string cols = match[2];
            std::istringstream iss(cols);
            std::string col;
            while(iss >> col) {
                q.columns.push_back(col);
            }
        }
    }
    // INSERT command
    else if(lowerQuery.find("insert") == 0) {
        q.type = "INSERT";
        std::regex insertRegex(R"(insert\s+into\s+(\w+)\s+values\s+(.+))");
        if(std::regex_match(lowerQuery, match, insertRegex)) {
            q.tableName = match[1];
            std::string valuesStr = match[2];
            q.values = extractValues(valuesStr);
        }
    }
    // DELETE command
    else if(lowerQuery.find("delete") == 0) {
        q.type = "DELETE";
        std::regex deleteRegex(R"(delete\s+from\s+(\w+)\s+where\s+(.+))");
        if(std::regex_match(lowerQuery, match, deleteRegex)) {
            q.tableName = match[1];
            q.condition = trim(match[2]);
        }
    }
    // UPDATE command
    else if(lowerQuery.find("update") == 0) {
        q.type = "UPDATE";
        // Pattern: update <tableName> set col1 = value1, col2 = value2 [where <condition>]
        std::regex updateRegex(R"(update\s+(\w+)\s+set\s+(.+?)(\s+where\s+(.+))?$)");
        if(std::regex_match(lowerQuery, match, updateRegex)) {
            q.tableName = match[1];
            std::string assignments = match[2];
            std::istringstream assignStream(assignments);
            std::string assign;
            while(getline(assignStream, assign, ',')) {
                size_t eqPos = assign.find('=');
                if(eqPos != std::string::npos) {
                    std::string col = trim(assign.substr(0, eqPos));
                    std::string val = trim(assign.substr(eqPos + 1));
                    q.updateAssignments.push_back({col, val});
                }
            }
            if(match.size() >= 5 && !match[4].str().empty()) {
                q.condition = trim(match[4]);
            }
        }
    }
    // MERGE command
    else if(lowerQuery.find("merge") == 0) {
        q.type = "MERGE";
        // Pattern: merge into <tableName> values <values>
        std::regex mergeRegex(R"(merge\s+into\s+(\w+)\s+values\s+(.+))");
        if(std::regex_match(lowerQuery, match, mergeRegex)) {
            q.tableName = match[1];
            std::string valuesStr = match[2];
            q.values = extractValues(valuesStr);
        }
    }
    
    // UPSERT command
    else if(lowerQuery.find("upsert") == 0) {
        q.type = "UPSERT";
        // Pattern: upsert into <tableName> values <values>
        std::regex upsertRegex(R"(upsert\s+into\s+(\w+)\s+values\s+(.+))");
        if(std::regex_match(lowerQuery, match, upsertRegex)) {
            q.tableName = match[1];
            std::string valuesStr = match[2];
            q.values = extractValues(valuesStr);
        }
    }
    
    // ALTER TABLE command (only support ADD column)
    else if(lowerQuery.find("alter") == 0) {
        q.type = "ALTER";
        // Pattern: alter table <tableName> add <columnName>
        std::regex alterRegex(R"(alter\s+table\s+(\w+)\s+add\s+(\w+))");
        if(std::regex_match(lowerQuery, match, alterRegex)) {
            q.tableName = match[1];
            q.columns.push_back(match[2]); // new column name to add
        }
    }
    // DROP TABLE command
    else if(lowerQuery.find("drop") == 0) {
        q.type = "DROP";
        std::regex dropRegex(R"(drop\s+table\s+(\w+))");
        if(std::regex_match(lowerQuery, match, dropRegex)) {
            q.tableName = match[1];
        }
    }
    // TRUNCATE TABLE command
    else if(lowerQuery.find("truncate") == 0) {
        q.type = "TRUNCATE";
        std::regex truncateRegex(R"(truncate\s+table\s+(\w+))");
        if(std::regex_match(lowerQuery, match, truncateRegex)) {
            q.tableName = match[1];
        }
    }
    // RENAME TABLE command
    else if(lowerQuery.find("rename") == 0) {
        q.type = "RENAME";
        // Pattern: rename table <oldName> to <newName>
        std::regex renameRegex(R"(rename\s+table\s+(\w+)\s+to\s+(\w+))");
        if(std::regex_match(lowerQuery, match, renameRegex)) {
            q.tableName = match[1];
            q.newTableName = match[2];
        }
    }
    // Transaction Control commands
    else if(lowerQuery.find("commit") == 0) {
        q.type = "COMMIT";
    }
    else if(lowerQuery.find("rollback") == 0) {
        q.type = "ROLLBACK";
    }
    else if(lowerQuery.find("savepoint") == 0) {
        q.type = "SAVEPOINT";
        // Optionally parse a name here.
    }
    else if(lowerQuery.find("set transaction") == 0) {
        q.type = "SET_TRANSACTION";
        // Use condition to store settings
        q.condition = lowerQuery.substr(15);
    }
    // Data Control commands
    else if(lowerQuery.find("grant") == 0) {
        q.type = "GRANT";
        // Further parsing can be added.
    }
    else if(lowerQuery.find("revoke") == 0) {
        q.type = "REVOKE";
    }
    else if(lowerQuery.find("deny") == 0) {
        q.type = "DENY";
    }
    else if(lowerQuery.find("show tables") == 0) {
        q.type = "SHOW_TABLES";
    }
    else {
        q.type = "UNKNOWN";
    }
    
    return q;
}

std::vector<std::vector<std::string>> Parser::extractValues(const std::string& query) {
    std::vector<std::vector<std::string>> values;
    size_t pos = 0;
    while((pos = query.find('(', pos)) != std::string::npos) {
        size_t endPos = query.find(')', pos);
        if(endPos == std::string::npos)
            break;
        std::string group = query.substr(pos + 1, endPos - pos - 1);
        std::vector<std::string> row;
        std::istringstream iss(group);
        std::string val;
        while(getline(iss, val, ',')) {
            row.push_back(trim(val));
        }
        values.push_back(row);
        pos = endPos + 1;
    }
    return values;
}
