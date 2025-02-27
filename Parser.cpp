#include "Parser.h"
#include "Utils.h"
#include <sstream>
#include <algorithm>
#include <unordered_set>
#include <iostream>

// Removed duplicate functions:
// - toUpperCase()
// - toLowerCase()
// - trim()
// - isValidDataType()
// (They are now properly sourced from Utils.h)

Query Parser::parseQuery(const std::string& queryStr) {
    Query q;
    std::istringstream iss(queryStr);
    std::string word;
    iss >> word;
    std::string command = toUpperCase(word);

    if (command == "CREATE") {
        iss >> word;
        if (toUpperCase(word) == "TABLE") {
            q.type = "CREATE";
            iss >> q.tableName;
            q.columns = extractColumns(queryStr);
    
            // Validate duplicate column names and data types.
            std::unordered_set<std::string> seen;
            for (const auto &colPair : q.columns) {
                if (seen.find(colPair.first) != seen.end()) {
                    std::cerr << "Error: Duplicate column name '" << colPair.first 
                              << "' in CREATE TABLE statement." << std::endl;
                    q.columns.clear();
                    return q;
                }
                seen.insert(colPair.first);
                if (!isValidDataType(colPair.second)) {
                    std::cerr << "Error: Invalid data type '" << colPair.second 
                              << "' for column '" << colPair.first << "'." << std::endl;
                    q.columns.clear();
                    return q;
                }
            }
        } else if (toUpperCase(word) == "INDEX") {
            q.type = "CREATEINDEX";
            iss >> q.indexName;
            iss >> word; // Expect "ON"
            iss >> q.tableName;
            size_t parenStart = queryStr.find('(', queryStr.find(q.tableName));
            size_t parenEnd = queryStr.find(')', parenStart);
            if (parenStart != std::string::npos && parenEnd != std::string::npos) {
                std::string col = queryStr.substr(parenStart + 1, parenEnd - parenStart - 1);
                q.columnName = trim(col);
            }
        }
    } else if (command == "INSERT") {
        q.type = "INSERT";
        iss >> word; // Expect "INTO"
        iss >> q.tableName;
        q.values = extractValues(queryStr);
    } else if (command == "SELECT") {
        q.type = "SELECT";
        q.selectColumns = extractSelectColumns(queryStr);
        size_t fromPos = toUpperCase(queryStr).find("FROM");
        if (fromPos != std::string::npos) {
            std::istringstream issFrom(queryStr.substr(fromPos + 4));
            issFrom >> q.tableName;
        }
        q.condition = extractCondition(queryStr);
        size_t orderPos = toUpperCase(queryStr).find("ORDER BY");
        if (orderPos != std::string::npos) {
            size_t endPos = queryStr.find_first_of("\n;", orderPos);
            std::string orderStr = queryStr.substr(orderPos + 8, endPos - orderPos - 8);
            std::istringstream issOrder(orderStr);
            std::string token;
            while (std::getline(issOrder, token, ',')) {
                q.orderByColumns.push_back(trim(token));
            }
        }
        size_t groupPos = toUpperCase(queryStr).find("GROUP BY");
        if (groupPos != std::string::npos) {
            size_t endPos = queryStr.find_first_of("\n;", groupPos);
            std::string groupStr = queryStr.substr(groupPos + 8, endPos - groupPos - 8);
            std::istringstream issGroup(groupStr);
            std::string token;
            while (std::getline(issGroup, token, ',')) {
                q.groupByColumns.push_back(trim(token));
            }
        }
        size_t havingPos = toUpperCase(queryStr).find("HAVING");
        if (havingPos != std::string::npos) {
            q.havingCondition = trim(queryStr.substr(havingPos + 6));
        }
        size_t joinPos = toUpperCase(queryStr).find("JOIN");
        if (joinPos != std::string::npos) {
            q.isJoin = true;
            std::istringstream issJoin(queryStr.substr(joinPos + 4));
            issJoin >> q.joinTable;
            size_t onPos = toUpperCase(queryStr).find("ON", joinPos);
            if (onPos != std::string::npos) {
                std::string joinCond = queryStr.substr(onPos + 2);
                joinCond = trim(joinCond);
                if (!joinCond.empty() && joinCond.back() == ';') {
                    joinCond.pop_back();
                    joinCond = trim(joinCond);
                }
                q.joinCondition = joinCond;
            }
        }
    } else if (command == "DELETE") {
        q.type = "DELETE";
        iss >> word; // Expect "FROM"
        iss >> q.tableName;
        q.condition = extractCondition(queryStr);
    } else if (command == "UPDATE") {
        q.type = "UPDATE";
        iss >> q.tableName;
        q.updates = extractUpdates(queryStr);
        q.condition = extractCondition(queryStr);
    } else if (command == "DROP") {
        iss >> word;
        if (toUpperCase(word) == "TABLE") {
            q.type = "DROP";
            iss >> q.tableName;
        } else if (toUpperCase(word) == "INDEX") {
            q.type = "DROPINDEX";
            iss >> q.indexName;
        }
    } else if (command == "ALTER") {
        q.type = "ALTER";
        iss >> word; // Expect "TABLE"
        iss >> q.tableName;
        iss >> word; // Expect action: ADD, DROP, or RENAME
        q.alterAction = toUpperCase(word);
        if (q.alterAction == "ADD") {
            std::string nextToken;
            if (iss >> nextToken) {
                if (toUpperCase(nextToken) == "COLUMN") {
                    std::string colName, colType;
                    iss >> colName >> colType;
                    q.alterColumn = { colName, colType };
                } else {
                    std::string colName = nextToken;
                    std::string colType;
                    iss >> colType;
                    q.alterColumn = { colName, colType };
                }
            }
        } else if (q.alterAction == "DROP") {
            iss >> word;
            if (toUpperCase(word) == "COLUMN")
                iss >> q.alterColumn.first;
            else
                q.alterColumn.first = word;
        } else if (q.alterAction == "RENAME") {
            iss >> word; // Expect "TO"
            iss >> q.newTableName;
        }
    } else if (command == "DESCRIBE") {
        q.type = "DESCRIBE";
        iss >> q.tableName;
    } else if (command == "SHOW") {
        q.type = "SHOW";
    } else if (command == "BEGIN") {
        q.type = "BEGIN";
    } else if (command == "COMMIT") {
        q.type = "COMMIT";
    } else if (command == "ROLLBACK") {
        q.type = "ROLLBACK";
    } else if (command == "TRUNCATE") {
        q.type = "TRUNCATE";
        iss >> word; // Expect "TABLE"
        iss >> q.tableName;
    } else if (command == "MERGE") {
        q.type = "MERGE";
        iss >> word; // Expect "INTO"
        iss >> q.tableName;
        size_t pos = queryStr.find(q.tableName);
        q.mergeCommand = trim(queryStr.substr(pos + q.tableName.size()));
    } else if (command == "REPLACE") {
        q.type = "REPLACE";
        iss >> word; // Expect "INTO"
        iss >> q.tableName;
        q.values = extractValues(queryStr);
    }
    return q;
}

std::vector<std::vector<std::string>> Parser::extractValues(const std::string& query) {
    std::vector<std::vector<std::string>> values;
    size_t start = 0;
    while ((start = query.find('(', start)) != std::string::npos) {
        ++start;
        size_t end = query.find(')', start);
        if (end == std::string::npos) break;
        std::string valuesStr = query.substr(start, end - start);
        std::istringstream iss(valuesStr);
        std::string value;
        std::vector<std::string> valueSet;
        while (std::getline(iss, value, ',')) {
            value = trim(value);
            if (!value.empty() && value.front() == '\'' && value.back() == '\'' && value.size() > 1)
                value = value.substr(1, value.size() - 2);
            valueSet.push_back(value);
        }
        values.push_back(valueSet);
        start = end + 1;
    }
    return values;
}

std::vector<std::pair<std::string, std::string>> Parser::extractColumns(const std::string& query) {
    std::vector<std::pair<std::string, std::string>> cols;
    size_t start = query.find('(');
    size_t end = query.find(')', start);
    if (start == std::string::npos || end == std::string::npos)
        return cols;
    std::string colsStr = query.substr(start + 1, end - start - 1);
    std::istringstream iss(colsStr);
    std::string col;
    while (std::getline(iss, col, ',')) {
        std::istringstream colStream(col);
        std::string colName, colType;
        colStream >> colName >> colType;
        cols.emplace_back(trim(colName), trim(colType));
    }
    return cols;
}

std::vector<std::pair<std::string, std::string>> Parser::extractUpdates(const std::string& query) {
    std::vector<std::pair<std::string, std::string>> updates;
    size_t pos = toUpperCase(query).find("SET");
    if (pos == std::string::npos)
        return updates;
    pos += 3;
    size_t endPos = toUpperCase(query).find("WHERE", pos);
    std::string updatesStr = (endPos == std::string::npos) ? query.substr(pos) : query.substr(pos, endPos - pos);
    std::istringstream iss(updatesStr);
    std::string update;
    while (std::getline(iss, update, ',')) {
        size_t eq = update.find('=');
        if (eq == std::string::npos) continue;
        std::string col = trim(update.substr(0, eq));
        std::string val = trim(update.substr(eq + 1));
        if (!val.empty() && val.front() == '\'' && val.back() == '\'' && val.size() > 1)
            val = val.substr(1, val.size() - 2);
        updates.emplace_back(col, val);
    }
    return updates;
}

std::vector<std::string> Parser::extractSelectColumns(const std::string& query) {
    std::vector<std::string> cols;
    size_t start = toUpperCase(query).find("SELECT");
    if (start == std::string::npos) return cols;
    start += 6;
    size_t end = toUpperCase(query).find("FROM", start);
    if (end == std::string::npos) return cols;
    std::string colsStr = query.substr(start, end - start);
    std::istringstream iss(colsStr);
    std::string col;
    while (std::getline(iss, col, ',')) {
        cols.push_back(trim(col));
    }
    return cols;
}

std::string Parser::extractCondition(const std::string& query) {
    size_t pos = toUpperCase(query).find("WHERE");
    if (pos == std::string::npos)
        return "";
    size_t endPos = std::string::npos;
    size_t orderPos = toUpperCase(query).find("ORDER BY", pos);
    size_t groupPos = toUpperCase(query).find("GROUP BY", pos);
    size_t havingPos = toUpperCase(query).find("HAVING", pos);
    if (orderPos != std::string::npos)
        endPos = orderPos;
    if (groupPos != std::string::npos && (endPos == std::string::npos || groupPos < endPos))
        endPos = groupPos;
    if (havingPos != std::string::npos && (endPos == std::string::npos || havingPos < endPos))
        endPos = havingPos;
    if (endPos == std::string::npos)
        return trim(query.substr(pos + 5));
    return trim(query.substr(pos + 5, endPos - pos - 5));
}