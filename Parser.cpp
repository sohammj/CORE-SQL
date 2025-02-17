#include "Parser.h"
#include "Utils.h"
#include <sstream>
#include <algorithm>

Query Parser::parseQuery(const std::string& queryStr) {
    Query q;
    std::istringstream iss(queryStr);
    std::string word;
    iss >> word;
    std::string command = toUpperCase(word);

    if (command == "CREATE") {
        q.type = "CREATE";
        iss >> word; // Expect "TABLE"
        iss >> q.tableName;
        q.columns = extractColumns(queryStr);
    } else if (command == "INSERT") {
        q.type = "INSERT";
        iss >> word; // Expect "INTO"
        iss >> q.tableName;
        q.values = extractValues(queryStr);
    } else if (command == "SELECT") {
        q.type = "SELECT";
        q.selectColumns = extractSelectColumns(queryStr);
        // Look for FROM
        size_t fromPos = toUpperCase(queryStr).find("FROM");
        if (fromPos != std::string::npos) {
            std::istringstream issFrom(queryStr.substr(fromPos + 4));
            issFrom >> q.tableName;
        }
        // Extract WHERE, GROUP BY, ORDER BY, HAVING if present.
        q.condition = extractCondition(queryStr);
        // Simple extraction for ORDER BY
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
        // GROUP BY extraction
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
        // HAVING extraction
        size_t havingPos = toUpperCase(queryStr).find("HAVING");
        if (havingPos != std::string::npos) {
            q.havingCondition = trim(queryStr.substr(havingPos + 6));
        }
        // JOIN extraction (very basic)
        size_t joinPos = toUpperCase(queryStr).find("JOIN");
        if (joinPos != std::string::npos) {
            q.isJoin = true;
            std::istringstream issJoin(queryStr.substr(joinPos + 4));
            issJoin >> q.joinTable;
            // Look for ON
            size_t onPos = toUpperCase(queryStr).find("ON", joinPos);
            if (onPos != std::string::npos) {
                q.joinCondition = trim(queryStr.substr(onPos + 2));
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
        q.type = "DROP";
        iss >> word; // Expect "TABLE"
        iss >> q.tableName;
    } else if (command == "ALTER") {
        q.type = "ALTER";
        iss >> word; // Expect "TABLE"
        iss >> q.tableName;
        iss >> word; // Expect action: ADD or DROP
        q.alterAction = toUpperCase(word);
        if (q.alterAction == "ADD") {
            // Expect column definition in parentheses.
            q.columns = extractColumns(queryStr);
            if (!q.columns.empty())
                q.alterColumn = q.columns[0];
        } else if (q.alterAction == "DROP") {
            iss >> q.alterColumn.first; // column name to drop
        }
    } else if (command == "DESCRIBE") {
        q.type = "DESCRIBE";
        iss >> q.tableName;
    } else if (command == "SHOW") {
        q.type = "SHOW";
        // Expect "TABLES"
    } else if (command == "BEGIN") {
        q.type = "BEGIN";
    } else if (command == "COMMIT") {
        q.type = "COMMIT";
    } else if (command == "ROLLBACK") {
        q.type = "ROLLBACK";
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
            valueSet.push_back(trim(value));
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
    // End at ORDER BY, GROUP BY, or HAVING if present.
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
