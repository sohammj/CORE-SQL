#include "Parser.h"
#include <sstream>
#include <algorithm>
#include "Utils.h"

Query Parser::parseQuery(const std::string& query) {
    Query q;
    std::istringstream iss(query);
    std::string word;
    iss >> word;
    word = toUpperCase(word);
    if (word == "CREATE") {
        q.type = "CREATE";
        iss >> word; // TABLE
        iss >> q.tableName;
        q.columns = extractColumns(query);
    } else if (word == "INSERT") {
        q.type = "INSERT";
        iss >> word; // INTO
        iss >> q.tableName;
        q.values = extractValues(query);
    } else if (word == "SELECT") {
        q.type = "SELECT";
        q.selectColumns = extractSelectColumns(query);
        iss >> word; // *
        iss >> word; // FROM
        iss >> q.tableName;
        q.condition = extractCondition(query);
    } else if (word == "DELETE") {
        q.type = "DELETE";
        iss >> word; // FROM
        iss >> q.tableName;
        q.condition = extractCondition(query);
    } else if (word == "UPDATE") {
        q.type = "UPDATE";
        iss >> q.tableName;
        q.updates = extractUpdates(query);
        q.condition = extractCondition(query);
    } else if (word == "SHOW") {
        q.type = "SHOW";
        iss >> word; // *
        iss >> word; // FROM
        iss >> q.tableName;
    }
    return q;
}

std::vector<std::vector<std::string>> Parser::extractValues(const std::string& query) {
    std::vector<std::vector<std::string>> values;
    std::string::size_type start = 0;
    while ((start = query.find('(', start)) != std::string::npos) {
        start++;
        std::string::size_type end = query.find(')', start);
        if (end == std::string::npos) break;
        std::string valuesStr = query.substr(start, end - start);
        std::istringstream iss(valuesStr);
        std::string value;
        std::vector<std::string> valueSet;
        while (std::getline(iss, value, ',')) {
            value = trim(value);
            valueSet.push_back(value);
        }
        values.push_back(valueSet);
        start = end + 1;
    }
    return values;
}

std::vector<std::pair<std::string, std::string>> Parser::extractColumns(const std::string& query) {
    std::vector<std::pair<std::string, std::string>> columns;
    std::string::size_type start = query.find('(') + 1;
    std::string::size_type end = query.find(')', start);
    std::string columnsStr = query.substr(start, end - start);
    std::istringstream iss(columnsStr);
    std::string column;
    while (std::getline(iss, column, ',')) {
        std::istringstream colStream(column);
        std::string colName, colType;
        colStream >> colName >> colType;
        columns.emplace_back(trim(colName), trim(colType));
    }
    return columns;
}

std::vector<std::pair<std::string, std::string>> Parser::extractUpdates(const std::string& query) {
    std::vector<std::pair<std::string, std::string>> updates;
    std::string::size_type start = query.find("SET") + 3;
    std::string::size_type end = query.find("WHERE", start);
    std::string updatesStr = query.substr(start, end - start);
    std::istringstream iss(updatesStr);
    std::string update;
    while (std::getline(iss, update, ',')) {
        std::string::size_type eq = update.find('=');
        std::string column = trim(update.substr(0, eq));
        std::string value = trim(update.substr(eq + 1));
        updates.emplace_back(column, value);
    }
    return updates;
}

std::vector<std::string> Parser::extractSelectColumns(const std::string& query) {
    std::vector<std::string> columns;
    std::string::size_type start = query.find("SELECT") + 7;
    std::string::size_type end = query.find("FROM", start);
    std::string columnsStr = query.substr(start, end - start);
    std::istringstream iss(columnsStr);
    std::string column;
    while (std::getline(iss, column, ',')) {
        columns.push_back(trim(column));
    }
    return columns;
}

std::string Parser::extractCondition(const std::string& query) {
    std::string::size_type start = query.find("WHERE");
    if (start == std::string::npos) return "";
    return query.substr(start + 6);
}