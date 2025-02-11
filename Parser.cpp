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
        std::string column;
        while (iss >> column) {
            q.columns.push_back(column);
        }
    } else if (word == "INSERT") {
        q.type = "INSERT";
        iss >> word; // INTO
        iss >> q.tableName;
        q.values = extractValues(query);
    } else if (word == "SELECT") {
        q.type = "SELECT";
        iss >> word; // *
        iss >> word; // FROM
        iss >> q.tableName;
        q.condition = extractCondition(query);
    } else if (word == "DELETE") {
        q.type = "DELETE";
        iss >> word; // FROM
        iss >> q.tableName;
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

std::string Parser::extractCondition(const std::string& query) {
    std::string::size_type start = query.find("WHERE");
    if (start == std::string::npos) return "";
    return query.substr(start + 6);
}