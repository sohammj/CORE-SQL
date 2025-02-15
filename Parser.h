#ifndef PARSER_H
#define PARSER_H

#include <string>
#include <vector>

struct Query {
    std::string type;
    std::string tableName;
    std::vector<std::pair<std::string, std::string>> columns;
    std::vector<std::vector<std::string>> values;
    std::vector<std::pair<std::string, std::string>> updates;
    std::vector<std::string> selectColumns;
    std::string condition;
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