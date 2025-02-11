#ifndef PARSER_H
#define PARSER_H

#include <string>
#include <vector>

struct Query {
    std::string type;
    std::string tableName;
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> values;
    std::string condition;
};

class Parser {
public:
    Query parseQuery(const std::string& query);
    std::vector<std::vector<std::string>> extractValues(const std::string& query);
    std::string extractCondition(const std::string& query);
};

#endif // PARSER_H