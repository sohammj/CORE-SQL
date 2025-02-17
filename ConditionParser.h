#ifndef CONDITIONPARSER_H
#define CONDITIONPARSER_H

#include <string>
#include <vector>
#include <memory>

// Abstract expression for evaluating conditions against a row.
// 'row' is the vector of values and 'columns' is the header list.
class ConditionExpression {
public:
    virtual ~ConditionExpression() = default;
    virtual bool evaluate(const std::vector<std::string>& row,
                          const std::vector<std::string>& columns) const = 0;
};

using ConditionExprPtr = std::unique_ptr<ConditionExpression>;

class ConditionParser {
public:
    ConditionParser(const std::string& condition);
    ConditionExprPtr parse();
private:
    std::vector<std::string> tokens;
    size_t current;
    void tokenize(const std::string& condition);
    std::string peek() const;
    std::string getNext();
    bool match(const std::string& token);
    // Recursive descent parsing functions
    ConditionExprPtr parseExpression();
    ConditionExprPtr parseTerm();
    ConditionExprPtr parseFactor();
    ConditionExprPtr parseComparison();
};

#endif // CONDITIONPARSER_H
