#include "ConditionParser.h"
#include "Utils.h"
#include <sstream>
#include <cctype>
#include <stdexcept>
#include <algorithm>

// --- Expression Subclasses ---
class ComparisonExpression : public ConditionExpression {
public:
    ComparisonExpression(const std::string& column, const std::string& op, const std::string& value)
        : column(column), op(op), value(value) {}

    bool evaluate(const std::vector<std::string>& row,
                  const std::vector<std::string>& columns) const override {
        auto it = std::find(columns.begin(), columns.end(), column);
        if(it == columns.end()) return false;
        int idx = std::distance(columns.begin(), it);
        const std::string& cell = row[idx];
        // Note: All comparisons are string‐based.
        if(op == "=")
            return cell == value;
        else if(op == "!=")
            return cell != value;
        else if(op == ">")
            return cell > value;
        else if(op == "<")
            return cell < value;
        else if(op == ">=")
            return cell >= value;
        else if(op == "<=")
            return cell <= value;
        return false;
    }
private:
    std::string column;
    std::string op;
    std::string value;
};

class AndExpression : public ConditionExpression {
public:
    AndExpression(ConditionExprPtr left, ConditionExprPtr right)
        : left(std::move(left)), right(std::move(right)) {}

    bool evaluate(const std::vector<std::string>& row,
                  const std::vector<std::string>& columns) const override {
        return left->evaluate(row, columns) && right->evaluate(row, columns);
    }
private:
    ConditionExprPtr left;
    ConditionExprPtr right;
};

class OrExpression : public ConditionExpression {
public:
    OrExpression(ConditionExprPtr left, ConditionExprPtr right)
        : left(std::move(left)), right(std::move(right)) {}

    bool evaluate(const std::vector<std::string>& row,
                  const std::vector<std::string>& columns) const override {
        return left->evaluate(row, columns) || right->evaluate(row, columns);
    }
private:
    ConditionExprPtr left;
    ConditionExprPtr right;
};

// --- ConditionParser Implementation ---
ConditionParser::ConditionParser(const std::string& condition) : current(0) {
    tokenize(condition);
}

void ConditionParser::tokenize(const std::string& condition) {
    std::string buffer;
    for (size_t i = 0; i < condition.size(); ++i) {
        char ch = condition[i];
        if (std::isspace(ch)) {
            if (!buffer.empty()) {
                tokens.push_back(buffer);
                buffer.clear();
            }
        } else if (ch == '(' || ch == ')') {
            if (!buffer.empty()) {
                tokens.push_back(buffer);
                buffer.clear();
            }
            tokens.push_back(std::string(1, ch));
        } else if (ch == '=' || ch == '!' || ch == '<' || ch == '>') {
            if (!buffer.empty()) {
                tokens.push_back(buffer);
                buffer.clear();
            }
            std::string op(1, ch);
            if (i + 1 < condition.size() && condition[i + 1] == '=') {
                op.push_back('=');
                ++i;
            }
            tokens.push_back(op);
        } else {
            buffer.push_back(ch);
        }
    }
    if (!buffer.empty())
        tokens.push_back(buffer);
}

std::string ConditionParser::peek() const {
    if (current < tokens.size())
        return tokens[current];
    return "";
}

std::string ConditionParser::getNext() {
    if (current < tokens.size())
        return tokens[current++];
    return "";
}

bool ConditionParser::matchCondition(const std::string& token) {
    if (peek() == token) {
        current++;
        return true;
    }
    return false;
}

ConditionExprPtr ConditionParser::parse() {
    return parseExpression();
}

ConditionExprPtr ConditionParser::parseExpression() {
    // expr -> term { OR term }
    auto left = parseTerm();
    while (toUpperCase(peek()) == "OR") {
        getNext(); // consume "OR"
        auto right = parseTerm();
        left = std::make_unique<OrExpression>(std::move(left), std::move(right));
    }
    return left;
}

ConditionExprPtr ConditionParser::parseTerm() {
    // term -> factor { AND factor }
    auto left = parseFactor();
    while (toUpperCase(peek()) == "AND") {
        getNext(); // consume "AND"
        auto right = parseFactor();
        left = std::make_unique<AndExpression>(std::move(left), std::move(right));
    }
    return left;
}

ConditionExprPtr ConditionParser::parseFactor() {
    // factor -> '(' expr ')' | comparison
    if (matchCondition("(")) {
        auto expr = parseExpression();
        if (!matchCondition(")"))
            throw std::runtime_error("Missing closing parenthesis");
        return expr;
    }
    return parseComparison();
}

ConditionExprPtr ConditionParser::parseComparison() {
    // comparison -> identifier operator literal
    std::string identifier = getNext();
    std::string op = getNext();
    std::string literal = getNext();
    // Remove single quotes if present.
    if (!literal.empty() && literal.front() == '\'' && literal.back() == '\'')
        literal = literal.substr(1, literal.size() - 2);
    return std::make_unique<ComparisonExpression>(identifier, op, literal);
}