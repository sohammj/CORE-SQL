#include "ConditionParser.h"
#include "Utils.h"
#include <sstream>
#include <cctype>
#include <stdexcept>
#include <algorithm>
#include <cmath>
#include <regex>

// --- Expression Subclasses ---

// Base expression for literal values
class LiteralExpression : public ConditionExpression {
public:
    explicit LiteralExpression(const std::string& value) : value(value) {}
    
    bool evaluate(const std::vector<std::string>& row,
                  const std::vector<std::string>& columns) const override {
        return !value.empty() && value != "0" && value != "FALSE" && toLowerCase(value) != "false";
    }
    
    std::string getStringValue() const {
        return value;
    }
    
    double getNumericValue() const {
        try {
            return std::stod(value);
        } catch (...) {
            return 0.0;
        }
    }
    
private:
    std::string value;
};

// Column reference expression
class ColumnExpression : public ConditionExpression {
public:
    explicit ColumnExpression(const std::string& column) : column(column) {}
    
    bool evaluate(const std::vector<std::string>& row,
                  const std::vector<std::string>& columns) const override {
        auto it = std::find(columns.begin(), columns.end(), column);
        if (it == columns.end()) return false;
        
        int idx = std::distance(columns.begin(), it);
        if (idx >= row.size()) return false;
        
        const std::string& cell = row[idx];
        return !cell.empty() && cell != "0" && cell != "FALSE" && toLowerCase(cell) != "false";
    }
    
    std::string getStringValue(const std::vector<std::string>& row,
                              const std::vector<std::string>& columns) const {
        auto it = std::find(columns.begin(), columns.end(), column);
        if (it == columns.end()) return "";
        
        int idx = std::distance(columns.begin(), it);
        if (idx >= row.size()) return "";
        
        return row[idx];
    }
    
    double getNumericValue(const std::vector<std::string>& row,
                          const std::vector<std::string>& columns) const {
        std::string strValue = getStringValue(row, columns);
        try {
            return std::stod(strValue);
        } catch (...) {
            return 0.0;
        }
    }
    
    std::string getColumnName() const {
        return column;
    }
    
private:
    std::string column;
};

// Comparison expression
class ComparisonExpression : public ConditionExpression {
public:
    ComparisonExpression(std::unique_ptr<ConditionExpression> left, 
                        const std::string& op, 
                        std::unique_ptr<ConditionExpression> right)
        : left(std::move(left)), op(op), right(std::move(right)) {}

    bool evaluate(const std::vector<std::string>& row,
                  const std::vector<std::string>& columns) const override {
        // Check for specific column expression cases
        auto leftCol = dynamic_cast<ColumnExpression*>(left.get());
        auto rightCol = dynamic_cast<ColumnExpression*>(right.get());
        auto leftLit = dynamic_cast<LiteralExpression*>(left.get());
        auto rightLit = dynamic_cast<LiteralExpression*>(right.get());
        
        if (leftCol && rightLit) {
            // Column <op> Literal
            std::string colValue = leftCol->getStringValue(row, columns);
            std::string litValue = rightLit->getStringValue();
            
            // Special case for LIKE operator
            if (op == "LIKE") {
                return matchLikePattern(colValue, litValue);
            }
            
            // Try numeric comparison first
            try {
                double colNum = std::stod(colValue);
                double litNum = std::stod(litValue);
                
                if (op == "=") return std::abs(colNum - litNum) < 1e-9;
                else if (op == "!=") return std::abs(colNum - litNum) >= 1e-9;
                else if (op == ">") return colNum > litNum;
                else if (op == "<") return colNum < litNum;
                else if (op == ">=") return colNum >= litNum;
                else if (op == "<=") return colNum <= litNum;
            } catch (...) {
                // Fall back to string comparison
                if (op == "=") return colValue == litValue;
                else if (op == "!=") return colValue != litValue;
                else if (op == ">") return colValue > litValue;
                else if (op == "<") return colValue < litValue;
                else if (op == ">=") return colValue >= litValue;
                else if (op == "<=") return colValue <= litValue;
            }
        } else if (rightCol && leftLit) {
            // Literal <op> Column
            std::string colValue = rightCol->getStringValue(row, columns);
            std::string litValue = leftLit->getStringValue();
            
            // Special case for LIKE operator
            if (op == "LIKE") {
                return matchLikePattern(colValue, litValue);
            }
            
            // Try numeric comparison first
            try {
                double colNum = std::stod(colValue);
                double litNum = std::stod(litValue);
                
                if (op == "=") return std::abs(litNum - colNum) < 1e-9;
                else if (op == "!=") return std::abs(litNum - colNum) >= 1e-9;
                else if (op == ">") return litNum > colNum;
                else if (op == "<") return litNum < colNum;
                else if (op == ">=") return litNum >= colNum;
                else if (op == "<=") return litNum <= colNum;
            } catch (...) {
                // Fall back to string comparison
                if (op == "=") return litValue == colValue;
                else if (op == "!=") return litValue != colValue;
                else if (op == ">") return litValue > colValue;
                else if (op == "<") return litValue < colValue;
                else if (op == ">=") return litValue >= colValue;
                else if (op == "<=") return litValue <= colValue;
            }
        } else if (leftCol && rightCol) {
            // Column <op> Column
            std::string leftValue = leftCol->getStringValue(row, columns);
            std::string rightValue = rightCol->getStringValue(row, columns);
            
            // Try numeric comparison first
            try {
                double leftNum = std::stod(leftValue);
                double rightNum = std::stod(rightValue);
                
                if (op == "=") return std::abs(leftNum - rightNum) < 1e-9;
                else if (op == "!=") return std::abs(leftNum - rightNum) >= 1e-9;
                else if (op == ">") return leftNum > rightNum;
                else if (op == "<") return leftNum < rightNum;
                else if (op == ">=") return leftNum >= rightNum;
                else if (op == "<=") return leftNum <= rightNum;
            } catch (...) {
                // Fall back to string comparison
                if (op == "=") return leftValue == rightValue;
                else if (op == "!=") return leftValue != rightValue;
                else if (op == ">") return leftValue > rightValue;
                else if (op == "<") return leftValue < rightValue;
                else if (op == ">=") return leftValue >= rightValue;
                else if (op == "<=") return leftValue <= rightValue;
            }
        } else {
            // Generic case - evaluate boolean expressions
            bool leftResult = left->evaluate(row, columns);
            bool rightResult = right->evaluate(row, columns);
            
            if (op == "=") return leftResult == rightResult;
            else if (op == "!=") return leftResult != rightResult;
            else return false; // Other operators don't make sense for boolean values
        }
        
        return false;
    }
    
private:
    std::unique_ptr<ConditionExpression> left;
    std::string op;
    std::unique_ptr<ConditionExpression> right;
};

class AndExpression : public ConditionExpression {
public:
    AndExpression(std::unique_ptr<ConditionExpression> left, std::unique_ptr<ConditionExpression> right)
        : left(std::move(left)), right(std::move(right)) {}

    bool evaluate(const std::vector<std::string>& row,
                  const std::vector<std::string>& columns) const override {
        return left->evaluate(row, columns) && right->evaluate(row, columns);
    }
    
private:
    std::unique_ptr<ConditionExpression> left;
    std::unique_ptr<ConditionExpression> right;
};

class OrExpression : public ConditionExpression {
public:
    OrExpression(std::unique_ptr<ConditionExpression> left, std::unique_ptr<ConditionExpression> right)
        : left(std::move(left)), right(std::move(right)) {}

    bool evaluate(const std::vector<std::string>& row,
                  const std::vector<std::string>& columns) const override {
        return left->evaluate(row, columns) || right->evaluate(row, columns);
    }
    
private:
    std::unique_ptr<ConditionExpression> left;
    std::unique_ptr<ConditionExpression> right;
};

class NotExpression : public ConditionExpression {
public:
    explicit NotExpression(std::unique_ptr<ConditionExpression> expr)
        : expr(std::move(expr)) {}

    bool evaluate(const std::vector<std::string>& row,
                  const std::vector<std::string>& columns) const override {
        return !expr->evaluate(row, columns);
    }
    
private:
    std::unique_ptr<ConditionExpression> expr;
};

class IsNullExpression : public ConditionExpression {
public:
    IsNullExpression(std::unique_ptr<ConditionExpression> expr, bool isNull)
        : expr(std::move(expr)), isNull(isNull) {}

    bool evaluate(const std::vector<std::string>& row,
                  const std::vector<std::string>& columns) const override {
        // Only makes sense for column expressions
        auto colExpr = dynamic_cast<ColumnExpression*>(expr.get());
        if (colExpr) {
            std::string value = colExpr->getStringValue(row, columns);
            return (value.empty()) == isNull;
        }
        return false;
    }
    
private:
    std::unique_ptr<ConditionExpression> expr;
    bool isNull; // true for IS NULL, false for IS NOT NULL
};

class BetweenExpression : public ConditionExpression {
public:
    BetweenExpression(std::unique_ptr<ConditionExpression> expr,
                     std::unique_ptr<ConditionExpression> lower,
                     std::unique_ptr<ConditionExpression> upper,
                     bool notBetween)
        : expr(std::move(expr)), lower(std::move(lower)), 
          upper(std::move(upper)), notBetween(notBetween) {}

    bool evaluate(const std::vector<std::string>& row,
                  const std::vector<std::string>& columns) const override {
        // Handles Column BETWEEN Literal AND Literal
        auto colExpr = dynamic_cast<ColumnExpression*>(expr.get());
        auto lowerLit = dynamic_cast<LiteralExpression*>(lower.get());
        auto upperLit = dynamic_cast<LiteralExpression*>(upper.get());
        
        if (colExpr && lowerLit && upperLit) {
            // Get column value
            std::string colValue = colExpr->getStringValue(row, columns);
            std::string lowerValue = lowerLit->getStringValue();
            std::string upperValue = upperLit->getStringValue();
            
            // Try numeric comparison first
            try {
                double colNum = std::stod(colValue);
                double lowerNum = std::stod(lowerValue);
                double upperNum = std::stod(upperValue);
                
                bool betweenResult = (colNum >= lowerNum && colNum <= upperNum);
                return notBetween ? !betweenResult : betweenResult;
            } catch (...) {
                // Fall back to string comparison
                bool betweenResult = (colValue >= lowerValue && colValue <= upperValue);
                return notBetween ? !betweenResult : betweenResult;
            }
        }
        
        return false;
    }
    
private:
    std::unique_ptr<ConditionExpression> expr;
    std::unique_ptr<ConditionExpression> lower;
    std::unique_ptr<ConditionExpression> upper;
    bool notBetween;
};

class InExpression : public ConditionExpression {
public:
    InExpression(std::unique_ptr<ConditionExpression> expr,
               std::vector<std::unique_ptr<ConditionExpression>> valueList,
               bool notIn)
        : expr(std::move(expr)), valueList(std::move(valueList)), notIn(notIn) {}

    bool evaluate(const std::vector<std::string>& row,
                  const std::vector<std::string>& columns) const override {
        // Only makes sense for column expressions
        auto colExpr = dynamic_cast<ColumnExpression*>(expr.get());
        if (colExpr) {
            std::string colValue = colExpr->getStringValue(row, columns);
            
            // Check if the column value is in the list
            for (const auto& valueExpr : valueList) {
                auto litExpr = dynamic_cast<LiteralExpression*>(valueExpr.get());
                if (litExpr && litExpr->getStringValue() == colValue) {
                    return !notIn; // Found a match
                }
            }
            
            return notIn; // No match found
        }
        
        return false;
    }
    
private:
    std::unique_ptr<ConditionExpression> expr;
    std::vector<std::unique_ptr<ConditionExpression>> valueList;
    bool notIn;
};

// --- ConditionParser Implementation ---
ConditionParser::ConditionParser(const std::string& condition) : current(0) {
    tokenize(condition);
}

void ConditionParser::tokenize(const std::string& condition) {
    std::string buffer;
    bool inQuotes = false;
    bool escapeNext = false;
    
    for (size_t i = 0; i < condition.size(); ++i) {
        char ch = condition[i];
        
        if (escapeNext) {
            buffer.push_back(ch);
            escapeNext = false;
            continue;
        }
        
        if (ch == '\\') {
            escapeNext = true;
            continue;
        }
        
        if (ch == '\'') {
            inQuotes = !inQuotes;
            buffer.push_back(ch);
        } else if (inQuotes) {
            buffer.push_back(ch);
        } else if (std::isspace(ch)) {
            if (!buffer.empty()) {
                tokens.push_back(buffer);
                buffer.clear();
            }
        } else if (ch == '(' || ch == ')' || ch == ',') {
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
    
    if (!buffer.empty()) {
        tokens.push_back(buffer);
    }
    
    // Process special tokens like IS NULL, BETWEEN, IN, etc.
    processSpecialTokens();
}

void ConditionParser::processSpecialTokens() {
    std::vector<std::string> processedTokens;
    
    for (size_t i = 0; i < tokens.size(); ++i) {
        std::string token = tokens[i];
        std::string upperToken = toUpperCase(token);
        
        if (i + 2 < tokens.size() && 
            upperToken == "IS" && 
            toUpperCase(tokens[i + 1]) == "NOT" && 
            toUpperCase(tokens[i + 2]) == "NULL") {
            processedTokens.push_back("IS NOT NULL");
            i += 2; // Skip the next two tokens
        } else if (i + 1 < tokens.size() && 
                  upperToken == "IS" && 
                  toUpperCase(tokens[i + 1]) == "NULL") {
            processedTokens.push_back("IS NULL");
            i += 1; // Skip the next token
        } else if (i + 1 < tokens.size() && 
                  upperToken == "NOT" && 
                  toUpperCase(tokens[i + 1]) == "BETWEEN") {
            processedTokens.push_back("NOT BETWEEN");
            i += 1; // Skip the next token
        } else if (i + 1 < tokens.size() && 
                  upperToken == "NOT" && 
                  toUpperCase(tokens[i + 1]) == "IN") {
            processedTokens.push_back("NOT IN");
            i += 1; // Skip the next token
        } else if (i + 1 < tokens.size() && 
                  upperToken == "NOT" && 
                  toUpperCase(tokens[i + 1]) == "LIKE") {
            processedTokens.push_back("NOT LIKE");
            i += 1; // Skip the next token
        } else {
            processedTokens.push_back(token);
        }
    }
    
    tokens = processedTokens;
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

bool ConditionParser::matchToken(const std::string& token) {
    if (toUpperCase(peek()) == toUpperCase(token)) {
        current++;
        return true;
    }
    return false;
}

ConditionExprPtr ConditionParser::parse() {
    if (tokens.empty()) {
        return std::make_unique<LiteralExpression>("TRUE");
    }
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
    // factor -> NOT factor | '(' expr ')' | predicate
    if (toUpperCase(peek()) == "NOT") {
        getNext(); // consume "NOT"
        auto expr = parseFactor();
        return std::make_unique<NotExpression>(std::move(expr));
    }
    
    if (matchToken("(")) {
        auto expr = parseExpression();
        if (!matchToken(")"))
            throw std::runtime_error("Missing closing parenthesis");
        return expr;
    }
    
    return parsePredicate();
}

ConditionExprPtr ConditionParser::parsePredicate() {
    // Handle various predicate types:
    // 1. value op value (comparison)
    // 2. value IS [NOT] NULL
    // 3. value [NOT] BETWEEN value AND value
    // 4. value [NOT] IN (value, value, ...)
    // 5. value [NOT] LIKE pattern
    
    // Get the left side of the predicate
    std::string identifier = getNext();
    ConditionExprPtr leftExpr;
    
    // Check if it's a literal or column reference
    if (identifier.size() >= 2 && identifier.front() == '\'' && identifier.back() == '\'') {
        // String literal: 'value'
        leftExpr = std::make_unique<LiteralExpression>(identifier.substr(1, identifier.size() - 2));
    } else if (std::isdigit(identifier[0]) || 
              (identifier.size() > 1 && identifier[0] == '-' && std::isdigit(identifier[1]))) {
        // Numeric literal: 123, -456
        leftExpr = std::make_unique<LiteralExpression>(identifier);
    } else {
        // Column reference
        leftExpr = std::make_unique<ColumnExpression>(identifier);
    }
    
    // Check for different predicate types based on the next token
    std::string op = toUpperCase(peek());
    
    if (op == "IS NULL" || op == "IS NOT NULL") {
        getNext(); // consume IS [NOT] NULL
        return std::make_unique<IsNullExpression>(
            std::move(leftExpr), op == "IS NULL");
    } else if (op == "BETWEEN" || op == "NOT BETWEEN") {
        getNext(); // consume [NOT] BETWEEN
        auto lowerExpr = parseSimpleValue();
        
        if (!matchToken("AND"))
            throw std::runtime_error("Missing AND in BETWEEN predicate");
        
        auto upperExpr = parseSimpleValue();
        
        return std::make_unique<BetweenExpression>(
            std::move(leftExpr), std::move(lowerExpr), std::move(upperExpr),
            op == "NOT BETWEEN");
    } else if (op == "IN" || op == "NOT IN") {
        getNext(); // consume [NOT] IN
        
        if (!matchToken("("))
            throw std::runtime_error("Missing opening parenthesis in IN predicate");
        
        std::vector<ConditionExprPtr> valueList;
        do {
            valueList.push_back(parseSimpleValue());
        } while (matchToken(","));
        
        if (!matchToken(")"))
            throw std::runtime_error("Missing closing parenthesis in IN predicate");
        
        return std::make_unique<InExpression>(
            std::move(leftExpr), std::move(valueList), op == "NOT IN");
    } else if (op == "LIKE" || op == "NOT LIKE") {
        getNext(); // consume [NOT] LIKE
        
        // Parse the pattern
        auto patternExpr = parseSimpleValue();
        
        // Create a specialized LIKE comparison
        return std::make_unique<ComparisonExpression>(
            std::move(leftExpr), "LIKE", std::move(patternExpr));
    } else if (op == "=" || op == "!=" ||
              op == ">" || op == "<" ||
              op == ">=" || op == "<=") {
        getNext(); // consume operator
        
        // Parse the right side of the comparison
        auto rightExpr = parseSimpleValue();
        
        // Create a comparison expression
        return std::make_unique<ComparisonExpression>(
            std::move(leftExpr), op, std::move(rightExpr));
    } else {
        // Just a value (like a boolean test)
        return leftExpr;
    }
}

ConditionExprPtr ConditionParser::parseSimpleValue() {
    std::string value = getNext();
    
    // Check if it's a literal or column reference
    if (value.size() >= 2 && value.front() == '\'' && value.back() == '\'') {
        // String literal: 'value'
        return std::make_unique<LiteralExpression>(value.substr(1, value.size() - 2));
    } else if (std::isdigit(value[0]) || 
              (value.size() > 1 && value[0] == '-' && std::isdigit(value[1]))) {
        // Numeric literal: 123, -456
        return std::make_unique<LiteralExpression>(value);
    } else {
        // Column reference
        return std::make_unique<ColumnExpression>(value);
    }
}