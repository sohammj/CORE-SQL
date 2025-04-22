#ifndef UTILS_H
#define UTILS_H

#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <cctype>
#include <iterator>
#include <functional>
#include <regex>
#include <unordered_map>
#include <memory>

// Converts a string to uppercase.
inline std::string toUpperCase(const std::string& str) {
    std::string upperStr = str;
    std::transform(upperStr.begin(), upperStr.end(), upperStr.begin(), ::toupper);
    return upperStr;
}

// Converts a string to lowercase.
inline std::string toLowerCase(const std::string& str) {
    std::string lowerStr = str;
    std::transform(lowerStr.begin(), lowerStr.end(), lowerStr.begin(), ::tolower);
    return lowerStr;
}

// Trims whitespace from both ends of a string.
inline std::string trim(const std::string& str) {
    std::string result = str;
    result.erase(result.begin(), std::find_if(result.begin(), result.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    result.erase(std::find_if(result.rbegin(), result.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), result.end());
    return result;
}

// Splits a string by the given delimiter.
static std::vector<std::string> split(const std::string &s, char delimiter) {
    std::vector<std::string> tokens;
    std::istringstream tokenStream(s);
    std::string token;
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(trim(token));
    }
    return tokens;
}

// Match a string against a SQL LIKE pattern
inline bool matchLikePattern(const std::string& str, const std::string& pattern) {
    std::string regexPattern;
    for (char c : pattern) {
        if (c == '%') {
            regexPattern += ".*";
        } else if (c == '_') {
            regexPattern += ".";
        } else if (c == '\\' || c == '.' || c == '^' || c == '$' || c == '|' || 
                 c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}' || c == '+' || c == '?') {
            regexPattern += "\\";
            regexPattern += c;
        } else {
            regexPattern += c;
        }
    }
    std::regex regex(regexPattern, std::regex::icase);
    return std::regex_match(str, regex);
}

// SQL Data Types
enum class DataType {
    CHAR,
    VARCHAR,
    TEXT,
    INT,
    SMALLINT,
    NUMERIC,
    REAL,
    DOUBLE_PRECISION,
    FLOAT,
    DATE,
    TIME,
    TIMESTAMP,
    BOOLEAN,
    BRANCH,     // Domain types
    CUSTOMER,
    LOAN,
    BORROWER,
    ACCOUNT,
    DEPOSITOR,
    USER_DEFINED,
    UNKNOWN
};

// Validates the data type.
inline DataType getDataType(const std::string& type) {
    // Using toUpperCase to ensure case-insensitive matching.
    std::string upperType = toUpperCase(type);
    
    if (upperType == "CHAR" || upperType.find("CHAR(") == 0)
        return DataType::CHAR;
    if (upperType == "VARCHAR" || upperType.find("VARCHAR(") == 0)
        return DataType::VARCHAR;
    if (upperType == "TEXT")
        return DataType::TEXT;
    if (upperType == "INT")
        return DataType::INT;
    if (upperType == "SMALLINT")
        return DataType::SMALLINT;
    if (upperType == "NUMERIC" || upperType.find("NUMERIC(") == 0)
        return DataType::NUMERIC;
    if (upperType == "REAL")
        return DataType::REAL;
    if (upperType == "DOUBLE PRECISION")
        return DataType::DOUBLE_PRECISION;
    if (upperType == "FLOAT" || upperType.find("FLOAT(") == 0)
        return DataType::FLOAT;
    if (upperType == "DATE")
        return DataType::DATE;
    if (upperType == "TIME")
        return DataType::TIME;
    if (upperType == "TIMESTAMP")
        return DataType::TIMESTAMP;
    if (upperType == "BOOLEAN")
        return DataType::BOOLEAN;
    if (upperType == "BRANCH")
        return DataType::BRANCH;
    if (upperType == "CUSTOMER")
        return DataType::CUSTOMER;
    if (upperType == "LOAN")
        return DataType::LOAN;
    if (upperType == "BORROWER")
        return DataType::BORROWER;
    if (upperType == "ACCOUNT")
        return DataType::ACCOUNT;
    if (upperType == "DEPOSITOR")
        return DataType::DEPOSITOR;
    
    return DataType::UNKNOWN;
}

inline bool isValidDataType(const std::string& type) {
    return getDataType(type) != DataType::UNKNOWN;
}

// Extract parameter part from type definition like CHAR(10) or NUMERIC(10,2)
inline std::pair<int, int> extractTypeParameters(const std::string& type) {
    std::pair<int, int> result = {0, 0};
    std::regex paramRegex("\\((\\d+)(,(\\d+))?\\)");
    std::smatch match;
    
    if (std::regex_search(type, match, paramRegex) && match.size() > 1) {
        result.first = std::stoi(match[1].str());
        if (match.size() > 3 && match[3].matched) {
            result.second = std::stoi(match[3].str());
        }
    }
    
    return result;
}

// Custom exceptions
class DatabaseException : public std::runtime_error {
public:
    DatabaseException(const std::string& message) : std::runtime_error(message) {}
};

class ConstraintViolationException : public DatabaseException {
public:
    ConstraintViolationException(const std::string& message) : DatabaseException(message) {}
};

class DataTypeException : public DatabaseException {
public:
    DataTypeException(const std::string& message) : DatabaseException(message) {}
};

class ReferentialIntegrityException : public ConstraintViolationException {
public:
    ReferentialIntegrityException(const std::string& message) : ConstraintViolationException(message) {}
};

class TransactionException : public DatabaseException {
public:
    TransactionException(const std::string& message) : DatabaseException(message) {}
};

class ConcurrencyException : public DatabaseException {
public:
    ConcurrencyException(const std::string& message) : DatabaseException(message) {}
};

class AuthorizationException : public DatabaseException {
public:
    AuthorizationException(const std::string& message) : DatabaseException(message) {}
};

// User-defined type management
struct UserDefinedType {
    std::string name;
    std::vector<std::pair<std::string, std::string>> attributes;
};

// Global registry for user-defined types
class UserTypeRegistry {
    private:
        static std::unordered_map<std::string, UserDefinedType> types;
    
    public:
        static void registerType(const UserDefinedType& type) {
            types[toLowerCase(type.name)] = type;
        }
        
        static bool typeExists(const std::string& typeName) {
            return types.find(toLowerCase(typeName)) != types.end();
        }
        
        static const UserDefinedType& getType(const std::string& typeName) {
            auto it = types.find(toLowerCase(typeName));
            if (it == types.end()) {
                throw DatabaseException("User-defined type '" + typeName + "' does not exist");
            }
            return it->second;
        }
        
        static void removeType(const std::string& typeName) {
            types.erase(toLowerCase(typeName));
        }
    };


#endif // UTILS_H