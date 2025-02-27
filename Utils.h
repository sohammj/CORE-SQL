#ifndef UTILS_H
#define UTILS_H

#include <string>
#include <sstream>   // Added for std::istringstream.
#include <vector>    // Added for std::vector.
#include <algorithm>
#include <cctype>
#include <iterator>
#include <functional>

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
        tokens.push_back(token);
    }
    return tokens;
}

// Validates the data type.
// Supported types: INT, VARCHAR, TEXT, FLOAT, BOOLEAN
inline bool isValidDataType(const std::string& type) {
    // Using toUpperCase to ensure case-insensitive matching.
    static const std::string validTypes[] = {"INT", "VARCHAR", "TEXT", "FLOAT", "BOOLEAN"};
    std::string upperType = toUpperCase(type);
    for (const auto& vt : validTypes) {
        if (upperType == vt)
            return true;
    }
    return false;
}

#endif // UTILS_H
