#ifndef UTILS_H
#define UTILS_H

#include <string>
#include <algorithm>
#include <cctype>
#include <iterator>
#include <functional>
#include <unordered_map>

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

// Validates the data type.
// Supported types: INT, VARCHAR, TEXT, FLOAT, BOOLEAN
inline bool isValidDataType(const std::string& type) {
    static const std::string validTypes[] = {"INT", "VARCHAR", "TEXT", "FLOAT", "BOOLEAN"};
    std::string upperType = toUpperCase(type);
    for (const auto& vt : validTypes) {
        if (upperType == vt)
            return true;
    }
    return false;
}

#endif // UTILS_H