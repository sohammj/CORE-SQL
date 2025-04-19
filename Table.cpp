#include "Table.h"
#include "Utils.h"
#include "ConditionParser.h"
#include "Aggregation.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <unordered_map>
#include <stdexcept>
#include <regex>
#include "Database.h"

// Table Class Implementation
// -------------------------

Table::Table(const std::string& name) : 
    tableName(name), 
    nextRowId(1) 
    {
        std::cout << "Debug: Table constructor called for " << name << std::endl;
        std::cout << std::flush;
    }

// Set Operations
// --------------

std::vector<std::vector<std::string>> Table::setUnion(const std::vector<std::vector<std::string>>& otherResult) {
    std::shared_lock<std::shared_mutex> lock(mutex);
    
    std::vector<std::vector<std::string>> result = rows;
    
    for (const auto& row : otherResult) {
        if (std::find(result.begin(), result.end(), row) == result.end()) {
            result.push_back(row);
        }
    }
    
    return result;
}

std::vector<std::vector<std::string>> Table::setIntersect(const std::vector<std::vector<std::string>>& otherResult) {
    std::shared_lock<std::shared_mutex> lock(mutex);
    
    std::vector<std::vector<std::string>> result;
    
    for (const auto& row : rows) {
        if (std::find(otherResult.begin(), otherResult.end(), row) != otherResult.end()) {
            result.push_back(row);
        }
    }
    
    return result;
}

std::vector<std::vector<std::string>> Table::setExcept(const std::vector<std::vector<std::string>>& otherResult) {
    std::shared_lock<std::shared_mutex> lock(mutex);
    
    std::vector<std::vector<std::string>> result;
    
    for (const auto& row : rows) {
        if (std::find(otherResult.begin(), otherResult.end(), row) == otherResult.end()) {
            result.push_back(row);
        }
    }
    
    return result;
}
// Add these near the top of table.cpp with other helper functions

// This function extracts an aggregation function and column from an expression like "AVG(salary)"
std::pair<std::string, std::string> extractAggregateFunction(const std::string& expr) {
    std::regex aggPattern(R"((\w+)\s*\(\s*([^)]+)\s*\))");
    std::smatch match;
    
    if (std::regex_search(expr, match, aggPattern) && match.size() > 2) {
        return {toUpperCase(match[1].str()), trim(match[2].str())};
    }
    
    return {"", ""};
}

// Add this helper function to handle aggregate calculations
std::string Table::applyAggregateFunction(const std::string& function, const std::vector<std::string>& values) {
    if (values.empty()) {
        return "NULL";
    }
    
    if (function == "AVG") {
        return std::to_string(Aggregation::computeMean(values));
    } else if (function == "MIN") {
        return std::to_string(Aggregation::computeMin(values));
    } else if (function == "MAX") {
        return std::to_string(Aggregation::computeMax(values));
    } else if (function == "SUM") {
        return std::to_string(Aggregation::computeSum(values));
    } else if (function == "COUNT") {
        return std::to_string(Aggregation::computeCount(values));
    } else if (function == "MEDIAN") {
        return Aggregation::computeMedian(values);
    } else if (function == "MODE") {
        return Aggregation::computeMode(values);
    } else if (function == "STDDEV" || function == "STDDEV_POP") {
        return Aggregation::computeStdDev(values, true);
    } else if (function == "STDDEV_SAMP") {
        return Aggregation::computeStdDev(values, false);
    } else if (function == "VAR" || function == "VARIANCE" || function == "VAR_POP") {
        return Aggregation::computeVariance(values, true);
    } else if (function == "VAR_SAMP") {
        return Aggregation::computeVariance(values, false);
    } else if (function.find("PERCENTILE_") == 0) {
        // Handle PERCENTILE_XX functions like PERCENTILE_50 (median)
        try {
            double percentile = std::stod(function.substr(11));
            return std::to_string(Aggregation::computePercentile(values, percentile));
        } catch (...) {
            return "NULL";
        }
    } else if (function.find("STRING_AGG") == 0 || function == "GROUP_CONCAT") {
        return Aggregation::computeStringConcat(values, ",");
    }
    
    return "NULL"; // Unsupported function
}
// Transaction Support
// -------------------

void Table::lockShared() {
    mutex.lock_shared();
}

void Table::lockExclusive() {
    mutex.lock();
}

void Table::unlock() {
    mutex.unlock();
}

// Utility Functions
// -----------------

void Table::sortRows(const std::string& columnName, bool ascending) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    auto it = std::find(columns.begin(), columns.end(), columnName);
    if (it == columns.end()) {
        throw DatabaseException("Column '" + columnName + "' does not exist");
    }
    
    int idx = std::distance(columns.begin(), it);
    std::sort(rows.begin(), rows.end(), 
        [idx, ascending](const auto& a, const auto& b) {
            try {
                double aVal = std::stod(a[idx]);
                double bVal = std::stod(b[idx]);
                return ascending ? (aVal < bVal) : (aVal > bVal);
            } catch (...) {
                return ascending ? (a[idx] < b[idx]) : (a[idx] > b[idx]);
            }
        });
}

int Table::getRowCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return rows.size();
}

bool Table::hasColumn(const std::string& columnName) const {
    std::cout << "Debug: Checking if column '" << columnName << "' exists" << std::endl;
    return std::find(columns.begin(), columns.end(), columnName) != columns.end();
}

int Table::getColumnIndex(const std::string& columnName) const {
    std::shared_lock<std::shared_mutex> lock(mutex);
    auto it = std::find(columns.begin(), columns.end(), columnName);
    return (it != columns.end()) ? std::distance(columns.begin(), it) : -1;
}

// Join Operations
// ---------------

// In Table.cpp, improve the innerJoin method
std::vector<std::vector<std::string>> Table::innerJoin(
    Table& rightTable,
    const std::string& condition,
    const std::vector<std::string>& selectColumns
) {
    std::shared_lock<std::shared_mutex> lockLeft(mutex);
    std::shared_lock<std::shared_mutex> lockRight(rightTable.mutex);
    
    // Create combined columns for the join result
    std::vector<std::string> joinedColumns;
    for (const auto& col : columns) {
        joinedColumns.push_back(col);
    }
    for (const auto& col : rightTable.columns) {
        joinedColumns.push_back(col);
    }
    
    // Parse join condition
    size_t eqPos = condition.find('=');
    if (eqPos == std::string::npos) {
        throw DatabaseException("Invalid join condition format");
    }
    
    std::string leftCol = trim(condition.substr(0, eqPos));
    std::string rightCol = trim(condition.substr(eqPos + 1));
    
    // Handle table aliases (e.g., "s.id" instead of just "id")
    size_t leftDot = leftCol.find('.');
    size_t rightDot = rightCol.find('.');
    
    std::string leftColName = (leftDot != std::string::npos) ? leftCol.substr(leftDot + 1) : leftCol;
    std::string rightColName = (rightDot != std::string::npos) ? rightCol.substr(rightDot + 1) : rightCol;
    
    // Find column indices
    int leftIdx = getColumnIndex(leftColName);
    int rightIdx = rightTable.getColumnIndex(rightColName);
    
    if (leftIdx == -1 || rightIdx == -1) {
        throw DatabaseException("Join columns not found: " + leftColName + " or " + rightColName);
    }
    
    std::vector<std::vector<std::string>> result;
    
    // Perform the join
    for (const auto& leftRow : rows) {
        for (const auto& rightRow : rightTable.rows) {
            if (leftRow[leftIdx] == rightRow[rightIdx]) {
                std::vector<std::string> joinedRow;
                
                // Combine the rows
                joinedRow.insert(joinedRow.end(), leftRow.begin(), leftRow.end());
                joinedRow.insert(joinedRow.end(), rightRow.begin(), rightRow.end());
                
                // Project only the requested columns
                std::vector<std::string> resultRow;
                for (const auto& col : selectColumns) {
                    // Handle column references with aliases
                    std::string colName = col;
                    size_t dotPos = col.find('.');
                    if (dotPos != std::string::npos) {
                        colName = col.substr(dotPos + 1);
                    }
                    
                    int idx = -1;
                    // Check in left table first
                    int leftColIdx = getColumnIndex(colName);
                    if (leftColIdx != -1) {
                        resultRow.push_back(leftRow[leftColIdx]);
                    } else {
                        // Try in right table
                        int rightColIdx = rightTable.getColumnIndex(colName);
                        if (rightColIdx != -1) {
                            resultRow.push_back(rightRow[rightColIdx]);
                        } else {
                            resultRow.push_back(""); // Column not found
                        }
                    }
                }
                
                result.push_back(resultRow);
            }
        }
    }
    
    return result;
}

std::vector<std::vector<std::string>> Table::leftOuterJoin(
    Table& rightTable,
    const std::string& condition,
    const std::vector<std::string>& selectColumns
) {
    std::shared_lock<std::shared_mutex> lockLeft(mutex);
    std::shared_lock<std::shared_mutex> lockRight(rightTable.mutex);
    
    // Parse join condition to get column names
    size_t eqPos = condition.find('=');
    if (eqPos == std::string::npos) {
        throw DatabaseException("Invalid join condition format");
    }
    
    std::string leftExpr = trim(condition.substr(0, eqPos));
    std::string rightExpr = trim(condition.substr(eqPos + 1));
    
    // Extract column names (handle table aliases)
    std::string leftColName = leftExpr;
    std::string rightColName = rightExpr;
    
    size_t leftDot = leftExpr.find('.');
    if (leftDot != std::string::npos) {
        leftColName = leftExpr.substr(leftDot + 1);
    }
    
    size_t rightDot = rightExpr.find('.');
    if (rightDot != std::string::npos) {
        rightColName = rightExpr.substr(rightDot + 1);
    }
    
    // Get column indices
    int leftIdx = getColumnIndex(leftColName);
    int rightIdx = rightTable.getColumnIndex(rightColName);
    
    if (leftIdx == -1 || rightIdx == -1) {
        throw DatabaseException("Join columns not found: " + leftColName + " or " + rightColName);
    }
    
    std::vector<std::vector<std::string>> result;
    std::vector<bool> rightRowMatched(rightTable.rows.size(), false);
    
    // For each row in the left table
    for (const auto& leftRow : rows) {
        bool foundMatch = false;
        
        // Try to find matches in the right table
        for (size_t r = 0; r < rightTable.rows.size(); ++r) {
            const auto& rightRow = rightTable.rows[r];
            
            if (leftRow[leftIdx] == rightRow[rightIdx]) {
                foundMatch = true;
                
                // Create combined row for this match
                std::vector<std::string> resultRow;
                for (const auto& col : selectColumns) {
                    // Handle column references with table aliases
                    std::string colName = col;
                    std::string tablePrefix = "";
                    
                    size_t dotPos = col.find('.');
                    if (dotPos != std::string::npos) {
                        tablePrefix = col.substr(0, dotPos);
                        colName = col.substr(dotPos + 1);
                    }
                    
                    // Try to find in left table first if no prefix or matching prefix
                    if (tablePrefix.empty() || toLowerCase(tablePrefix) == "c") {  // assume "c" is left table alias
                        int idx = getColumnIndex(colName);
                        if (idx != -1 && idx < leftRow.size()) {
                            resultRow.push_back(leftRow[idx]);
                            continue;
                        }
                    }
                    
                    // Try to find in right table if not found in left
                    if (tablePrefix.empty() || toLowerCase(tablePrefix) == "o") {  // assume "o" is right table alias
                        int idx = rightTable.getColumnIndex(colName);
                        if (idx != -1 && idx < rightRow.size()) {
                            resultRow.push_back(rightRow[idx]);
                            continue;
                        }
                    }
                    
                    // Column not found in either table
                    resultRow.push_back("");
                }
                
                result.push_back(resultRow);
            }
        }
        
        // If no match was found, add left row with NULLs for right columns
        if (!foundMatch) {
            std::vector<std::string> resultRow;
            for (const auto& col : selectColumns) {
                std::string colName = col;
                std::string tablePrefix = "";
                
                size_t dotPos = col.find('.');
                if (dotPos != std::string::npos) {
                    tablePrefix = col.substr(0, dotPos);
                    colName = col.substr(dotPos + 1);
                }
                
                // Include left table column
                if (tablePrefix.empty() || toLowerCase(tablePrefix) == "c") {
                    int idx = getColumnIndex(colName);
                    if (idx != -1 && idx < leftRow.size()) {
                        resultRow.push_back(leftRow[idx]);
                        continue;
                    }
                }
                
                // Add empty string for right table column
                resultRow.push_back("");
            }
            
            result.push_back(resultRow);
        }
    }
    
    return result;
}



std::vector<std::vector<std::string>> Table::rightOuterJoin(
    Table& rightTable,
    const std::string& condition,
    const std::vector<std::string>& selectColumns
) {
    // We need to reverse the condition for the swapped tables
    size_t eqPos = condition.find('=');
    if (eqPos == std::string::npos) {
        throw DatabaseException("Invalid join condition format");
    }
    
    std::string leftExpr = trim(condition.substr(0, eqPos));
    std::string rightExpr = trim(condition.substr(eqPos + 1));
    
    // Swap the expressions for the reversed join
    std::string reversedCondition = rightExpr + " = " + leftExpr;
    
    // Use the reversed condition when calling leftOuterJoin
    return rightTable.leftOuterJoin(*this, reversedCondition, selectColumns);
}

std::vector<std::vector<std::string>> Table::naturalJoin(
    Table& rightTable,
    const std::vector<std::string>& selectColumns
) {
    std::shared_lock<std::shared_mutex> lockLeft(mutex);
    std::shared_lock<std::shared_mutex> lockRight(rightTable.mutex);
    
    // Find common column names between the tables
    std::vector<std::string> commonColumns;
    std::vector<int> leftIndices;
    std::vector<int> rightIndices;
    
    for (size_t i = 0; i < columns.size(); i++) {
        const std::string& leftCol = columns[i];
        for (size_t j = 0; j < rightTable.columns.size(); j++) {
            const std::string& rightCol = rightTable.columns[j];
            if (toLowerCase(leftCol) == toLowerCase(rightCol)) {
                commonColumns.push_back(leftCol);
                leftIndices.push_back(i);
                rightIndices.push_back(j);
            }
        }
    }
    
    if (commonColumns.empty()) {
        // No common columns, do a cross join
        std::vector<std::vector<std::string>> result;
        for (const auto& leftRow : rows) {
            for (const auto& rightRow : rightTable.rows) {
                std::vector<std::string> combinedRow;
                
                // Add columns from left row
                for (size_t i = 0; i < leftRow.size(); i++) {
                    combinedRow.push_back(leftRow[i]);
                }
                
                // Add columns from right row
                for (size_t i = 0; i < rightRow.size(); i++) {
                    combinedRow.push_back(rightRow[i]);
                }
                
                // Project to select columns
                std::vector<std::string> resultRow;
                for (const auto& col : selectColumns) {
                    // Handle column references with aliases
                    std::string colName = col;
                    std::string tableAlias = "";
                    
                    size_t dotPos = col.find('.');
                    if (dotPos != std::string::npos) {
                        tableAlias = col.substr(0, dotPos);
                        colName = col.substr(dotPos + 1);
                    }
                    
                    // Try to find in combined row
                    int idx = -1;
                    
                    // First look in left table
                    for (size_t i = 0; i < columns.size(); i++) {
                        if (toLowerCase(columns[i]) == toLowerCase(colName)) {
                            idx = i;
                            break;
                        }
                    }
                    
                    if (idx != -1 && idx < leftRow.size()) {
                        resultRow.push_back(leftRow[idx]);
                    } else {
                        // Look in right table
                        for (size_t i = 0; i < rightTable.columns.size(); i++) {
                            if (toLowerCase(rightTable.columns[i]) == toLowerCase(colName)) {
                                idx = i;
                                break;
                            }
                        }
                        
                        if (idx != -1 && idx < rightRow.size()) {
                            resultRow.push_back(rightRow[idx]);
                        } else {
                            resultRow.push_back("");
                        }
                    }
                }
                
                result.push_back(resultRow);
            }
        }
        
        return result;
    }
    
    // Do join on common columns
    std::vector<std::vector<std::string>> result;
    for (const auto& leftRow : rows) {
        for (const auto& rightRow : rightTable.rows) {
            bool match = true;
            
            // Check if all common columns match
            for (size_t i = 0; i < commonColumns.size(); i++) {
                int leftIdx = leftIndices[i];
                int rightIdx = rightIndices[i];
                
                if (leftIdx >= leftRow.size() || rightIdx >= rightRow.size() || 
                    leftRow[leftIdx] != rightRow[rightIdx]) {
                    match = false;
                    break;
                }
            }
            
            if (match) {
                std::vector<std::string> combinedRow;
                
                // Add columns from left row
                for (size_t i = 0; i < leftRow.size(); i++) {
                    combinedRow.push_back(leftRow[i]);
                }
                
                // Add non-duplicate columns from right row
                for (size_t i = 0; i < rightRow.size(); i++) {
                    if (std::find(rightIndices.begin(), rightIndices.end(), i) == rightIndices.end()) {
                        combinedRow.push_back(rightRow[i]);
                    }
                }
                
                // Project to select columns
                std::vector<std::string> resultRow;
                for (const auto& col : selectColumns) {
                    // Handle column references with aliases
                    std::string colName = col;
                    std::string tableAlias = "";
                    
                    size_t dotPos = col.find('.');
                    if (dotPos != std::string::npos) {
                        tableAlias = col.substr(0, dotPos);
                        colName = col.substr(dotPos + 1);
                    }
                    
                    // Try to find in left table first
                    int idx = -1;
                    for (size_t i = 0; i < columns.size(); i++) {
                        if (toLowerCase(columns[i]) == toLowerCase(colName)) {
                            idx = i;
                            break;
                        }
                    }
                    
                    if (idx != -1 && idx < leftRow.size()) {
                        resultRow.push_back(leftRow[idx]);
                    } else {
                        // Try right table
                        for (size_t i = 0; i < rightTable.columns.size(); i++) {
                            if (toLowerCase(rightTable.columns[i]) == toLowerCase(colName)) {
                                idx = i;
                                break;
                            }
                        }
                        
                        if (idx != -1 && idx < rightRow.size()) {
                            resultRow.push_back(rightRow[idx]);
                        } else {
                            resultRow.push_back("");
                        }
                    }
                }
                
                result.push_back(resultRow);
            }
        }
    }
    
    return result;
}
std::vector<std::vector<std::string>> Table::fullOuterJoin(
    Table& rightTable,
    const std::string& condition,
    const std::vector<std::string>& selectColumns
) {
    std::shared_lock<std::shared_mutex> lockLeft(mutex);
    std::shared_lock<std::shared_mutex> lockRight(rightTable.mutex);
    
    // Get left join results first
    std::vector<std::vector<std::string>> leftJoinResults = leftOuterJoin(rightTable, condition, selectColumns);
    
    // Then get right-only results (rows in right table with no match in left table)
    // Parse join condition
    size_t eqPos = condition.find('=');
    if (eqPos == std::string::npos) {
        throw DatabaseException("Invalid join condition format");
    }
    
    std::string leftExpr = trim(condition.substr(0, eqPos));
    std::string rightExpr = trim(condition.substr(eqPos + 1));
    
    // Extract column names (handle table aliases)
    std::string leftColName = leftExpr;
    std::string rightColName = rightExpr;
    
    size_t leftDot = leftExpr.find('.');
    if (leftDot != std::string::npos) {
        leftColName = leftExpr.substr(leftDot + 1);
    }
    
    size_t rightDot = rightExpr.find('.');
    if (rightDot != std::string::npos) {
        rightColName = rightExpr.substr(rightDot + 1);
    }
    
    // Get column indices
    int leftIdx = getColumnIndex(leftColName);
    int rightIdx = rightTable.getColumnIndex(rightColName);
    
    if (leftIdx == -1 || rightIdx == -1) {
        throw DatabaseException("Join columns not found: " + leftColName + " or " + rightColName);
    }
    
    // Track which right rows have matches
    std::vector<bool> rightRowMatched(rightTable.rows.size(), false);
    
    // Mark matched right rows
    for (const auto& leftRow : rows) {
        for (size_t r = 0; r < rightTable.rows.size(); ++r) {
            const auto& rightRow = rightTable.rows[r];
            if (leftRow[leftIdx] == rightRow[rightIdx]) {
                rightRowMatched[r] = true;
            }
        }
    }
    
    // Add unmatched right rows
    for (size_t r = 0; r < rightTable.rows.size(); ++r) {
        if (!rightRowMatched[r]) {
            const auto& rightRow = rightTable.rows[r];
            
            std::vector<std::string> resultRow;
            for (const auto& col : selectColumns) {
                std::string colName = col;
                std::string tablePrefix = "";
                
                size_t dotPos = col.find('.');
                if (dotPos != std::string::npos) {
                    tablePrefix = col.substr(0, dotPos);
                    colName = col.substr(dotPos + 1);
                }
                
                // Try right table first for unmatched right rows
                if (tablePrefix.empty() || toLowerCase(tablePrefix) == "o") {
                    int idx = rightTable.getColumnIndex(colName);
                    if (idx != -1 && idx < rightRow.size()) {
                        resultRow.push_back(rightRow[idx]);
                        continue;
                    }
                }
                
                // Empty for left table columns
                resultRow.push_back("");
            }
            
            leftJoinResults.push_back(resultRow);
        }
    }
    
    return leftJoinResults;
}
// Data Manipulation
// -----------------

void Table::deleteRows(const std::string& condition) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    if (condition.empty()) {
        rows.clear();
        return;
    }
    
    ConditionParser cp(condition);
    auto expr = cp.parse();
    
    rows.erase(
        std::remove_if(rows.begin(), rows.end(), 
            [&](const std::vector<std::string>& row) {
                return expr->evaluate(row, columns);
            }), 
        rows.end()
    );
}

void Table::updateRows(
    const std::vector<std::pair<std::string, std::string>>& updates,
    const std::string& condition
) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    ConditionExprPtr expr = nullptr;
    if (!condition.empty()) {
        ConditionParser cp(condition);
        expr = cp.parse();
    }
    
    for (auto& row : rows) {
        if (!expr || expr->evaluate(row, columns)) {
            bool rowChanged = false;
            std::vector<std::string> newRow = row;
            
            for (const auto& update : updates) {
                auto it = std::find(columns.begin(), columns.end(), update.first);
                if (it != columns.end()) {
                    int idx = std::distance(columns.begin(), it);
                    std::string newValue = update.second;
                    enforceDataType(idx, newValue);
                    
                    if (newRow[idx] != newValue) {
                        newRow[idx] = newValue;
                        rowChanged = true;
                    }
                }
            }
            
            if (rowChanged) {
                validateConstraintsForUpdate(row, newRow);
                row = newRow;
            }
        }
    }
}

void Table::clearRows() {
    std::unique_lock<std::shared_mutex> lock(mutex);
    rows.clear();
}

// Schema Modification
// -------------------

void Table::addColumn(const std::string& columnName, const std::string& type, bool isNotNull) {
    std::cout << "Debug: Adding column " << columnName << " of type " << type << std::endl;
    std::cout << std::flush;
    
    std::unique_lock<std::shared_mutex> lock(mutex);
    std::cout << "Debug: Acquired column mutex lock" << std::endl;
    std::cout << std::flush;
    
    
    if (hasColumn(columnName)) {
        throw DatabaseException("Column '" + columnName + "' already exists");
    }
    
    columns.push_back(columnName);
    columnTypes.push_back(type);
    notNullConstraints.push_back(isNotNull);
    
    for (auto& row : rows) {
        row.push_back("");
    }
}

bool Table::dropColumn(const std::string& columnName) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    auto it = std::find(columns.begin(), columns.end(), columnName);
    if (it == columns.end()) {
        return false;
    }
    
    int idx = std::distance(columns.begin(), it);
    columns.erase(it);
    columnTypes.erase(columnTypes.begin() + idx);
    notNullConstraints.erase(notNullConstraints.begin() + idx);
    
    for (auto& row : rows) {
        if (idx < row.size()) {
            row.erase(row.begin() + idx);
        }
    }
    
    return true;
}

void Table::renameColumn(const std::string& oldName, const std::string& newName) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    auto it = std::find(columns.begin(), columns.end(), oldName);
    if (it == columns.end()) {
        throw DatabaseException("Column '" + oldName + "' does not exist");
    }
    
    if (std::find(columns.begin(), columns.end(), newName) != columns.end()) {
        throw DatabaseException("Column '" + newName + "' already exists");
    }
    
    *it = newName;
}

// Constraint Management
// ---------------------

void Table::addConstraint(const Constraint& constraint) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    for (const auto& col : constraint.columns) {
        if (!hasColumn(col)) {
            throw DatabaseException("Column '" + col + "' does not exist");
        }
    }
    
    for (const auto& existing : constraints) {
        if (existing.name == constraint.name) {
            throw DatabaseException("Constraint '" + constraint.name + "' already exists");
        }
    }
    
    constraints.push_back(constraint);
}

bool Table::dropConstraint(const std::string& constraintName) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    auto it = std::find_if(constraints.begin(), constraints.end(),
        [&](const Constraint& c) { return c.name == constraintName; });
    
    if (it == constraints.end()) {
        return false;
    }
    
    constraints.erase(it);
    return true;
}

// Data Validation
// ---------------

bool Table::validateConstraints(const std::vector<std::string>& row) {
    for (size_t i = 0; i < notNullConstraints.size(); ++i) {
        if (notNullConstraints[i] && (i >= row.size() || row[i].empty())) {
            throw ConstraintViolationException("NOT NULL constraint violated for column '" + columns[i] + "'");
        }
    }
    
    for (const auto& constraint : constraints) {
        switch (constraint.type) {
            case Constraint::Type::PRIMARY_KEY:
            case Constraint::Type::UNIQUE:
                if (!validateUniqueConstraint(constraint, row)) {
                    throw ConstraintViolationException(
                        std::string(constraint.type == Constraint::Type::PRIMARY_KEY ? "PRIMARY KEY" : "UNIQUE") +
                        " constraint '" + constraint.name + "' violated");
                }
                break;
            case Constraint::Type::FOREIGN_KEY:
                if (!validateForeignKeyConstraint(constraint, row)) {
                    throw ReferentialIntegrityException("FOREIGN KEY constraint '" + constraint.name + "' violated");
                }
                break;
            case Constraint::Type::CHECK:
                if (!validateCheckConstraint(constraint, row)) {
                    throw ConstraintViolationException("CHECK constraint '" + constraint.name + "' violated");
                }
                break;
            case Constraint::Type::NOT_NULL:
                break;
        }
    }
    
    return true;
}

// Data Insertion
// --------------

// In Table.cpp, make sure addRow calls validateConstraints before adding the row:
int Table::addRow(const std::vector<std::string>& values) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    if (values.size() != columns.size()) {
        throw DatabaseException("Incorrect number of values for row");
    }
    
    std::vector<std::string> rowValues = values;
    
    // Apply data type enforcement
    for (size_t i = 0; i < rowValues.size(); ++i) {
        enforceDataType(i, rowValues[i]);
    }
    
    // Validate all constraints
    for (const auto& constraint : constraints) {
        switch (constraint.type) {
            case Constraint::Type::PRIMARY_KEY:
            case Constraint::Type::UNIQUE:
                if (!validateUniqueConstraint(constraint, rowValues)) {
                    // This will throw an exception if constraint is violated
                    return -1;
                }
                break;
            case Constraint::Type::FOREIGN_KEY:
                if (!validateForeignKeyConstraint(constraint, rowValues)) {
                    // This will throw an exception if constraint is violated
                    return -1;
                }
                break;
            case Constraint::Type::CHECK:
                if (!validateCheckConstraint(constraint, rowValues)) {
                    throw ConstraintViolationException("CHECK constraint '" + constraint.name + "' violated");
                }
                break;
            case Constraint::Type::NOT_NULL:
                for (const auto& col : constraint.columns) {
                    int idx = getColumnIndex(col);
                    if (idx >= 0 && idx < rowValues.size() && rowValues[idx].empty()) {
                        throw ConstraintViolationException("NOT NULL constraint violated for column '" + col + "'");
                    }
                }
                break;
        }
    }
    
    // Validate not null constraints
    for (size_t i = 0; i < notNullConstraints.size(); ++i) {
        if (notNullConstraints[i] && (i >= rowValues.size() || rowValues[i].empty())) {
            throw ConstraintViolationException("NOT NULL constraint violated for column '" + columns[i] + "'");
        }
    }
    
    rows.push_back(rowValues);
    return nextRowId++;
}

int Table::addRowWithId(int rowId, const std::vector<std::string>& values) {
    if (values.size() != columns.size()) {
        throw DatabaseException("Incorrect number of values for row");
    }
    
    std::vector<std::string> rowValues = values;
    
    for (size_t i = 0; i < rowValues.size(); ++i) {
        enforceDataType(i, rowValues[i]);
    }
    
    validateConstraints(rowValues);
    rows.push_back(rowValues);
    
    return rowId;
}

// Data Querying
// -------------

// In Table.cpp, improve the selectRows method for GROUP BY
std::vector<std::vector<std::string>> Table::selectRows(
    const std::vector<std::string>& selectColumns,
    const std::string& condition,
    const std::vector<std::string>& orderByColumns,
    const std::vector<std::string>& groupByColumns,
    const std::string& havingCondition
) {
    std::shared_lock<std::shared_mutex> lock(mutex);
    
    std::vector<std::string> displayColumns;
    if (selectColumns.size() == 1 && selectColumns[0] == "*") {
        displayColumns = columns;
    } else {
        displayColumns = selectColumns;
    }
    
    std::vector<std::vector<std::string>> filteredRows;
    if (!condition.empty()) {
        ConditionParser cp(condition);
        auto expr = cp.parse();
        
        for (const auto& row : rows) {
            if (expr->evaluate(row, columns)) {
                filteredRows.push_back(row);
            }
        }
    } else {
        filteredRows = rows;
    }
    
    // Handle GROUP BY clause
    if (!groupByColumns.empty()) {
        std::unordered_map<std::string, std::vector<std::vector<std::string>>> groups;
        
        // Group rows based on the GROUP BY columns
        for (const auto& row : filteredRows) {
            std::string groupKey;
            for (const auto& groupCol : groupByColumns) {
                auto it = std::find(columns.begin(), columns.end(), groupCol);
                if (it != columns.end()) {
                    int idx = std::distance(columns.begin(), it);
                    groupKey += row[idx] + "|";
                }
            }
            groups[groupKey].push_back(row);
        }
        
        std::vector<std::vector<std::string>> groupedResult;
        
        // Process each group
        for (const auto& [key, groupRows] : groups) {
            std::vector<std::string> resultRow;
            
            for (const auto& colExpr : displayColumns) {
                size_t pos1 = colExpr.find('(');
                size_t pos2 = colExpr.find(')');
                
                if (pos1 != std::string::npos && pos2 != std::string::npos) {
                    // This is an aggregate function
                    std::string func = toUpperCase(trim(colExpr.substr(0, pos1)));
                    std::string colName = trim(colExpr.substr(pos1 + 1, pos2 - pos1 - 1));
                    
                    std::vector<std::string> colValues;
                    if (colName == "*") {
                        // COUNT(*) case
                        if (func == "COUNT") {
                            resultRow.push_back(std::to_string(groupRows.size()));
                        }
                    } else {
                        // Get column values for this group
                        auto it = std::find(columns.begin(), columns.end(), colName);
                        if (it != columns.end()) {
                            int idx = std::distance(columns.begin(), it);
                            for (const auto& groupRow : groupRows) {
                                colValues.push_back(groupRow[idx]);
                            }
                            
                            // Apply aggregate function
                            if (func == "AVG") {
                                resultRow.push_back(std::to_string(Aggregation::computeMean(colValues)));
                            } else if (func == "MIN") {
                                resultRow.push_back(std::to_string(Aggregation::computeMin(colValues)));
                            } else if (func == "MAX") {
                                resultRow.push_back(std::to_string(Aggregation::computeMax(colValues)));
                            } else if (func == "SUM") {
                                resultRow.push_back(std::to_string(Aggregation::computeSum(colValues)));
                            } else if (func == "COUNT") {
                                resultRow.push_back(std::to_string(Aggregation::computeCount(colValues)));
                            }
                        } else {
                            resultRow.push_back("");
                        }
                    }
                } else {
                    // Regular column, must be in GROUP BY
                    auto it = std::find(columns.begin(), columns.end(), colExpr);
                    if (it != columns.end()) {
                        int idx = std::distance(columns.begin(), it);
                        if (!groupRows.empty()) {
                            resultRow.push_back(groupRows[0][idx]);
                        } else {
                            resultRow.push_back("");
                        }
                    } else {
                        resultRow.push_back("");
                    }
                }
            }
            
            groupedResult.push_back(resultRow);
        }
        
        return groupedResult;
    }
    
    // If no GROUP BY, handle simple queries
    std::vector<std::vector<std::string>> result;
    for (const auto& row : filteredRows) {
        std::vector<std::string> resultRow;
        for (const auto& col : displayColumns) {
            size_t pos1 = col.find('(');
            size_t pos2 = col.find(')');
            
            if (pos1 != std::string::npos && pos2 != std::string::npos) {
                // This is an aggregate function (without GROUP BY)
                std::string func = toUpperCase(trim(col.substr(0, pos1)));
                std::string colName = trim(col.substr(pos1 + 1, pos2 - pos1 - 1));
                
                std::vector<std::string> colValues;
                if (colName == "*") {
                    // COUNT(*) case
                    if (func == "COUNT") {
                        resultRow.push_back(std::to_string(filteredRows.size()));
                    }
                } else {
                    // Get all column values
                    auto it = std::find(columns.begin(), columns.end(), colName);
                    if (it != columns.end()) {
                        int idx = std::distance(columns.begin(), it);
                        for (const auto& frow : filteredRows) {
                            colValues.push_back(frow[idx]);
                        }
                        
                        // Apply aggregate function
                        if (func == "AVG") {
                            resultRow.push_back(std::to_string(Aggregation::computeMean(colValues)));
                        } else if (func == "MIN") {
                            resultRow.push_back(std::to_string(Aggregation::computeMin(colValues)));
                        } else if (func == "MAX") {
                            resultRow.push_back(std::to_string(Aggregation::computeMax(colValues)));
                        } else if (func == "SUM") {
                            resultRow.push_back(std::to_string(Aggregation::computeSum(colValues)));
                        } else if (func == "COUNT") {
                            resultRow.push_back(std::to_string(Aggregation::computeCount(colValues)));
                        }
                    } else {
                        resultRow.push_back("");
                    }
                }
            } else {
                // Regular column
                auto it = std::find(columns.begin(), columns.end(), col);
                if (it != columns.end()) {
                    int idx = std::distance(columns.begin(), it);
                    resultRow.push_back(row[idx]);
                } else {
                    resultRow.push_back("");
                }
            }
        }
        
        result.push_back(resultRow);
        
        // If this is an aggregate query without GROUP BY, we only need one row
        if (std::any_of(displayColumns.begin(), displayColumns.end(), [](const std::string& col) {
            return col.find('(') != std::string::npos && col.find(')') != std::string::npos;
        })) {
            break;
        }
    }
    
    // Handle ORDER BY
    if (!orderByColumns.empty()) {
        std::sort(result.begin(), result.end(), 
            [&](const auto& a, const auto& b) {
                for (const auto& token : orderByColumns) {
                    std::string colName = token;
                    bool desc = false;
                    
                    size_t pos = toUpperCase(token).find(" DESC");
                    if (pos != std::string::npos) {
                        desc = true;
                        colName = trim(token.substr(0, pos));
                    }
                    
                    auto it = std::find(displayColumns.begin(), displayColumns.end(), colName);
                    if (it != displayColumns.end()) {
                        int idx = std::distance(displayColumns.begin(), it);
                        
                        if (idx < a.size() && idx < b.size()) {
                            // Try numeric comparison first
                            try {
                                double aVal = std::stod(a[idx]);
                                double bVal = std::stod(b[idx]);
                                if (aVal != bVal) {
                                    return desc ? (aVal > bVal) : (aVal < bVal);
                                }
                            } catch (...) {
                                // Fall back to string comparison
                                if (a[idx] != b[idx]) {
                                    return desc ? (a[idx] > b[idx]) : (a[idx] < b[idx]);
                                }
                            }
                        }
                    }
                }
                return false;
            });
    }
    
    return result;
}

// Debugging
// ---------

void Table::printTable() {
    std::shared_lock<std::shared_mutex> lock(mutex);
    
    for (const auto& col : columns) {
        std::cout << col << "\t";
    }
    std::cout << std::endl;
    
    for (const auto& type : columnTypes) {
        std::cout << type << "\t";
    }
    std::cout << std::endl;
    
    for (size_t i = 0; i < columns.size(); ++i) {
        std::cout << "--------\t";
    }
    std::cout << std::endl;
    
    for (const auto& row : rows) {
        for (size_t i = 0; i < columns.size(); ++i) {
            std::cout << (i < row.size() ? row[i] : "NULL") << "\t";
        }
        std::cout << std::endl;
    }
}
// Add these implementations to Table.cpp

void Table::enforceDataType(int columnIndex, std::string& value) {
    if (columnIndex >= columnTypes.size()) {
        throw DataTypeException("Column index out of range");
    }
    
    if (value.empty()) {
        // Empty strings are allowed (NULL values)
        return;
    }
    
    std::string type = columnTypes[columnIndex];
    DataType dataType = getDataType(type);
    
    switch (dataType) {
        case DataType::INT:
        case DataType::SMALLINT: {
            try {
                int val = std::stoi(value);
                value = std::to_string(val);
            } catch (const std::exception& e) {
                throw DataTypeException("Invalid integer value: " + value);
            }
            break;
        }
        case DataType::NUMERIC:
        case DataType::REAL:
        case DataType::DOUBLE_PRECISION:
        case DataType::FLOAT: {
            try {
                double val = std::stod(value);
                value = std::to_string(val);
            } catch (const std::exception& e) {
                throw DataTypeException("Invalid numeric value: " + value);
            }
            break;
        }
        case DataType::CHAR:
        case DataType::VARCHAR: {
            // Extract size parameter (e.g., CHAR(10))
            auto params = extractTypeParameters(type);
            int maxSize = params.first;
            
            // Remove quotes if present
            if (value.size() >= 2 && value.front() == '\'' && value.back() == '\'') {
                value = value.substr(1, value.size() - 2);
            }
            
            if (maxSize > 0 && value.size() > static_cast<size_t>(maxSize)) {
                if (dataType == DataType::CHAR) {
                    // Truncate to max size for CHAR
                    value = value.substr(0, maxSize);
                } else {
                    // VARCHAR throws an error if too long
                    throw DataTypeException("Value too long for VARCHAR(" + std::to_string(maxSize) + "): " + value);
                }
            }
            break;
        }
        case DataType::DATE: {
            // Simple date validation: YYYY-MM-DD
            std::regex dateRegex("^\\d{4}-\\d{2}-\\d{2}$");
            if (!std::regex_match(value, dateRegex)) {
                throw DataTypeException("Invalid date format: " + value + ". Expected YYYY-MM-DD");
            }
            break;
        }
        case DataType::TIME: {
            // Simple time validation: HH:MM:SS
            std::regex timeRegex("^\\d{2}:\\d{2}:\\d{2}$");
            if (!std::regex_match(value, timeRegex)) {
                throw DataTypeException("Invalid time format: " + value + ". Expected HH:MM:SS");
            }
            break;
        }
        case DataType::TIMESTAMP: {
            // Simple timestamp validation: YYYY-MM-DD HH:MM:SS
            std::regex timestampRegex("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$");
            if (!std::regex_match(value, timestampRegex)) {
                throw DataTypeException("Invalid timestamp format: " + value + ". Expected YYYY-MM-DD HH:MM:SS");
            }
            break;
        }
        case DataType::BOOLEAN: {
            // Normalize boolean values
            std::string upperVal = toUpperCase(value);
            if (upperVal == "TRUE" || upperVal == "1" || upperVal == "T" || upperVal == "YES" || upperVal == "Y") {
                value = "TRUE";
            } else if (upperVal == "FALSE" || upperVal == "0" || upperVal == "F" || upperVal == "NO" || upperVal == "N") {
                value = "FALSE";
            } else {
                throw DataTypeException("Invalid boolean value: " + value);
            }
            break;
        }
        case DataType::USER_DEFINED: {
            // For user-defined types, if the type is registered, validate attributes
            if (UserTypeRegistry::typeExists(type)) {
                // For simplicity, we assume the value is a string representation that meets type requirements
                // In a real system, this would need proper parsing and validation
            } else {
                throw DataTypeException("Unknown user-defined type: " + type);
            }
            break;
        }
        default:
            // For other types, we allow any value for now
            break;
    }
}

// In Table.cpp, update the validateUniqueConstraint method:
bool Table::validateUniqueConstraint(const Constraint& constraint, const std::vector<std::string>& newRow) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    // Extract column indices for the constraint
    std::vector<int> colIndices;
    for (const auto& colName : constraint.columns) {
        auto it = std::find(columns.begin(), columns.end(), colName);
        if (it == columns.end()) {
            throw DatabaseException("Column '" + colName + "' not found in unique constraint");
        }
        colIndices.push_back(std::distance(columns.begin(), it));
    }
    
    // Check if any existing row has the same values for the constrained columns
    for (const auto& row : rows) {
        bool allMatch = true;
        for (int idx : colIndices) {
            if (idx >= row.size() || idx >= newRow.size()) {
                allMatch = false;
                break;
            }
            
            // If values are different, no violation
            if (row[idx] != newRow[idx]) {
                allMatch = false;
                break;
            }
        }
        
        if (allMatch) {
            // Found a row with the same key values - constraint violation
            if (constraint.type == Constraint::Type::PRIMARY_KEY) {
                throw ConstraintViolationException("PRIMARY KEY constraint violated");
            } else {
                throw ConstraintViolationException("UNIQUE constraint violated");
            }
            return false;
        }
    }
    
    return true; // Constraint satisfied
}

// In Table.cpp, update the validateForeignKeyConstraint method:
bool Table::validateForeignKeyConstraint(const Constraint& constraint, const std::vector<std::string>& row) {
    // This normally requires access to the referenced table
    // You'll need to modify the Database class to expose table access to Table instances
    
    // For now, we'll implement a simplified version that assumes access to a global db pointer
    extern Database* g_db; // You'll need to define this in a shared location
    
    if (!g_db) {
        throw DatabaseException("Database reference not available for FK validation");
    }
    
    // Get referenced table
    Table* refTable = g_db->getTable(constraint.referencedTable);
    if (!refTable) {
        throw DatabaseException("Referenced table '" + constraint.referencedTable + "' does not exist");
    }
    
    // Extract values from this row for the constrained columns
    std::vector<std::string> fkValues;
    for (const auto& colName : constraint.columns) {
        int colIdx = getColumnIndex(colName);
        if (colIdx == -1 || colIdx >= row.size()) {
            throw DatabaseException("Column '" + colName + "' not found in foreign key");
        }
        fkValues.push_back(row[colIdx]);
    }
    
    // Extract indices for referenced columns
    std::vector<int> refColIndices;
    for (const auto& colName : constraint.referencedColumns) {
        int colIdx = refTable->getColumnIndex(colName);
        if (colIdx == -1) {
            throw DatabaseException("Referenced column '" + colName + "' not found");
        }
        refColIndices.push_back(colIdx);
    }
    
    // Check if any row in the referenced table matches our FK values
    for (const auto& refRow : refTable->getRows()) {
        bool allMatch = true;
        for (size_t i = 0; i < fkValues.size(); i++) {
            int refIdx = refColIndices[i];
            if (refIdx >= refRow.size() || fkValues[i] != refRow[refIdx]) {
                allMatch = false;
                break;
            }
        }
        
        if (allMatch) {
            return true; // Found a matching reference
        }
    }
    
    // No matching reference found
    throw ReferentialIntegrityException("FOREIGN KEY constraint violated");
    return false;
}

bool Table::validateCheckConstraint(const Constraint& constraint, const std::vector<std::string>& row) {
    // Parse and evaluate the check expression against the row
    ConditionParser parser(constraint.checkExpression);
    auto expr = parser.parse();
    return expr->evaluate(row, columns);
}

bool Table::validateConstraintsForUpdate(const std::vector<std::string>& oldRow, const std::vector<std::string>& newRow) {
    // For updates, we need to:
    // 1. Check NOT NULL constraints
    for (size_t i = 0; i < notNullConstraints.size(); ++i) {
        if (notNullConstraints[i] && (i >= newRow.size() || newRow[i].empty())) {
            throw ConstraintViolationException("NOT NULL constraint violated for column '" + columns[i] + "'");
        }
    }
    
    // 2. Check all other constraints
    for (const auto& constraint : constraints) {
        switch (constraint.type) {
            case Constraint::Type::PRIMARY_KEY:
            case Constraint::Type::UNIQUE: {
                // For unique constraints, we need to check if any OTHER row has the same values
                std::vector<int> colIndices;
                for (const auto& colName : constraint.columns) {
                    auto it = std::find(columns.begin(), columns.end(), colName);
                    if (it == columns.end()) {
                        throw DatabaseException("Column '" + colName + "' not found in unique constraint");
                    }
                    colIndices.push_back(std::distance(columns.begin(), it));
                }
                
                bool changed = false;
                for (int idx : colIndices) {
                    if (idx < oldRow.size() && idx < newRow.size() && oldRow[idx] != newRow[idx]) {
                        changed = true;
                        break;
                    }
                }
                
                if (changed) {
                    // Only check if the constrained columns were changed
                    for (const auto& row : rows) {
                        if (row == oldRow) {
                            continue; // Skip the row being updated
                        }
                        
                        bool allMatch = true;
                        for (int idx : colIndices) {
                            if (idx >= row.size() || idx >= newRow.size() || row[idx] != newRow[idx]) {
                                allMatch = false;
                                break;
                            }
                        }
                        if (allMatch) {
                            throw ConstraintViolationException(
                                std::string(constraint.type == Constraint::Type::PRIMARY_KEY ? "PRIMARY KEY" : "UNIQUE") +
                                " constraint '" + constraint.name + "' violated");
                        }
                    }
                }
                break;
            }
            case Constraint::Type::FOREIGN_KEY:
                if (!validateForeignKeyConstraint(constraint, newRow)) {
                    throw ReferentialIntegrityException("FOREIGN KEY constraint '" + constraint.name + "' violated");
                }
                break;
            case Constraint::Type::CHECK:
                if (!validateCheckConstraint(constraint, newRow)) {
                    throw ConstraintViolationException("CHECK constraint '" + constraint.name + "' violated");
                }
                break;
            case Constraint::Type::NOT_NULL:
                // Already checked above
                break;
        }
    }
    
    return true;
}