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

std::vector<std::vector<std::string>> Table::innerJoin(
    Table& rightTable,
    const std::string& condition,
    const std::vector<std::string>& selectColumns
) {
    std::shared_lock<std::shared_mutex> lockLeft(mutex);
    std::shared_lock<std::shared_mutex> lockRight(rightTable.mutex);
    
    std::vector<std::string> joinedColumns = columns;
    joinedColumns.insert(joinedColumns.end(), rightTable.columns.begin(), rightTable.columns.end());
    
    ConditionParser cp(condition);
    auto expr = cp.parse();
    
    std::vector<std::vector<std::string>> result;
    
    for (const auto& leftRow : rows) {
        for (const auto& rightRow : rightTable.rows) {
            std::vector<std::string> combinedRow;
            combinedRow.insert(combinedRow.end(), leftRow.begin(), leftRow.end());
            combinedRow.insert(combinedRow.end(), rightRow.begin(), rightRow.end());
            
            if (expr->evaluate(combinedRow, joinedColumns)) {
                std::vector<std::string> resultRow;
                for (const auto& col : selectColumns) {
                    auto it = std::find(joinedColumns.begin(), joinedColumns.end(), col);
                    if (it != joinedColumns.end()) {
                        int idx = std::distance(joinedColumns.begin(), it);
                        resultRow.push_back(combinedRow[idx]);
                    } else {
                        resultRow.push_back("");
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
    
    std::vector<std::string> joinedColumns = columns;
    joinedColumns.insert(joinedColumns.end(), rightTable.columns.begin(), rightTable.columns.end());
    
    ConditionParser cp(condition);
    auto expr = cp.parse();
    
    std::vector<std::vector<std::string>> result;
    
    for (const auto& leftRow : rows) {
        bool foundMatch = false;
        
        for (const auto& rightRow : rightTable.rows) {
            std::vector<std::string> combinedRow;
            combinedRow.insert(combinedRow.end(), leftRow.begin(), leftRow.end());
            combinedRow.insert(combinedRow.end(), rightRow.begin(), rightRow.end());
            
            if (expr->evaluate(combinedRow, joinedColumns)) {
                foundMatch = true;
                
                std::vector<std::string> resultRow;
                for (const auto& col : selectColumns) {
                    auto it = std::find(joinedColumns.begin(), joinedColumns.end(), col);
                    if (it != joinedColumns.end()) {
                        int idx = std::distance(joinedColumns.begin(), it);
                        resultRow.push_back(combinedRow[idx]);
                    } else {
                        resultRow.push_back("");
                    }
                }
                result.push_back(resultRow);
            }
        }
        
        if (!foundMatch) {
            std::vector<std::string> combinedRow = leftRow;
            combinedRow.insert(combinedRow.end(), rightTable.columns.size(), "");
            
            std::vector<std::string> resultRow;
            for (const auto& col : selectColumns) {
                auto it = std::find(joinedColumns.begin(), joinedColumns.end(), col);
                if (it != joinedColumns.end()) {
                    int idx = std::distance(joinedColumns.begin(), it);
                    resultRow.push_back(combinedRow[idx]);
                } else {
                    resultRow.push_back("");
                }
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
    std::shared_lock<std::shared_mutex> lockLeft(mutex);
    std::shared_lock<std::shared_mutex> lockRight(rightTable.mutex);
    
    std::vector<std::string> joinedColumns = columns;
    joinedColumns.insert(joinedColumns.end(), rightTable.columns.begin(), rightTable.columns.end());
    
    ConditionParser cp(condition);
    auto expr = cp.parse();
    
    std::vector<std::vector<std::string>> result;
    
    for (const auto& rightRow : rightTable.rows) {
        bool foundMatch = false;
        
        for (const auto& leftRow : rows) {
            std::vector<std::string> combinedRow;
            combinedRow.insert(combinedRow.end(), leftRow.begin(), leftRow.end());
            combinedRow.insert(combinedRow.end(), rightRow.begin(), rightRow.end());
            
            if (expr->evaluate(combinedRow, joinedColumns)) {
                foundMatch = true;
                
                std::vector<std::string> resultRow;
                for (const auto& col : selectColumns) {
                    auto it = std::find(joinedColumns.begin(), joinedColumns.end(), col);
                    if (it != joinedColumns.end()) {
                        int idx = std::distance(joinedColumns.begin(), it);
                        resultRow.push_back(combinedRow[idx]);
                    } else {
                        resultRow.push_back("");
                    }
                }
                result.push_back(resultRow);
            }
        }
        
        if (!foundMatch) {
            std::vector<std::string> combinedRow;
            combinedRow.insert(combinedRow.end(), columns.size(), "");
            combinedRow.insert(combinedRow.end(), rightRow.begin(), rightRow.end());
            
            std::vector<std::string> resultRow;
            for (const auto& col : selectColumns) {
                auto it = std::find(joinedColumns.begin(), joinedColumns.end(), col);
                if (it != joinedColumns.end()) {
                    int idx = std::distance(joinedColumns.begin(), it);
                    resultRow.push_back(combinedRow[idx]);
                } else {
                    resultRow.push_back("");
                }
            }
            result.push_back(resultRow);
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
    
    std::vector<std::string> joinedColumns = columns;
    joinedColumns.insert(joinedColumns.end(), rightTable.columns.begin(), rightTable.columns.end());
    
    ConditionParser cp(condition);
    auto expr = cp.parse();
    
    std::vector<std::vector<std::string>> result;
    std::vector<bool> rightMatched(rightTable.rows.size(), false);
    
    for (const auto& leftRow : rows) {
        bool foundMatch = false;
        
        for (size_t r = 0; r < rightTable.rows.size(); ++r) {
            const auto& rightRow = rightTable.rows[r];
            
            std::vector<std::string> combinedRow;
            combinedRow.insert(combinedRow.end(), leftRow.begin(), leftRow.end());
            combinedRow.insert(combinedRow.end(), rightRow.begin(), rightRow.end());
            
            if (expr->evaluate(combinedRow, joinedColumns)) {
                foundMatch = true;
                rightMatched[r] = true;
                
                std::vector<std::string> resultRow;
                for (const auto& col : selectColumns) {
                    auto it = std::find(joinedColumns.begin(), joinedColumns.end(), col);
                    if (it != joinedColumns.end()) {
                        int idx = std::distance(joinedColumns.begin(), it);
                        resultRow.push_back(combinedRow[idx]);
                    } else {
                        resultRow.push_back("");
                    }
                }
                result.push_back(resultRow);
            }
        }
        
        if (!foundMatch) {
            std::vector<std::string> combinedRow = leftRow;
            combinedRow.insert(combinedRow.end(), rightTable.columns.size(), "");
            
            std::vector<std::string> resultRow;
            for (const auto& col : selectColumns) {
                auto it = std::find(joinedColumns.begin(), joinedColumns.end(), col);
                if (it != joinedColumns.end()) {
                    int idx = std::distance(joinedColumns.begin(), it);
                    resultRow.push_back(combinedRow[idx]);
                } else {
                    resultRow.push_back("");
                }
            }
            result.push_back(resultRow);
        }
    }
    
    for (size_t r = 0; r < rightTable.rows.size(); ++r) {
        if (!rightMatched[r]) {
            std::vector<std::string> combinedRow;
            combinedRow.insert(combinedRow.end(), columns.size(), "");
            combinedRow.insert(combinedRow.end(), rightTable.rows[r].begin(), rightTable.rows[r].end());
            
            std::vector<std::string> resultRow;
            for (const auto& col : selectColumns) {
                auto it = std::find(joinedColumns.begin(), joinedColumns.end(), col);
                if (it != joinedColumns.end()) {
                    int idx = std::distance(joinedColumns.begin(), it);
                    resultRow.push_back(combinedRow[idx]);
                } else {
                    resultRow.push_back("");
                }
            }
            result.push_back(resultRow);
        }
    }
    
    return result;
}

std::vector<std::vector<std::string>> Table::naturalJoin(
    Table& rightTable,
    const std::vector<std::string>& selectColumns
) {
    std::shared_lock<std::shared_mutex> lockLeft(mutex);
    std::shared_lock<std::shared_mutex> lockRight(rightTable.mutex);
    
    std::vector<std::string> commonColumns;
    for (const auto& col : columns) {
        if (rightTable.hasColumn(col)) {
            commonColumns.push_back(col);
        }
    }
    
    if (commonColumns.empty()) {
        return innerJoin(rightTable, "1=1", selectColumns);
    }
    
    std::vector<std::string> joinedColumns = columns;
    for (const auto& col : rightTable.columns) {
        if (!hasColumn(col)) {
            joinedColumns.push_back(col);
        }
    }
    
    std::vector<std::vector<std::string>> result;
    
    for (const auto& leftRow : rows) {
        for (const auto& rightRow : rightTable.rows) {
            bool match = true;
            for (const auto& col : commonColumns) {
                int leftIdx = getColumnIndex(col);
                int rightIdx = rightTable.getColumnIndex(col);
                
                if (leftRow[leftIdx] != rightRow[rightIdx]) {
                    match = false;
                    break;
                }
            }
            
            if (match) {
                std::vector<std::string> combinedRow = leftRow;
                for (const auto& col : rightTable.columns) {
                    if (!hasColumn(col)) {
                        int idx = rightTable.getColumnIndex(col);
                        combinedRow.push_back(rightRow[idx]);
                    }
                }
                
                std::vector<std::string> resultRow;
                for (const auto& col : selectColumns) {
                    auto it = std::find(joinedColumns.begin(), joinedColumns.end(), col);
                    if (it != joinedColumns.end()) {
                        int idx = std::distance(joinedColumns.begin(), it);
                        resultRow.push_back(combinedRow[idx]);
                    } else {
                        resultRow.push_back("");
                    }
                }
                result.push_back(resultRow);
            }
        }
    }
    
    return result;
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

int Table::addRow(const std::vector<std::string>& values) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    return addRowWithId(nextRowId++, values);
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
    
    // Handle aggregation
    bool hasAggregate = false;
    std::vector<std::string> aggregateResults;
    
    for (const auto& colExpr : displayColumns) {
        size_t pos1 = colExpr.find('(');
        size_t pos2 = colExpr.find(')');
        
        if (pos1 != std::string::npos && pos2 != std::string::npos) {
            hasAggregate = true;
            std::string func = toUpperCase(trim(colExpr.substr(0, pos1)));
            std::string colName = trim(colExpr.substr(pos1 + 1, pos2 - pos1 - 1));
            
            auto it = std::find(columns.begin(), columns.end(), colName);
            if (it == columns.end() && colName != "*") {
                aggregateResults.push_back("");
                continue;
            }
            
            int idx = (colName == "*") ? -1 : std::distance(columns.begin(), it);
            std::vector<std::string> colValues;
            
            if (idx >= 0) {
                for (const auto& row : filteredRows) {
                    colValues.push_back(row[idx]);
                }
            }
            
            if (func == "COUNT" && colName == "*") {
                aggregateResults.push_back(std::to_string(filteredRows.size()));
            } else if (func == "AVG") {
                aggregateResults.push_back(std::to_string(Aggregation::computeMean(colValues)));
            } else if (func == "MIN") {
                aggregateResults.push_back(std::to_string(Aggregation::computeMin(colValues)));
            } else if (func == "MAX") {
                aggregateResults.push_back(std::to_string(Aggregation::computeMax(colValues)));
            } else if (func == "SUM") {
                aggregateResults.push_back(std::to_string(Aggregation::computeSum(colValues)));
            } else if (func == "MEDIAN") {
                aggregateResults.push_back(Aggregation::computeMedian(colValues));
            } else if (func == "MODE") {
                aggregateResults.push_back(Aggregation::computeMode(colValues));
            }
        } else {
            if (hasAggregate && std::find(groupByColumns.begin(), groupByColumns.end(), colExpr) == groupByColumns.end()) {
                throw DatabaseException("Column '" + colExpr + "' must appear in GROUP BY clause");
            }
            aggregateResults.push_back("");
        }
    }
    
    std::vector<std::vector<std::string>> resultRows;
    
    if (hasAggregate && groupByColumns.empty()) {
        std::vector<std::string> resultRow;
        for (size_t i = 0; i < displayColumns.size(); ++i) {
            if (!aggregateResults[i].empty()) {
                resultRow.push_back(aggregateResults[i]);
            } else {
                auto it = std::find(columns.begin(), columns.end(), displayColumns[i]);
                if (it != columns.end() && !filteredRows.empty()) {
                    int idx = std::distance(columns.begin(), it);
                    resultRow.push_back(filteredRows[0][idx]);
                } else {
                    resultRow.push_back("");
                }
            }
        }
        resultRows.push_back(resultRow);
        return resultRows;
    }
    
    // Handle GROUP BY
    if (!groupByColumns.empty()) {
        // Implementation for GROUP BY
        // ...
    }
    
    // Handle ORDER BY
    if (!orderByColumns.empty()) {
        std::sort(filteredRows.begin(), filteredRows.end(), 
            [&](const auto& a, const auto& b) {
                for (const auto& token : orderByColumns) {
                    std::string colName = token;
                    bool desc = false;
                    
                    size_t pos = toUpperCase(token).find(" DESC");
                    if (pos != std::string::npos) {
                        desc = true;
                        colName = trim(token.substr(0, pos));
                    }
                    
                    auto it = std::find(columns.begin(), columns.end(), colName);
                    if (it != columns.end()) {
                        int idx = std::distance(columns.begin(), it);
                        return desc ? (a[idx] > b[idx]) : (a[idx] < b[idx]);
                    }
                }
                return false;
            });
    }
    
    // Project columns
    for (const auto& row : filteredRows) {
        std::vector<std::string> resultRow;
        for (const auto& col : displayColumns) {
            auto it = std::find(columns.begin(), columns.end(), col);
            if (it != columns.end()) {
                int idx = std::distance(columns.begin(), it);
                resultRow.push_back(row[idx]);
            } else {
                resultRow.push_back("");
            }
        }
        resultRows.push_back(resultRow);
    }
    
    return resultRows;
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
            if (idx >= row.size() || idx >= newRow.size() || row[idx] != newRow[idx]) {
                allMatch = false;
                break;
            }
        }
        if (allMatch) {
            return false; // Constraint violated
        }
    }
    
    return true; // Constraint satisfied
}

bool Table::validateForeignKeyConstraint(const Constraint& constraint, const std::vector<std::string>& row) {
    // This function needs external database access to check references
    // For simplicity, we'll assume references are valid for now
    // A real implementation would need to look up the referenced table and check values
    
    // The implementation would check:
    // 1. Get the referenced table
    // 2. Extract values from this row for the constrained columns
    // 3. Check if matching values exist in referenced table's referenced columns
    
    // For now, just return true
    return true;
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