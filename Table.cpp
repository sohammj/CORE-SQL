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

Table::Table(const std::string& name) : tableName(name), nextRowId(1) {}

void Table::addColumn(const std::string& columnName, const std::string& type, bool isNotNull) {
    // Acquire exclusive lock
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    // Check if column already exists
    if (std::find(columns.begin(), columns.end(), columnName) != columns.end()) {
        throw DatabaseException("Column '" + columnName + "' already exists in table '" + tableName + "'");
    }
    
    // Validate data type
    if (!isValidDataType(type) && !UserTypeRegistry::typeExists(type)) {
        throw DataTypeException("Invalid data type '" + type + "' for column '" + columnName + "'");
    }
    
    columns.push_back(columnName);
    columnTypes.push_back(type);
    notNullConstraints.push_back(isNotNull);
    
    // Add empty values for the new column to all existing rows
    for (auto& row : rows) {
        row.push_back(isNotNull ? "" : "");
    }
}

bool Table::dropColumn(const std::string& columnName) {
    // Acquire exclusive lock
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    auto it = std::find(columns.begin(), columns.end(), columnName);
    if (it == columns.end()) {
        return false;
    }
    
    int index = std::distance(columns.begin(), it);
    
    // Check if this column is part of any constraint
    for (const auto& constraint : constraints) {
        if (std::find(constraint.columns.begin(), constraint.columns.end(), columnName) != constraint.columns.end()) {
            throw DatabaseException("Cannot drop column '" + columnName + "' because it is used in constraint '" + constraint.name + "'");
        }
    }
    
    columns.erase(it);
    columnTypes.erase(columnTypes.begin() + index);
    notNullConstraints.erase(notNullConstraints.begin() + index);
    
    for (auto& row : rows) {
        if (index < row.size()) {
            row.erase(row.begin() + index);
        }
    }
    
    return true;
}

void Table::renameColumn(const std::string& oldName, const std::string& newName) {
    // Acquire exclusive lock
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    auto it = std::find(columns.begin(), columns.end(), oldName);
    if (it == columns.end()) {
        throw DatabaseException("Column '" + oldName + "' does not exist in table '" + tableName + "'");
    }
    
    // Check if the new name already exists
    if (std::find(columns.begin(), columns.end(), newName) != columns.end()) {
        throw DatabaseException("Column '" + newName + "' already exists in table '" + tableName + "'");
    }
    
    // Update column name
    *it = newName;
    
    // Update any constraints that reference this column
    for (auto& constraint : constraints) {
        for (auto& col : constraint.columns) {
            if (col == oldName) {
                col = newName;
            }
        }
    }
}

void Table::addConstraint(const Constraint& constraint) {
    // Acquire exclusive lock
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    // Validate the constraint
    for (const auto& col : constraint.columns) {
        if (std::find(columns.begin(), columns.end(), col) == columns.end()) {
            throw DatabaseException("Column '" + col + "' referenced in constraint '" + constraint.name + "' does not exist in table '" + tableName + "'");
        }
    }
    
    // Check if constraint with this name already exists
    for (const auto& existingConstraint : constraints) {
        if (existingConstraint.name == constraint.name) {
            throw DatabaseException("Constraint '" + constraint.name + "' already exists in table '" + tableName + "'");
        }
    }
    
    // For PRIMARY KEY and UNIQUE constraints, verify that existing data complies
    if (constraint.type == Constraint::Type::PRIMARY_KEY || constraint.type == Constraint::Type::UNIQUE) {
        if (!validateUniqueConstraint(constraint, std::vector<std::string>())) {
            tthrow ConstraintViolationException(std::string("New ") +
                (constraint.type == Constraint::Type::PRIMARY_KEY ? "PRIMARY KEY" : "UNIQUE") + 
                " constraint '" + constraint.name + "' violated by existing data");
        }
    }
    
    // For NOT NULL constraints, verify existing data
    if (constraint.type == Constraint::Type::NOT_NULL) {
        for (const auto& col : constraint.columns) {
            int idx = getColumnIndex(col);
            for (const auto& row : rows) {
                if (row[idx].empty()) {
                    throw ConstraintViolationException("New NOT NULL constraint '" + constraint.name + "' violated by existing data");
                }
            }
        }
    }
    
    // For CHECK constraints, verify existing data
    if (constraint.type == Constraint::Type::CHECK) {
        if (!validateCheckConstraint(constraint, std::vector<std::string>())) {
            throw ConstraintViolationException("New CHECK constraint '" + constraint.name + "' violated by existing data");
        }
    }
    
    constraints.push_back(constraint);
}

bool Table::dropConstraint(const std::string& constraintName) {
    // Acquire exclusive lock
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    auto it = std::find_if(constraints.begin(), constraints.end(),
                          [&](const Constraint& c) { return c.name == constraintName; });
    
    if (it == constraints.end()) {
        return false;
    }
    
    constraints.erase(it);
    return true;
}

bool Table::validateConstraints(const std::vector<std::string>& row) {
    // Validate all constraints for a new row
    
    // First check NOT NULL constraints
    for (size_t i = 0; i < notNullConstraints.size(); ++i) {
        if (notNullConstraints[i] && (i >= row.size() || row[i].empty())) {
            throw ConstraintViolationException("NOT NULL constraint violated for column '" + columns[i] + "'");
        }
    }
    
    // Then check all other constraints
    for (const auto& constraint : constraints) {
        switch (constraint.type) {
            case Constraint::Type::PRIMARY_KEY:
            case Constraint::Type::UNIQUE:
                if (!validateUniqueConstraint(constraint, row)) {
                    throw ConstraintViolationException(
                        (constraint.type == Constraint::Type::PRIMARY_KEY ? "PRIMARY KEY" : "UNIQUE") + 
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
                // Already checked above
                break;
        }
    }
    
    return true;
}

bool Table::validateConstraintsForUpdate(const std::vector<std::string>& oldRow, const std::vector<std::string>& newRow) {
    // For UPDATE operations, we need special validation:
    // 1. Skip UNIQUE/PK validation if those columns aren't changing
    // 2. Handle NOT NULL constraints
    // 3. Handle CHECK constraints
    // 4. Handle FOREIGN KEY constraints
    
    // First check NOT NULL constraints for the new row
    for (size_t i = 0; i < notNullConstraints.size(); ++i) {
        if (notNullConstraints[i] && (i >= newRow.size() || newRow[i].empty())) {
            throw ConstraintViolationException("NOT NULL constraint violated for column '" + columns[i] + "'");
        }
    }
    
    // Then check all other constraints
    for (const auto& constraint : constraints) {
        switch (constraint.type) {
            case Constraint::Type::PRIMARY_KEY:
            case Constraint::Type::UNIQUE: {
                // Only validate if at least one constrained column is changing
                bool columnsChanged = false;
                for (const auto& col : constraint.columns) {
                    int idx = getColumnIndex(col);
                    if (oldRow[idx] != newRow[idx]) {
                        columnsChanged = true;
                        break;
                    }
                }
                
                if (columnsChanged && !validateUniqueConstraint(constraint, newRow)) {
                    throw ConstraintViolationException(
                        (constraint.type == Constraint::Type::PRIMARY_KEY ? "PRIMARY KEY" : "UNIQUE") + 
                        " constraint '" + constraint.name + "' violated");
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

bool Table::validateForeignKeyConstraint(const Constraint& constraint, const std::vector<std::string>& row) {
    // This is a stub implementation
    // In a real implementation, this would look up the referenced table and verify
    // that the values exist in the referenced columns
    
    // For now, just assume the constraint is satisfied
    return true;
}

bool Table::validateUniqueConstraint(const Constraint& constraint, const std::vector<std::string>& newRow) {
    if (newRow.empty()) {
        // We're validating the whole table (for adding a new constraint)
        std::unordered_map<std::string, bool> seen;
        
        for (const auto& row : rows) {
            std::string key;
            for (const auto& col : constraint.columns) {
                int idx = getColumnIndex(col);
                key += row[idx] + "|";
            }
            
            if (seen[key]) {
                return false; // Duplicate found
            }
            
            seen[key] = true;
        }
    } else {
        // We're validating a single new row
        std::string newKey;
        for (const auto& col : constraint.columns) {
            int idx = getColumnIndex(col);
            newKey += newRow[idx] + "|";
        }
        
        for (const auto& row : rows) {
            std::string key;
            for (const auto& col : constraint.columns) {
                int idx = getColumnIndex(col);
                key += row[idx] + "|";
            }
            
            if (key == newKey) {
                return false; // Duplicate found
            }
        }
    }
    
    return true;
}

bool Table::validateCheckConstraint(const Constraint& constraint, const std::vector<std::string>& row) {
    // This is a stub implementation
    // In a real implementation, this would evaluate the check expression
    // against the row values
    
    // For example, if the check is "salary > 0", we would verify that
    // the salary column's value is indeed greater than 0
    
    // For now, just assume the constraint is satisfied
    return true;
}

bool Table::validateNotNullConstraint(int columnIndex, const std::string& value) {
    if (columnIndex < 0 || columnIndex >= notNullConstraints.size()) {
        return true; // Invalid column index, so can't validate
    }
    
    return !notNullConstraints[columnIndex] || !value.empty();
}

void Table::enforceDataType(int columnIndex, std::string& value) {
    if (columnIndex < 0 || columnIndex >= columnTypes.size()) {
        return; // Invalid column index
    }
    
    std::string type = columnTypes[columnIndex];
    DataType dataType = getDataType(type);
    
    switch (dataType) {
        case DataType::INT:
        case DataType::SMALLINT: {
            if (!value.empty()) {
                try {
                    int intValue = std::stoi(value);
                    value = std::to_string(intValue);
                } catch (const std::exception& e) {
                    throw DataTypeException("Invalid integer value '" + value + "' for column '" + columns[columnIndex] + "'");
                }
            }
            break;
        }
        case DataType::NUMERIC:
        case DataType::REAL:
        case DataType::DOUBLE_PRECISION:
        case DataType::FLOAT: {
            if (!value.empty()) {
                try {
                    double doubleValue = std::stod(value);
                    
                    // For NUMERIC/DECIMAL, apply precision/scale if specified
                    if (dataType == DataType::NUMERIC) {
                        auto params = extractTypeParameters(type);
                        int precision = params.first;
                        int scale = params.second;
                        
                        if (precision > 0) {
                            // Format the number according to precision and scale
                            std::ostringstream oss;
                            oss.precision(scale);
                            oss << std::fixed << doubleValue;
                            value = oss.str();
                            
                            // Truncate to precision
                            std::string::size_type pos = value.find('.');
                            if (pos != std::string::npos) {
                                int integerDigits = pos;
                                if (integerDigits + scale > precision) {
                                    throw DataTypeException("Numeric value '" + value + "' exceeds precision " + 
                                                           std::to_string(precision) + " for column '" + columns[columnIndex] + "'");
                                }
                            }
                        }
                    }
                } catch (const std::exception& e) {
                    throw DataTypeException("Invalid numeric value '" + value + "' for column '" + columns[columnIndex] + "'");
                }
            }
            break;
        }
        case DataType::CHAR: {
            if (!value.empty()) {
                auto params = extractTypeParameters(type);
                int length = params.first;
                
                if (length > 0) {
                    if (value.length() > static_cast<size_t>(length)) {
                        value = value.substr(0, length);
                    } else {
                        value.append(length - value.length(), ' ');
                    }
                }
            }
            break;
        }
        case DataType::VARCHAR: {
            if (!value.empty()) {
                auto params = extractTypeParameters(type);
                int length = params.first;
                
                if (length > 0 && value.length() > static_cast<size_t>(length)) {
                    value = value.substr(0, length);
                }
            }
            break;
        }
        case DataType::DATE: {
            if (!value.empty()) {
                // Simple date validation (YYYY-MM-DD)
                std::regex dateRegex("^\\d{4}-\\d{2}-\\d{2}$");
                if (!std::regex_match(value, dateRegex)) {
                    throw DataTypeException("Invalid date format '" + value + "' for column '" + columns[columnIndex] + "'. Expected YYYY-MM-DD.");
                }
            }
            break;
        }
        case DataType::TIME: {
            if (!value.empty()) {
                // Simple time validation (HH:MM:SS)
                std::regex timeRegex("^\\d{2}:\\d{2}:\\d{2}$");
                if (!std::regex_match(value, timeRegex)) {
                    throw DataTypeException("Invalid time format '" + value + "' for column '" + columns[columnIndex] + "'. Expected HH:MM:SS.");
                }
            }
            break;
        }
        case DataType::TIMESTAMP: {
            if (!value.empty()) {
                // Simple timestamp validation (YYYY-MM-DD HH:MM:SS)
                std::regex timestampRegex("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$");
                if (!std::regex_match(value, timestampRegex)) {
                    throw DataTypeException("Invalid timestamp format '" + value + "' for column '" + columns[columnIndex] + "'. Expected YYYY-MM-DD HH:MM:SS.");
                }
            }
            break;
        }
        case DataType::BOOLEAN: {
            if (!value.empty()) {
                std::string upperValue = toUpperCase(value);
                if (upperValue == "TRUE" || upperValue == "T" || upperValue == "1" || upperValue == "YES" || upperValue == "Y") {
                    value = "TRUE";
                } else if (upperValue == "FALSE" || upperValue == "F" || upperValue == "0" || upperValue == "NO" || upperValue == "N") {
                    value = "FALSE";
                } else {
                    throw DataTypeException("Invalid boolean value '" + value + "' for column '" + columns[columnIndex] + "'");
                }
            }
            break;
        }
        // Handle domain types
        case DataType::BRANCH:
        case DataType::CUSTOMER:
        case DataType::LOAN:
        case DataType::BORROWER:
        case DataType::ACCOUNT:
        case DataType::DEPOSITOR:
            // These would have their own validation logic
            break;
            
        case DataType::USER_DEFINED:
        case DataType::TEXT:
        case DataType::UNKNOWN:
            // No validation for these types
            break;
    }
}

int Table::addRow(const std::vector<std::string>& values) {
    // Acquire exclusive lock
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    return addRowWithId(nextRowId++, values);
}

int Table::addRowWithId(int rowId, const std::vector<std::string>& values) {
    if (values.size() != columns.size()) {
        throw DatabaseException("Incorrect number of values for row. Expected " + 
                               std::to_string(columns.size()) + ", got " + 
                               std::to_string(values.size()));
    }
    
    // Create a copy of the values for validation and type conversion
    std::vector<std::string> rowValues = values;
    
    // Enforce data types
    for (size_t i = 0; i < rowValues.size(); ++i) {
        enforceDataType(i, rowValues[i]);
    }
    
    // Validate constraints
    validateConstraints(rowValues);
    
    // Add the row
    rows.push_back(rowValues);
    
    return rowId;
}

std::vector<std::vector<std::string>> Table::selectRows(
    const std::vector<std::string>& selectColumns,
    const std::string& condition,
    const std::vector<std::string>& orderByColumns,
    const std::vector<std::string>& groupByColumns,
    const std::string& havingCondition)
{
    // Acquire shared lock
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
    
    // Check for aggregate functions (AVG, MIN, MAX, SUM, COUNT)
    bool hasAggregate = false;
    std::vector<std::string> aggregateResults;
    std::vector<std::vector<std::string>> resultRows;
    
    for (const auto& colExpr : displayColumns) {
        std::string func, colName;
        size_t pos1 = colExpr.find('(');
        size_t pos2 = colExpr.find(')');
        
        if (pos1 != std::string::npos && pos2 != std::string::npos) {
            hasAggregate = true;
            func = toUpperCase(trim(colExpr.substr(0, pos1)));
            colName = trim(colExpr.substr(pos1 + 1, pos2 - pos1 - 1));
            
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
                double avg = Aggregation::computeMean(colValues);
                aggregateResults.push_back(std::to_string(avg));
            } else if (func == "MIN") {
                double min = Aggregation::computeMin(colValues);
                aggregateResults.push_back(std::to_string(min));
            } else if (func == "MAX") {
                double max = Aggregation::computeMax(colValues);
                aggregateResults.push_back(std::to_string(max));
            } else if (func == "SUM") {
                double sum = Aggregation::computeSum(colValues);
                aggregateResults.push_back(std::to_string(sum));
            } else if (func == "MEDIAN") {
                std::string median = Aggregation::computeMedian(colValues);
                aggregateResults.push_back(median);
            } else if (func == "MODE") {
                std::string mode = Aggregation::computeMode(colValues);
                aggregateResults.push_back(mode);
            } else {
                aggregateResults.push_back(filteredRows.empty() ? "" : filteredRows[0][idx]);
            }
        } else {
            // Not an aggregate function
            if (hasAggregate) {
                // If we have aggregates mixed with non-aggregates, 
                // the non-aggregates must be part of GROUP BY
                if (std::find(groupByColumns.begin(), groupByColumns.end(), colExpr) == groupByColumns.end()) {
                    throw DatabaseException("Column '" + colExpr + "' must appear in GROUP BY clause");
                }
            }
            aggregateResults.push_back("");
        }
    }
    
    // If there are aggregate functions, return the result
    if (hasAggregate && groupByColumns.empty()) {
        std::vector<std::string> resultRow;
        for (size_t i = 0; i < displayColumns.size(); ++i) {
            if (!aggregateResults[i].empty()) {
                resultRow.push_back(aggregateResults[i]);
            } else {
                // Find non-aggregate columns from first row
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

void Table::printTable() {
    // Acquire shared lock
    std::shared_lock<std::shared_mutex> lock(mutex);
    
    // Print column headers
    for (const auto& col : columns) {
        std::cout << col << "\t";
    }
    std::cout << std::endl;
    
    // Print column types
    for (const auto& type : columnTypes) {
        std::cout << type << "\t";
    }
    std::cout << std::endl;
    
    // Print separator
    for (size_t i = 0; i < columns.size(); ++i) {
        std::cout << "--------\t";
    }
    std::cout << std::endl;
    
    // Print rows
    for (const auto& row : rows) {
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i < row.size()) {
                std::cout << row[i] << "\t";
            } else {
                std::cout << "NULL\t";
            }
        }
        std::cout << std::endl;
    }
}

void Table::deleteRows(const std::string& condition) {
    // Acquire exclusive lock
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

void Table::updateRows(const std::vector<std::pair<std::string, std::string>>& updates,
                     const std::string& condition) {
    // Acquire exclusive lock
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    ConditionExprPtr expr = nullptr;
    if (!condition.empty()) {
        ConditionParser cp(condition);
        expr = cp.parse();
    }
    
    for (auto& row : rows) {
        if (!expr || expr->evaluate(row, columns)) {
            std::vector<std::string> newRow = row;
            bool rowChanged = false;
            
            for (const auto& update : updates) {
                auto it = std::find(columns.begin(), columns.end(), update.first);
                if (it != columns.end()) {
                    int index = std::distance(columns.begin(), it);
                    std::string newValue = update.second;
                    
                    // Enforce data type
                    enforceDataType(index, newValue);
                    
                    if (row[index] != newValue) {
                        newRow[index] = newValue;
                        rowChanged = true;
                    }
                }
            }
            
            if (rowChanged) {
                // Validate constraints for the new row
                try {
                    validateConstraintsForUpdate(row, newRow);
                    row = newRow;
                } catch (const ConstraintViolationException& e) {
                    // Skip this row if it violates constraints
                    std::cerr << "Warning: " << e.what() << " (row skipped)" << std::endl;
                }
            }
        }
    }
}

void Table::clearRows() {
    // Acquire exclusive lock
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    rows.clear();
}

// Join operations
std::vector<std::vector<std::string>> Table::innerJoin(
    Table& rightTable,
    const std::string& condition,
    const std::vector<std::string>& selectColumns) 
{
    // Acquire shared locks on both tables
    std::shared_lock<std::shared_mutex> lockLeft(mutex);
    std::shared_lock<std::shared_mutex> lockRight(rightTable.mutex);
    
    std::vector<std::vector<std::string>> result;
    
    // Prepare joined columns
    std::vector<std::string> joinedColumns = columns;
    for (const auto& col : rightTable.columns) {
        joinedColumns.push_back(col);
    }
    
    // Parse join condition
    ConditionParser cp(condition);
    auto expr = cp.parse();
    
    // Perform left outer join
    for (const auto& leftRow : rows) {
        bool foundMatch = false;
        
        for (const auto& rightRow : rightTable.rows) {
            // Create a combined row
            std::vector<std::string> combinedRow;
            combinedRow.insert(combinedRow.end(), leftRow.begin(), leftRow.end());
            combinedRow.insert(combinedRow.end(), rightRow.begin(), rightRow.end());
            
            // Evaluate condition
            if (expr->evaluate(combinedRow, joinedColumns)) {
                foundMatch = true;
                
                // Project columns
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
        
        // If no matching right row, add a row with NULLs for right table columns
        if (!foundMatch) {
            std::vector<std::string> combinedRow;
            combinedRow.insert(combinedRow.end(), leftRow.begin(), leftRow.end());
            
            // Add NULLs for right table columns
            for (size_t i = 0; i < rightTable.columns.size(); ++i) {
                combinedRow.push_back("");
            }
            
            // Project columns
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
    
    return result; joined columns
    std::vector<std::string> joinedColumns = columns;
    for (const auto& col : rightTable.columns) {
        joinedColumns.push_back(col);
    }
    
    // Parse join condition
    ConditionParser cp(condition);
    auto expr = cp.parse();
    
    // Perform nested-loop join
    for (const auto& leftRow : rows) {
        for (const auto& rightRow : rightTable.rows) {
            // Create a combined row
            std::vector<std::string> combinedRow;
            combinedRow.insert(combinedRow.end(), leftRow.begin(), leftRow.end());
            combinedRow.insert(combinedRow.end(), rightRow.begin(), rightRow.end());
            
            // Evaluate condition
            if (expr->evaluate(combinedRow, joinedColumns)) {
                // Project columns
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
    const std::vector<std::string>& selectColumns) 
{
    // Acquire shared locks on both tables
    std::shared_lock<std::shared_mutex> lockLeft(mutex);
    std::shared_lock<std::shared_mutex> lockRight(rightTable.mutex);
    
    std::vector<std::vector<std::string>> result;
    
    // Prepare
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
                } else {
                    pos = toUpperCase(token).find(" ASC");
                    if (pos != std::string::npos) {
                        colName = trim(token.substr(0, pos));
                    }
                }
                
                auto it = std::find(displayColumns.begin(), displayColumns.end(), colName);
                if (it != displayColumns.end()) {
                    int idx = std::distance(displayColumns.begin(), it);
                    if (idx < a.size() && idx < b.size()) {
                        if (a[idx] == b[idx]) {
                            continue;
                        }
                        
                        // Try numeric comparison first
                        try {
                            double aVal = std::stod(a[idx]);
                            double bVal = std::stod(b[idx]);
                            if (aVal != bVal) {
                                return desc ? (aVal > bVal) : (aVal < bVal);
                            }
                        } catch (...) {
                            // If not numeric, fall back to string comparison
                            return desc ? (a[idx] > b[idx]) : (a[idx] < b[idx]);
                        }
                    }
                }
            }
            return false;
        });
}
                        colName = trim(token.substr(0, pos));
                    } else {
                        pos = toUpperCase(token).find(" ASC");
                        if (pos != std::string::npos) {
                            colName = trim(token.substr(0, pos));
                        }
                    }
                    
                    auto it = std::find(columns.begin(), columns.end(), colName);
                    if (it != columns.end()) {
                        int idx = std::distance(columns.begin(), it);
                        if (a[idx] == b[idx]) {
                            continue;
                        }
                        
                        // Try numeric comparison first
                        try {
                            double aVal = std::stod(a[idx]);
                            double bVal = std::stod(b[idx]);
                            if (aVal != bVal) {
                                return desc ? (aVal > bVal) : (aVal < bVal);
                            }
                        } catch (...) {
                            // If not numeric, fall back to string comparison
                            return desc ? (a[idx] > b[idx]) : (a[idx] < b[idx]);
                        }
                    }
                }
                return false;
            });
    }
    
    // Project the columns for the result
    for (const auto& row : filteredRows) {
        std::vector<std::string> resultRow;
        for (const auto& col : displayColumns) {
            auto it = std::find(columns.begin(), columns.end(), col);
            if (it != columns.end()) {
                int idx = std::distance(columns.begin(), it);
                resultRow.push_back(row[idx]);
            } else {
                // Handle expressions or aliases
                resultRow.push_back("");
            }
        }
        resultRows.push_back(resultRow);
    }
    
    return resultRows;
    }
    
    // Handle GROUP BY clause
    if (!groupByColumns.empty()) {
        // Build groups based on the GROUP BY columns
        std::unordered_map<std::string, std::vector<std::vector<std::string>>> groups;
        
        for (const auto& row : filteredRows) {
            std::string key;
            for (const auto& grpCol : groupByColumns) {
                auto it = std::find(columns.begin(), columns.end(), grpCol);
                if (it != columns.end()) {
                    int idx = std::distance(columns.begin(), it);
                    key += row[idx] + "|";
                }
            }
            groups[key].push_back(row);
        }
        
        // Process HAVING clause
        if (!havingCondition.empty()) {
            std::unordered_map<std::string, std::vector<std::vector<std::string>>> filteredGroups;
            ConditionParser havingParser(havingCondition);
            auto havingExpr = havingParser.parse();
            
            for (const auto& group : groups) {
                // Build a representative row for the group to evaluate the HAVING condition
                std::vector<std::string> groupRow;
                if (!group.second.empty()) {
                    groupRow = group.second[0]; // Start with the first row
                    
                    // Apply aggregates
                    for (size_t i = 0; i < displayColumns.size(); ++i) {
                        std::string colExpr = displayColumns[i];
                        size_t pos1 = colExpr.find('(');
                        size_t pos2 = colExpr.find(')');
                        
                        if (pos1 != std::string::npos && pos2 != std::string::npos) {
                            std::string func = toUpperCase(trim(colExpr.substr(0, pos1)));
                            std::string colName = trim(colExpr.substr(pos1 + 1, pos2 - pos1 - 1));
                            
                            auto it = std::find(columns.begin(), columns.end(), colName);
                            if (it == columns.end() && colName != "*") {
                                continue;
                            }
                            
                            int idx = (colName == "*") ? -1 : std::distance(columns.begin(), it);
                            std::vector<std::string> colValues;
                            
                            if (idx >= 0) {
                                for (const auto& r : group.second) {
                                    colValues.push_back(r[idx]);
                                }
                            }
                            
                            if (func == "COUNT" && colName == "*") {
                                // Create a virtual column for COUNT(*)
                                if (groupRow.size() <= columns.size()) {
                                    groupRow.resize(columns.size() + 1);
                                }
                                groupRow.push_back(std::to_string(group.second.size()));
                            } else if (func == "AVG") {
                                double avg = Aggregation::computeMean(colValues);
                                // Create a virtual column for the aggregate
                                if (groupRow.size() <= columns.size()) {
                                    groupRow.resize(columns.size() + 1);
                                }
                                groupRow.push_back(std::to_string(avg));
                            } else if (func == "MIN") {
                                double min = Aggregation::computeMin(colValues);
                                if (groupRow.size() <= columns.size()) {
                                    groupRow.resize(columns.size() + 1);
                                }
                                groupRow.push_back(std::to_string(min));
                            } else if (func == "MAX") {
                                double max = Aggregation::computeMax(colValues);
                                if (groupRow.size() <= columns.size()) {
                                    groupRow.resize(columns.size() + 1);
                                }
                                groupRow.push_back(std::to_string(max));
                            } else if (func == "SUM") {
                                double sum = Aggregation::computeSum(colValues);
                                if (groupRow.size() <= columns.size()) {
                                    groupRow.resize(columns.size() + 1);
                                }
                                groupRow.push_back(std::to_string(sum));
                            }
                        }
                    }
                    
                    // Evaluate the HAVING condition using the augmented row
                    if (havingExpr->evaluate(groupRow, columns)) {
                        filteredGroups[group.first] = group.second;
                    }
                }
            }
            
            // Replace groups with filtered groups
            groups = std::move(filteredGroups);
        }
        
        // Build result rows from groups
        for (const auto& group : groups) {
            std::vector<std::string> resultRow;
            
            for (const auto& col : displayColumns) {
                size_t pos1 = col.find('(');
                size_t pos2 = col.find(')');
                
                if (pos1 != std::string::npos && pos2 != std::string::npos) {
                    // Aggregate function
                    std::string func = toUpperCase(trim(col.substr(0, pos1)));
                    std::string colName = trim(col.substr(pos1 + 1, pos2 - pos1 - 1));
                    
                    auto it = std::find(columns.begin(), columns.end(), colName);
                    if (it == columns.end() && colName != "*") {
                        resultRow.push_back("");
                        continue;
                    }
                    
                    int idx = (colName == "*") ? -1 : std::distance(columns.begin(), it);
                    std::vector<std::string> colValues;
                    
                    if (idx >= 0) {
                        for (const auto& row : group.second) {
                            colValues.push_back(row[idx]);
                        }
                    }
                    
                    if (func == "COUNT" && colName == "*") {
                        resultRow.push_back(std::to_string(group.second.size()));
                    } else if (func == "AVG") {
                        double avg = Aggregation::computeMean(colValues);
                        resultRow.push_back(std::to_string(avg));
                    } else if (func == "MIN") {
                        double min = Aggregation::computeMin(colValues);
                        resultRow.push_back(std::to_string(min));
                    } else if (func == "MAX") {
                        double max = Aggregation::computeMax(colValues);
                        resultRow.push_back(std::to_string(max));
                    } else if (func == "SUM") {
                        double sum = Aggregation::computeSum(colValues);
                        resultRow.push_back(std::to_string(sum));
                    } else if (func == "MEDIAN") {
                        std::string median = Aggregation::computeMedian(colValues);
                        resultRow.push_back(median);
                    } else if (func == "MODE") {
                        std::string mode = Aggregation::computeMode(colValues);
                        resultRow.push_back(mode);
                    } else {
                        resultRow.push_back(group.second.empty() ? "" : group.second[0][idx]);
                    }
                } else {
                    // Regular column - must be in GROUP BY
                    auto it = std::find(columns.begin(), columns.end(), col);
                    if (it != columns.end() && !group.second.empty()) {
                        int idx = std::distance(columns.begin(), it);
                        resultRow.push_back(group.second[0][idx]);
                    } else {
                        resultRow.push_back("");
                    }
                }
            }
            
            resultRows.push_back(resultRow);
        }
        
        // Sort the result if needed
        if (!orderByColumns.empty()) {
            sortResultRows(resultRows, displayColumns, orderByColumns);
        }
        
        return resultRows;