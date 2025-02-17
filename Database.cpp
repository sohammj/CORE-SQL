#include "Database.h"
#include "Utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>

void Database::createTable(const std::string& tableName,
                             const std::vector<std::pair<std::string, std::string>>& cols) {
    Table table;
    for (const auto& col : cols) {
        table.addColumn(col.first, col.second);
    }
    tables[toLowerCase(tableName)] = table;
    std::cout << "Table " << tableName << " created." << std::endl;
}

void Database::dropTable(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.erase(lowerName))
        std::cout << "Table " << tableName << " dropped." << std::endl;
    else
        std::cout << "Table " << tableName << " does not exist." << std::endl;
}

void Database::alterTableAddColumn(const std::string& tableName, const std::pair<std::string, std::string>& column) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    tables[lowerName].addColumn(column.first, column.second);
    std::cout << "Column " << column.first << " added to " << tableName << "." << std::endl;
}

void Database::alterTableDropColumn(const std::string& tableName, const std::string& columnName) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    tables[lowerName].dropColumn(columnName);
    std::cout << "Column " << columnName << " dropped from " << tableName << "." << std::endl;
}

void Database::describeTable(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    std::cout << "Schema for " << tableName << ":" << std::endl;
    // For simplicity, we print the header from the Table.
    // (A real implementation would also show column types.)
    const auto& cols = tables[lowerName].getColumns();
    for (const auto& col : cols)
        std::cout << col << "\t";
    std::cout << std::endl;
}

void Database::insertRecord(const std::string& tableName,
                              const std::vector<std::vector<std::string>>& values) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    for (const auto& valueSet : values) {
        tables[lowerName].addRow(valueSet);
    }
    std::cout << "Record(s) inserted into " << tableName << "." << std::endl;
}

// A very simple join: only supports inner join on equality between one column from each table.
void Database::selectRecords(const std::string& tableName,
                             const std::vector<std::string>& selectColumns,
                             const std::string& condition,
                             const std::vector<std::string>& orderByColumns,
                             const std::vector<std::string>& groupByColumns,
                             const std::string& havingCondition,
                             bool isJoin,
                             const std::string& joinTable,
                             const std::string& joinCondition) {
    if (!isJoin) {
        std::string lowerName = toLowerCase(tableName);
        if (tables.find(lowerName) == tables.end()) {
            std::cout << "Table " << tableName << " does not exist." << std::endl;
            return;
        }
        tables[lowerName].selectRows(selectColumns, condition, orderByColumns, groupByColumns, havingCondition);
    } else {
        // JOIN implementation
        std::string leftName = toLowerCase(tableName);
        std::string rightName = toLowerCase(joinTable);
        if (tables.find(leftName) == tables.end() || tables.find(rightName) == tables.end()) {
            std::cout << "One or both tables in JOIN do not exist." << std::endl;
            return;
        }
        // Parse joinCondition: expecting format "table1.col = table2.col"
        size_t eqPos = joinCondition.find('=');
        if (eqPos == std::string::npos) {
            std::cout << "Invalid join condition." << std::endl;
            return;
        }
        std::string leftJoin = trim(joinCondition.substr(0, eqPos));
        std::string rightJoin = trim(joinCondition.substr(eqPos + 1));
        // Extract column names (optionally strip table qualifiers)
        size_t dotPos = leftJoin.find('.');
        if (dotPos != std::string::npos)
            leftJoin = leftJoin.substr(dotPos + 1);
        dotPos = rightJoin.find('.');
        if (dotPos != std::string::npos)
            rightJoin = rightJoin.substr(dotPos + 1);
        
        // Retrieve both tables.
        Table& leftTable = tables[leftName];
        Table& rightTable = tables[rightName];
        const auto& leftCols = leftTable.getColumns();
        const auto& rightCols = rightTable.getColumns();
        // Find indices.
        auto lit = std::find(leftCols.begin(), leftCols.end(), leftJoin);
        auto rit = std::find(rightCols.begin(), rightCols.end(), rightJoin);
        if (lit == leftCols.end() || rit == rightCols.end()) {
            std::cout << "Join columns not found." << std::endl;
            return;
        }
        int leftIdx = std::distance(leftCols.begin(), lit);
        int rightIdx = std::distance(rightCols.begin(), rit);
        
        // Perform join: for each row in leftTable, match rows in rightTable.
        std::vector<std::vector<std::string>> joinResult;
        // Prepare combined header.
        std::vector<std::string> combinedHeader = leftCols;
        for (const auto& col : rightCols)
            combinedHeader.push_back(col);
        
        // For simplicity, we ignore join-level filtering aside from the equality.
        // (You can extend this by integrating the ConditionParser for combined rows.)
        // (Also, here we do a nested loop join.)
        // Assuming we had a way to get all rows from a table (we would need to add an accessor).
        // For demonstration, we assume that Table::printTable() prints all rows;
        // here we simulate the join by reusing the internal data (not ideal in production code).
        // ---
        // In our toy example, we will simply print a message.
        std::cout << "JOIN functionality not fully implemented. (This is a stub.)" << std::endl;
    }
}

void Database::deleteRecords(const std::string& tableName, const std::string& condition) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    tables[lowerName].deleteRows(condition);
    std::cout << "Records deleted from " << tableName << "." << std::endl;
}

void Database::updateRecords(const std::string& tableName,
                             const std::vector<std::pair<std::string, std::string>>& updates,
                             const std::string& condition) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    tables[lowerName].updateRows(updates, condition);
    std::cout << "Records updated in " << tableName << "." << std::endl;
}

void Database::showTables() {
    std::cout << "Available Tables:" << std::endl;
    for (const auto& pair : tables)
        std::cout << pair.first << std::endl;
}

// Transaction commands (simulated)
void Database::beginTransaction() {
    std::cout << "Transaction started." << std::endl;
}
void Database::commitTransaction() {
    std::cout << "Transaction committed." << std::endl;
}
void Database::rollbackTransaction() {
    std::cout << "Transaction rolled back." << std::endl;
}
