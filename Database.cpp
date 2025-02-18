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
    bool success = tables[lowerName].dropColumn(columnName);
    if (success)
        std::cout << "Column " << columnName << " dropped from " << tableName << "." << std::endl;
    else
        std::cout << "Column " << columnName << " does not exist in " << tableName << "." << std::endl;
}

void Database::describeTable(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    std::cout << "Schema for " << tableName << ":" << std::endl;
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
        // JOIN implementation (nested-loop inner join)
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
        // Remove table qualifiers if present.
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
        auto lit = std::find(leftCols.begin(), leftCols.end(), leftJoin);
        auto rit = std::find(rightCols.begin(), rightCols.end(), rightJoin);
        if (lit == leftCols.end() || rit == rightCols.end()) {
            std::cout << "Join columns not found." << std::endl;
            return;
        }
        int leftIdx = std::distance(leftCols.begin(), lit);
        int rightIdx = std::distance(rightCols.begin(), rit);
        
        // Perform join.
        std::vector<std::vector<std::string>> joinResult;
        // Combined header is concatenation of left and right headers.
        std::vector<std::string> combinedHeader = leftCols;
        for (const auto& col : rightCols)
            combinedHeader.push_back(col);
        
        const auto& leftRows = leftTable.getRows();
        const auto& rightRows = rightTable.getRows();
        for (const auto& lrow : leftRows) {
            for (const auto& rrow : rightRows) {
                if (lrow[leftIdx] == rrow[rightIdx]) {
                    std::vector<std::string> combinedRow = lrow;
                    combinedRow.insert(combinedRow.end(), rrow.begin(), rrow.end());
                    joinResult.push_back(combinedRow);
                }
            }
        }
        
        // Determine which columns to display.
        std::vector<std::string> displayColumns;
        if (selectColumns.size() == 1 && selectColumns[0] == "*")
            displayColumns = combinedHeader;
        else
            displayColumns = selectColumns;
        
        // Print header.
        for (const auto& col : displayColumns)
            std::cout << col << "\t";
        std::cout << std::endl;
        // Print joined rows.
        for (const auto& row : joinResult) {
            for (const auto& col : displayColumns) {
                auto it = std::find(combinedHeader.begin(), combinedHeader.end(), col);
                if (it != combinedHeader.end()) {
                    int idx = std::distance(combinedHeader.begin(), it);
                    std::cout << row[idx] << "\t";
                }
            }
            std::cout << std::endl;
        }
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

// Transaction functions with basic backup/rollback.
void Database::beginTransaction() {
    if (!inTransaction) {
        backupTables = tables; // Save backup copy.
        inTransaction = true;
    }
    std::cout << "Transaction started." << std::endl;
}

void Database::commitTransaction() {
    if (inTransaction) {
        inTransaction = false;
        backupTables.clear();
    }
    std::cout << "Transaction committed." << std::endl;
}

void Database::rollbackTransaction() {
    if (inTransaction) {
        tables = backupTables; // Restore backup.
        backupTables.clear();
        inTransaction = false;
    }
    std::cout << "Transaction rolled back." << std::endl;
}
