#include "Database.h"
#include "Utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>

void Database::createTable(const std::string& tableName,
    const std::vector<std::pair<std::string, std::string>>& cols) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) != tables.end()) {
        std::cout << "Error: Table " << tableName << " already exists." << std::endl;
        return;
    }
    Table table;
    for (const auto& col : cols) {
        table.addColumn(col.first, col.second);
    }
    tables[lowerName] = table;
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
        size_t eqPos = joinCondition.find('=');
        if (eqPos == std::string::npos) {
            std::cout << "Invalid join condition." << std::endl;
            return;
        }
        std::string leftJoin = trim(joinCondition.substr(0, eqPos));
        std::string rightJoin = trim(joinCondition.substr(eqPos + 1));
        size_t dotPos = leftJoin.find('.');
        if (dotPos != std::string::npos)
            leftJoin = leftJoin.substr(dotPos + 1);
        dotPos = rightJoin.find('.');
        if (dotPos != std::string::npos)
            rightJoin = rightJoin.substr(dotPos + 1);
        
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
        
        std::vector<std::vector<std::string>> joinResult;
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
        
        std::vector<std::string> displayColumns;
        if (selectColumns.size() == 1 && selectColumns[0] == "*")
            displayColumns = combinedHeader;
        else
            displayColumns = selectColumns;
        
        std::vector<std::string> processedDisplayColumns;
        for (const auto& col : displayColumns) {
            std::string processed = col;
            size_t dotPos = processed.find('.');
            if (dotPos != std::string::npos)
                processed = processed.substr(dotPos + 1);
            processedDisplayColumns.push_back(processed);
        }
        
        for (const auto& col : displayColumns)
            std::cout << col << "\t";
        std::cout << std::endl;
        
        for (const auto& row : joinResult) {
            for (const auto& procCol : processedDisplayColumns) {
                auto it = std::find(combinedHeader.begin(), combinedHeader.end(), procCol);
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

// Transaction functions
void Database::beginTransaction() {
    if (!inTransaction) {
        backupTables = tables;
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
        tables = backupTables;
        backupTables.clear();
        inTransaction = false;
    }
    std::cout << "Transaction rolled back." << std::endl;
}

// New functionalities

void Database::truncateTable(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    tables[lowerName].clearRows();
    std::cout << "Table " << tableName << " truncated." << std::endl;
}

void Database::renameTable(const std::string& oldName, const std::string& newName) {
    std::string lowerOld = toLowerCase(oldName);
    std::string lowerNew = toLowerCase(newName);
    if (tables.find(lowerOld) == tables.end()) {
        std::cout << "Table " << oldName << " does not exist." << std::endl;
        return;
    }
    tables[lowerNew] = tables[lowerOld];
    tables.erase(lowerOld);
    std::cout << "Table " << oldName << " renamed to " << newName << "." << std::endl;
}

void Database::createIndex(const std::string& indexName, const std::string& tableName, const std::string& columnName) {
    std::string lowerTable = toLowerCase(tableName);
    if (tables.find(lowerTable) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    indexes[toLowerCase(indexName)] = {lowerTable, columnName};
    std::cout << "Index " << indexName << " created on " << tableName << "(" << columnName << ")." << std::endl;
}

void Database::dropIndex(const std::string& indexName) {
    std::string lowerIndex = toLowerCase(indexName);
    if (indexes.erase(lowerIndex))
        std::cout << "Index " << indexName << " dropped." << std::endl;
    else
        std::cout << "Index " << indexName << " does not exist." << std::endl;
}

void Database::mergeRecords(const std::string& tableName, const std::string& mergeCommand) {
    std::cout << "MERGE command received for table " << tableName << ": " << mergeCommand << std::endl;
}

void Database::replaceInto(const std::string& tableName, const std::vector<std::vector<std::string>>& values) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    for (const auto& row : values) {
        bool replaced = false;
        for (auto& existingRow : tables[lowerName].getRowsNonConst()) {
            if (!existingRow.empty() && !row.empty() && existingRow[0] == row[0]) {
                existingRow = row;
                replaced = true;
                break;
            }
        }
        if (!replaced) {
            tables[lowerName].addRow(row);
        }
    }
    std::cout << "REPLACE INTO executed on " << tableName << "." << std::endl;
}