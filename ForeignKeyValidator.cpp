#include "ForeignKeyValidator.h"
#include "Table.h" // For Constraint definition
#include "Utils.h"
#include <iostream>
#include <algorithm>

void ForeignKeyValidator::registerTable(
    const std::string& tableName, 
    const std::vector<std::string>& columns,
    std::function<bool(const std::string&, const std::string&)> valueExists,
    std::function<std::vector<std::vector<std::string>>()> getAllRows
) {
    std::lock_guard<std::mutex> lock(mutex);
    
    std::string lowerName = toLowerCase(tableName);
    FKTableInfo info;
    info.tableName = tableName;
    info.columns = columns;
    info.valueExists = valueExists;
    info.getAllRows = getAllRows;
    tables[lowerName] = info;
    
    std::cout << "FKValidator: Registered table " << tableName << " with validator" << std::endl;
}

void ForeignKeyValidator::unregisterTable(const std::string& tableName) {
    std::lock_guard<std::mutex> lock(mutex);
    
    std::string lowerName = toLowerCase(tableName);
    tables.erase(lowerName);
    std::cout << "FKValidator: Unregistered table " << tableName << std::endl;
}

// Modified with better debugging and deadlock prevention
bool ForeignKeyValidator::validateForeignKey(
    const Constraint& constraint, 
    const std::vector<std::string>& row,
    const std::vector<std::string>& sourceColumns
) {
    std::cout << "FKValidator: Starting validation for " << constraint.name << std::endl;
    std::cout << std::flush;
    
    // Create a copy of the tables map to avoid holding the lock for too long
    std::unordered_map<std::string, FKTableInfo> tablesCopy;
    {
        // Smaller scope for the lock to minimize time held
        std::lock_guard<std::mutex> lock(mutex);
        std::cout << "FKValidator: Acquired mutex lock" << std::endl;
        std::cout << std::flush;
        
        // Get the referenced table info
        std::string lowerRefTable = toLowerCase(constraint.referencedTable);
        auto tableIt = tables.find(lowerRefTable);
        
        if (tableIt == tables.end()) {
            std::cout << "FKValidator: Referenced table not found: " << constraint.referencedTable << std::endl;
            std::cout << std::flush;
            return false;
        }
        
        // Make a copy of the needed table info
        tablesCopy[lowerRefTable] = tableIt->second;
    }
    
    std::cout << "FKValidator: Released mutex lock, continuing validation" << std::endl;
    std::cout << std::flush;
    
    // Continue validation with the copied data
    std::string lowerRefTable = toLowerCase(constraint.referencedTable);
    const FKTableInfo& refTableInfo = tablesCopy[lowerRefTable];
    
    // Extract FK values from the row
    std::vector<std::string> fkValues;
    std::vector<int> fkIndices;
    
    for (const auto& colName : constraint.columns) {
        std::cout << "FKValidator: Looking for column " << colName << " in source columns" << std::endl;
        std::cout << std::flush;
        
        auto colIt = std::find(sourceColumns.begin(), sourceColumns.end(), colName);
        if (colIt == sourceColumns.end()) {
            std::cout << "FKValidator: Column not found: " << colName << std::endl;
            std::cout << std::flush;
            return false;
        }
        
        int colIdx = std::distance(sourceColumns.begin(), colIt);
        if (colIdx >= row.size()) {
            std::cout << "FKValidator: Column index out of range" << std::endl;
            std::cout << std::flush;
            return false;
        }
        
        // Allow NULL values in FK
        if (row[colIdx].empty() || toLowerCase(row[colIdx]) == "null") {
            std::cout << "FKValidator: NULL value in FK (allowed)" << std::endl;
            std::cout << std::flush;
            return true;
        }
        
        fkValues.push_back(row[colIdx]);
        fkIndices.push_back(colIdx);
        
        std::cout << "FKValidator: Added FK value: " << row[colIdx] << std::endl;
        std::cout << std::flush;
    }
    
    // Method 1: Try the direct value check first for simple cases
    if (constraint.columns.size() == 1 && constraint.referencedColumns.size() == 1) {
        std::cout << "FKValidator: Trying direct value check for single column FK" << std::endl;
        std::cout << std::flush;
        
        bool exists = refTableInfo.valueExists(constraint.referencedColumns[0], fkValues[0]);
        if (exists) {
            std::cout << "FKValidator: Match found via direct check - constraint satisfied" << std::endl;
            std::cout << std::flush;
            return true;
        }
    }
    
    // Method 2: Use the getAllRows function to check for matches directly
    std::cout << "FKValidator: Getting all rows from referenced table" << std::endl;
    std::cout << std::flush;
    
    auto refRows = refTableInfo.getAllRows();
    std::cout << "FKValidator: Checking " << refRows.size() << " rows in referenced table" << std::endl;
    std::cout << std::flush;
    
    // Get PK indices from referenced table
    std::vector<int> pkIndices;
    for (const auto& colName : constraint.referencedColumns) {
        auto colIt = std::find(refTableInfo.columns.begin(), refTableInfo.columns.end(), colName);
        if (colIt == refTableInfo.columns.end()) {
            std::cout << "FKValidator: Referenced column not found: " << colName << std::endl;
            std::cout << std::flush;
            return false;
        }
        
        int colIdx = std::distance(refTableInfo.columns.begin(), colIt);
        pkIndices.push_back(colIdx);
        
        std::cout << "FKValidator: Found referenced column " << colName << " at index " << colIdx << std::endl;
        std::cout << std::flush;
    }
    
    for (const auto& refRow : refRows) {
        bool allMatch = true;
        
        for (size_t i = 0; i < fkValues.size(); i++) {
            int pkIdx = pkIndices[i];
            
            // Safety check for index bounds
            if (pkIdx >= refRow.size()) {
                std::cout << "FKValidator: PK index " << pkIdx << " out of range for row with size " << refRow.size() << std::endl;
                std::cout << std::flush;
                allMatch = false;
                break;
            }
            
            std::cout << "FKValidator: Comparing FK value '" << fkValues[i] 
                     << "' with PK value '" << refRow[pkIdx] << "'" << std::endl;
            std::cout << std::flush;
            
            if (fkValues[i] != refRow[pkIdx]) {
                allMatch = false;
                break;
            }
        }
        
        if (allMatch) {
            std::cout << "FKValidator: Match found - constraint satisfied" << std::endl;
            std::cout << std::flush;
            return true;
        }
    }
    
    std::cout << "FKValidator: No match found - constraint violated" << std::endl;
    std::cout << std::flush;
    return false;
}