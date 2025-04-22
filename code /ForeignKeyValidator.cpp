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
    
    
}
void ForeignKeyValidator::unregisterTable(const std::string& tableName) {
    std::lock_guard<std::mutex> lock(mutex);
    
    std::string lowerName = toLowerCase(tableName);
    tables.erase(lowerName);
    
}
// Modified with better debugging and deadlock prevention
bool ForeignKeyValidator::validateForeignKey(
    const Constraint& constraint, 
    const std::vector<std::string>& row,
    const std::vector<std::string>& sourceColumns
) {
    
    
    
    // Create a copy of the tables map to avoid holding the lock for too long
    std::unordered_map<std::string, FKTableInfo> tablesCopy;
    {
        // Smaller scope for the lock to minimize time held
        std::lock_guard<std::mutex> lock(mutex);
        
        
        
        // Get the referenced table info
        std::string lowerRefTable = toLowerCase(constraint.referencedTable);
        auto tableIt = tables.find(lowerRefTable);
        
        if (tableIt == tables.end()) {
            
            
            return false;
        }
        
        // Make a copy of the needed table info
        tablesCopy[lowerRefTable] = tableIt->second;
    }
    
    
    
    
    // Continue validation with the copied data
    std::string lowerRefTable = toLowerCase(constraint.referencedTable);
    const FKTableInfo& refTableInfo = tablesCopy[lowerRefTable];
    
    // Extract FK values from the row
    std::vector<std::string> fkValues;
    std::vector<int> fkIndices;
    
    for (const auto& colName : constraint.columns) {
        
        
        
        auto colIt = std::find(sourceColumns.begin(), sourceColumns.end(), colName);
        if (colIt == sourceColumns.end()) {
            
            
            return false;
        }
        
        int colIdx = std::distance(sourceColumns.begin(), colIt);
        if (colIdx >= row.size()) {
            
            
            return false;
        }
        
        // Allow NULL values in FK
        if (row[colIdx].empty() || toLowerCase(row[colIdx]) == "null") {
            
            
            return true;
        }
        
        fkValues.push_back(row[colIdx]);
        fkIndices.push_back(colIdx);
        
        
        
    }
    
    // Method 1: Try the direct value check first for simple cases
    if (constraint.columns.size() == 1 && constraint.referencedColumns.size() == 1) {
        
        
        
        bool exists = refTableInfo.valueExists(constraint.referencedColumns[0], fkValues[0]);
        if (exists) {
            
            
            return true;
        }
    }
    
    // Method 2: Use the getAllRows function to check for matches directly
    
    
    
    auto refRows = refTableInfo.getAllRows();
    
    
    
    // Get PK indices from referenced table
    std::vector<int> pkIndices;
    for (const auto& colName : constraint.referencedColumns) {
        auto colIt = std::find(refTableInfo.columns.begin(), refTableInfo.columns.end(), colName);
        if (colIt == refTableInfo.columns.end()) {
            
            
            return false;
        }
        
        int colIdx = std::distance(refTableInfo.columns.begin(), colIt);
        pkIndices.push_back(colIdx);
        
        
        
    }
    
    for (const auto& refRow : refRows) {
        bool allMatch = true;
        
        for (size_t i = 0; i < fkValues.size(); i++) {
            int pkIdx = pkIndices[i];
            
            // Safety check for index bounds
            if (pkIdx >= refRow.size()) {
                
                
                allMatch = false;
                break;
            }
            
            
                    
            
            
            if (fkValues[i] != refRow[pkIdx]) {
                allMatch = false;
                break;
            }
        }
        
        if (allMatch) {
            
            
            return true;
        }
    }
    
    
    
    return false;
}