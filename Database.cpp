#include "Database.h"
#include "Utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include "ConditionParser.h"
#include "Parser.h"

// Create table
void Database::createTable(const std::string& tableName,
    const std::vector<std::pair<std::string, std::string>>& cols,
    const std::vector<Constraint>& constraints) {
    
    std::cout << "Debug: Starting creation of table " << tableName << " with " << cols.size() << " columns" << std::endl;
    std::cout << std::flush;
    
    std::unique_lock<std::mutex> lock(databaseMutex);
    std::cout << "Debug: Acquired mutex lock" << std::endl;
    std::cout << std::flush;
    
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) != tables.end()) {
        throw DatabaseException("Table '" + tableName + "' already exists");
    }
    
    std::cout << "Debug: Table name check passed" << std::endl;
    std::cout << std::flush;
    
    // Create the table using make_unique
    auto table = std::make_unique<Table>(tableName);
    
    std::cout << "Debug: Table object created" << std::endl;
    std::cout << std::flush;
    
    // Add columns
    for (const auto& col : cols) {
        table->addColumn(col.first, col.second);
        std::cout << "Debug: Added column " << col.first << std::endl;
        std::cout << std::flush;
    }
    
    // Add constraints
    for (const auto& constraint : constraints) {
        try {
            std::cout << "Debug: Validating constraint" << std::endl;
            std::cout << std::flush;
            validateReferences(constraint);
            std::cout << "Debug: Adding constraint to table" << std::endl;
            std::cout << std::flush;
            table->addConstraint(constraint);
        } catch (const DatabaseException& e) {
            std::cout << "Debug: Constraint error: " << e.what() << std::endl;
            std::cout << std::flush;
            throw DatabaseException("Failed to create table '" + tableName + "': " + e.what());
        }
    }
    
    std::cout << "Debug: Adding table to collection" << std::endl;
    std::cout << std::flush;
    tables[lowerName] = std::move(table);
    std::cout << "Table " << tableName << " created." << std::endl;
    std::cout << std::flush;
}

void Database::dropTable(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.erase(lowerName)){
        std::cout << "Table " << tableName << " dropped." << std::endl;
        std::cout << std::flush;}
    else{
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        std::cout << std::flush;}
}

void Database::alterTableAddColumn(const std::string& tableName, const std::pair<std::string, std::string>& column, bool isNotNull) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        std::cout << std::flush;
        return;
    }
    tables[lowerName]->addColumn(column.first, column.second);
    std::cout << "Column " << column.first << " added to " << tableName << "." << std::endl;
    std::cout << std::flush;
}

void Database::alterTableDropColumn(const std::string& tableName, const std::string& columnName) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        std::cout << std::flush;
        return;
    }
    bool success = tables[lowerName]->dropColumn(columnName);
    if (success){
        std::cout << "Column " << columnName << " dropped from " << tableName << "." << std::endl;
        std::cout << std::flush;}
    else{
        std::cout << "Column " << columnName << " does not exist in " << tableName << "." << std::endl;
        std::cout << std::flush;}
}

void Database::describeTable(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        std::cout << std::flush;
        return;
    }
    std::cout << "Schema for " << tableName << ":" << std::endl;
    std::cout << std::flush;
    const auto& cols = tables[lowerName]->getColumns();
    for (const auto& col : cols)
        std::cout << col << "\t";
    std::cout << std::endl;
}

void Database::insertRecord(const std::string& tableName,
                              const std::vector<std::vector<std::string>>& values) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        std::cout << std::flush;
        return;
    }
    for (const auto& valueSet : values) {
        tables[lowerName]->addRow(valueSet);
    }
    std::cout << "Record(s) inserted into " << tableName << "." << std::endl;
    std::cout << std::flush;
}

// In Database.cpp, fix the selectRecords method
void Database::selectRecords(const std::string& tableName,
    const std::vector<std::string>& selectColumns,
    const std::string& condition,
    const std::vector<std::string>& orderByColumns,
    const std::vector<std::string>& groupByColumns,
    const std::string& havingCondition,
    bool isJoin,
    const std::string& joinTable,
    const std::string& joinCondition,
    const std::string& joinType) {
    
    // Check if this is a view
    std::string lowerName = toLowerCase(tableName);
    if (views.find(lowerName) != views.end()) {
        try {
            // Execute the view query
            auto result = executeViewQuery(tableName);
            
            // Get the column names from the view definition
            Parser parser;
            Query query = parser.parseQuery(views[lowerName]);
            std::vector<std::string> viewColumns = query.selectColumns;
            
            // Print header
            for (const auto& col : selectColumns == std::vector<std::string>{"*"} ? viewColumns : selectColumns) {
                std::cout << col << "\t";
            }
            std::cout << "\n";
            
            // Print each row
            for (const auto& row : result) {
                for (const auto& val : row) {
                    std::cout << val << "\t";
                }
                std::cout << "\n";
            }
            return;
        } catch (const std::exception& e) {
            std::cout << "Error executing view: " << e.what() << std::endl;
            return;
        }
    }
    
    if (!isJoin) {
        std::string lowerName = toLowerCase(tableName);
        if (tables.find(lowerName) == tables.end()) {
            std::cout << "Table " << tableName << " does not exist." << std::endl;
            std::cout << std::flush;
            return;
        }
        auto result = tables[lowerName]->selectRows(selectColumns, condition, orderByColumns, groupByColumns, havingCondition);

        // Get the column names to print headers
        const auto& columns = tables[lowerName]->getColumns();

        // Print header
        for (const auto& col : selectColumns == std::vector<std::string>{"*"} ? columns : selectColumns) {
            std::cout << col << "\t";
        }
        std::cout << "\n";

        // Print each row
        for (const auto& row : result) {
            for (const auto& val : row) {
                std::cout << val << "\t";
            }
            std::cout << "\n";
        }
    } else { // JOIN implementation
        std::string leftName = toLowerCase(tableName);
        std::string rightName = toLowerCase(joinTable);
    
        if (tables.find(leftName) == tables.end()) {
            std::cout << "Table '" << tableName << "' in JOIN does not exist." << std::endl;
            return;
        }
        
        if (tables.find(rightName) == tables.end()) {
            std::cout << "Table '" << joinTable << "' in JOIN does not exist." << std::endl;
            return;
        }
    
        // Parse JOIN condition to get left and right column names
        std::string leftCol, rightCol;
        std::string leftAlias, rightAlias;
        
        // Handle different join syntax types
        if (joinCondition.find('=') != std::string::npos) {
            // ON syntax: "table1.col1 = table2.col2"
            size_t eqPos = joinCondition.find('=');
            std::string leftExpr = trim(joinCondition.substr(0, eqPos));
            std::string rightExpr = trim(joinCondition.substr(eqPos + 1));
            
            // Extract table aliases and column names
            size_t leftDotPos = leftExpr.find('.');
            if (leftDotPos != std::string::npos) {
                leftAlias = trim(leftExpr.substr(0, leftDotPos));
                leftCol = trim(leftExpr.substr(leftDotPos + 1));
            } else {
                leftCol = leftExpr;
            }
            
            size_t rightDotPos = rightExpr.find('.');
            if (rightDotPos != std::string::npos) {
                rightAlias = trim(rightExpr.substr(0, rightDotPos));
                rightCol = trim(rightExpr.substr(rightDotPos + 1));
            } else {
                rightCol = rightExpr;
            }
        } else if (joinCondition.find("USING") != std::string::npos) {
            // USING syntax: "USING (col)"
            std::regex usingRegex(R"(USING\s*\(\s*([^)]+)\s*\))");
            std::smatch match;
            if (std::regex_search(joinCondition, match, usingRegex) && match.size() > 1) {
                leftCol = rightCol = trim(match[1].str());
            } else {
                std::cout << "Invalid USING clause in JOIN." << std::endl;
                return;
            }
        } else {
            std::cout << "Invalid JOIN condition format." << std::endl;
            return;
        }
        
        // Get references to tables
        Table* leftTable = tables[leftName].get();
        Table* rightTable = tables[rightName].get();
        
        // Get column indices for join columns
        int leftColIdx = leftTable->getColumnIndex(leftCol);
        int rightColIdx = rightTable->getColumnIndex(rightCol);
        
        if (leftColIdx == -1) {
            std::cout << "Column '" << leftCol << "' not found in table '" << tableName << "'." << std::endl;
            return;
        }
        
        if (rightColIdx == -1) {
            std::cout << "Column '" << rightCol << "' not found in table '" << joinTable << "'." << std::endl;
            return;
        }
        
        // Create combined column list for result display
        std::vector<std::string> resultColumns;
        
        for (const auto& col : selectColumns) {
            if (col == "*") {
                // Expand * to all columns from both tables
                for (const auto& lcol : leftTable->getColumns()) {
                    resultColumns.push_back(leftAlias.empty() ? lcol : (leftAlias + "." + lcol));
                }
                for (const auto& rcol : rightTable->getColumns()) {
                    resultColumns.push_back(rightAlias.empty() ? rcol : (rightAlias + "." + rcol));
                }
            } else {
                resultColumns.push_back(col);
            }
        }
        
        // Print header
        for (const auto& col : resultColumns) {
            std::cout << col << "\t";
        }
        std::cout << std::endl;
        
        // Perform the appropriate join
        std::vector<std::vector<std::string>> joinResult;
        
        if (toUpperCase(joinType) == "INNER") {
            joinResult = leftTable->innerJoin(*rightTable, joinCondition, resultColumns);
        } else if (toUpperCase(joinType) == "LEFT" || toUpperCase(joinType) == "LEFT OUTER") {
            joinResult = leftTable->leftOuterJoin(*rightTable, joinCondition, resultColumns);
        } else if (toUpperCase(joinType) == "RIGHT" || toUpperCase(joinType) == "RIGHT OUTER") {
            joinResult = rightTable->leftOuterJoin(*leftTable, joinCondition, resultColumns);
        } else if (toUpperCase(joinType) == "FULL" || toUpperCase(joinType) == "FULL OUTER") {
            joinResult = leftTable->fullOuterJoin(*rightTable, joinCondition, resultColumns);
        } else if (toUpperCase(joinType) == "NATURAL") {
            joinResult = leftTable->naturalJoin(*rightTable, resultColumns);
        } else {
            // Default to inner join
            joinResult = leftTable->innerJoin(*rightTable, joinCondition, resultColumns);
        }
        
        // Print results
        for (const auto& row : joinResult) {
            for (const auto& value : row) {
                std::cout << value << "\t";
            }
            std::cout << std::endl;
        }
    }
}
















void Database::deleteRecords(const std::string& tableName, const std::string& condition) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        std::cout << std::flush;
        return;
    }
    tables[lowerName]->deleteRows(condition);
    std::cout << "Records deleted from " << tableName << "." << std::endl;
    std::cout << std::flush;
}

void Database::updateRecords(const std::string& tableName,
                             const std::vector<std::pair<std::string, std::string>>& updates,
                             const std::string& condition) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        std::cout << std::flush;
        return;
    }
    tables[lowerName]->updateRows(updates, condition);
    std::cout << "Records updated in " << tableName << "." << std::endl;
    std::cout << std::flush;
}

void Database::showTables() {
    std::cout << "Available Tables:" << std::endl;
    std::cout << std::flush;
    for (const auto& pair : tables)
        std::cout << pair.first << std::endl;
}

// Transaction functions
// In Database.cpp, fix the beginTransaction method
// Fix for Database.cpp: beginTransaction method
Transaction* Database::beginTransaction() {
    std::unique_lock<std::mutex> lock(databaseMutex);
    std::cout << "Debug: beginTransaction called. Current inTransaction state: " 
              << (inTransaction ? "true" : "false") << std::endl;
    
    if (inTransaction) {
        std::cout << "Transaction already in progress." << std::endl;
        return nullptr;
    }
    
    // Set transaction flag
    inTransaction = true;
    
    // Create backup of tables for potential rollback
    backupTables.clear();
    for (const auto& [name, tablePtr] : tables) {
        auto tableCopy = std::make_unique<Table>(tablePtr->getName());
        
        // Copy columns and constraints
        const auto& columns = tablePtr->getColumns();
        const auto& columnTypes = tablePtr->getColumnTypes();
        const auto& notNullConstraints = tablePtr->getNotNullConstraints();
        
        for (size_t i = 0; i < columns.size(); ++i) {
            tableCopy->addColumn(columns[i], columnTypes[i], notNullConstraints[i]);
        }
        
        // Copy constraints
        for (const auto& constraint : tablePtr->getConstraints()) {
            tableCopy->addConstraint(constraint);
        }
        
        // Copy rows
        for (const auto& row : tablePtr->getRows()) {
            tableCopy->addRow(row);
        }
        
        backupTables[name] = std::move(tableCopy);
    }
    
    std::cout << "Transaction started." << std::endl;
    return nullptr;  // We're not actually creating Transaction objects
}

// Fix for commitTransaction
Transaction* Database::commitTransaction() {
    std::unique_lock<std::mutex> lock(databaseMutex);
    std::cout << "Debug: commitTransaction called. Current inTransaction state: " 
              << (inTransaction ? "true" : "false") << std::endl;
    
    if (!inTransaction) {
        std::cout << "Error: No active transaction to commit" << std::endl;
        return nullptr;
    }
    
    // On commit, we just discard the backups
    backupTables.clear();
    
    // Clear transaction flag
    inTransaction = false;
    
    std::cout << "Transaction committed." << std::endl;
    return nullptr;
}

// Fix for rollbackTransaction
Transaction* Database::rollbackTransaction() {
    std::unique_lock<std::mutex> lock(databaseMutex);
    std::cout << "Debug: rollbackTransaction called. Current inTransaction state: " 
              << (inTransaction ? "true" : "false") << std::endl;
    if (!inTransaction) {
        std::cout << "Error: No active transaction to rollback" << std::endl;
        return nullptr;
    }
    
    // Restore from backups
    tables = std::move(backupTables);
    backupTables.clear();
    
    // Clear transaction flag
    inTransaction = false;
    
    std::cout << "Transaction rolled back." << std::endl;
    return nullptr;
}








// New functionalities

void Database::truncateTable(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        std::cout << std::flush;
        return;
    }
    tables[lowerName]->clearRows();
    std::cout << "Table " << tableName << " truncated." << std::endl;
    std::cout << std::flush;
}

void Database::renameTable(const std::string& oldName, const std::string& newName) {
    std::string lowerOld = toLowerCase(oldName);
    std::string lowerNew = toLowerCase(newName);
    if (tables.find(lowerOld) == tables.end()) {
        std::cout << "Table " << oldName << " does not exist." << std::endl;
        std::cout << std::flush;
        return;
    }
    tables[lowerNew] = std::move(tables[lowerOld]);
    tables.erase(lowerOld);
    std::cout << "Table " << oldName << " renamed to " << newName << "." << std::endl;
    std::cout << std::flush;
}

void Database::createIndex(const std::string& indexName, const std::string& tableName, const std::string& columnName) {
    std::string lowerTable = toLowerCase(tableName);
    if (tables.find(lowerTable) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        std::cout << std::flush;
        return;
    }
    indexes[toLowerCase(indexName)] = {lowerTable, columnName};
    std::cout << "Index " << indexName << " created on " << tableName << "(" << columnName << ")." << std::endl;
    std::cout << std::flush;
}

void Database::dropIndex(const std::string& indexName) {
    std::string lowerIndex = toLowerCase(indexName);
    if (indexes.erase(lowerIndex)){
        std::cout << "Index " << indexName << " dropped." << std::endl;
        std::cout << std::flush;}
    else{
        std::cout << "Index " << indexName << " does not exist." << std::endl;
        std::cout << std::flush;}
}

void Database::mergeRecords(const std::string& tableName, const std::string& mergeCommand) {
    // --- Step 1: Locate key clauses ---
    // Expected syntax:
    // MERGE INTO tableName USING (SELECT ... AS col, ... ) AS src
    // ON tableName.col = src.col
    // WHEN MATCHED THEN UPDATE SET col = <value>, ...
    // WHEN NOT MATCHED THEN INSERT VALUES (<value>, ...);
    std::string commandUpper = toUpperCase(mergeCommand);
    size_t usingPos = commandUpper.find("USING");
    size_t onPos = commandUpper.find("ON");
    size_t whenMatchedPos = commandUpper.find("WHEN MATCHED THEN UPDATE SET");
    size_t whenNotMatchedPos = commandUpper.find("WHEN NOT MATCHED THEN INSERT VALUES");
    
    if (usingPos == std::string::npos || onPos == std::string::npos ||
        whenMatchedPos == std::string::npos || whenNotMatchedPos == std::string::npos) {
        std::cout << "MERGE: Invalid MERGE syntax." << std::endl;
        std::cout << std::flush;
        return;
    }
    
    // --- Step 2: Extract the source subquery from the USING clause ---
    size_t sourceStart = mergeCommand.find("(", usingPos);
    size_t sourceEnd = mergeCommand.find(")", sourceStart);
    if (sourceStart == std::string::npos || sourceEnd == std::string::npos) {
        std::cout << "MERGE: Invalid source subquery syntax." << std::endl;
        std::cout << std::flush;
        return;
    }
    std::string sourceSubquery = mergeCommand.substr(sourceStart + 1, sourceEnd - sourceStart - 1);
    // Expect the subquery to start with SELECT.
    size_t selectPos = toUpperCase(sourceSubquery).find("SELECT");
    if (selectPos == std::string::npos) {
        std::cout << "MERGE: Source subquery must start with SELECT." << std::endl;
        std::cout << std::flush;
        return;
    }
    std::string selectExpressions = sourceSubquery.substr(selectPos + 6);
    // (For simplicity, we assume the SELECT returns a comma‑separated list of expressions)
    std::vector<std::string> exprList = split(selectExpressions, ',');
    // Build a source record map: alias (lowercase) -> literal value.
    std::unordered_map<std::string, std::string> srcRecord;
    for (const auto &expr : exprList) {
        std::string trimmedExpr = trim(expr);
        size_t asPos = toUpperCase(trimmedExpr).find("AS");
        if (asPos == std::string::npos)
            continue;
        std::string literal = trim(trimmedExpr.substr(0, asPos));
        std::string alias = trim(trimmedExpr.substr(asPos + 2));
        // Remove quotes if the literal is quoted.
        if (!literal.empty() && literal.front() == '\'' && literal.back() == '\'' && literal.size() > 1)
            literal = literal.substr(1, literal.size() - 2);
        srcRecord[toLowerCase(alias)] = literal;
    }
    
    // --- Step 3: Parse the ON clause ---
    // Expected format: ON tableName.col = src.col
    std::string onClause = mergeCommand.substr(onPos, whenMatchedPos - onPos);
    // Remove the leading "ON"
    onClause = trim(onClause.substr(2));
    size_t eqPos = onClause.find('=');
    if (eqPos == std::string::npos) {
        std::cout << "MERGE: Invalid ON clause." << std::endl;
        std::cout << std::flush;
        return;
    }
    std::string targetExpr = trim(onClause.substr(0, eqPos));
    std::string srcExpr = trim(onClause.substr(eqPos + 1));
    size_t dotPos = targetExpr.find('.');
    std::string targetColumn = (dotPos != std::string::npos) ? toLowerCase(trim(targetExpr.substr(dotPos + 1))) : toLowerCase(targetExpr);
    dotPos = srcExpr.find('.');
    std::string srcColumn = (dotPos != std::string::npos) ? toLowerCase(trim(srcExpr.substr(dotPos + 1))) : toLowerCase(srcExpr);
    
    // --- Step 4: Parse the UPDATE clause (WHEN MATCHED) ---
    size_t updateClauseStart = whenMatchedPos + std::string("WHEN MATCHED THEN UPDATE SET").length();
    std::cout << std::flush;
    std::string updateClause = mergeCommand.substr(updateClauseStart, whenNotMatchedPos - updateClauseStart);
    updateClause = trim(updateClause);
    // Split assignments by commas.
    std::unordered_map<std::string, std::string> updateAssignments;
    {
        std::vector<std::string> assignments = split(updateClause, ',');
        for (const auto &assign : assignments) {
            size_t eqAssign = assign.find('=');
            if (eqAssign == std::string::npos)
                continue;
            std::string col = toLowerCase(trim(assign.substr(0, eqAssign)));
            std::string val = trim(assign.substr(eqAssign + 1));
            // Replace references like "src.col" with the corresponding source value.
            if (toUpperCase(val).find("SRC.") != std::string::npos) {
                size_t pos = toUpperCase(val).find("SRC.");
                std::string refCol = toLowerCase(trim(val.substr(pos + 4)));
                if (srcRecord.find(refCol) != srcRecord.end()) {
                    val = srcRecord[refCol];
                }
            } else if (!val.empty() && val.front() == '\'' && val.back() == '\'' && val.size() > 1) {
                val = val.substr(1, val.size() - 2);
            }
            updateAssignments[col] = val;
        }
    }
    
    // --- Step 5: Parse the INSERT clause (WHEN NOT MATCHED) ---
    size_t insertClauseStart = whenNotMatchedPos + std::string("WHEN NOT MATCHED THEN INSERT VALUES").length();
    std::cout << std::flush;
    std::string insertClause = mergeCommand.substr(insertClauseStart);
    insertClause = trim(insertClause);
    // Remove surrounding parentheses if present.
    if (!insertClause.empty() && insertClause.front() == '(' && insertClause.back() == ')') {
        insertClause = insertClause.substr(1, insertClause.size() - 2);
        insertClause = trim(insertClause);
    }
    std::vector<std::string> insertValues = split(insertClause, ',');
    for (auto &val : insertValues) {
        val = trim(val);
        if (toUpperCase(val).find("SRC.") != std::string::npos) {
            size_t pos = toUpperCase(val).find("SRC.");
            std::string refCol = toLowerCase(trim(val.substr(pos + 4)));
            if (srcRecord.find(refCol) != srcRecord.end())
                val = srcRecord[refCol];
        } else if (!val.empty() && val.front() == '\'' && val.back() == '\'' && val.size() > 1) {
            val = val.substr(1, val.size() - 2);
        }
    }
    
    // --- Step 6: Apply the MERGE to the target table ---
    std::string lowerTable = toLowerCase(tableName);
    if (tables.find(lowerTable) == tables.end()) {
        std::cout << "MERGE: Table " << tableName << " does not exist." << std::endl;
        return;
    }
    const auto& targetCols = tables[lowerTable]->getColumns();
    // Find the index of the target column in the table.
    int targetIndex = -1;
    for (size_t i = 0; i < targetCols.size(); i++) {
        if (toLowerCase(targetCols[i]) == targetColumn) {
            targetIndex = i;
            break;
        }
    }
    if (targetIndex == -1) {
        std::cout << "MERGE: Target column " << targetColumn << " not found in table." << std::endl;
        return;
    }
    
    bool matched = false;
    // Check each row for a match on the ON condition.
    for (auto &row : tables[lowerTable]->getRowsNonConst()) {
        if (row.size() > targetIndex && toLowerCase(row[targetIndex]) == toLowerCase(srcRecord[srcColumn])) {
            // When matched, update the row using the UPDATE assignments.
            for (size_t j = 0; j < targetCols.size(); j++) {
                std::string colName = toLowerCase(targetCols[j]);
                if (updateAssignments.find(colName) != updateAssignments.end()) {
                    row[j] = updateAssignments[colName];
                }
            }
            matched = true;
        }
    }
    
    if (!matched) {
        // No matching row found; build a new row using the INSERT values.
        // For simplicity, assume the number of insert values equals the number of columns.
        std::vector<std::string> newRow;
        for (size_t i = 0; i < targetCols.size(); i++) {
            if (i < insertValues.size())
                newRow.push_back(insertValues[i]);
            else
                newRow.push_back("");
        }
        tables[lowerTable]->addRow(newRow);
    }
    
    std::cout << "MERGE command executed on " << tableName << "." << std::endl;
}

void Database::replaceInto(const std::string& tableName, const std::vector<std::vector<std::string>>& values) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    for (const auto& row : values) {
        bool replaced = false;
        for (auto& existingRow : tables[lowerName]->getRowsNonConst()) {
            if (!existingRow.empty() && !row.empty() && existingRow[0] == row[0]) {
                existingRow = row;
                replaced = true;
                break;
            }
        }
        if (!replaced) {
            tables[lowerName]->addRow(row);
        }
    }
    std::cout << "REPLACE INTO executed on " << tableName << "." << std::endl;
}
// Add these implementations to Database.cpp

Database::Database() {
    // Initialize the catalog
    catalog = Catalog();
    
    // Create admin user by default
    users["admin"] = User("admin", "admin");
}

Database::~Database() {
    // Clean up any resources
    // For tables, the unique_ptr will automatically clean up
}

// Authentication and authorization
bool Database::createUser(const std::string& username, const std::string& password) {
    std::string lowerName = toLowerCase(username);
    if (users.find(lowerName) != users.end()) {
        std::cout << "User '" << username << "' already exists." << std::endl;
        return false;
    }
    
    users[lowerName] = User(username, password);
    std::cout << "User '" << username << "' created." << std::endl;
    return true;
}

bool Database::authenticate(const std::string& username, const std::string& password) {
    std::string lowerName = toLowerCase(username);
    if (users.find(lowerName) == users.end()) {
        return false;
    }
    
    return users[lowerName].authenticate(password);
}

void Database::grantPrivilege(const std::string& username, const std::string& tableName, 
                             const std::string& privilege) {
    std::string lowerUser = toLowerCase(username);
    if (users.find(lowerUser) == users.end()) {
        std::cout << "User '" << username << "' does not exist." << std::endl;
        return;
    }
    
    std::string lowerTable = toLowerCase(tableName);
    if (tables.find(lowerTable) == tables.end() && views.find(lowerTable) == views.end()) {
        std::cout << "Table or view '" << tableName << "' does not exist." << std::endl;
        return;
    }
    
    std::string upperPriv = toUpperCase(privilege);
    Privilege::Type privType;
    
    if (upperPriv == "SELECT") {
        privType = Privilege::Type::SELECT;
    } else if (upperPriv == "INSERT") {
        privType = Privilege::Type::INSERT;
    } else if (upperPriv == "UPDATE") {
        privType = Privilege::Type::UPDATE;
    } else if (upperPriv == "DELETE") {
        privType = Privilege::Type::DELETE;
    } else if (upperPriv == "ALL") {
        privType = Privilege::Type::ALL;
    } else {
        std::cout << "Invalid privilege type: " << privilege << std::endl;
        return;
    }
    
    users[lowerUser].grantPrivilege(tableName, privType);
    std::cout << "Granted " << privilege << " on " << tableName << " to " << username << std::endl;
}

void Database::revokePrivilege(const std::string& username, const std::string& tableName, 
                              const std::string& privilege) {
    std::string lowerUser = toLowerCase(username);
    if (users.find(lowerUser) == users.end()) {
        std::cout << "User '" << username << "' does not exist." << std::endl;
        return;
    }
    
    std::string upperPriv = toUpperCase(privilege);
    Privilege::Type privType;
    
    if (upperPriv == "SELECT") {
        privType = Privilege::Type::SELECT;
    } else if (upperPriv == "INSERT") {
        privType = Privilege::Type::INSERT;
    } else if (upperPriv == "UPDATE") {
        privType = Privilege::Type::UPDATE;
    } else if (upperPriv == "DELETE") {
        privType = Privilege::Type::DELETE;
    } else if (upperPriv == "ALL") {
        privType = Privilege::Type::ALL;
    } else {
        std::cout << "Invalid privilege type: " << privilege << std::endl;
        return;
    }
    
    users[lowerUser].revokePrivilege(tableName, privType);
    std::cout << "Revoked " << privilege << " on " << tableName << " from " << username << std::endl;
}

bool Database::checkPrivilege(const std::string& username, const std::string& tableName, 
                             const std::string& privilege) {
    // Admin always has all privileges
    if (toLowerCase(username) == "admin") {
        return true;
    }
    
    std::string lowerUser = toLowerCase(username);
    if (users.find(lowerUser) == users.end()) {
        return false;
    }
    
    std::string upperPriv = toUpperCase(privilege);
    Privilege::Type privType;
    
    if (upperPriv == "SELECT") {
        privType = Privilege::Type::SELECT;
    } else if (upperPriv == "INSERT") {
        privType = Privilege::Type::INSERT;
    } else if (upperPriv == "UPDATE") {
        privType = Privilege::Type::UPDATE;
    } else if (upperPriv == "DELETE") {
        privType = Privilege::Type::DELETE;
    } else if (upperPriv == "ALL") {
        privType = Privilege::Type::ALL;
    } else {
        return false;
    }
    
    return users[lowerUser].hasPrivilege(tableName, privType);
}

// Custom type support
void Database::createType(const std::string& typeName, const std::vector<std::pair<std::string, std::string>>& attributes) {
    // Register the type in the global registry
    UserDefinedType type;
    type.name = typeName;
    type.attributes = attributes;
    
    UserTypeRegistry::registerType(type);
    
    // Add to catalog
    catalog.addType(typeName, attributes);
    
    std::cout << "Type " << typeName << " created." << std::endl;
}

// View management
void Database::createView(const std::string& viewName, const std::string& viewDefinition) {
    std::string lowerName = toLowerCase(viewName);
    if (views.find(lowerName) != views.end() || tables.find(lowerName) != tables.end()) {
        std::cout << "View or table '" << viewName << "' already exists." << std::endl;
        return;
    }
    
    views[lowerName] = viewDefinition;
    
    // Add to catalog
    catalog.addView(viewName, viewDefinition);
    
    std::cout << "View " << viewName << " created." << std::endl;
}

void Database::dropView(const std::string& viewName) {
    std::string lowerName = toLowerCase(viewName);
    if (views.find(lowerName) == views.end()) {
        std::cout << "View '" << viewName << "' does not exist." << std::endl;
        return;
    }
    
    views.erase(lowerName);
    
    // Remove from catalog
    catalog.removeView(viewName);
    
    std::cout << "View " << viewName << " dropped." << std::endl;
}

// Assertion management
void Database::createAssertion(const std::string& name, const std::string& condition) {
    if (assertions.find(toLowerCase(name)) != assertions.end()) {
        std::cout << "Assertion '" << name << "' already exists." << std::endl;
        return;
    }
    
    // Verify the condition is valid
    try {
        ConditionParser parser(condition);
        parser.parse();
    } catch (const std::exception& e) {
        std::cout << "Invalid assertion condition: " << e.what() << std::endl;
        return;
    }
    
    assertions[toLowerCase(name)] = condition;
    
    // Add to catalog
    catalog.addAssertion(name, condition);
    
    std::cout << "Assertion " << name << " created." << std::endl;
}

// Table modification
void Database::alterTableAddConstraint(const std::string& tableName, const Constraint& constraint) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table '" << tableName << "' does not exist." << std::endl;
        return;
    }
    
    try {
        validateReferences(constraint);
        tables[lowerName]->addConstraint(constraint);
        std::cout << "Constraint added to " << tableName << "." << std::endl;
    } catch (const std::exception& e) {
        std::cout << "Failed to add constraint: " << e.what() << std::endl;
    }
}

void Database::alterTableDropConstraint(const std::string& tableName, const std::string& constraintName) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table '" << tableName << "' does not exist." << std::endl;
        return;
    }
    
    bool success = tables[lowerName]->dropConstraint(constraintName);
    if (success) {
        std::cout << "Constraint " << constraintName << " dropped from " << tableName << "." << std::endl;
    } else {
        std::cout << "Constraint " << constraintName << " does not exist in " << tableName << "." << std::endl;
    }
}

// Reference validation
void Database::validateReferences(const Constraint& constraint) {
    if (constraint.type != Constraint::Type::FOREIGN_KEY) {
        return; // Nothing to validate for non-foreign key constraints
    }
    
    std::string lowerRefTable = toLowerCase(constraint.referencedTable);
    if (tables.find(lowerRefTable) == tables.end()) {
        throw DatabaseException("Referenced table '" + constraint.referencedTable + "' does not exist");
    }
    
    // Get the referenced table
    Table* refTable = tables[lowerRefTable].get();
    
    // Check if referenced columns exist in the referenced table
    for (const auto& col : constraint.referencedColumns) {
        if (!refTable->hasColumn(col)) {
            throw DatabaseException("Referenced column '" + col + "' does not exist in table '" + 
                                  constraint.referencedTable + "'");
        }
    }
    
    // Columns count should match
    if (constraint.columns.size() != constraint.referencedColumns.size()) {
        throw DatabaseException("Number of columns in foreign key constraint does not match referenced columns");
    }
}

// Set operations
void Database::setOperation(const std::string& operation, 
                           const std::string& leftQuery, 
                           const std::string& rightQuery) {
    // Parse and execute both queries
    Parser parser;
    Query leftQ = parser.parseQuery(leftQuery);
    Query rightQ = parser.parseQuery(rightQuery);
    
    // Get results from both queries
    std::vector<std::vector<std::string>> leftResult;
    std::vector<std::vector<std::string>> rightResult;
    
    // Execute left query
    std::string lowerLeftTable = toLowerCase(leftQ.tableName);
    if (tables.find(lowerLeftTable) == tables.end()) {
        std::cout << "Table '" << leftQ.tableName << "' does not exist." << std::endl;
        return;
    }
    
    leftResult = tables[lowerLeftTable]->selectRows(
        leftQ.selectColumns, leftQ.condition, 
        leftQ.orderByColumns, leftQ.groupByColumns, leftQ.havingCondition);
    
    // Execute right query
    std::string lowerRightTable = toLowerCase(rightQ.tableName);
    if (tables.find(lowerRightTable) == tables.end()) {
        std::cout << "Table '" << rightQ.tableName << "' does not exist." << std::endl;
        return;
    }
    
    rightResult = tables[lowerRightTable]->selectRows(
        rightQ.selectColumns, rightQ.condition, 
        rightQ.orderByColumns, rightQ.groupByColumns, rightQ.havingCondition);
    
    // Apply set operation
    std::vector<std::vector<std::string>> result;
    std::string upperOp = toUpperCase(operation);
    
    if (upperOp == "UNION") {
        result = tables[lowerLeftTable]->setUnion(rightResult);
    } else if (upperOp == "INTERSECT") {
        result = tables[lowerLeftTable]->setIntersect(rightResult);
    } else if (upperOp == "EXCEPT") {
        result = tables[lowerLeftTable]->setExcept(rightResult);
    } else {
        std::cout << "Unsupported set operation: " << operation << std::endl;
        return;
    }
    
    // Display results
    std::vector<std::string> displayColumns = leftQ.selectColumns;
    for (const auto& col : displayColumns) {
        std::cout << col << "\t";
    }
    std::cout << std::endl;
    
    for (const auto& row : result) {
        for (const auto& cell : row) {
            std::cout << cell << "\t";
        }
        std::cout << std::endl;
    }
}

// Join tables
void Database::joinTables(const std::string& leftTable, 
                          const std::string& rightTable,
                          const std::string& joinType,
                          const std::string& joinCondition,
                          const std::vector<std::string>& selectColumns) {
    std::string lowerLeftTable = toLowerCase(leftTable);
    std::string lowerRightTable = toLowerCase(rightTable);
    
    if (tables.find(lowerLeftTable) == tables.end()) {
        std::cout << "Table '" << leftTable << "' does not exist." << std::endl;
        return;
    }
    
    if (tables.find(lowerRightTable) == tables.end()) {
        std::cout << "Table '" << rightTable << "' does not exist." << std::endl;
        return;
    }
    
    std::vector<std::vector<std::string>> result;
    std::string upperJoinType = toUpperCase(joinType);
    
    if (upperJoinType == "INNER") {
        result = tables[lowerLeftTable]->innerJoin(*tables[lowerRightTable], joinCondition, selectColumns);
    } else if (upperJoinType == "LEFT OUTER" || upperJoinType == "LEFT") {
        result = tables[lowerLeftTable]->leftOuterJoin(*tables[lowerRightTable], joinCondition, selectColumns);
    } else if (upperJoinType == "RIGHT OUTER" || upperJoinType == "RIGHT") {
        result = tables[lowerLeftTable]->rightOuterJoin(*tables[lowerRightTable], joinCondition, selectColumns);
    } else if (upperJoinType == "FULL OUTER" || upperJoinType == "FULL") {
        result = tables[lowerLeftTable]->fullOuterJoin(*tables[lowerRightTable], joinCondition, selectColumns);
    } else if (upperJoinType == "NATURAL") {
        result = tables[lowerLeftTable]->naturalJoin(*tables[lowerRightTable], selectColumns);
    } else {
        std::cout << "Unsupported join type: " << joinType << std::endl;
        return;
    }
    
    // Display results
    for (const auto& col : selectColumns) {
        std::cout << col << "\t";
    }
    std::cout << std::endl;
    
    for (const auto& row : result) {
        for (const auto& cell : row) {
            std::cout << cell << "\t";
        }
        std::cout << std::endl;
    }
}

// Table access
Table* Database::getTable(const std::string& tableName, bool exclusiveLock) {
    std::string lowerName = toLowerCase(tableName);
    auto it = tables.find(lowerName);
    if (it == tables.end()) {
        return nullptr;
    }
    
    Table* table = it->second.get();
    
    if (exclusiveLock) {
        table->lockExclusive();
    } else {
        table->lockShared();
    }
    
    return table;
}

// Schema information
void Database::showSchema() {
    catalog.showSchema();
}
void Database::showViews() {
    std::cout << "Available Views:" << std::endl;
    for (const auto& pair : views) {
        std::cout << pair.first << std::endl;
    }
}
std::vector<std::vector<std::string>> Database::executeViewQuery(const std::string& viewName) {
    std::string lowerName = toLowerCase(viewName);
    if (views.find(lowerName) == views.end()) {
        throw DatabaseException("View '" + viewName + "' does not exist");
    }
    
    // Parse the view definition (which should be a SELECT query)
    Parser parser;
    Query query = parser.parseQuery(views[lowerName]);
    
    if (toUpperCase(query.type) != "SELECT") {
        throw DatabaseException("Invalid view definition");
    }
    
    // Execute the query against the database
    std::string tableName = query.tableName;
    std::string lowerTableName = toLowerCase(tableName);
    
    if (tables.find(lowerTableName) == tables.end()) {
        throw DatabaseException("Table '" + tableName + "' referenced in view does not exist");
    }
    
    return tables[lowerTableName]->selectRows(
        query.selectColumns,
        query.condition,
        query.orderByColumns,
        query.groupByColumns,
        query.havingCondition);
}