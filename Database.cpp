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
    std::unique_lock<std::mutex> lock(databaseMutex);
    
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) != tables.end()) {
        throw DatabaseException("Table '" + tableName + "' already exists");
    }
    
    // Create the table using make_unique
    auto table = std::make_unique<Table>(tableName);
    
    // Add columns
    for (const auto& col : cols) {
        table->addColumn(col.first, col.second);
    }
    
    // Add constraints
    for (const auto& constraint : constraints) {
        try {
            validateReferences(constraint);
            table->addConstraint(constraint);
        } catch (const DatabaseException& e) {
            throw DatabaseException("Failed to create table '" + tableName + "': " + e.what());
        }
    }
    
    tables[lowerName] = std::move(table);
    std::cout << "Table " << tableName << " created." << std::endl;
}

void Database::dropTable(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.erase(lowerName))
        std::cout << "Table " << tableName << " dropped." << std::endl;
    else
        std::cout << "Table " << tableName << " does not exist." << std::endl;
}

void Database::alterTableAddColumn(const std::string& tableName, const std::pair<std::string, std::string>& column, bool isNotNull) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    tables[lowerName]->addColumn(column.first, column.second);
    std::cout << "Column " << column.first << " added to " << tableName << "." << std::endl;
}

void Database::alterTableDropColumn(const std::string& tableName, const std::string& columnName) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    bool success = tables[lowerName]->dropColumn(columnName);
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
        return;
    }
    for (const auto& valueSet : values) {
        tables[lowerName]->addRow(valueSet);
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
        tables[lowerName]->selectRows(selectColumns, condition, orderByColumns, groupByColumns, havingCondition);
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
        
        Table* leftTable = tables[leftName].get();
        Table* rightTable = tables[rightName].get();
        const auto& leftCols = leftTable->getColumns();
        const auto& rightCols = rightTable->getColumns();
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
        
        const auto& leftRows = leftTable->getRows();
        const auto& rightRows = rightTable->getRows();
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
    tables[lowerName]->deleteRows(condition);
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
    tables[lowerName]->updateRows(updates, condition);
    std::cout << "Records updated in " << tableName << "." << std::endl;
}

void Database::showTables() {
    std::cout << "Available Tables:" << std::endl;
    for (const auto& pair : tables)
        std::cout << pair.first << std::endl;
}

// Transaction functions
Transaction* Database::beginTransaction() {
    if (!inTransaction) {
        // Deep copy tables
        backupTables.clear();
        for (const auto& [name, tablePtr] : tables) {
            // Create a new table with the same name
            auto tableCopy = std::make_unique<Table>(tablePtr->getName());
            // Copy columns, rows, constraints, etc. as needed
            // This depends on having copy methods in your Table class
            
            backupTables[name] = std::move(tableCopy);
        }
        inTransaction = true;
        std::cout << "Transaction started." << std::endl;
        return new Transaction(this);
    }  // Pass 'this' as the database pointer
    else {
        std::cout << "Transaction already in progress." << std::endl;
        return nullptr;
    }
}

Transaction* Database::commitTransaction() {
    if (!inTransaction) {
       std::cout << "No active transaction to commit." << std::endl;
       return nullptr;
    }
    inTransaction = false;
    backupTables.clear();
    std::cout << "Transaction committed." << std::endl;
    return nullptr;
}

Transaction* Database::rollbackTransaction() {
    if (!inTransaction) {
       std::cout << "No active transaction to rollback." << std::endl;
       return nullptr;
    }
    tables = std::move(backupTables);
    backupTables.clear();
    inTransaction = false;
    std::cout << "Transaction rolled back." << std::endl;
    return nullptr;
}

// New functionalities

void Database::truncateTable(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    tables[lowerName]->clearRows();
    std::cout << "Table " << tableName << " truncated." << std::endl;
}

void Database::renameTable(const std::string& oldName, const std::string& newName) {
    std::string lowerOld = toLowerCase(oldName);
    std::string lowerNew = toLowerCase(newName);
    if (tables.find(lowerOld) == tables.end()) {
        std::cout << "Table " << oldName << " does not exist." << std::endl;
        return;
    }
    tables[lowerNew] = std::move(tables[lowerOld]);
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
        return;
    }
    
    // --- Step 2: Extract the source subquery from the USING clause ---
    size_t sourceStart = mergeCommand.find("(", usingPos);
    size_t sourceEnd = mergeCommand.find(")", sourceStart);
    if (sourceStart == std::string::npos || sourceEnd == std::string::npos) {
        std::cout << "MERGE: Invalid source subquery syntax." << std::endl;
        return;
    }
    std::string sourceSubquery = mergeCommand.substr(sourceStart + 1, sourceEnd - sourceStart - 1);
    // Expect the subquery to start with SELECT.
    size_t selectPos = toUpperCase(sourceSubquery).find("SELECT");
    if (selectPos == std::string::npos) {
        std::cout << "MERGE: Source subquery must start with SELECT." << std::endl;
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