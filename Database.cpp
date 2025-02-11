#include "Database.h"
#include "Utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <unordered_set>
#include <regex>

void Database::showTables() {
    if (tables.empty()) {
        std::cout << "No tables found in the database." << std::endl;
        return;
    }
    std::cout << "Tables in the database:" << std::endl;
    for (const auto &pair : tables) {
        std::cout << pair.first << std::endl;
    }
}



// Helper structure for condition evaluation.
struct Condition {
    std::string column;
    std::string op;
    std::string value;
};

static Condition parseConditionFull(const std::string &condition) {
    std::regex condRegex(R"(\s*(\w+)\s*(=|>=|<=|>|<)\s*(.+)\s*)");
    std::smatch m;
    Condition cond;
    if (std::regex_match(condition, m, condRegex)) {
        cond.column = m[1];
        cond.op = m[2];
        cond.value = m[3];
        if (!cond.value.empty() && cond.value.front() == '\'' && cond.value.back() == '\'')
            cond.value = cond.value.substr(1, cond.value.size()-2);
    }
    return cond;
}

static bool evaluateCondition(const std::string &cellValue, const std::string &op, const std::string &condValue) {
    try {
        double cell = std::stod(cellValue);
        double cond = std::stod(condValue);
        if (op == "=") return cell == cond;
        else if (op == ">") return cell > cond;
        else if (op == "<") return cell < cond;
        else if (op == ">=") return cell >= cond;
        else if (op == "<=") return cell <= cond;
        else return false;
    } catch (...) {
        if (op == "=") return cellValue == condValue;
        return false;
    }
}

// DDL and DML implementations:
void Database::createTable(const std::string& tableName, const std::vector<std::string>& columns) {
    Table table;
    for (const auto& col : columns) {
        table.addColumn(col, "string");
    }
    tables[toLowerCase(tableName)] = table;
    std::cout << "Table " << tableName << " created." << std::endl;
}

void Database::insertRecord(const std::string& tableName, const std::vector<std::vector<std::string>>& values) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    for (auto &val : values) {
        tables[lowerName].addRow(val);
    }
    std::cout << "Record(s) inserted into " << tableName << "." << std::endl;
}

void Database::selectRecords(const Query& q) {
    // For JOIN, a similar logic as before is applied.
    if (!q.joinTable.empty()) {
        std::string leftName = toLowerCase(q.tableName);
        std::string rightName = toLowerCase(q.joinTable);
        if (tables.find(leftName) == tables.end() || tables.find(rightName) == tables.end()) {
            std::cout << "One of the tables does not exist." << std::endl;
            return;
        }
        Table& leftTable = tables[leftName];
        Table& rightTable = tables[rightName];
        Condition joinCond = parseConditionFull(q.joinCondition);
        std::string leftCol = joinCond.column;
        std::string rightCol = joinCond.value; // assuming format "leftcol = rightcol"
        int leftColIdx = leftTable.getColumnIndex(leftCol);
        int rightColIdx = rightTable.getColumnIndex(rightCol);
        if (leftColIdx == -1 || rightColIdx == -1) {
            std::cout << "Invalid join condition." << std::endl;
            return;
        }
        Table joinResult;
        for (auto &col : leftTable.getColumns()) {
            joinResult.addColumn(q.tableName + "." + col, "string");
        }
        for (auto &col : rightTable.getColumns()) {
            joinResult.addColumn(q.joinTable + "." + col, "string");
        }
        for (auto &leftRow : leftTable.getRows()) {
            for (auto &rightRow : rightTable.getRows()) {
                if (leftRow[leftColIdx] == rightRow[rightColIdx]) {
                    std::vector<std::string> combined = leftRow;
                    combined.insert(combined.end(), rightRow.begin(), rightRow.end());
                    joinResult.addRow(combined);
                }
            }
        }
        std::vector<std::vector<std::string>> resultRows = joinResult.getRows();
        if (!q.condition.empty()) {
            Condition cond = parseConditionFull(q.condition);
            int colIdx = joinResult.getColumnIndex(cond.column);
            if (colIdx != -1) {
                std::vector<std::vector<std::string>> filtered;
                for (auto &row : resultRows) {
                    if (evaluateCondition(row[colIdx], cond.op, cond.value))
                        filtered.push_back(row);
                }
                resultRows = filtered;
            }
        }
        // (For brevity, GROUP BY/HAVING are not fully implemented here.)
        if (q.limit > 0 && resultRows.size() > (size_t)q.limit) {
            resultRows.resize(q.limit);
        }
        // OFFSET processing:
        if (q.offset > 0 && resultRows.size() > (size_t)q.offset) {
            resultRows = std::vector<std::vector<std::string>>(resultRows.begin()+q.offset, resultRows.end());
        }
        joinResult.printCustomTable(resultRows);
    }
    else {
        std::string lowerName = toLowerCase(q.tableName);
        if (tables.find(lowerName) == tables.end()) {
            std::cout << "Table " << q.tableName << " does not exist." << std::endl;
            return;
        }
        Table table = tables[lowerName];
        std::vector<std::vector<std::string>> rows = table.getRows();
        if (!q.condition.empty()) {
            Condition cond = parseConditionFull(q.condition);
            int colIdx = table.getColumnIndex(cond.column);
            if (colIdx != -1) {
                std::vector<std::vector<std::string>> filtered;
                for (auto &row : rows) {
                    if (evaluateCondition(row[colIdx], cond.op, cond.value))
                        filtered.push_back(row);
                }
                rows = filtered;
            }
        }
        // (GROUP BY/HAVING not fully processed.)
        if (q.limit > 0 && rows.size() > (size_t)q.limit) {
            rows.resize(q.limit);
        }
        if (q.offset > 0 && rows.size() > (size_t)q.offset) {
            rows = std::vector<std::vector<std::string>>(rows.begin()+q.offset, rows.end());
        }
        // If selecting specific columns:
        if (q.columns.size() == 1 && q.columns[0] == "*") {
            table.printCustomTable(rows);
        } else if (!q.columns.empty()) {
            std::vector<int> indices;
            for (auto &colName : q.columns) {
                int idx = table.getColumnIndex(colName);
                if (idx != -1)
                    indices.push_back(idx);
            }
            table.printCustomTable(rows, indices);
        } else {
            table.printCustomTable(rows);
        }
    }
}

void Database::deleteRecords(const std::string& tableName, const std::string& condition) {
    std::string lowerName = toLowerCase(tableName);
    if (tables.find(lowerName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    Table& table = tables[lowerName];
    std::vector<std::vector<std::string>> rows = table.getRows();
    if (!condition.empty()) {
        Condition cond = parseConditionFull(condition);
        int colIdx = table.getColumnIndex(cond.column);
        if (colIdx != -1) {
            std::vector<std::vector<std::string>> remaining;
            for (auto &row : rows) {
                if (!evaluateCondition(row[colIdx], cond.op, cond.value)) {
                    remaining.push_back(row);
                }
            }
            table.setRows(remaining);
        }
    }
    std::cout << "Records deleted from " << tableName << "." << std::endl;
}

void Database::updateRecords(const std::string& tableName, const std::vector<std::pair<std::string, std::string>>& assignments, const std::string& condition) {
    std::string lowerName = toLowerCase(tableName);
    if(tables.find(lowerName) == tables.end()){
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    Table& table = tables[lowerName];
    std::vector<std::vector<std::string>> rows = table.getRows();
    Condition cond;
    int condColIdx = -1;
    if(!condition.empty()){
        cond = parseConditionFull(condition);
        condColIdx = table.getColumnIndex(cond.column);
    }
    for(auto &row : rows) {
        bool meetsCondition = true;
        if(!condition.empty() && condColIdx != -1)
            meetsCondition = evaluateCondition(row[condColIdx], cond.op, cond.value);
        if(meetsCondition) {
            for(const auto &assign : assignments) {
                int colIdx = table.getColumnIndex(assign.first);
                if(colIdx != -1)
                    row[colIdx] = assign.second;
            }
        }
    }
    table.setRows(rows);
    std::cout << "Records updated in " << tableName << "." << std::endl;
}

// For simplicity, we implement UPSERT by checking if a record with the same primary key (first column) exists.
void Database::upsertRecords(const Query& q) {
    std::string lowerName = toLowerCase(q.tableName);
    if(tables.find(lowerName) == tables.end()){
        std::cout << "Table " << q.tableName << " does not exist." << std::endl;
        return;
    }
    Table& table = tables[lowerName];
    // Assume q.values contains one or more rows to upsert.
    for(const auto &newRow : q.values) {
        if(newRow.empty()){
            continue;
        }
        std::string pk = newRow[0]; // primary key assumed as first column
        bool found = false;
        std::vector<std::vector<std::string>> rows = table.getRows();
        for(auto &row : rows) {
            if(!row.empty() && row[0] == pk) {
                // Update the entire row with newRow data.
                row = newRow;
                found = true;
                break;
            }
        }
        if(!found) {
            table.addRow(newRow);
        }
    }
    std::cout << "UPSERT completed on " << q.tableName << "." << std::endl;
}

void Database::mergeRecords(const Query& q) {
    // For our toy engine, treat MERGE the same as UPSERT.
    upsertRecords(q);
}

// DDL functions:
void Database::alterTable(const std::string& tableName, const std::vector<std::string>& newColumns) {
    std::string lowerName = toLowerCase(tableName);
    if(tables.find(lowerName) == tables.end()){
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    for(auto &col : newColumns){
        tables[lowerName].addColumn(col, "string");
    }
    std::cout << "Table " << tableName << " altered (new columns added)." << std::endl;
}

void Database::dropTable(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    if(tables.erase(lowerName))
        std::cout << "Table " << tableName << " dropped." << std::endl;
    else
        std::cout << "Table " << tableName << " does not exist." << std::endl;
}

void Database::truncateTable(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    if(tables.find(lowerName) != tables.end()){
        tables[lowerName].setRows({});
        std::cout << "Table " << tableName << " truncated." << std::endl;
    } else {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
    }
}

void Database::renameTable(const std::string& oldName, const std::string& newName) {
    std::string lowerOld = toLowerCase(oldName);
    std::string lowerNew = toLowerCase(newName);
    if(tables.find(lowerOld) != tables.end()){
        tables[lowerNew] = tables[lowerOld];
        tables.erase(lowerOld);
        std::cout << "Table " << oldName << " renamed to " << newName << "." << std::endl;
    } else {
        std::cout << "Table " << oldName << " does not exist." << std::endl;
    }
}

// TCL functions:
void Database::beginTransaction() {
    if(inTransaction) {
        std::cout << "Transaction already in progress." << std::endl;
        return;
    }
    backupTables = tables; // make a backup copy
    inTransaction = true;
    std::cout << "Transaction begun." << std::endl;
}

void Database::commitTransaction() {
    if(!inTransaction) {
        std::cout << "No transaction in progress." << std::endl;
        return;
    }
    backupTables.clear();
    inTransaction = false;
    std::cout << "Transaction committed." << std::endl;
}

void Database::rollbackTransaction() {
    if(!inTransaction) {
        std::cout << "No transaction in progress." << std::endl;
        return;
    }
    tables = backupTables; // restore backup
    backupTables.clear();
    inTransaction = false;
    std::cout << "Transaction rolled back." << std::endl;
}

void Database::savepointTransaction(const std::string& name) {
    if(!inTransaction) {
        std::cout << "No transaction in progress. Begin a transaction first." << std::endl;
        return;
    }
    // Save current state as a savepoint.
    savepoints[name] = tables;
    std::cout << "Savepoint '" << name << "' created." << std::endl;
}

void Database::setTransaction(const std::string& settings) {
    // For simplicity, just print the settings.
    std::cout << "SET TRANSACTION: " << settings << " (not fully implemented)." << std::endl;
}

// DCL functions:
void Database::grantPrivilege(const std::string& privilege, const std::string& tableName, const std::string& user) {
    Privilege p = { user, toLowerCase(tableName), privilege };
    privileges.push_back(p);
    std::cout << "Granted " << privilege << " on " << tableName << " to " << user << "." << std::endl;
}

void Database::revokePrivilege(const std::string& privilege, const std::string& tableName, const std::string& user) {
    auto it = std::remove_if(privileges.begin(), privileges.end(),
        [&](const Privilege& p) {
            return p.user == user && p.table == toLowerCase(tableName) && p.privilege == privilege;
        });
    if(it != privileges.end()) {
        privileges.erase(it, privileges.end());
        std::cout << "Revoked " << privilege << " on " << tableName << " from " << user << "." << std::endl;
    } else {
        std::cout << "No such privilege found." << std::endl;
    }
}

void Database::denyPrivilege(const std::string& privilege, const std::string& tableName, const std::string& user) {
    // For simulation, we simply print a message.
    std::cout << "Privilege " << privilege << " on " << tableName << " denied to " << user << "." << std::endl;
}
