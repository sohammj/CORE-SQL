#include "Transaction.h"
#include "Database.h"
#include "Table.h"
#include "Utils.h"
#include <iostream>

// Initialize static member
int Transaction::nextTransactionId = 1;

Transaction::Transaction(Database* db, IsolationLevel level)
    : database(db), isolationLevel(level), active(false), transactionId(nextTransactionId++) {}

Transaction::~Transaction() {
    if (active) {
        try {
            rollback();
        } catch (const std::exception& e) {
            std::cerr << "Error during transaction destructor rollback: " << e.what() << std::endl;
        }
    }
}

void Transaction::begin() {
    if (active) {
        throw TransactionException("Transaction already active");
    }
    
    active = true;
    std::cout << "Transaction " << transactionId << " started." << std::endl;
}

void Transaction::commit() {
    if (!active) {
        throw TransactionException("No active transaction to commit");
    }
    
    // Release all locks
    releaseLocks();
    
    // Clear stored table states
    tableStates.clear();
    
    active = false;
    std::cout << "Transaction " << transactionId << " committed." << std::endl;
}

void Transaction::rollback() {
    if (!active) {
        throw TransactionException("No active transaction to rollback");
    }
    
    // Restore all modified tables to their original state
    for (const auto& entry : tableStates) {
        restoreTableState(entry.first);
    }
    
    // Release all locks
    releaseLocks();
    
    // Clear stored table states
    tableStates.clear();
    
    active = false;
    std::cout << "Transaction " << transactionId << " rolled back." << std::endl;
}

void Transaction::createTable(const std::string& tableName,
                            const std::vector<std::pair<std::string, std::string>>& columns) {
    if (!active) {
        throw TransactionException("No active transaction");
    }
    
    // The actual table creation is handled by the database
    // Here we just register the operation for potential rollback
    std::vector<Constraint> constraints; // Empty constraints for now
    database->createTable(tableName, columns, constraints);
    
    // We don't need to save state here since dropping the table is sufficient for rollback
    // Just keep track of created tables for rollback
    TableState state;
    state.tableName = tableName;
    state.columns.clear(); // Empty indicates this is a new table
    tableStates[toLowerCase(tableName)] = state;
}

void Transaction::dropTable(const std::string& tableName) {
    if (!active) {
        throw TransactionException("No active transaction");
    }
    
    // Save the table state before dropping it
    saveTableState(tableName);
    
    // The actual table drop is handled by the database
    database->dropTable(tableName);
}

void Transaction::alterTable(const std::string& tableName, const std::string& operation) {
    if (!active) {
        throw TransactionException("No active transaction");
    }
    
    // Save the table state before altering it
    saveTableState(tableName);
    
    // The actual ALTER operation will be handled by the database
    // For now, this is just a placeholder since the operation is complex
    // and depends on the specific ALTER action
}

void Transaction::insertRecord(const std::string& tableName, const std::vector<std::string>& values) {
    if (!active) {
        throw TransactionException("No active transaction");
    }
    
    // Acquire exclusive lock on the table
    lockTableExclusive(tableName);
    
    // Save table state if not already saved
    if (tableStates.find(toLowerCase(tableName)) == tableStates.end()) {
        saveTableState(tableName);
    }
    
    // Convert to the format expected by Database::insertRecord
    std::vector<std::vector<std::string>> valuesList;
    valuesList.push_back(values);
    
    database->insertRecord(tableName, valuesList);
}

void Transaction::deleteRecords(const std::string& tableName, const std::string& condition) {
    if (!active) {
        throw TransactionException("No active transaction");
    }
    
    // Acquire exclusive lock on the table
    lockTableExclusive(tableName);
    
    // Save table state if not already saved
    if (tableStates.find(toLowerCase(tableName)) == tableStates.end()) {
        saveTableState(tableName);
    }
    
    database->deleteRecords(tableName, condition);
}

void Transaction::updateRecords(const std::string& tableName,
                              const std::vector<std::pair<std::string, std::string>>& updates,
                              const std::string& condition) {
    if (!active) {
        throw TransactionException("No active transaction");
    }
    
    // Acquire exclusive lock on the table
    lockTableExclusive(tableName);
    
    // Save table state if not already saved
    if (tableStates.find(toLowerCase(tableName)) == tableStates.end()) {
        saveTableState(tableName);
    }
    
    database->updateRecords(tableName, updates, condition);
}

void Transaction::lockTableShared(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    
    // If we already have an exclusive lock, no need to acquire a shared lock
    if (exclusiveLocks.find(lowerName) != exclusiveLocks.end()) {
        return;
    }
    
    // Acquire the lock from the table
    Table* table = database->getTable(tableName);
    if (!table) {
        throw DatabaseException("Table '" + tableName + "' does not exist");
    }
    
    table->lockShared();
    sharedLocks.insert(lowerName);
}

void Transaction::lockTableExclusive(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    
    // If we already have an exclusive lock, no need to reacquire
    if (exclusiveLocks.find(lowerName) != exclusiveLocks.end()) {
        return;
    }
    
    // Acquire the lock from the table
    Table* table = database->getTable(tableName, true); // true for exclusive lock
    if (!table) {
        throw DatabaseException("Table '" + tableName + "' does not exist");
    }
    
    // Remove from shared locks if present
    sharedLocks.erase(lowerName);
    
    // Add to exclusive locks
    exclusiveLocks.insert(lowerName);
}

void Transaction::releaseLocks() {
    // Release all shared locks
    for (const auto& tableName : sharedLocks) {
        Table* table = database->getTable(tableName);
        if (table) {
            table->unlock();
        }
    }
    sharedLocks.clear();
    
    // Release all exclusive locks
    for (const auto& tableName : exclusiveLocks) {
        Table* table = database->getTable(tableName);
        if (table) {
            table->unlock();
        }
    }
    exclusiveLocks.clear();
}

void Transaction::saveTableState(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    
    // Check if we already saved this table's state
    if (tableStates.find(lowerName) != tableStates.end()) {
        return;
    }
    
    Table* table = database->getTable(tableName);
    if (!table) {
        throw DatabaseException("Table '" + tableName + "' does not exist");
    }
    
    // Create a new table state
    TableState state;
    state.tableName = tableName;
    state.originalRows = table->getRows();
    state.columns = table->getColumns();
    state.columnTypes = table->getColumnTypes();
    
    // Store the table state
    tableStates[lowerName] = state;
}

void Transaction::restoreTableState(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    
    // Find the saved state
    auto it = tableStates.find(lowerName);
    if (it == tableStates.end()) {
        return; // Nothing to restore
    }
    
    const TableState& state = it->second;
    
    Table* table = database->getTable(tableName, true); // Get with exclusive lock
    
    if (!table) {
        // If the table was created during this transaction and we're rolling back,
        // then drop the table
        if (state.columns.empty()) {
            database->dropTable(tableName);
        } else {
            // The table was dropped during this transaction, so recreate it
            std::vector<std::pair<std::string, std::string>> columns;
            for (size_t i = 0; i < state.columns.size(); ++i) {
                columns.emplace_back(state.columns[i], state.columnTypes[i]);
            }
            database->createTable(tableName, columns, {});
            
            // Then restore its rows
            table = database->getTable(tableName, true);
            table->clearRows();
            for (const auto& row : state.originalRows) {
                table->addRow(row);
            }
        }
    } else {
        // The table exists, so just restore its rows
        table->clearRows();
        for (const auto& row : state.originalRows) {
            table->addRow(row);
        }
    }
}
// Add to Transaction.cpp if needed

// When main.cpp calls database->commitTransaction(transaction)
void Database::commitTransaction(Transaction* transaction) {
    if (!transaction || !transaction->isActive()) {
        std::cout << "No active transaction to commit." << std::endl;
        return;
    }
    
    transaction->commit();
    
    // Remove from active transactions
    activeTransactions.erase(transaction);
}

// When main.cpp calls database->rollbackTransaction(transaction)
void Database::rollbackTransaction(Transaction* transaction) {
    if (!transaction || !transaction->isActive()) {
        std::cout << "No active transaction to rollback." << std::endl;
        return;
    }
    
    transaction->rollback();
    
    // Remove from active transactions
    activeTransactions.erase(transaction);
}