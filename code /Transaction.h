#ifndef TRANSACTION_H
#define TRANSACTION_H

#include <string>
#include <unordered_map>
#include <vector>
#include <set>
#include <memory>

// Forward declarations
class Database;
class Table;

// Transaction isolation levels
enum class IsolationLevel {
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE
};

// Transaction class to handle ACID properties
class Transaction {
public:
    Transaction(Database* db, IsolationLevel level = IsolationLevel::SERIALIZABLE);
    ~Transaction();
    
    // Begin the transaction
    void begin();
    
    // Commit the transaction
    void commit();
    
    // Rollback the transaction
    void rollback();
    
    // DDL operations
    void createTable(const std::string& tableName,
                    const std::vector<std::pair<std::string, std::string>>& columns);
    void dropTable(const std::string& tableName);
    void alterTable(const std::string& tableName, const std::string& operation);
    
    // DML operations
    void insertRecord(const std::string& tableName, const std::vector<std::string>& values);
    void deleteRecords(const std::string& tableName, const std::string& condition);
    void updateRecords(const std::string& tableName,
                      const std::vector<std::pair<std::string, std::string>>& updates,
                      const std::string& condition);
    
    // Transaction status
    bool isActive() const { return active; }
    
    // Transaction ID
    int getId() const { return transactionId; }
    
    // Lock a table for reading
    void lockTableShared(const std::string& tableName);
    
    // Lock a table for writing
    void lockTableExclusive(const std::string& tableName);
    
    // Release locks
    void releaseLocks();
    
private:
    Database* database;
    IsolationLevel isolationLevel;
    bool active;
    int transactionId;
    
    // Tables modified during the transaction (for rollback)
    struct TableState {
        std::string tableName;
        std::vector<std::vector<std::string>> originalRows;
        std::vector<std::string> columns;
        std::vector<std::string> columnTypes;
    };
    
    // Track table states for rollback
    std::unordered_map<std::string, TableState> tableStates;
    
    // Track locked tables
    std::set<std::string> sharedLocks;
    std::set<std::string> exclusiveLocks;
    
    // Save table state for rollback
    void saveTableState(const std::string& tableName);
    
    // Restore table state on rollback
    void restoreTableState(const std::string& tableName);
    
    static int nextTransactionId;
};

#endif // TRANSACTION_H