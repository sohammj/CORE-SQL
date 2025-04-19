#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <set>
#include <queue>
#include "Table.h"
#include "Storage.h"
#include "Catalog.h"
#include "Transaction.h"
#include "user.h"
extern Database* _g_db;

class Database {
public:
Transaction* commitTransaction();
    Transaction* rollbackTransaction();
    Database();
    ~Database();
    
    // DDL operations
    void createTable(const std::string& tableName,
                     const std::vector<std::pair<std::string, std::string>>& columns,
                     const std::vector<Constraint>& constraints);
    void dropTable(const std::string& tableName);
    void alterTableAddColumn(const std::string& tableName, const std::pair<std::string, std::string>& column,
                            bool isNotNull = false);
    void alterTableDropColumn(const std::string& tableName, const std::string& columnName);
    void alterTableRenameColumn(const std::string& tableName, const std::string& oldName, const std::string& newName);
    void alterTableAddConstraint(const std::string& tableName, const Constraint& constraint);
    void alterTableDropConstraint(const std::string& tableName, const std::string& constraintName);
    void describeTable(const std::string& tableName);

    // DML operations
    // Add this to the Database class declaration in database.h
    std::vector<std::vector<std::string>> executeViewQuery(const std::string& viewName);
    void insertRecord(const std::string& tableName, const std::vector<std::vector<std::string>>& values);
    void selectRecords(const std::string& tableName,
        const std::vector<std::string>& selectColumns,
        const std::string& condition,
        const std::vector<std::string>& orderByColumns,
        const std::vector<std::string>& groupByColumns,
        const std::string& havingCondition,
        bool isJoin = false,
        const std::string& joinTable = "",
        const std::string& joinCondition = "",
        const std::string& joinType = "INNER");
    void deleteRecords(const std::string& tableName, const std::string& condition);
    void updateRecords(const std::string& tableName,
                       const std::vector<std::pair<std::string, std::string>>& updates,
                       const std::string& condition);
    void showTables();

    // JOIN operations
    void joinTables(const std::string& leftTable, 
                    const std::string& rightTable,
                    const std::string& joinType,
                    const std::string& joinCondition,
                    const std::vector<std::string>& selectColumns);

    // Transaction management
    Transaction* beginTransaction();
    void commitTransaction(Transaction* transaction);
    void rollbackTransaction(Transaction* transaction);

    // User-defined type management
    void createType(const std::string& typeName, const std::vector<std::pair<std::string, std::string>>& attributes);
    void dropType(const std::string& typeName);

    // View management
    void createView(const std::string& viewName, const std::string& viewDefinition);
    void dropView(const std::string& viewName);
    void describeView(const std::string& viewName);

    // Authentication and authorization
    bool createUser(const std::string& username, const std::string& password);
    bool authenticate(const std::string& username, const std::string& password);
    void grantPrivilege(const std::string& username, const std::string& tableName, 
                       const std::string& privilege);
    void revokePrivilege(const std::string& username, const std::string& tableName, 
                        const std::string& privilege);
    bool checkPrivilege(const std::string& username, const std::string& tableName, 
                       const std::string& privilege);
     void showUserPrivileges(const std::string& username);
    // Assertion management
    void createAssertion(const std::string& name, const std::string& condition);
    void dropAssertion(const std::string& name);

    // Utility functions
    void truncateTable(const std::string& tableName);
    void renameTable(const std::string& oldName, const std::string& newName);
    void createIndex(const std::string& indexName, const std::string& tableName, const std::string& columnName);
    void dropIndex(const std::string& indexName);
    void mergeRecords(const std::string& tableName, const std::string& mergeCommand);
    void replaceInto(const std::string& tableName, const std::vector<std::vector<std::string>>& values);

    // Set operations
    void setOperation(const std::string& operation, 
                     const std::string& leftQuery, 
                     const std::string& rightQuery);

    // Subquery support
    std::vector<std::vector<std::string>> executeSubquery(const std::string& subquery);

    // With clause support
    std::vector<std::vector<std::string>> executeWithClause(
        const std::string& commonTableExpression,
        const std::string& mainQuery);

    // Recursive query support
    std::vector<std::vector<std::string>> executeRecursiveQuery(
        const std::string& initialQuery,
        const std::string& recursiveQuery);

    // Catalog operations
    void showSchema();
    void showViews(); 
    void showCatalog();

    // Get table reference (with appropriate locking)
    Table* getTable(const std::string& tableName, bool exclusiveLock = false);

private:
    bool inTransaction = false;
    std::unordered_map<std::string, std::unique_ptr<Table>> backupTables;
    std::unordered_map<std::string, std::unique_ptr<Table>> tables;
    std::unordered_map<std::string, std::string> views;
    std::unordered_map<std::string, std::pair<std::string, std::string>> indexes;
    std::unordered_map<std::string, std::string> assertions;
    std::unordered_map<std::string, User> users;

    // Track active transactions
    std::set<Transaction*> activeTransactions;

    // Concurrency control
    std::mutex databaseMutex;

    // Catalog information
    Catalog catalog;

    // Helper methods
    bool tableExists(const std::string& tableName);
    bool viewExists(const std::string& viewName);
    void validateReferences(const Constraint& constraint);
    std::vector<std::vector<std::string>> evaluateViewQuery(const std::string& viewName);
    void executeWithClauseHelper(const std::vector<std::pair<std::string, std::string>>& cteList,
                                const std::string& mainQuery);
};

#endif // DATABASE_H