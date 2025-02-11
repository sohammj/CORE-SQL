#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <vector>
#include <unordered_map>
#include "Table.h"
#include "Parser.h"  // For Query

class Database {
public:
    // Existing commands
    void createTable(const std::string& tableName, const std::vector<std::string>& columns);
    void insertRecord(const std::string& tableName, const std::vector<std::vector<std::string>>& values);
    void selectRecords(const Query& q);
    void deleteRecords(const std::string& tableName, const std::string& condition);
    void showTables();


    // DML commands
    void updateRecords(const std::string& tableName, const std::vector<std::pair<std::string, std::string>>& assignments, const std::string& condition);
    void mergeRecords(const Query& q);
    void upsertRecords(const Query& q);

    // DDL commands
    void alterTable(const std::string& tableName, const std::vector<std::string>& newColumns);
    void dropTable(const std::string& tableName);
    void truncateTable(const std::string& tableName);
    void renameTable(const std::string& oldName, const std::string& newName);

    // TCL commands
    void beginTransaction();
    void commitTransaction();
    void rollbackTransaction();
    void savepointTransaction(const std::string& name);
    void setTransaction(const std::string& settings);

    // DCL commands
    void grantPrivilege(const std::string& privilege, const std::string& tableName, const std::string& user);
    void revokePrivilege(const std::string& privilege, const std::string& tableName, const std::string& user);
    void denyPrivilege(const std::string& privilege, const std::string& tableName, const std::string& user);

private:
    std::unordered_map<std::string, Table> tables; // stored in lowercase

    // Transaction support
    bool inTransaction = false;
    std::unordered_map<std::string, Table> backupTables; // backup at BEGIN

    // Savepoints: key = savepoint name, value = backup copy of entire tables
    std::unordered_map<std::string, std::unordered_map<std::string, Table>> savepoints;

    // Privileges (simple simulation)
    struct Privilege {
        std::string user;
        std::string table;
        std::string privilege;
    };
    std::vector<Privilege> privileges;
};

#endif // DATABASE_H
