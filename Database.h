#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <vector>
#include <unordered_map>
#include "Table.h"

class Database {
public:
    // DDL
    void createTable(const std::string& tableName,
                     const std::vector<std::pair<std::string, std::string>>& columns);
    void dropTable(const std::string& tableName);
    void alterTableAddColumn(const std::string& tableName, const std::pair<std::string, std::string>& column);
    void alterTableDropColumn(const std::string& tableName, const std::string& columnName);
    void describeTable(const std::string& tableName);

    // DML
    void insertRecord(const std::string& tableName,
                      const std::vector<std::vector<std::string>>& values);
    void selectRecords(const std::string& tableName,
                       const std::vector<std::string>& selectColumns,
                       const std::string& condition,
                       const std::vector<std::string>& orderByColumns = {},
                       const std::vector<std::string>& groupByColumns = {},
                       const std::string& havingCondition = "",
                       bool isJoin = false,
                       const std::string& joinTable = "",
                       const std::string& joinCondition = "");
    void deleteRecords(const std::string& tableName, const std::string& condition);
    void updateRecords(const std::string& tableName,
                       const std::vector<std::pair<std::string, std::string>>& updates,
                       const std::string& condition);
    void showTables();

    // Transaction commands (simulated)
    void beginTransaction();
    void commitTransaction();
    void rollbackTransaction();
private:
    std::unordered_map<std::string, Table> tables;
    // For basic transaction simulation.
    bool inTransaction = false;
    std::unordered_map<std::string, Table> backupTables;
};

#endif // DATABASE_H
