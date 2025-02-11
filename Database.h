#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <vector>
#include <unordered_map>
#include "Table.h"

class Database {
public:
    void createTable(const std::string& tableName, const std::vector<std::string>& columns);
    void insertRecord(const std::string& tableName, const std::vector<std::vector<std::string>>& values);
    void selectRecords(const std::string& tableName, const std::string& condition);
    void deleteRecords(const std::string& tableName, const std::string& condition);
    void showTables();

private:
    std::unordered_map<std::string, Table> tables;
};

#endif // DATABASE_H