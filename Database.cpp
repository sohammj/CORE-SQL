#include "Database.h"
#include <iostream>
#include "Utils.h"

void Database::createTable(const std::string& tableName, const std::vector<std::string>& columns) {
    Table table;
    for (const auto& column : columns) {
        table.addColumn(column, "string"); // For simplicity, assume all columns are of type string
    }
    tables[toLowerCase(tableName)] = table;
    std::cout << "Table " << tableName << " created." << std::endl;
}

void Database::insertRecord(const std::string& tableName, const std::vector<std::vector<std::string>>& values) {
    std::string lowerTableName = toLowerCase(tableName);
    if (tables.find(lowerTableName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    for (const auto& valueSet : values) {
        tables[lowerTableName].addRow(valueSet);
    }
    std::cout << "Record(s) inserted into " << tableName << "." << std::endl;
}

void Database::selectRecords(const std::string& tableName, const std::string& condition) {
    std::string lowerTableName = toLowerCase(tableName);
    if (tables.find(lowerTableName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    tables[lowerTableName].printTable();
}

void Database::deleteRecords(const std::string& tableName, const std::string& condition) {
    std::string lowerTableName = toLowerCase(tableName);
    if (tables.find(lowerTableName) == tables.end()) {
        std::cout << "Table " << tableName << " does not exist." << std::endl;
        return;
    }
    tables[lowerTableName].deleteRows(condition);
    std::cout << "Records deleted from " << tableName << "." << std::endl;
}

void Database::showTables() {
    for (const auto& table : tables) {
        std::cout << table.first << std::endl;
    }
}