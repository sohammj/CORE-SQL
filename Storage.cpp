#include "Storage.h"
#include <fstream>

void Storage::saveTableToFile(const Table& table, const std::string& tableName) {
    std::ofstream file(tableName + ".txt");
    // Serialization code would go here.
    file.close();
}

Table Storage::loadTableFromFile(const std::string& tableName) {
    Table table;
    std::ifstream file(tableName + ".txt");
    // Deserialization code would go here.
    file.close();
    return table;
}