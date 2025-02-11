#include "Storage.h"
#include <fstream>

void Storage::saveTableToFile(const Table& table, const std::string& tableName) {
    std::ofstream file(tableName + ".txt");
    // Here you would serialize the table data to a file
    file.close();
}

Table Storage::loadTableFromFile(const std::string& tableName) {
    Table table;
    std::ifstream file(tableName + ".txt");
    // Here you would deserialize the table data from a file
    file.close();
    return table;
}