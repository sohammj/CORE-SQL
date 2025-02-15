#ifndef STORAGE_H
#define STORAGE_H

#include "Table.h"
#include <string>

class Storage {
public:
void saveTableToFile(const Table& table, const std::string& tableName);
Table loadTableFromFile(const std::string& tableName);
};

#endif // STORAGE_H