#ifndef STORAGE_H
#define STORAGE_H

#include "Table.h"
#include <string>
#include <map>

class Storage {
public:
    // Table persistence
    void saveTableToFile(const Table& table, const std::string& tableName);
    Table* loadTableFromFile(const std::string& tableName);
    
    // Database persistence
    void saveDatabase(const std::string& dbName, 
                     const std::map<std::string, Table*>& tables,
                     const std::map<std::string, std::string>& views);
    void loadDatabase(const std::string& dbName, 
                     std::map<std::string, Table*>& tables,
                     std::map<std::string, std::string>& views);
};

#endif // STORAGE_H