#ifndef FOREIGN_KEY_VALIDATOR_H
#define FOREIGN_KEY_VALIDATOR_H

#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <mutex>

// Forward declaration
struct Constraint;

// Rename to avoid conflict with TableInfo in Catalog.h
struct FKTableInfo {
    std::string tableName;
    std::vector<std::string> columns;
    // Function to check if a value exists in the table's column
    std::function<bool(const std::string&, const std::string&)> valueExists;
    // Function to retrieve all rows for validation
    std::function<std::vector<std::vector<std::string>>()> getAllRows;
};

class ForeignKeyValidator {
public:
    // Singleton access
    static ForeignKeyValidator& getInstance() {
        static ForeignKeyValidator instance;
        return instance;
    }

    // Register a table for FK validation
    void registerTable(
        const std::string& tableName, 
        const std::vector<std::string>& columns,
        std::function<bool(const std::string&, const std::string&)> valueExists,
        std::function<std::vector<std::vector<std::string>>()> getAllRows
    );

    // Unregister a table (when dropping table)
    void unregisterTable(const std::string& tableName);

    // Validate a foreign key constraint
    bool validateForeignKey(
        const Constraint& constraint, 
        const std::vector<std::string>& row,
        const std::vector<std::string>& sourceColumns
    );

private:
    // Private constructor for singleton
    ForeignKeyValidator() = default;
    ~ForeignKeyValidator() = default;

    // Prevent copying
    ForeignKeyValidator(const ForeignKeyValidator&) = delete;
    ForeignKeyValidator& operator=(const ForeignKeyValidator&) = delete;

    // Registered tables
    std::unordered_map<std::string, FKTableInfo> tables;
    
    // Mutex for thread safety
    std::mutex mutex;
};

#endif // FOREIGN_KEY_VALIDATOR_H