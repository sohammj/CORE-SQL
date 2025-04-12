#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include "Utils.h"

// Structure to store table constraints
struct Constraint {
    enum class Type {
        PRIMARY_KEY,
        FOREIGN_KEY,
        UNIQUE,
        CHECK,
        NOT_NULL
    };
    
    Type type;
    std::string name;
    std::vector<std::string> columns;
    std::string checkExpression;  // For CHECK constraints
    std::string referencedTable;  // For FOREIGN KEY
    std::vector<std::string> referencedColumns; // For FOREIGN KEY
    bool cascadeDelete = false;   // For FOREIGN KEY
    bool cascadeUpdate = false;   // For FOREIGN KEY
    
    Constraint(Type t, const std::string& n) : type(t), name(n) {}
};

class Table {
public:
    Table(const std::string& name);
    virtual ~Table() = default;
    
    // Schema operations
    void addColumn(const std::string& columnName, const std::string& type, bool isNotNull = false);
    bool dropColumn(const std::string& columnName);
    void renameColumn(const std::string& oldName, const std::string& newName);
    
    // Constraint operations
    void addConstraint(const Constraint& constraint);
    bool dropConstraint(const std::string& constraintName);
    bool validateConstraints(const std::vector<std::string>& row);
    bool validateConstraintsForUpdate(const std::vector<std::string>& oldRow, const std::vector<std::string>& newRow);
    
    // DML operations
    int addRow(const std::vector<std::string>& values);
    int addRowWithId(int rowId, const std::vector<std::string>& values);
    std::vector<std::vector<std::string>> selectRows(
        const std::vector<std::string>& selectColumns,
        const std::string& condition = "",
        const std::vector<std::string>& orderByColumns = {},
        const std::vector<std::string>& groupByColumns = {},
        const std::string& havingCondition = "");
    void printTable();
    void deleteRows(const std::string& condition);
    void updateRows(const std::vector<std::pair<std::string, std::string>>& updates,
                    const std::string& condition);
    void clearRows();
    
    // Join operations
    std::vector<std::vector<std::string>> innerJoin(
        Table& rightTable,
        const std::string& condition,
        const std::vector<std::string>& selectColumns);
    std::vector<std::vector<std::string>> leftOuterJoin(
        Table& rightTable,
        const std::string& condition,
        const std::vector<std::string>& selectColumns);
    std::vector<std::vector<std::string>> rightOuterJoin(
        Table& rightTable,
        const std::string& condition,
        const std::vector<std::string>& selectColumns);
    std::vector<std::vector<std::string>> fullOuterJoin(
        Table& rightTable,
        const std::string& condition,
        const std::vector<std::string>& selectColumns);
    std::vector<std::vector<std::string>> naturalJoin(
        Table& rightTable,
        const std::vector<std::string>& selectColumns);
    
    // Utility functions
    void sortRows(const std::string& columnName, bool ascending = true);
    int getRowCount() const;
    bool hasColumn(const std::string& columnName) const;
    int getColumnIndex(const std::string& columnName) const;
    
    // Set operations
    std::vector<std::vector<std::string>> setUnion(const std::vector<std::vector<std::string>>& otherResult);
    std::vector<std::vector<std::string>> setIntersect(const std::vector<std::vector<std::string>>& otherResult);
    std::vector<std::vector<std::string>> setExcept(const std::vector<std::vector<std::string>>& otherResult);
    
    // Accessors/mutators
    const std::string& getName() const { return tableName; }
    const std::vector<std::string>& getColumns() const { return columns; }
    const std::vector<std::string>& getColumnTypes() const { return columnTypes; }
    const std::vector<bool>& getNotNullConstraints() const { return notNullConstraints; }
    const std::vector<std::vector<std::string>>& getRows() const { return rows; }
    std::vector<std::vector<std::string>>& getRowsNonConst() { return rows; }
    const std::vector<Constraint>& getConstraints() const { return constraints; }
    
    // Transaction support
    void lockShared();
    void lockExclusive();
    void unlock();
    
    friend class Transaction;
    friend class Database;

private:
    std::string tableName;
    std::vector<std::string> columns;
    std::vector<std::string> columnTypes;
    std::vector<bool> notNullConstraints;
    std::vector<std::vector<std::string>> rows;
    std::vector<Constraint> constraints;
    int nextRowId = 1;
    
    // Concurrency control
    mutable std::shared_mutex mutex;
    
    // Helper methods
    bool validateForeignKeyConstraint(const Constraint& constraint, const std::vector<std::string>& row);
    bool validateUniqueConstraint(const Constraint& constraint, const std::vector<std::string>& row);
    bool validateCheckConstraint(const Constraint& constraint, const std::vector<std::string>& row);
    bool validateNotNullConstraint(int columnIndex, const std::string& value);
    void enforceDataType(int columnIndex, std::string& value);
};

#endif // TABLE_H