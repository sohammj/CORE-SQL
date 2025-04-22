#ifndef CATALOG_H
#define CATALOG_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include "Utils.h"
#include "Table.h"

// Structure to store table metadata
struct TableInfo {
    std::string name;
    std::vector<std::string> columns;
    std::vector<std::string> columnTypes;
    std::vector<bool> notNullConstraints;
    std::vector<Constraint> constraints;
};

// Structure to store view metadata
struct ViewInfo {
    std::string name;
    std::string definition;
    bool isUpdatable;
};

// Structure to store index metadata
struct IndexInfo {
    std::string name;
    std::string tableName;
    std::string columnName;
    bool isUnique;
};

// Structure to store user-defined type metadata
struct TypeInfo {
    std::string name;
    std::vector<std::pair<std::string, std::string>> attributes;
};

// Structure to store assertion metadata
struct AssertionInfo {
    std::string name;
    std::string condition;
};

// Structure to store privilege metadata
struct PrivilegeInfo {
    std::string username;
    std::string objectName;
    std::string privilegeType;  // SELECT, INSERT, UPDATE, DELETE, ALL
    bool withGrantOption;
};

// The Catalog maintains metadata about database objects
class Catalog {
public:
    // Initialize catalog
    Catalog();
    
    // Table metadata
    void addTable(const std::string& tableName, 
                 const std::vector<std::string>& columns,
                 const std::vector<std::string>& columnTypes,
                 const std::vector<bool>& notNullConstraints,
                 const std::vector<Constraint>& constraints);
    void removeTable(const std::string& tableName);
    void renameTable(const std::string& oldName, const std::string& newName);
    bool tableExists(const std::string& tableName) const;
    const TableInfo& getTableInfo(const std::string& tableName) const;
    
    // View metadata
    void addView(const std::string& viewName, const std::string& definition, bool isUpdatable = false);
    void removeView(const std::string& viewName);
    bool viewExists(const std::string& viewName) const;
    const ViewInfo& getViewInfo(const std::string& viewName) const;
    
    // Index metadata
    void addIndex(const std::string& indexName, const std::string& tableName, 
                 const std::string& columnName, bool isUnique = false);
    void removeIndex(const std::string& indexName);
    bool indexExists(const std::string& indexName) const;
    const IndexInfo& getIndexInfo(const std::string& indexName) const;
    
    // User-defined type metadata
    void addType(const std::string& typeName, const std::vector<std::pair<std::string, std::string>>& attributes);
    void removeType(const std::string& typeName);
    bool typeExists(const std::string& typeName) const;
    const TypeInfo& getTypeInfo(const std::string& typeName) const;
    
    // Assertion metadata
    void addAssertion(const std::string& assertionName, const std::string& condition);
    void removeAssertion(const std::string& assertionName);
    bool assertionExists(const std::string& assertionName) const;
    const AssertionInfo& getAssertionInfo(const std::string& assertionName) const;
    
    // Privilege metadata
    void addPrivilege(const std::string& username, const std::string& objectName, 
                     const std::string& privilegeType, bool withGrantOption = false);
    void removePrivilege(const std::string& username, const std::string& objectName, 
                        const std::string& privilegeType);
    bool checkPrivilege(const std::string& username, const std::string& objectName, 
                       const std::string& privilegeType) const;
    std::vector<PrivilegeInfo> getUserPrivileges(const std::string& username) const;
    
    // Schema information
    void showSchema() const;
    void showTables() const;
    void showViews() const;
    void showIndexes() const;
    void showTypes() const;
    void showAssertions() const;
    void showPrivileges() const;
    
    // Get lists of object names
    std::vector<std::string> getTableNames() const;
    std::vector<std::string> getViewNames() const;
    std::vector<std::string> getIndexNames() const;
    std::vector<std::string> getTypeNames() const;
    std::vector<std::string> getAssertionNames() const;
    
private:
    std::unordered_map<std::string, TableInfo> tables;
    std::unordered_map<std::string, ViewInfo> views;
    std::unordered_map<std::string, IndexInfo> indexes;
    std::unordered_map<std::string, TypeInfo> types;
    std::unordered_map<std::string, AssertionInfo> assertions;
    std::vector<PrivilegeInfo> privileges;
};

#endif // CATALOG_H