#include "Catalog.h"
#include <iostream>
#include <algorithm>
#include "Utils.h"
Catalog::Catalog() {}
// Table metadata management
void Catalog::addTable(const std::string& tableName, 
                     const std::vector<std::string>& columns,
                     const std::vector<std::string>& columnTypes,
                     const std::vector<bool>& notNullConstraints,
                     const std::vector<Constraint>& constraints) {
    std::string lowerName = toLowerCase(tableName);
    TableInfo info;
    info.name = tableName;
    info.columns = columns;
    info.columnTypes = columnTypes;
    info.notNullConstraints = notNullConstraints;
    info.constraints = constraints;
    tables[lowerName] = info;
}
void Catalog::removeTable(const std::string& tableName) {
    std::string lowerName = toLowerCase(tableName);
    tables.erase(lowerName);
    
    // Remove any indexes associated with this table
    for (auto it = indexes.begin(); it != indexes.end();) {
        if (toLowerCase(it->second.tableName) == lowerName) {
            it = indexes.erase(it);
        } else {
            ++it;
        }
    }
    
    // Remove any privileges associated with this table
    privileges.erase(
        std::remove_if(privileges.begin(), privileges.end(),
            [&](const PrivilegeInfo& p) { 
                return toLowerCase(p.objectName) == lowerName; 
            }),
        privileges.end());
}
void Catalog::renameTable(const std::string& oldName, const std::string& newName) {
    std::string lowerOldName = toLowerCase(oldName);
    std::string lowerNewName = toLowerCase(newName);
    
    if (tables.find(lowerOldName) != tables.end()) {
        TableInfo info = tables[lowerOldName];
        info.name = newName;
        tables[lowerNewName] = info;
        tables.erase(lowerOldName);
        
        // Update any indexes associated with this table
        for (auto& indexPair : indexes) {
            if (toLowerCase(indexPair.second.tableName) == lowerOldName) {
                indexPair.second.tableName = newName;
            }
        }
        
        // Update any privileges associated with this table
        for (auto& privilege : privileges) {
            if (toLowerCase(privilege.objectName) == lowerOldName) {
                privilege.objectName = newName;
            }
        }
    }
}
bool Catalog::tableExists(const std::string& tableName) const {
    return tables.find(toLowerCase(tableName)) != tables.end();
}
const TableInfo& Catalog::getTableInfo(const std::string& tableName) const {
    auto it = tables.find(toLowerCase(tableName));
    if (it == tables.end()) {
        throw DatabaseException("Table '" + tableName + "' does not exist");
    }
    return it->second;
}
// View metadata management
void Catalog::addView(const std::string& viewName, const std::string& definition, bool isUpdatable) {
    std::string lowerName = toLowerCase(viewName);
    ViewInfo info;
    info.name = viewName;
    info.definition = definition;
    info.isUpdatable = isUpdatable;
    views[lowerName] = info;
}
void Catalog::removeView(const std::string& viewName) {
    views.erase(toLowerCase(viewName));
    
    // Remove any privileges associated with this view
    privileges.erase(
        std::remove_if(privileges.begin(), privileges.end(),
            [&](const PrivilegeInfo& p) { 
                return toLowerCase(p.objectName) == toLowerCase(viewName); 
            }),
        privileges.end());
}
bool Catalog::viewExists(const std::string& viewName) const {
    return views.find(toLowerCase(viewName)) != views.end();
}
const ViewInfo& Catalog::getViewInfo(const std::string& viewName) const {
    auto it = views.find(toLowerCase(viewName));
    if (it == views.end()) {
        throw DatabaseException("View '" + viewName + "' does not exist");
    }
    return it->second;
}
// Index metadata management
void Catalog::addIndex(const std::string& indexName, const std::string& tableName, 
                      const std::string& columnName, bool isUnique) {
    std::string lowerName = toLowerCase(indexName);
    IndexInfo info;
    info.name = indexName;
    info.tableName = tableName;
    info.columnName = columnName;
    info.isUnique = isUnique;
    indexes[lowerName] = info;
}
void Catalog::removeIndex(const std::string& indexName) {
    indexes.erase(toLowerCase(indexName));
}
bool Catalog::indexExists(const std::string& indexName) const {
    return indexes.find(toLowerCase(indexName)) != indexes.end();
}
const IndexInfo& Catalog::getIndexInfo(const std::string& indexName) const {
    auto it = indexes.find(toLowerCase(indexName));
    if (it == indexes.end()) {
        throw DatabaseException("Index '" + indexName + "' does not exist");
    }
    return it->second;
}
// User-defined type metadata management
void Catalog::addType(const std::string& typeName, const std::vector<std::pair<std::string, std::string>>& attributes) {
    std::string lowerName = toLowerCase(typeName);
    TypeInfo info;
    info.name = typeName;
    info.attributes = attributes;
    types[lowerName] = info;
}
void Catalog::removeType(const std::string& typeName) {
    types.erase(toLowerCase(typeName));
}
bool Catalog::typeExists(const std::string& typeName) const {
    return types.find(toLowerCase(typeName)) != types.end();
}
const TypeInfo& Catalog::getTypeInfo(const std::string& typeName) const {
    auto it = types.find(toLowerCase(typeName));
    if (it == types.end()) {
        throw DatabaseException("Type '" + typeName + "' does not exist");
    }
    return it->second;
}
// Assertion metadata management
void Catalog::addAssertion(const std::string& assertionName, const std::string& condition) {
    std::string lowerName = toLowerCase(assertionName);
    AssertionInfo info;
    info.name = assertionName;
    info.condition = condition;
    assertions[lowerName] = info;
}
void Catalog::removeAssertion(const std::string& assertionName) {
    assertions.erase(toLowerCase(assertionName));
}
bool Catalog::assertionExists(const std::string& assertionName) const {
    return assertions.find(toLowerCase(assertionName)) != assertions.end();
}
const AssertionInfo& Catalog::getAssertionInfo(const std::string& assertionName) const {
    auto it = assertions.find(toLowerCase(assertionName));
    if (it == assertions.end()) {
        throw DatabaseException("Assertion '" + assertionName + "' does not exist");
    }
    return it->second;
}
// Privilege metadata management
void Catalog::addPrivilege(const std::string& username, const std::string& objectName, 
                          const std::string& privilegeType, bool withGrantOption) {
    PrivilegeInfo info;
    info.username = username;
    info.objectName = objectName;
    info.privilegeType = privilegeType;
    info.withGrantOption = withGrantOption;
    
    // Remove any existing privilege of the same type on the same object
    privileges.erase(
        std::remove_if(privileges.begin(), privileges.end(),
            [&](const PrivilegeInfo& p) { 
                return toLowerCase(p.username) == toLowerCase(username) && 
                       toLowerCase(p.objectName) == toLowerCase(objectName) && 
                       toLowerCase(p.privilegeType) == toLowerCase(privilegeType); 
            }),
        privileges.end());
    
    privileges.push_back(info);
}
void Catalog::removePrivilege(const std::string& username, const std::string& objectName, 
                             const std::string& privilegeType) {
    privileges.erase(
        std::remove_if(privileges.begin(), privileges.end(),
            [&](const PrivilegeInfo& p) { 
                return toLowerCase(p.username) == toLowerCase(username) && 
                       toLowerCase(p.objectName) == toLowerCase(objectName) && 
                       (toLowerCase(p.privilegeType) == toLowerCase(privilegeType) || 
                        toLowerCase(privilegeType) == "all"); 
            }),
        privileges.end());
}
bool Catalog::checkPrivilege(const std::string& username, const std::string& objectName, 
                            const std::string& privilegeType) const {
    for (const auto& p : privileges) {
        if (toLowerCase(p.username) == toLowerCase(username) && 
            toLowerCase(p.objectName) == toLowerCase(objectName) && 
            (toLowerCase(p.privilegeType) == toLowerCase(privilegeType) || 
             toLowerCase(p.privilegeType) == "all")) {
            return true;
        }
    }
    return false;
}
std::vector<PrivilegeInfo> Catalog::getUserPrivileges(const std::string& username) const {
    std::vector<PrivilegeInfo> result;
    for (const auto& p : privileges) {
        if (toLowerCase(p.username) == toLowerCase(username)) {
            result.push_back(p);
        }
    }
    return result;
}
// Schema display methods
void Catalog::showSchema() const {
    std::cout << "=== DATABASE SCHEMA ===" << std::endl;
    
    // Show tables
    std::cout << "\n=== TABLES ===" << std::endl;
    for (const auto& tablePair : tables) {
        const TableInfo& table = tablePair.second;
        std::cout << "Table: " << table.name << std::endl;
        
        // Print columns
        std::cout << "  Columns:" << std::endl;
        for (size_t i = 0; i < table.columns.size(); ++i) {
            std::cout << "    " << table.columns[i] << " " << table.columnTypes[i];
            if (table.notNullConstraints[i]) {
                std::cout << " NOT NULL";
            }
            std::cout << std::endl;
        }
        
        // Print constraints
        if (!table.constraints.empty()) {
            std::cout << "  Constraints:" << std::endl;
            for (const auto& constraint : table.constraints) {
                std::cout << "    " << constraint.name << " (";
                switch (constraint.type) {
                    case Constraint::Type::PRIMARY_KEY:
                        std::cout << "PRIMARY KEY";
                        break;
                    case Constraint::Type::FOREIGN_KEY:
                        std::cout << "FOREIGN KEY";
                        break;
                    case Constraint::Type::UNIQUE:
                        std::cout << "UNIQUE";
                        break;
                    case Constraint::Type::CHECK:
                        std::cout << "CHECK";
                        break;
                    case Constraint::Type::NOT_NULL:
                        std::cout << "NOT NULL";
                        break;
                }
                std::cout << ")" << std::endl;
            }
        }
        
        std::cout << std::endl;
    }
    
    // Show views
    if (!views.empty()) {
        std::cout << "\n=== VIEWS ===" << std::endl;
        for (const auto& viewPair : views) {
            const ViewInfo& view = viewPair.second;
            std::cout << "View: " << view.name << std::endl;
            std::cout << "  Definition: " << view.definition << std::endl;
            std::cout << "  Updatable: " << (view.isUpdatable ? "Yes" : "No") << std::endl;
            std::cout << std::endl;
        }
    }
    
    // Show indexes
    if (!indexes.empty()) {
        std::cout << "\n=== INDEXES ===" << std::endl;
        for (const auto& indexPair : indexes) {
            const IndexInfo& index = indexPair.second;
            std::cout << "Index: " << index.name << std::endl;
            std::cout << "  Table: " << index.tableName << std::endl;
            std::cout << "  Column: " << index.columnName << std::endl;
            std::cout << "  Unique: " << (index.isUnique ? "Yes" : "No") << std::endl;
            std::cout << std::endl;
        }
    }
    
    // Show user-defined types
    if (!types.empty()) {
        std::cout << "\n=== USER-DEFINED TYPES ===" << std::endl;
        for (const auto& typePair : types) {
            const TypeInfo& type = typePair.second;
            std::cout << "Type: " << type.name << std::endl;
            std::cout << "  Attributes:" << std::endl;
            for (const auto& attr : type.attributes) {
                std::cout << "    " << attr.first << " " << attr.second << std::endl;
            }
            std::cout << std::endl;
        }
    }
    
    // Show assertions
    if (!assertions.empty()) {
        std::cout << "\n=== ASSERTIONS ===" << std::endl;
        for (const auto& assertionPair : assertions) {
            const AssertionInfo& assertion = assertionPair.second;
            std::cout << "Assertion: " << assertion.name << std::endl;
            std::cout << "  Condition: " << assertion.condition << std::endl;
            std::cout << std::endl;
        }
    }
}
void Catalog::showTables() const {
    std::cout << "Tables:" << std::endl;
    for (const auto& tablePair : tables) {
        std::cout << "  " << tablePair.second.name << std::endl;
    }
}
void Catalog::showViews() const {
    std::cout << "Views:" << std::endl;
    for (const auto& viewPair : views) {
        std::cout << "  " << viewPair.second.name << std::endl;
    }
}
void Catalog::showIndexes() const {
    std::cout << "Indexes:" << std::endl;
    for (const auto& indexPair : indexes) {
        std::cout << "  " << indexPair.second.name << " on " 
                 << indexPair.second.tableName << "(" << indexPair.second.columnName << ")" << std::endl;
    }
}
void Catalog::showTypes() const {
    std::cout << "User-defined Types:" << std::endl;
    for (const auto& typePair : types) {
        std::cout << "  " << typePair.second.name << std::endl;
    }
}
void Catalog::showAssertions() const {
    std::cout << "Assertions:" << std::endl;
    for (const auto& assertionPair : assertions) {
        std::cout << "  " << assertionPair.second.name << std::endl;
    }
}
void Catalog::showPrivileges() const {
    std::cout << "Privileges:" << std::endl;
    for (const auto& privilege : privileges) {
        std::cout << "  " << privilege.username << " has " << privilege.privilegeType 
                 << " on " << privilege.objectName;
        if (privilege.withGrantOption) {
            std::cout << " WITH GRANT OPTION";
        }
        std::cout << std::endl;
    }
}
// Get lists of object names
std::vector<std::string> Catalog::getTableNames() const {
    std::vector<std::string> result;
    for (const auto& tablePair : tables) {
        result.push_back(tablePair.second.name);
    }
    return result;
}
std::vector<std::string> Catalog::getViewNames() const {
    std::vector<std::string> result;
    for (const auto& viewPair : views) {
        result.push_back(viewPair.second.name);
    }
    return result;
}
std::vector<std::string> Catalog::getIndexNames() const {
    std::vector<std::string> result;
    for (const auto& indexPair : indexes) {
        result.push_back(indexPair.second.name);
    }
    return result;
}
std::vector<std::string> Catalog::getTypeNames() const {
    std::vector<std::string> result;
    for (const auto& typePair : types) {
        result.push_back(typePair.second.name);
    }
    return result;
}
std::vector<std::string> Catalog::getAssertionNames() const {
    std::vector<std::string> result;
    for (const auto& assertionPair : assertions) {
        result.push_back(assertionPair.second.name);
    }
    return result;
}