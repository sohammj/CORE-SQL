#include "Storage.h"
#include "Utils.h"
#include <fstream>
#include <iostream>
#include <sstream>

void Storage::saveTableToFile(const Table& table, const std::string& tableName) {
    try {
        std::ofstream tableFile(tableName + ".tbl");
        if (!tableFile) {
            throw DatabaseException("Failed to open file for writing: " + tableName + ".tbl");
        }
        
        // Write table schema
        const auto& columns = table.getColumns();
        const auto& columnTypes = table.getColumnTypes();
        const auto& notNullConstraints = table.getNotNullConstraints();
        const auto& constraints = table.getConstraints();
        
        // Write column definitions
        tableFile << columns.size() << std::endl;
        for (size_t i = 0; i < columns.size(); ++i) {
            tableFile << columns[i] << "," << columnTypes[i] << "," << (notNullConstraints[i] ? "1" : "0") << std::endl;
        }
        
        // Write constraints
        tableFile << constraints.size() << std::endl;
        for (const auto& constraint : constraints) {
            // Write constraint type
            int type = 0;
            switch (constraint.type) {
                case Constraint::Type::PRIMARY_KEY: type = 1; break;
                case Constraint::Type::FOREIGN_KEY: type = 2; break;
                case Constraint::Type::UNIQUE: type = 3; break;
                case Constraint::Type::CHECK: type = 4; break;
                case Constraint::Type::NOT_NULL: type = 5; break;
            }
            
            tableFile << constraint.name << "," << type << ",";
            
            // Write constraint columns
            tableFile << constraint.columns.size();
            for (const auto& col : constraint.columns) {
                tableFile << "," << col;
            }
            
            // Write additional info for foreign keys
            if (constraint.type == Constraint::Type::FOREIGN_KEY) {
                tableFile << "," << constraint.referencedTable;
                tableFile << "," << constraint.referencedColumns.size();
                for (const auto& col : constraint.referencedColumns) {
                    tableFile << "," << col;
                }
                tableFile << "," << (constraint.cascadeDelete ? "1" : "0");
                tableFile << "," << (constraint.cascadeUpdate ? "1" : "0");
            } else if (constraint.type == Constraint::Type::CHECK) {
                tableFile << "," << constraint.checkExpression;
            }
            
            tableFile << std::endl;
        }
        
        // Write rows
        const auto& rows = table.getRows();
        tableFile << rows.size() << std::endl;
        for (const auto& row : rows) {
            tableFile << row.size();
            for (const auto& cell : row) {
                // Escape commas in cell values
                std::string escapedCell = cell;
                size_t pos = 0;
                while ((pos = escapedCell.find(",", pos)) != std::string::npos) {
                    escapedCell.replace(pos, 1, "\\,");
                    pos += 2;
                }
                tableFile << "," << escapedCell;
            }
            tableFile << std::endl;
        }
        
        tableFile.close();
    } catch (const std::exception& e) {
        throw DatabaseException("Error saving table: " + std::string(e.what()));
    }
}

Table* Storage::loadTableFromFile(const std::string& tableName) {
    try {
        std::ifstream tableFile(tableName + ".tbl");
        if (!tableFile) {
            throw DatabaseException("Failed to open file for reading: " + tableName + ".tbl");
        }
        
        std::string line;
        
        // Create table
        Table* table = new Table(tableName);
        
        // Read column definitions
        if (!std::getline(tableFile, line)) {
            throw DatabaseException("Failed to read column count");
        }
        int columnCount = std::stoi(line);
        
        for (int i = 0; i < columnCount; ++i) {
            if (!std::getline(tableFile, line)) {
                throw DatabaseException("Failed to read column definition");
            }
            
            std::vector<std::string> parts = split(line, ',');
            if (parts.size() < 3) {
                throw DatabaseException("Invalid column definition format");
            }
            
            std::string columnName = parts[0];
            std::string columnType = parts[1];
            bool notNull = (parts[2] == "1");
            
            table->addColumn(columnName, columnType, notNull);
        }
        
        // Read constraints
        if (!std::getline(tableFile, line)) {
            throw DatabaseException("Failed to read constraint count");
        }
        int constraintCount = std::stoi(line);
        
        for (int i = 0; i < constraintCount; ++i) {
            if (!std::getline(tableFile, line)) {
                throw DatabaseException("Failed to read constraint definition");
            }
            
            std::vector<std::string> parts = split(line, ',');
            if (parts.size() < 3) {
                throw DatabaseException("Invalid constraint definition format");
            }
            
            std::string constraintName = parts[0];
            int type = std::stoi(parts[1]);
            int columnCount = std::stoi(parts[2]);
            
            Constraint::Type constraintType;
            switch (type) {
                case 1: constraintType = Constraint::Type::PRIMARY_KEY; break;
                case 2: constraintType = Constraint::Type::FOREIGN_KEY; break;
                case 3: constraintType = Constraint::Type::UNIQUE; break;
                case 4: constraintType = Constraint::Type::CHECK; break;
                case 5: constraintType = Constraint::Type::NOT_NULL; break;
                default: throw DatabaseException("Invalid constraint type");
            }
            
            Constraint constraint(constraintType, constraintName);
            
            // Read constraint columns
            for (int j = 0; j < columnCount; ++j) {
                if (3 + j >= parts.size()) {
                    throw DatabaseException("Invalid constraint column count");
                }
                constraint.columns.push_back(parts[3 + j]);
            }
            
            // Read additional info for foreign keys
            if (constraintType == Constraint::Type::FOREIGN_KEY) {
                if (3 + columnCount >= parts.size()) {
                    throw DatabaseException("Invalid foreign key constraint format");
                }
                
                constraint.referencedTable = parts[3 + columnCount];
                int refColumnCount = std::stoi(parts[4 + columnCount]);
                
                for (int j = 0; j < refColumnCount; ++j) {
                    if (5 + columnCount + j >= parts.size()) {
                        throw DatabaseException("Invalid foreign key referenced column count");
                    }
                    constraint.referencedColumns.push_back(parts[5 + columnCount + j]);
                }
                
                if (5 + columnCount + refColumnCount >= parts.size()) {
                    throw DatabaseException("Invalid foreign key cascade options");
                }
                
                constraint.cascadeDelete = (parts[5 + columnCount + refColumnCount] == "1");
                constraint.cascadeUpdate = (parts[6 + columnCount + refColumnCount] == "1");
            } else if (constraintType == Constraint::Type::CHECK) {
                if (3 + columnCount >= parts.size()) {
                    throw DatabaseException("Invalid check constraint format");
                }
                
                constraint.checkExpression = parts[3 + columnCount];
            }
            
            table->addConstraint(constraint);
        }
        
        // Read rows
        if (!std::getline(tableFile, line)) {
            throw DatabaseException("Failed to read row count");
        }
        int rowCount = std::stoi(line);
        
        for (int i = 0; i < rowCount; ++i) {
            if (!std::getline(tableFile, line)) {
                throw DatabaseException("Failed to read row data");
            }
            
            // Split the line, but handle escaped commas
            std::vector<std::string> cellValues;
            std::string currentCell;
            bool escaped = false;
            
            for (char c : line) {
                if (escaped) {
                    if (c == ',') {
                        currentCell += c;
                    } else {
                        currentCell += '\\';
                        currentCell += c;
                    }
                    escaped = false;
                } else if (c == '\\') {
                    escaped = true;
                } else if (c == ',') {
                    cellValues.push_back(currentCell);
                    currentCell.clear();
                } else {
                    currentCell += c;
                }
            }
            
            if (!currentCell.empty() || cellValues.empty()) {
                cellValues.push_back(currentCell);
            }
            
            // First cell is the count
            if (cellValues.size() < 1) {
                throw DatabaseException("Invalid row format");
            }
            
            int cellCount = std::stoi(cellValues[0]);
            if (cellCount + 1 != cellValues.size()) {
                throw DatabaseException("Invalid cell count in row");
            }
            
            std::vector<std::string> rowData(cellValues.begin() + 1, cellValues.end());
            table->addRow(rowData);
        }
        
        tableFile.close();
        return table;
    } catch (const std::exception& e) {
        throw DatabaseException("Error loading table: " + std::string(e.what()));
    }
}

void Storage::saveDatabase(const std::string& dbName, 
                          const std::map<std::string, Table*>& tables,
                          const std::map<std::string, std::string>& views) {
    try {
        std::ofstream dbFile(dbName + ".db");
        if (!dbFile) {
            throw DatabaseException("Failed to open database file for writing: " + dbName + ".db");
        }
        
        // Write tables
        dbFile << tables.size() << std::endl;
        for (const auto& [tableName, table] : tables) {
            dbFile << tableName << std::endl;
            saveTableToFile(*table, tableName);
        }
        
        // Write views
        dbFile << views.size() << std::endl;
        for (const auto& [viewName, viewDef] : views) {
            dbFile << viewName << "," << viewDef << std::endl;
        }
        
        dbFile.close();
    } catch (const std::exception& e) {
        throw DatabaseException("Error saving database: " + std::string(e.what()));
    }
}

void Storage::loadDatabase(const std::string& dbName, 
                          std::map<std::string, Table*>& tables,
                          std::map<std::string, std::string>& views) {
    try {
        std::ifstream dbFile(dbName + ".db");
        if (!dbFile) {
            throw DatabaseException("Failed to open database file for reading: " + dbName + ".db");
        }
        
        std::string line;
        
        // Read tables
        if (!std::getline(dbFile, line)) {
            throw DatabaseException("Failed to read table count");
        }
        int tableCount = std::stoi(line);
        
        for (int i = 0; i < tableCount; ++i) {
            if (!std::getline(dbFile, line)) {
                throw DatabaseException("Failed to read table name");
            }
            
            std::string tableName = line;
            Table* table = loadTableFromFile(tableName);
            tables[toLowerCase(tableName)] = table;
        }
        
        // Read views
        if (!std::getline(dbFile, line)) {
            throw DatabaseException("Failed to read view count");
        }
        int viewCount = std::stoi(line);
        
        for (int i = 0; i < viewCount; ++i) {
            if (!std::getline(dbFile, line)) {
                throw DatabaseException("Failed to read view definition");
            }
            
            size_t commaPos = line.find(',');
            if (commaPos == std::string::npos) {
                throw DatabaseException("Invalid view definition format");
            }
            
            std::string viewName = line.substr(0, commaPos);
            std::string viewDef = line.substr(commaPos + 1);
            
            views[toLowerCase(viewName)] = viewDef;
        }
        
        dbFile.close();
    } catch (const std::exception& e) {
        throw DatabaseException("Error loading database: " + std::string(e.what()));
    }
}