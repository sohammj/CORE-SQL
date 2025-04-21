#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <memory>
#include "Database.h"
#include "Parser.h"
#include "Utils.h"
#include "Transaction.h"
Database* g_db = nullptr;


int main() {
    try {
        // Create database instance
        Database db;
        Parser parser;
        std::string commandBuffer;
        std::string line;
        
        // Current user context
        std::string currentUser = "admin"; // Default admin user
        bool authenticated = true; // Admin is pre-authenticated
        
        // Create default admin user
        db.createUser("admin", "admin");
        
        // Current transaction
        Transaction* currentTransaction = nullptr;
        
        std::cout << "Advanced SQL Database Management System" << std::endl;
        std::cout << "Type 'HELP' for available commands or 'EXIT' to quit" << std::endl;
        
        while (true) {
            // Primary prompt if starting a new command; continuation prompt otherwise
            if (commandBuffer.empty()) {
                std::cout << currentUser << "@sql> ";
            } else {
                std::cout << "   -> ";
            }
            
            if (!std::getline(std::cin, line))
                break;  // Exit on EOF
            
            line = trim(line);
            
            // Special handling for HELP command
            if (toUpperCase(line) == "HELP") {
                std::cout << "\nAvailable commands:\n";
                std::cout << "  DDL Commands:\n";
                std::cout << "    CREATE TABLE tableName (column1 type1, column2 type2, ...)\n";
                std::cout << "    ALTER TABLE tableName ADD columnName dataType\n";
                std::cout << "    ALTER TABLE tableName DROP columnName\n";
                std::cout << "    DROP TABLE tableName\n";
                std::cout << "    CREATE INDEX indexName ON tableName (columnName)\n";
                std::cout << "    DROP INDEX indexName\n";
                std::cout << "    CREATE VIEW viewName AS selectQuery\n";
                std::cout << "    DROP VIEW viewName\n";
                std::cout << "    CREATE TYPE typeName (attr1 type1, attr2 type2, ...)\n";
                
                std::cout << "\n  DML Commands:\n";
                std::cout << "    INSERT INTO tableName VALUES (value1, value2, ...)\n";
                std::cout << "    SELECT column1, column2, ... FROM tableName [WHERE condition]\n";
                std::cout << "    UPDATE tableName SET column1=value1, ... [WHERE condition]\n";
                std::cout << "    DELETE FROM tableName [WHERE condition]\n";
                std::cout << "    MERGE INTO target USING source ON condition ...\n";
                std::cout << "    REPLACE INTO tableName VALUES (value1, value2, ...)\n";
                
                std::cout << "\n  Transaction Control:\n";
                std::cout << "    BEGIN [TRANSACTION]\n";
                std::cout << "    COMMIT\n";
                std::cout << "    ROLLBACK\n";
                
                std::cout << "\n  User Management:\n";
                std::cout << "    CREATE USER username PASSWORD 'password'\n";
                std::cout << "    GRANT privilege ON tableName TO username\n";
                std::cout << "    REVOKE privilege ON tableName FROM username\n";
                std::cout << "    LOGIN username PASSWORD 'password'\n";
                std::cout << "    LOGOUT\n";
                
                std::cout << "\n  Utility Commands:\n";
                std::cout << "    DESCRIBE tableName\n";
                std::cout << "    SHOW TABLES\n";
                std::cout << "    SHOW VIEWS\n";
                std::cout << "    SHOW SCHEMA\n";
                std::cout << "    TRUNCATE TABLE tableName\n";
                std::cout << "    EXIT or QUIT\n";
                
                std::cout << "\nFor more details on a specific command, type 'HELP command'\n";
                continue;
            }
            
            // Help for specific commands
            if (toUpperCase(line).find("HELP ") == 0) {
                std::string helpTopic = line.substr(5);
                std::string upperTopic = toUpperCase(helpTopic);
                
                if (upperTopic == "CREATE TABLE") {
                    std::cout << "\nCREATE TABLE Command:\n";
                    std::cout << "  Syntax: CREATE TABLE tableName (\n";
                    std::cout << "            column1 dataType1 [NOT NULL] [PRIMARY KEY],\n";
                    std::cout << "            column2 dataType2 [NOT NULL],\n";
                    std::cout << "            ...,\n";
                    std::cout << "            [CONSTRAINT constraintName PRIMARY KEY (column1, ...)],\n";
                    std::cout << "            [CONSTRAINT constraintName FOREIGN KEY (column1, ...) \n";
                    std::cout << "               REFERENCES otherTable (otherColumn1, ...)],\n";
                    std::cout << "            [CONSTRAINT constraintName UNIQUE (column1, ...)],\n";
                    std::cout << "            [CONSTRAINT constraintName CHECK (condition)]\n";
                    std::cout << "          )\n\n";
                    std::cout << "  Supported data types: CHAR(n), VARCHAR(n), TEXT, INT, SMALLINT,\n";
                    std::cout << "                       NUMERIC(p,d), REAL, DOUBLE PRECISION,\n";
                    std::cout << "                       FLOAT(n), DATE, TIME, TIMESTAMP,\n";
                    std::cout << "                       BRANCH, CUSTOMER, LOAN, BORROWER,\n";
                    std::cout << "                       ACCOUNT, DEPOSITOR\n";
                } else if (upperTopic == "SELECT") {
                    std::cout << "\nSELECT Command:\n";
                    std::cout << "  Basic syntax: SELECT column1, column2, ... FROM tableName\n";
                    std::cout << "                [WHERE condition]\n";
                    std::cout << "                [GROUP BY column1, column2, ...]\n";
                    std::cout << "                [HAVING condition]\n";
                    std::cout << "                [ORDER BY column1 [ASC|DESC], ...]\n\n";
                    std::cout << "  Aggregate functions: AVG, MIN, MAX, SUM, COUNT, MEDIAN, MODE\n";
                    std::cout << "  Examples:\n";
                    std::cout << "    SELECT * FROM employees WHERE salary > 50000\n";
                    std::cout << "    SELECT dept, AVG(salary) FROM employees GROUP BY dept\n";
                    std::cout << "    SELECT * FROM employees ORDER BY salary DESC\n";
                } else if (upperTopic == "JOIN") {
                    std::cout << "\nJOIN Types:\n";
                    std::cout << "  INNER JOIN: Returns rows when there is a match in both tables\n";
                    std::cout << "    Example: SELECT * FROM orders INNER JOIN customers ON orders.customer_id = customers.id\n\n";
                    std::cout << "  LEFT OUTER JOIN: Returns all rows from left table, and matched rows from right table\n";
                    std::cout << "    Example: SELECT * FROM customers LEFT OUTER JOIN orders ON customers.id = orders.customer_id\n\n";
                    std::cout << "  RIGHT OUTER JOIN: Returns all rows from right table, and matched rows from left table\n";
                    std::cout << "    Example: SELECT * FROM orders RIGHT OUTER JOIN customers ON orders.customer_id = customers.id\n\n";
                    std::cout << "  FULL OUTER JOIN: Returns rows when there is a match in one of the tables\n";
                    std::cout << "    Example: SELECT * FROM customers FULL OUTER JOIN orders ON customers.id = orders.customer_id\n\n";
                    std::cout << "  NATURAL JOIN: Joins tables by matching columns with same name\n";
                    std::cout << "    Example: SELECT * FROM customers NATURAL JOIN orders\n";
                } else {
                    std::cout << "Help topic not found: " << helpTopic << std::endl;
                }
                continue;
            }
            
            // Add the line to command buffer
            commandBuffer += " " + line;
            
            // Process when a semicolon is detected or a standalone command is entered
            bool processCommand = false;
            
            if (commandBuffer.find(';') != std::string::npos) {
                processCommand = true;
            } else {
                // Check for standalone commands (those that don't need semicolons)
                std::string upperCmd = toUpperCase(trim(commandBuffer));
                if (upperCmd == "EXIT" || upperCmd == "QUIT" || 
                    upperCmd == "BEGIN" || upperCmd == "BEGIN TRANSACTION" ||
                    upperCmd == "COMMIT" || upperCmd == "ROLLBACK" ||
                    upperCmd.find("HELP") == 0 || upperCmd.find("LOGIN") == 0 || 
                    upperCmd == "LOGOUT" || upperCmd == "SHOW TABLES" || 
                    upperCmd == "SHOW VIEWS" || upperCmd == "SHOW SCHEMA") {
                    processCommand = true;
                }
            }
            
            if (!processCommand) {
                continue;
            }
            
            // Use the split function from Utils.h to split the command buffer by semicolon
            std::vector<std::string> commands = split(commandBuffer, ';');
            for (const auto &cmd : commands) {
                std::string trimmedCmd = trim(cmd);
                if (trimmedCmd.empty())
                    continue;
                
                // Check for special commands first
                std::string upperCmd = toUpperCase(trimmedCmd);
                
                if (upperCmd == "EXIT" || upperCmd == "QUIT") {
                    // Rollback any active transaction
                    if (currentTransaction) {
                        try {
                            db.rollbackTransaction(currentTransaction);
                            currentTransaction = nullptr;
                        } catch (const std::exception& e) {
                            std::cerr << "Error during transaction rollback: " << e.what() << std::endl;
                        }
                    }
                    return 0;
                } else if (upperCmd.find("LOGIN") == 0) {
                    // Login command: LOGIN username PASSWORD 'password'
                    std::istringstream iss(trimmedCmd);
                    std::string token, username, password;
                    iss >> token >> username >> token; // Skip "LOGIN" and get username, then skip "PASSWORD"
                    
                    // Extract password (might be quoted)
                    std::string rest;
                    std::getline(iss, rest);
                    rest = trim(rest);
                    
                    if (rest.size() >= 2 && rest.front() == '\'' && rest.back() == '\'') {
                        password = rest.substr(1, rest.size() - 2);
                    } else {
                        password = rest;
                    }
                    
                    try {
                        authenticated = db.authenticate(username, password);
                        if (authenticated) {
                            currentUser = username;
                            std::cout << "Login successful. Current user: " << currentUser << std::endl;
                        } else {
                            std::cout << "Login failed: Invalid username or password" << std::endl;
                        }
                    } catch (const std::exception& e) {
                        std::cerr << "Login error: " << e.what() << std::endl;
                    }
                    continue;
                } else if (upperCmd == "LOGOUT") {
                    currentUser = "guest";
                    authenticated = false;
                    std::cout << "Logged out. Please login to access the database." << std::endl;
                    continue;
                } else if (upperCmd.find("CREATE USER") == 0) {
                    // CREATE USER command: CREATE USER username PASSWORD 'password'
                    std::istringstream iss(trimmedCmd);
                    std::string token, username, password;
                    iss >> token >> token >> username >> token; // Skip "CREATE USER" and get username, then skip "PASSWORD"
                    
                    // Extract password (might be quoted)
                    std::string rest;
                    std::getline(iss, rest);
                    rest = trim(rest);
                    
                    if (rest.size() >= 2 && rest.front() == '\'' && rest.back() == '\'') {
                        password = rest.substr(1, rest.size() - 2);
                    } else {
                        password = rest;
                    }
                    
                    // Only admin can create users
                    if (currentUser != "admin") {
                        std::cout << "Error: Only admin can create users" << std::endl;
                        continue;
                    }
                    
                    try {
                        db.createUser(username, password);
                    } catch (const std::exception& e) {
                        std::cerr << "Error creating user: " << e.what() << std::endl;
                    }
                    continue;
                }
                
               // Process transaction control commands
                if (upperCmd == "BEGIN" || upperCmd == "BEGIN TRANSACTION") {
                    if (!authenticated) {
                        std::cout << "Error: You must be logged in to begin a transaction" << std::endl;
                        continue;
                    }
                    
                    try {
                        db.beginTransaction();
                    } catch (const std::exception& e) {
                        std::cerr << "Error beginning transaction: " << e.what() << std::endl;
                    }
                    continue;
                } else if (upperCmd == "COMMIT") {
                    try {
                        db.commitTransaction();
                    } catch (const std::exception& e) {
                        std::cerr << "Error committing transaction: " << e.what() << std::endl;
                    }
                    continue;
                } else if (upperCmd == "ROLLBACK") {
                    try {
                        db.rollbackTransaction();
                    } catch (const std::exception& e) {
                        std::cerr << "Error rolling back transaction: " << e.what() << std::endl;
                    }
                    continue;
                }
                // Process the complete command
                try {
                    Query query = parser.parseQuery(trimmedCmd);
                    std::string qType = toUpperCase(query.type);
                    
                    // Check if user has necessary privileges (except for admin)
                    if (currentUser != "admin") {
                        // Determine required privilege based on command type
                        std::string requiredPrivilege;
                        
                        if (qType == "SELECT") {
                            requiredPrivilege = "SELECT";
                        } else if (qType == "INSERT") {
                            requiredPrivilege = "INSERT";
                        } else if (qType == "UPDATE") {
                            requiredPrivilege = "UPDATE";
                        } else if (qType == "DELETE") {
                            requiredPrivilege = "DELETE";
                        } else if (qType == "CREATE" || qType == "ALTER" || qType == "DROP") {
                            // For DDL operations, need ALL privileges
                            requiredPrivilege = "ALL";
                        }
                        
                        // Check privilege if needed
                        if (!requiredPrivilege.empty() && !query.tableName.empty()) {
                            if (!db.checkPrivilege(currentUser, query.tableName, requiredPrivilege)) {
                                std::cout << "Error: User '" << currentUser << "' does not have " 
                                          << requiredPrivilege << " privilege on " << query.tableName << std::endl;
                                continue;
                            }
                        }
                    }
                    
                    // Execute the command
                    if (qType == "CREATE") {
                        db.createTable(query.tableName, query.columns, query.constraints);
                    } else if (qType == "INSERT") {
                        db.insertRecord(query.tableName, query.values);
                    } else if (qType == "SELECT") {
                        if (query.isJoin) {
                            db.joinTables(query.tableName, query.joinTable, query.joinType, 
                                         query.joinCondition, query.selectColumns);
                        } else if (!query.setOperation.empty()) {
                            db.setOperation(query.setOperation, query.tableName, query.rightQuery);
                        } else {
                            db.selectRecords(query.tableName, query.selectColumns, query.condition,
                                         query.orderByColumns, query.groupByColumns, query.havingCondition);
                        }
                    } else if (qType == "DELETE") {
                        db.deleteRecords(query.tableName, query.condition);
                    } else if (qType == "UPDATE") {
                        db.updateRecords(query.tableName, query.updates, query.condition);
                    } else if (qType == "DROP") {
                        db.dropTable(query.tableName);
                    } else if (qType == "DROPTABLE") {
                        db.dropTable(query.tableName);
                    } else if (qType == "DROPVIEW") {
                        db.dropView(query.tableName);
                    } else if (qType == "DROPINDEX") {
                        db.dropIndex(query.indexName);
                    } else if (qType == "ALTER") {
                        if (query.alterAction == "ADD") {
                            db.alterTableAddColumn(query.tableName, query.alterColumn);
                        } else if (query.alterAction == "DROP") {
                            db.alterTableDropColumn(query.tableName, query.alterColumn.first);
                        } else if (query.alterAction == "RENAME") {
                            db.renameTable(query.tableName, query.newTableName);
                        } else if (query.alterAction == "ADD CONSTRAINT") {
                            if (!query.constraints.empty()) {
                                db.alterTableAddConstraint(query.tableName, query.constraints.front());
                            }
                        } else if (query.alterAction == "DROP CONSTRAINT") {
                            if (!query.constraints.empty()) {
                                db.alterTableDropConstraint(query.tableName, query.constraints.front().name);
                            }
                        }
                    } else if (qType == "DESCRIBE") {
                        db.describeTable(query.tableName);
                    }else if (qType == "SHOW") {
                        if (query.tableName == "TABLES") {
                            db.showTables();
                        } else if (query.tableName == "VIEWS") {
                            db.showViews();
                        } else if (query.tableName == "SCHEMA") {
                            db.showSchema();
                        } else if (query.tableName == "INDEXES") {
                            db.showIndexes();
                        } else if (query.tableName.find("GRANTS FOR") == 0) {
                            // Extract username from "GRANTS FOR username"
                            std::string username = query.tableName.substr(10); // "GRANTS FOR " is 10 chars
                            username = trim(username);
                            db.showUserPrivileges(username);
                        } else {
                            db.showTables();
                        }
                    } else if (qType == "TRUNCATE") {
                        db.truncateTable(query.tableName);
                    } else if (qType == "CREATEINDEX") {
                        db.createIndex(query.indexName, query.tableName, query.columnName);
                    } else if (qType == "CREATEVIEW") {
                        db.createView(query.viewName, query.viewDefinition);
                    } else if (qType == "CREATETYPE") {
                        db.createType(query.typeName, query.columns);
                    } else if (qType == "CREATEASSERTION") {
                        db.createAssertion(query.assertionName, query.assertionCondition);
                    } else if (qType == "GRANT") {
                        if (query.multiplePrivileges) {
                            for (const auto& privilege : query.privileges) {
                                db.grantPrivilege(query.username, query.tableName, trim(privilege));
                            }
                        } else {
                            db.grantPrivilege(query.username, query.tableName, query.privilege);
                        }
                    } else if (qType == "REVOKE") {
                        db.revokePrivilege(query.username, query.tableName, query.privilege);
                    } else if (qType == "MERGE") {
                        db.mergeRecords(query.tableName, query.mergeCommand);
                    } else if (qType == "REPLACE") {
                        db.replaceInto(query.tableName, query.values);
                    } else {
                        std::cout << "Unsupported command: " << qType << std::endl;
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Error: " << e.what() << std::endl;
                }
            }
            
            commandBuffer.clear();
        }
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}