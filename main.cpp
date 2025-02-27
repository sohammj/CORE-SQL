#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include "Database.h"
#include "Parser.h"
#include "Utils.h"

int main() {
    Database db;
    Parser parser;
    std::string commandBuffer;
    std::string line;

    std::cout << "MY SQL Command Line Interface" << std::endl;

    while (true) {
        // Primary prompt if starting a new command; continuation prompt otherwise.
        if (commandBuffer.empty()) {
            std::cout << "sql> ";
        } else {
            std::cout << "   -> ";
        }

        if (!std::getline(std::cin, line))
            break;  // Exit on EOF

        line = trim(line);
        commandBuffer += " " + line;

        // Process when a semicolon is detected.
        if (commandBuffer.find(';') == std::string::npos) {
            continue;
        }

        // Use the split function from Utils.h to split the command buffer by semicolon.
        std::vector<std::string> commands = split(commandBuffer, ';');
        for (const auto &cmd : commands) {
            std::string trimmedCmd = trim(cmd);
            if (trimmedCmd.empty())
                continue;
            
            // Check for exit commands.
            std::string upperCmd = toUpperCase(trimmedCmd);
            if (upperCmd == "EXIT" || upperCmd == "QUIT")
                return 0;

            // Process the complete command.
            Query query = parser.parseQuery(trimmedCmd);
            std::string qType = toUpperCase(query.type);

            if (qType == "CREATE") {
                db.createTable(query.tableName, query.columns);
            } else if (qType == "INSERT") {
                db.insertRecord(query.tableName, query.values);
            } else if (qType == "SELECT") {
                db.selectRecords(query.tableName, query.selectColumns, query.condition,
                                 query.orderByColumns, query.groupByColumns, query.havingCondition,
                                 query.isJoin, query.joinTable, query.joinCondition);
            } else if (qType == "DELETE") {
                db.deleteRecords(query.tableName, query.condition);
            } else if (qType == "UPDATE") {
                db.updateRecords(query.tableName, query.updates, query.condition);
            } else if (qType == "DROP") {
                db.dropTable(query.tableName);
            } else if (qType == "ALTER") {
                // Handle ALTER actions: ADD, DROP, and RENAME
                if (query.alterAction == "ADD")
                    db.alterTableAddColumn(query.tableName, query.alterColumn);
                else if (query.alterAction == "DROP")
                    db.alterTableDropColumn(query.tableName, query.alterColumn.first);
                else if (query.alterAction == "RENAME")
                    db.renameTable(query.tableName, query.newTableName);
            } else if (qType == "DESCRIBE") {
                db.describeTable(query.tableName);
            } else if (qType == "SHOW") {
                db.showTables();
            } else if (qType == "BEGIN") {
                db.beginTransaction();
            } else if (qType == "COMMIT") {
                db.commitTransaction();
            } else if (qType == "ROLLBACK") {
                db.rollbackTransaction();
            } else if (qType == "TRUNCATE") {
                db.truncateTable(query.tableName);
            } else if (qType == "CREATEINDEX") {
                db.createIndex(query.indexName, query.tableName, query.columnName);
            } else if (qType == "DROPINDEX") {
                db.dropIndex(query.indexName);
            } else if (qType == "MERGE") {
                db.mergeRecords(query.tableName, query.mergeCommand);
            } else if (qType == "REPLACE") {
                db.replaceInto(query.tableName, query.values);
            } else {
                std::cout << "Invalid command." << std::endl;
            }
        }
        commandBuffer.clear();
    }
    return 0;
}
