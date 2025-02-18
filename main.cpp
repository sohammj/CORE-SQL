#include <iostream>
#include <sstream>
#include <string>
#include "Database.h"
#include "Parser.h"
#include "Utils.h"

int main() {
    Database db;
    Parser parser;
    std::string commandBuffer;
    std::string line;

    std::cout << "MY SQL Command Line Interface (Extended Version)" << std::endl;
    std::cout << "Type your SQL commands. End each command with a semicolon (;) and press Enter." << std::endl;

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

        // Wait until at least one semicolon is found.
        if (commandBuffer.find(';') == std::string::npos) {
            continue;
        }

        // Remove all semicolons.
        while (commandBuffer.find(';') != std::string::npos) {
            size_t pos = commandBuffer.find(';');
            commandBuffer.erase(pos, 1);
        }
        commandBuffer = trim(commandBuffer);

        // Check if the command is an exit command.
        std::string upperCommand = toUpperCase(commandBuffer);
        if (upperCommand == "EXIT" || upperCommand == "QUIT")
            break;

        // Process the complete command.
        Query query = parser.parseQuery(commandBuffer);
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
            if (query.alterAction == "ADD")
                db.alterTableAddColumn(query.tableName, query.alterColumn);
            else if (query.alterAction == "DROP")
                db.alterTableDropColumn(query.tableName, query.alterColumn.first);
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
        } else {
            std::cout << "Invalid command." << std::endl;
        }

        commandBuffer.clear();
    }
    return 0;
}
