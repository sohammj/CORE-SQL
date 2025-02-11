#include <iostream>
#include <string>
#include "Database.h"
#include "Parser.h"
#include "Utils.h"

int main() {
    Database db;
    Parser parser;
    std::string command;

    std::cout << "SimpleDB Command Line Interface" << std::endl;
    while (true) {
        std::cout << "> ";
        std::getline(std::cin, command);
        command = trim(command);

        if (toUpperCase(command) == "EXIT") break;

        auto query = parser.parseQuery(command);
        if (query.type == "CREATE") {
            db.createTable(query.tableName, query.columns);
        } else if (query.type == "INSERT") {
            db.insertRecord(query.tableName, query.values);
        } else if (query.type == "SELECT") {
            db.selectRecords(query.tableName, query.condition);
        } else if (query.type == "DELETE") {
            db.deleteRecords(query.tableName, query.condition);
        } else if (toUpperCase(command) == "SHOW TABLES") {
            db.showTables();
        } else if (query.type == "SHOW") {
            db.selectRecords(query.tableName, query.condition);
        } else {
            std::cout << "Invalid command." << std::endl;
        }
    }

    return 0;
}