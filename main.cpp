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
        if (toUpperCase(command) == "EXIT")
            break;

        Query query = parser.parseQuery(command);
        if (query.type == "BEGIN") {
            db.beginTransaction();
        } else if (query.type == "CREATE") {
            db.createTable(query.tableName, query.columns);
        } else if (query.type == "INSERT") {
            db.insertRecord(query.tableName, query.values);
        } else if (query.type == "SELECT") {
            db.selectRecords(query);
        } else if (query.type == "DELETE") {
            db.deleteRecords(query.tableName, query.condition);
        } else if (query.type == "UPDATE") {
            db.updateRecords(query.tableName, query.updateAssignments, query.condition);
        } else if (query.type == "MERGE") {
            db.mergeRecords(query);
        } else if (query.type == "UPSERT") {
            db.upsertRecords(query);
        } else if (query.type == "ALTER") {
            db.alterTable(query.tableName, query.columns);
        } else if (query.type == "DROP") {
            db.dropTable(query.tableName);
        } else if (query.type == "TRUNCATE") {
            db.truncateTable(query.tableName);
        } else if (query.type == "RENAME") {
            db.renameTable(query.tableName, query.newTableName);
        } else if (query.type == "COMMIT") {
            db.commitTransaction();
        } else if (query.type == "ROLLBACK") {
            db.rollbackTransaction();
        } else if (query.type == "SAVEPOINT") {
            db.savepointTransaction("default"); // or parse a name if desired
        } else if (query.type == "SET_TRANSACTION") {
            db.setTransaction(query.condition);
        } else if (query.type == "GRANT") {
            db.grantPrivilege("dummy_priv", "dummy_table", "dummy_user"); // Extend parsing as needed.
        } else if (query.type == "REVOKE") {
            db.revokePrivilege("dummy_priv", "dummy_table", "dummy_user");
        } else if (query.type == "DENY") {
            db.denyPrivilege("dummy_priv", "dummy_table", "dummy_user");
        } else if (query.type == "SHOW_TABLES") {
            db.showTables();
        } else {
            std::cout << "Invalid command." << std::endl;
        }
    }
    return 0;
}
