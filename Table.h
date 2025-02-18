#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>

class Table {
public:
    // DDL: Create schema
    void addColumn(const std::string& columnName, const std::string& type);
    // Modified dropColumn returns bool.
    bool dropColumn(const std::string& columnName);

    // DML: Row operations
    void addRow(const std::vector<std::string>& values);
    void selectRows(const std::vector<std::string>& selectColumns,
                    const std::string& condition,
                    const std::vector<std::string>& orderByColumns = {},
                    const std::vector<std::string>& groupByColumns = {},
                    const std::string& havingCondition = "");
    void printTable();

    void deleteRows(const std::string& condition);
    void updateRows(const std::vector<std::pair<std::string, std::string>>& updates,
                    const std::string& condition);

    // Accessors for columns and rows (used in JOINs)
    const std::vector<std::string>& getColumns() const { return columns; }
    const std::vector<std::vector<std::string>>& getRows() const { return rows; }
private:
    std::vector<std::string> columns;
    std::vector<std::string> columnTypes;
    std::vector<std::vector<std::string>> rows;
};

#endif // TABLE_H
