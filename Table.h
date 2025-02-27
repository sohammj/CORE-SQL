#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>

class Table {
public:
    // DDL: Create schema
    void addColumn(const std::string& columnName, const std::string& type, bool isNotNull = false);
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
    void clearRows(); // New: remove all rows

    const std::vector<std::string>& getColumns() const { return columns; }
    const std::vector<std::vector<std::string>>& getRows() const { return rows; }
    std::vector<std::vector<std::string>>& getRowsNonConst() { return rows; }
private:
    std::vector<std::string> columns;
    std::vector<std::string> columnTypes;
    std::vector<bool> notNullConstraints;
    std::vector<std::vector<std::string>> rows;
};

#endif // TABLE_H