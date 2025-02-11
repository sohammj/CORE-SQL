#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>

class Table {
public:
    void addColumn(const std::string& columnName, const std::string& type);
    void addRow(const std::vector<std::string>& values);
    void printTable();
    void deleteRows(const std::string& condition);

private:
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

#endif // TABLE_H