#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>

class Table {
public:
    void addColumn(const std::string& columnName, const std::string& type);
    void addRow(const std::vector<std::string>& values);
    void printTable();
    void deleteRows(const std::string& condition); // legacy
    int getColumnIndex(const std::string& columnName) const;
    std::vector<std::string> getColumns() const;
    std::vector<std::vector<std::string>> getRows() const;
    void printCustomTable(const std::vector<std::vector<std::string>>& rows) const;
    void printCustomTable(const std::vector<std::vector<std::string>>& rows, const std::vector<int>& indices) const;
    void setRows(const std::vector<std::vector<std::string>>& newRows);
    
private:
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

#endif // TABLE_H
