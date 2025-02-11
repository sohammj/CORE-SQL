#include "Table.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include "Utils.h"

void Table::addColumn(const std::string& columnName, const std::string& type) {
    columns.push_back(columnName);
}

void Table::addRow(const std::vector<std::string>& values) {
    rows.push_back(values);
}

void Table::printTable() {
    for (const auto &col : columns) {
        std::cout << col << "\t";
    }
    std::cout << std::endl;
    for (const auto &row : rows) {
        for (const auto &val : row) {
            std::cout << val << "\t";
        }
        std::cout << std::endl;
    }
}

void Table::deleteRows(const std::string& condition) {
    // Legacy function (not used)
}

int Table::getColumnIndex(const std::string& columnName) const {
    for (size_t i = 0; i < columns.size(); i++) {
        if (toLowerCase(columns[i]) == toLowerCase(columnName))
            return i;
    }
    return -1;
}

std::vector<std::string> Table::getColumns() const {
    return columns;
}

std::vector<std::vector<std::string>> Table::getRows() const {
    return rows;
}

void Table::printCustomTable(const std::vector<std::vector<std::string>>& customRows) const {
    for (const auto &col : columns) {
        std::cout << col << "\t";
    }
    std::cout << std::endl;
    for (const auto &row : customRows) {
        for (const auto &val : row) {
            std::cout << val << "\t";
        }
        std::cout << std::endl;
    }
}

void Table::printCustomTable(const std::vector<std::vector<std::string>>& customRows, const std::vector<int>& indices) const {
    for (auto idx : indices) {
        if (idx >= 0 && idx < static_cast<int>(columns.size()))
            std::cout << columns[idx] << "\t";
    }
    std::cout << std::endl;
    for (const auto &row : customRows) {
        for (auto idx : indices) {
            if (idx >= 0 && idx < static_cast<int>(row.size()))
                std::cout << row[idx] << "\t";
        }
        std::cout << std::endl;
    }
}

void Table::setRows(const std::vector<std::vector<std::string>>& newRows) {
    rows = newRows;
}
