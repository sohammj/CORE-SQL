#include "Table.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include "Utils.h"

// Helper function to extract column name and value from condition
std::pair<std::string, std::string> parseCondition(const std::string& condition) {
    std::istringstream iss(condition);
    std::string column, value;
    std::getline(iss, column, '=');
    std::getline(iss, value);
    column = trim(column);
    value = trim(value);
    return {column, value};
}

void Table::addColumn(const std::string& columnName, const std::string& type) {
    columns.push_back(columnName);
    columnTypes.push_back(type);
}

void Table::addRow(const std::vector<std::string>& values) {
    rows.push_back(values);
}

void Table::selectRows(const std::vector<std::string>& columns, const std::string& condition) {
    for (const auto& column : columns) {
        std::cout << column << "\t";
    }
    std::cout << std::endl;

    for (const auto& row : rows) {
        for (const auto& column : columns) {
            auto colIt = std::find(this->columns.begin(), this->columns.end(), column);
            if (colIt != this->columns.end()) {
                int colIndex = std::distance(this->columns.begin(), colIt);
                std::cout << row[colIndex] << "\t";
            }
        }
        std::cout << std::endl;
    }
}

void Table::printTable() {
    for (const auto& column : columns) {
        std::cout << column << "\t";
    }
    std::cout << std::endl;

    for (const auto& row : rows) {
        for (const auto& value : row) {
            std::cout << value << "\t";
        }
        std::cout << std::endl;
    }
}

void Table::deleteRows(const std::string& condition) {
    std::pair<std::string, std::string> cond = parseCondition(condition);
    std::string column = cond.first;
    std::string value = cond.second;

    auto colIt = std::find(columns.begin(), columns.end(), column);
    if (colIt == columns.end()) {
        std::cout << "Column " << column << " does not exist." << std::endl;
        return;
    }
    int colIndex = std::distance(columns.begin(), colIt);

    rows.erase(std::remove_if(rows.begin(), rows.end(), [&](const std::vector<std::string>& row) {
        return row[colIndex] == value;
    }), rows.end());
}

void Table::updateRows(const std::vector<std::pair<std::string, std::string>>& updates, const std::string& condition) {
    std::pair<std::string, std::string> cond = parseCondition(condition);
    std::string column = cond.first;
    std::string value = cond.second;

    auto colIt = std::find(columns.begin(), columns.end(), column);
    if (colIt == columns.end()) {
        std::cout << "Column " << column << " does not exist." << std::endl;
        return;
    }
    int colIndex = std::distance(columns.begin(), colIt);

    for (auto& row : rows) {
        if (row[colIndex] == value) {
            for (const auto& update : updates) {
                auto updateColIt = std::find(columns.begin(), columns.end(), update.first);
                if (updateColIt != columns.end()) {
                    int updateColIndex = std::distance(columns.begin(), updateColIt);
                    row[updateColIndex] = update.second;
                }
            }
        }
    }
}