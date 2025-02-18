#include "Table.h"
#include "Utils.h"
#include "ConditionParser.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <unordered_map>
#include <stdexcept>

void Table::addColumn(const std::string& columnName, const std::string& type) {
    columns.push_back(columnName);
    columnTypes.push_back(type);
    // Append an empty value to each existing row.
    for (auto& row : rows) {
        row.push_back("");
    }
}

bool Table::dropColumn(const std::string& columnName) {
    auto it = std::find(columns.begin(), columns.end(), columnName);
    if (it == columns.end()) {
        std::cout << "Column " << columnName << " does not exist." << std::endl;
        return false;
    }
    int index = std::distance(columns.begin(), it);
    columns.erase(it);
    columnTypes.erase(columnTypes.begin() + index);
    for (auto& row : rows) {
        if (index < row.size())
            row.erase(row.begin() + index);
    }
    return true;
}

void Table::addRow(const std::vector<std::string>& values) {
    // Pad with empty strings if values.size() is less than number of columns.
    std::vector<std::string> newRow = values;
    while (newRow.size() < columns.size())
        newRow.push_back("");
    rows.push_back(newRow);
}

void Table::printTable() {
    for (const auto& col : columns) {
        std::cout << col << "\t";
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
    if (condition.empty()) {
        rows.clear();
        return;
    }
    ConditionParser cp(condition);
    auto expr = cp.parse();
    rows.erase(std::remove_if(rows.begin(), rows.end(), [&](const std::vector<std::string>& row) {
        return expr->evaluate(row, columns);
    }), rows.end());
}

void Table::updateRows(const std::vector<std::pair<std::string, std::string>>& updates,
                       const std::string& condition) {
    ConditionExprPtr expr = nullptr;
    if (!condition.empty()) {
        ConditionParser cp(condition);
        expr = cp.parse();
    }
    for (auto& row : rows) {
        if (!expr || expr->evaluate(row, columns)) {
            for (const auto& update : updates) {
                auto it = std::find(columns.begin(), columns.end(), update.first);
                if (it != columns.end()) {
                    int index = std::distance(columns.begin(), it);
                    row[index] = update.second;
                }
            }
        }
    }
}

void Table::selectRows(const std::vector<std::string>& selectColumns,
                       const std::string& condition,
                       const std::vector<std::string>& orderByColumns,
                       const std::vector<std::string>& groupByColumns,
                       const std::string& havingCondition) {
    // Determine which columns to display.
    std::vector<std::string> displayColumns;
    if (selectColumns.size() == 1 && selectColumns[0] == "*")
        displayColumns = columns;
    else
        displayColumns = selectColumns;

    // Step 1: Filter rows based on WHERE clause.
    std::vector<std::vector<std::string>> filteredRows;
    if (!condition.empty()) {
        ConditionParser cp(condition);
        auto expr = cp.parse();
        for (const auto& row : rows) {
            if (expr->evaluate(row, columns))
                filteredRows.push_back(row);
        }
    } else {
        filteredRows = rows;
    }

    // Special aggregate: COUNT(*) if that is the only column selected.
    if (displayColumns.size() == 1 && toUpperCase(trim(displayColumns[0])) == "COUNT(*)") {
        std::cout << "Count: " << filteredRows.size() << std::endl;
        return;
    }

    // Step 2: Grouping (if GROUP BY is specified)
    if (!groupByColumns.empty()) {
        std::unordered_map<std::string, std::vector<std::vector<std::string>>> groups;
        for (const auto& row : filteredRows) {
            std::string key;
            for (const auto& grpCol : groupByColumns) {
                auto it = std::find(columns.begin(), columns.end(), grpCol);
                if (it != columns.end()) {
                    int idx = std::distance(columns.begin(), it);
                    key += row[idx] + "|";
                }
            }
            groups[key].push_back(row);
        }
        // Print header.
        for (const auto& col : displayColumns)
            std::cout << col << "\t";
        std::cout << std::endl;
        // For each group, compute aggregate row.
        for (const auto& kv : groups) {
            const auto& groupRows = kv.second;
            std::vector<std::string> resultRow;
            for (const auto& col : displayColumns) {
                if (toUpperCase(col) == "COUNT(*)") {
                    resultRow.push_back(std::to_string(groupRows.size()));
                } else {
                    auto it = std::find(columns.begin(), columns.end(), col);
                    if (it != columns.end()) {
                        int idx = std::distance(columns.begin(), it);
                        resultRow.push_back(groupRows[0][idx]);
                    } else {
                        resultRow.push_back("");
                    }
                }
            }
            for (const auto& cell : resultRow)
                std::cout << cell << "\t";
            std::cout << std::endl;
        }
        return;
    }

    // Step 3: Ordering (if ORDER BY is specified)
    if (!orderByColumns.empty()) {
        std::sort(filteredRows.begin(), filteredRows.end(), [&](const auto& a, const auto& b) {
            for (const auto& colName : orderByColumns) {
                auto it = std::find(columns.begin(), columns.end(), colName);
                if (it != columns.end()) {
                    int idx = std::distance(columns.begin(), it);
                    if (a[idx] < b[idx]) return true;
                    else if (a[idx] > b[idx]) return false;
                }
            }
            return false;
        });
    }

    // Step 4: Print header.
    for (const auto& col : displayColumns)
        std::cout << col << "\t";
    std::cout << std::endl;
    // Print rows.
    for (const auto& row : filteredRows) {
        for (const auto& col : displayColumns) {
            auto it = std::find(columns.begin(), columns.end(), col);
            if (it != columns.end()) {
                int idx = std::distance(columns.begin(), it);
                std::cout << row[idx] << "\t";
            }
        }
        std::cout << std::endl;
    }
}
