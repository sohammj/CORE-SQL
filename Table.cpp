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
    // When adding a new column, append an empty value to each existing row.
    for (auto& row : rows) {
        row.push_back("");
    }
}

void Table::dropColumn(const std::string& columnName) {
    auto it = std::find(columns.begin(), columns.end(), columnName);
    if (it == columns.end()) {
        std::cout << "Column " << columnName << " does not exist." << std::endl;
        return;
    }
    int index = std::distance(columns.begin(), it);
    columns.erase(it);
    columnTypes.erase(columnTypes.begin() + index);
    for (auto& row : rows) {
        if (index < row.size())
            row.erase(row.begin() + index);
    }
}

void Table::addRow(const std::vector<std::string>& values) {
    // (A real implementation would validate against column types.)
    rows.push_back(values);
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

    // Special aggregate: COUNT(*)
    if (selectColumns.size() == 1 && toUpperCase(trim(selectColumns[0])) == "COUNT(*)") {
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
        // (HAVING clause filtering is not fully implemented; placeholder here.)
        for (const auto& kv : groups) {
            std::cout << "Group: " << kv.first << std::endl;
            auto groupRows = kv.second;
            // Order within each group if ORDER BY is specified.
            if (!orderByColumns.empty()) {
                std::sort(groupRows.begin(), groupRows.end(), [&](const auto& a, const auto& b) {
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
            // Print header and group rows.
            for (const auto& col : selectColumns)
                std::cout << col << "\t";
            std::cout << std::endl;
            for (const auto& row : groupRows) {
                for (const auto& col : selectColumns) {
                    auto it = std::find(columns.begin(), columns.end(), col);
                    if (it != columns.end()) {
                        int idx = std::distance(columns.begin(), it);
                        std::cout << row[idx] << "\t";
                    }
                }
                std::cout << std::endl;
            }
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

    // Step 4: Print the result.
    for (const auto& col : selectColumns)
        std::cout << col << "\t";
    std::cout << std::endl;
    for (const auto& row : filteredRows) {
        for (const auto& col : selectColumns) {
            auto it = std::find(columns.begin(), columns.end(), col);
            if (it != columns.end()) {
                int idx = std::distance(columns.begin(), it);
                std::cout << row[idx] << "\t";
            }
        }
        std::cout << std::endl;
    }
}
