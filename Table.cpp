#include "Table.h"
#include "Utils.h"
#include "ConditionParser.h"
#include "Aggregation.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <unordered_map>
#include <stdexcept>

void Table::addColumn(const std::string& columnName, const std::string& type, bool isNotNull) {
    columns.push_back(columnName);
    columnTypes.push_back(type);
    notNullConstraints.push_back(isNotNull);
    for (auto& row : rows) {
        row.push_back(isNotNull ? "" : "");
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
    notNullConstraints.erase(notNullConstraints.begin() + index);
    for (auto& row : rows) {
        if (index < row.size())
            row.erase(row.begin() + index);
    }
    return true;
}

void Table::addRow(const std::vector<std::string>& values) {
    if (values.size() != columns.size()) {
        std::cerr << "Error: Incorrect number of values for row." << std::endl;
        return;
    }
    for (size_t i = 0; i < values.size(); ++i) {
        if (notNullConstraints[i] && values[i].empty()) {
            std::cerr << "Error: NOT NULL constraint violated for column " << columns[i] << "." << std::endl;
            return;
        }
    }
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

void Table::clearRows() {
    rows.clear();
}

static bool parseAggregate(const std::string& colExpr, std::string& func, std::string& colName) {
    size_t pos1 = colExpr.find('(');
    size_t pos2 = colExpr.find(')');
    if (pos1 == std::string::npos || pos2 == std::string::npos) return false;
    func = toUpperCase(trim(colExpr.substr(0, pos1)));
    colName = trim(colExpr.substr(pos1 + 1, pos2 - pos1 - 1));
    return true;
}

void Table::selectRows(const std::vector<std::string>& selectColumns,
                       const std::string& condition,
                       const std::vector<std::string>& orderByColumns,
                       const std::vector<std::string>& groupByColumns,
                       const std::string& havingCondition) {
    std::vector<std::string> displayColumns;
    if (selectColumns.size() == 1 && selectColumns[0] == "*")
        displayColumns = columns;
    else
        displayColumns = selectColumns;

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

    // Check for aggregate functions (AVG, MIN, MAX, SUM, COUNT)
    bool hasAggregate = false;
    std::vector<std::string> aggregateResults;
    for (const auto& colExpr : displayColumns) {
        std::string func, colName;
        if (parseAggregate(colExpr, func, colName)) {
            hasAggregate = true;
            auto it = std::find(columns.begin(), columns.end(), colName);
            if (it == columns.end()) {
                aggregateResults.push_back("");
                continue;
            }
            int idx = std::distance(columns.begin(), it);
            std::vector<std::string> colValues;
            for (const auto& row : filteredRows) {
                if (idx < row.size())
                    colValues.push_back(row[idx]);
            }
            if (func == "COUNT" && colExpr.find("*") != std::string::npos) {
                aggregateResults.push_back(std::to_string(filteredRows.size()));
            } else if (func == "AVG") {
                double avg = Aggregation::computeMean(colValues);
                aggregateResults.push_back(std::to_string(avg));
            } else if (func == "MIN") {
                double min = Aggregation::computeMin(colValues);
                aggregateResults.push_back(std::to_string(min));
            } else if (func == "MAX") {
                double max = Aggregation::computeMax(colValues);
                aggregateResults.push_back(std::to_string(max));
            } else if (func == "SUM") {
                double sum = Aggregation::computeSum(colValues);
                aggregateResults.push_back(std::to_string(sum));
            } else {
                aggregateResults.push_back(filteredRows.empty() ? "" : filteredRows[0][idx]);
            }
        } else {
            aggregateResults.push_back("");
        }
    }
    if (hasAggregate) {
        for (const auto& res : aggregateResults)
            std::cout << res << "\t";
        std::cout << std::endl;
        return;
    }

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
        for (const auto& col : displayColumns)
            std::cout << col << "\t";
        std::cout << std::endl;
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

    if (!orderByColumns.empty()) {
        std::sort(filteredRows.begin(), filteredRows.end(), [&](const auto& a, const auto& b) {
            for (const auto& token : orderByColumns) {
                std::string colName = token;
                bool desc = false;
                size_t pos = toUpperCase(token).find(" DESC");
                if (pos != std::string::npos) {
                    desc = true;
                    colName = trim(token.substr(0, pos));
                } else {
                    pos = toUpperCase(token).find(" ASC");
                    if (pos != std::string::npos) {
                        colName = trim(token.substr(0, pos));
                    }
                }
                auto it = std::find(columns.begin(), columns.end(), colName);
                if (it != columns.end()) {
                    int idx = std::distance(columns.begin(), it);
                    if (a[idx] == b[idx])
                        continue;
                    return desc ? (a[idx] > b[idx]) : (a[idx] < b[idx]);
                }
            }
            return false;
        });
    }

    for (const auto& col : displayColumns)
        std::cout << col << "\t";
    std::cout << std::endl;
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