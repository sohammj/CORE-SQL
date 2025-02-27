#include "Index.h"

Index::Index(const std::string& columnName) : column(columnName) {}

void Index::build(const std::vector<std::vector<std::string>>& rows, int colIndex) {
    indexMap.clear();
    for (int i = 0; i < rows.size(); i++) {
        if (colIndex < rows[i].size()) {
            indexMap[rows[i][colIndex]].push_back(i);
        }
    }
}

std::vector<int> Index::lookup(const std::string& value) const {
    auto it = indexMap.find(value);
    if (it != indexMap.end())
        return it->second;
    return {};
}