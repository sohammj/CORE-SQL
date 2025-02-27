#ifndef INDEX_H
#define INDEX_H

#include <string>
#include <unordered_map>
#include <vector>

class Index {
public:
    Index(const std::string& columnName);
    // Build the index given table rows and the column index.
    void build(const std::vector<std::vector<std::string>>& rows, int colIndex);
    // Retrieve row indices for a given column value.
    std::vector<int> lookup(const std::string& value) const;
private:
    std::string column;
    std::unordered_map<std::string, std::vector<int>> indexMap;
};

#endif // INDEX_H