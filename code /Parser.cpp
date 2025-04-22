#include "Parser.h"
#include "Utils.h"
#include "ConditionParser.h"
#include "Aggregation.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <unordered_map>
#include <stdexcept>
#include <regex>
Query Parser::parseQuery(const std::string& queryStr) {
    Query q;
    std::istringstream iss(queryStr);
    std::string word;
    iss >> word;
    std::string command = toUpperCase(word);
    // WITH clause should be processed first
    if (command == "WITH") {
        q.withClauses = parseWithClause(queryStr);
        
        // Find the main query part after WITH clause
        size_t mainQueryPos = queryStr.find(";", 0);
        while (mainQueryPos != std::string::npos) {
            if (mainQueryPos + 1 < queryStr.length() &&
                queryStr[mainQueryPos + 1] != ' ' &&
                queryStr[mainQueryPos + 1] != '\n') {
                mainQueryPos = queryStr.find(";", mainQueryPos + 1);
            } else {
                break;
            }
        }
        
        if (mainQueryPos == std::string::npos) {
            mainQueryPos = queryStr.find("SELECT", 4);
            if (mainQueryPos == std::string::npos) {
                throw DatabaseException("Invalid WITH clause syntax");
            }
        } else {
            mainQueryPos++; // Skip the semicolon
        }
        
        // Parse the main query
        std::string mainQuery = queryStr.substr(mainQueryPos);
        return parseQuery(mainQuery);
    }
    if (command == "CREATE") {
        iss >> word;
        std::string objectType = toUpperCase(word);
        
        if (objectType == "TABLE") {
            return parseCreateTable(queryStr);
        } else if (objectType == "INDEX") {
            return parseCreateIndex(queryStr);
        } else if (objectType == "VIEW") {
            return parseCreateView(queryStr);
        } else if (objectType == "TYPE") {
            return parseCreateType(queryStr);
        } else if (objectType == "ASSERTION") {
            return parseCreateAssertion(queryStr);
        }
    } else if (command == "ALTER") {
        return parseAlterTable(queryStr);
    } else if (command == "DROP") {
        iss >> word;
        std::string objectType = toUpperCase(word);
        
        q.type = "DROP" + objectType;
        iss >> q.tableName; // This works for table, view, index, etc.
        
        // Special case for index
        if (objectType == "INDEX") {
            q.indexName = q.tableName;
            q.tableName = "";
        }
    } else if (command == "INSERT") {
        return parseInsert(queryStr);
    } else if (command == "SELECT") {
        return parseSelect(queryStr);
    } else if (command == "UPDATE") {
        return parseUpdate(queryStr);
    } else if (command == "DELETE") {
        return parseDelete(queryStr);
    } else if (command == "GRANT") {
        return parseGrant(queryStr);
    } else if (command == "REVOKE") {
        return parseRevoke(queryStr);
    } else if (command == "TRUNCATE") {
        q.type = "TRUNCATE";
        iss >> word; // Expect "TABLE"
        iss >> q.tableName;
    } else if (command == "DESCRIBE") {
        q.type = "DESCRIBE";
        iss >> q.tableName;
    } else if (command == "SHOW") {
        q.type = "SHOW";
        iss >> word; // Optional object type (tables, views, etc.)
        if (!word.empty()) {
            q.tableName = word; // Reuse tableName for the object type
        }
    } else if (command == "BEGIN") {
        q.type = "BEGIN";
    } else if (command == "COMMIT") {
        q.type = "COMMIT";
    } else if (command == "ROLLBACK") {
        q.type = "ROLLBACK";
    } else if (command == "MERGE") {
        q.type = "MERGE";
        iss >> word; // Expect "INTO"
        iss >> q.tableName;
        size_t pos = queryStr.find(q.tableName);
        q.mergeCommand = trim(queryStr.substr(pos + q.tableName.size()));
    } else if (command == "REPLACE") {
        q.type = "REPLACE";
        iss >> word; // Expect "INTO"
        iss >> q.tableName;
        q.values = extractValues(queryStr);
    }
    
    return q;
}
std::vector<std::vector<std::string>> Parser::extractValues(const std::string& query) {
    std::vector<std::vector<std::string>> values;
    
    // Find all value lists between parentheses
    std::regex valuesRegex(R"(\((.*?)\))");
    std::string::const_iterator searchStart(query.cbegin());
    std::smatch match;
    
    while (std::regex_search(searchStart, query.cend(), match, valuesRegex)) {
        std::string valueList = match[1];
        std::vector<std::string> rowValues;
        
        // Split comma-separated values
        std::istringstream valueStream(valueList);
        std::string value;
        bool inQuotes = false;
        std::string currentValue;
        
        for (char c : valueList) {
            if (c == '\'' && (currentValue.empty() || currentValue.back() != '\\')) {
                inQuotes = !inQuotes;
                currentValue += c;
            } else if (c == ',' && !inQuotes) {
                rowValues.push_back(trim(currentValue));
                currentValue.clear();
            } else {
                currentValue += c;
            }
        }
        
        if (!currentValue.empty()) {
            rowValues.push_back(trim(currentValue));
        }
        
        // Process each value (remove quotes, etc.)
        for (auto& val : rowValues) {
            val = trim(val);
            if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
                val = val.substr(1, val.size() - 2);
            }
        }
        
        values.push_back(rowValues);
        searchStart = match.suffix().first;
    }
    
    return values;
}
std::vector<std::pair<std::string, std::string>> Parser::extractColumns(const std::string& query) {
    std::vector<std::pair<std::string, std::string>> cols;
    
    // Find column definitions between parentheses
    size_t start = query.find('(');
    size_t end = query.rfind(')');
    
    if (start == std::string::npos || end == std::string::npos) {
        return cols;
    }
    
    std::string colsStr = query.substr(start + 1, end - start - 1);
    
    // Split columns by commas, but handle nested parentheses
    std::vector<std::string> columnDefs;
    int parenDepth = 0;
    std::string currentDef;
    
    for (size_t i = 0; i < colsStr.length(); i++) {
        char c = colsStr[i];
        
        if (c == '(') {
            parenDepth++;
            currentDef += c;
        } else if (c == ')') {
            parenDepth--;
            currentDef += c;
        } else if (c == ',' && parenDepth == 0) {
            // Found a column separator
            columnDefs.push_back(trim(currentDef));
            currentDef.clear();
        } else {
            currentDef += c;
        }
    }
    
    // Add the last column
    if (!currentDef.empty()) {
        columnDefs.push_back(trim(currentDef));
    }
    
    // Process each column definition
    for (const auto& def : columnDefs) {
        std::string trimmedDef = trim(def);
        
        // Skip if it's a constraint definition
        if (toUpperCase(trimmedDef).find("CONSTRAINT") == 0 ||
            toUpperCase(trimmedDef).find("PRIMARY KEY") == 0 ||
            toUpperCase(trimmedDef).find("FOREIGN KEY") == 0 ||
            toUpperCase(trimmedDef).find("UNIQUE") == 0 ||
            toUpperCase(trimmedDef).find("CHECK") == 0) {
            continue;
        }
        
        // Extract column name and type
        std::istringstream iss(trimmedDef);
        std::string colName, colType, word;
        
        // Get the column name (first word)
        iss >> colName;
        
        // Get the column type (next word, might include modifiers like NOT NULL)
        std::string typeWithModifiers;
        iss >> typeWithModifiers;
        
        // Handle data types with parameters like VARCHAR(50)
        if (typeWithModifiers.find('(') != std::string::npos && 
            typeWithModifiers.find(')') == std::string::npos) {
            // Type has opening parenthesis but no closing one
            // Read until we find the closing parenthesis
            std::string nextWord;
            while (iss >> nextWord) {
                typeWithModifiers += " " + nextWord;
                if (nextWord.find(')') != std::string::npos) {
                    break;
                }
            }
        }
        
        // Find where the data type ends
        size_t notPos = toUpperCase(typeWithModifiers).find(" NOT");
        size_t primaryPos = toUpperCase(typeWithModifiers).find(" PRIMARY");
        size_t uniquePos = toUpperCase(typeWithModifiers).find(" UNIQUE");
        
        size_t modifierPos = std::min({
            notPos, primaryPos, uniquePos,
            (notPos == std::string::npos ? std::string::npos : notPos),
            (primaryPos == std::string::npos ? std::string::npos : primaryPos),
            (uniquePos == std::string::npos ? std::string::npos : uniquePos)
        });
        
        // Extract just the data type part
        colType = typeWithModifiers;
        if (modifierPos != std::string::npos) {
            colType = typeWithModifiers.substr(0, modifierPos);
        }
        
        cols.emplace_back(trim(colName), trim(colType));
    }
    
    return cols;
}
std::vector<Constraint> Parser::extractConstraints(const std::string& query) {
    std::vector<Constraint> constraints;
    
    // Find column definitions between parentheses
    size_t start = query.find('(');
    size_t end = query.find_last_of(')');
    
    if (start == std::string::npos || end == std::string::npos) {
        return constraints;
    }
    
    std::string defsStr = query.substr(start + 1, end - start - 1);
    
    // Regex patterns for different constraints
    std::regex primaryKeyRegex(R"(CONSTRAINT\s+(\w+)\s+PRIMARY\s+KEY\s*\((.*?)\))");
    std::regex foreignKeyRegex(R"(CONSTRAINT\s+(\w+)\s+FOREIGN\s+KEY\s*\((.*?)\)\s+REFERENCES\s+(\w+)\s*(?:\((.*?)\))?)");
    std::regex uniqueRegex(R"(CONSTRAINT\s+(\w+)\s+UNIQUE\s*\((.*?)\))");
    std::regex checkRegex(R"(CONSTRAINT\s+(\w+)\s+CHECK\s*\((.*?)\))");
    std::regex notNullRegex(R"(CONSTRAINT\s+(\w+)\s+NOT\s+NULL\s*\((.*?)\))");
    
    std::string::const_iterator searchStart(defsStr.cbegin());
    std::smatch match;
    
    // Extract PRIMARY KEY constraints
    while (std::regex_search(searchStart, defsStr.cend(), match, primaryKeyRegex)) {
        Constraint c(Constraint::Type::PRIMARY_KEY, match[1]);
        
        // Split comma-separated column list
        std::string colList = match[2];
        std::istringstream colStream(colList);
        std::string col;
        while (std::getline(colStream, col, ',')) {
            c.columns.push_back(trim(col));
        }
        
        constraints.push_back(c);
        searchStart = match.suffix().first;
    }
    
    // Extract FOREIGN KEY constraints
    searchStart = defsStr.cbegin();
    while (std::regex_search(searchStart, defsStr.cend(), match, foreignKeyRegex)) {
        Constraint c(Constraint::Type::FOREIGN_KEY, match[1]);
        
        // Split comma-separated column list
        std::string colList = match[2];
        std::istringstream colStream(colList);
        std::string col;
        while (std::getline(colStream, col, ',')) {
            c.columns.push_back(trim(col));
        }
        
        c.referencedTable = match[3];
        
        // Referenced columns (if specified)
        if (match[4].matched) {
            std::string refColList = match[4];
            std::istringstream refColStream(refColList);
            std::string refCol;
            while (std::getline(refColStream, refCol, ',')) {
                c.referencedColumns.push_back(trim(refCol));
            }
        }
        
        // Check for CASCADE options
        if (defsStr.find("ON DELETE CASCADE") != std::string::npos) {
            c.cascadeDelete = true;
        }
        
        if (defsStr.find("ON UPDATE CASCADE") != std::string::npos) {
            c.cascadeUpdate = true;
        }
        
        constraints.push_back(c);
        searchStart = match.suffix().first;
    }
    
    // Extract UNIQUE constraints
    searchStart = defsStr.cbegin();
    while (std::regex_search(searchStart, defsStr.cend(), match, uniqueRegex)) {
        Constraint c(Constraint::Type::UNIQUE, match[1]);
        
        // Split comma-separated column list
        std::string colList = match[2];
        std::istringstream colStream(colList);
        std::string col;
        while (std::getline(colStream, col, ',')) {
            c.columns.push_back(trim(col));
        }
        
        constraints.push_back(c);
        searchStart = match.suffix().first;
    }
    
    // Extract CHECK constraints
    searchStart = defsStr.cbegin();
    while (std::regex_search(searchStart, defsStr.cend(), match, checkRegex)) {
        Constraint c(Constraint::Type::CHECK, match[1]);
        c.checkExpression = match[2];
        constraints.push_back(c);
        searchStart = match.suffix().first;
    }
    
    // Extract NOT NULL constraints
    searchStart = defsStr.cbegin();
    while (std::regex_search(searchStart, defsStr.cend(), match, notNullRegex)) {
        Constraint c(Constraint::Type::NOT_NULL, match[1]);
        
        // Split comma-separated column list
        std::string colList = match[2];
        std::istringstream colStream(colList);
        std::string col;
        while (std::getline(colStream, col, ',')) {
            c.columns.push_back(trim(col));
        }
        
        constraints.push_back(c);
        searchStart = match.suffix().first;
    }
    
    return constraints;
}
std::vector<std::pair<std::string, std::string>> Parser::extractWithClauses(const std::string& query) {
    std::vector<std::pair<std::string, std::string>> clauses;
    
    // Find the WITH clause
    size_t withPos = query.find("WITH");
    if (withPos == std::string::npos) {
        return clauses;
    }
    
    // Extract the WITH clause part
    size_t mainQueryPos = query.find(";", withPos);
    if (mainQueryPos == std::string::npos) {
        mainQueryPos = query.find("SELECT", withPos);
        if (mainQueryPos == std::string::npos) {
            return clauses;
        }
    }
    
    std::string withClauseStr = query.substr(withPos, mainQueryPos - withPos);
    
    // Split into individual CTEs
    std::regex cteRegex(R"((\w+)\s*AS\s*\((.*?)\))");
    std::string::const_iterator searchStart(withClauseStr.cbegin());
    std::smatch match;
    
    while (std::regex_search(searchStart, withClauseStr.cend(), match, cteRegex)) {
        std::string cteName = match[1];
        std::string cteQuery = match[2];
        clauses.emplace_back(cteName, cteQuery);
        searchStart = match.suffix().first;
    }
    
    return clauses;
}
Query Parser::parseCreateTable(const std::string& query) {
    Query q;
    q.type = "CREATE";
    
    // Extract table name
    std::regex tableNameRegex(R"(CREATE\s+TABLE\s+(\w+))");
    std::smatch match;
    if (std::regex_search(query, match, tableNameRegex)) {
        q.tableName = match[1];
    }
    
    // Extract columns and constraints
    q.columns = extractColumns(query);
    q.constraints = extractConstraints(query);
    
    // Validate duplicate column names and data types
    std::unordered_set<std::string> seen;
    for (const auto &colPair : q.columns) {
        if (seen.find(colPair.first) != seen.end()) {
            throw DatabaseException("Duplicate column name '" + colPair.first + "' in CREATE TABLE statement");
        }
        seen.insert(colPair.first);
        if (!isValidDataType(colPair.second) && !UserTypeRegistry::typeExists(colPair.second)) {
            throw DataTypeException("Invalid data type '" + colPair.second + "' for column '" + colPair.first + "'");
        }
    }
    
    return q;
}
Query Parser::parseCreateIndex(const std::string& query) {
    Query q;
    q.type = "CREATEINDEX";
    
    // Extract index name
    std::regex indexNameRegex(R"(CREATE\s+INDEX\s+(\w+))");
    std::smatch match;
    if (std::regex_search(query, match, indexNameRegex)) {
        q.indexName = match[1];
    }
    
    // Extract table name
    std::regex tableNameRegex(R"(ON\s+(\w+))");
    if (std::regex_search(query, match, tableNameRegex)) {
        q.tableName = match[1];
    }
    
    // Extract column name
    std::regex columnNameRegex(R"(\(\s*(\w+)\s*\))");
    if (std::regex_search(query, match, columnNameRegex)) {
        q.columnName = match[1];
    }
    
    return q;
}
Query Parser::parseCreateView(const std::string& query) {
    Query q;
    q.type = "CREATEVIEW";
    
    // Extract view name
    std::regex viewNameRegex(R"(CREATE\s+VIEW\s+(\w+))");
    std::smatch match;
    if (std::regex_search(query, match, viewNameRegex)) {
        q.viewName = match[1];
    }
    
    // Extract view definition
    std::regex viewDefRegex(R"(AS\s+(.*))");
    if (std::regex_search(query, match, viewDefRegex)) {
        q.viewDefinition = match[1];
    }
    
    return q;
}
Query Parser::parseCreateType(const std::string& query) {
    Query q;
    q.type = "CREATETYPE";
    
    // Extract type name
    std::regex typeNameRegex(R"(CREATE\s+TYPE\s+(\w+))");
    std::smatch match;
    if (std::regex_search(query, match, typeNameRegex)) {
        q.typeName = match[1];
    }
    
    // Extract type attributes (similar to table columns)
    q.columns = extractColumns(query);
    
    return q;
}
Query Parser::parseCreateAssertion(const std::string& query) {
    Query q;
    q.type = "CREATEASSERTION";
    
    // Extract assertion name
    std::regex assertionNameRegex(R"(CREATE\s+ASSERTION\s+(\w+))");
    std::smatch match;
    if (std::regex_search(query, match, assertionNameRegex)) {
        q.assertionName = match[1];
    }
    
    // Extract assertion condition
    std::regex conditionRegex(R"(CHECK\s+\((.*)\))");
    if (std::regex_search(query, match, conditionRegex)) {
        q.assertionCondition = match[1];
    }
    
    return q;
}
Query Parser::parseAlterTable(const std::string& query) {
    Query q;
    q.type = "ALTER";
    
    // Extract table name
    std::regex tableNameRegex(R"(ALTER\s+TABLE\s+(\w+))");
    std::smatch match;
    if (std::regex_search(query, match, tableNameRegex)) {
        q.tableName = match[1];
    }
    
    // Extract alter action
    if (query.find("ADD CONSTRAINT") != std::string::npos) {
        q.alterAction = "ADD CONSTRAINT";
        q.constraints = extractConstraints(query);
    } else if (query.find("DROP CONSTRAINT") != std::string::npos) {
        q.alterAction = "DROP CONSTRAINT";
        
        // Extract constraint name
        std::regex constraintNameRegex(R"(DROP\s+CONSTRAINT\s+(\w+))");
        if (std::regex_search(query, match, constraintNameRegex)) {
            Constraint c(Constraint::Type::UNIQUE, match[1]);
            q.constraints.push_back(c);
        }
    } else if (query.find("ADD") != std::string::npos) {
        q.alterAction = "ADD";
        
        // Extract column definition
        std::regex columnDefRegex(R"(ADD\s+(?:COLUMN\s+)?(\w+)\s+(\w+(?:\(\d+(?:,\d+)?\))?))");
        if (std::regex_search(query, match, columnDefRegex)) {
            q.alterColumn = {match[1], match[2]};
        }
    } else if (query.find("DROP") != std::string::npos) {
        q.alterAction = "DROP";
        
        // Extract column name
        std::regex columnNameRegex(R"(DROP\s+(?:COLUMN\s+)?(\w+))");
        if (std::regex_search(query, match, columnNameRegex)) {
            q.alterColumn = {match[1], ""};
        }
    } else if (query.find("RENAME") != std::string::npos) {
        if (query.find("RENAME TO") != std::string::npos) {
            q.alterAction = "RENAME";
            
            // Extract new table name
            std::regex newTableNameRegex(R"(RENAME\s+TO\s+(\w+))");
            if (std::regex_search(query, match, newTableNameRegex)) {
                q.newTableName = match[1];
            }
        } else if (query.find("RENAME COLUMN") != std::string::npos) {
            q.alterAction = "RENAME COLUMN";
            
            // Extract old and new column names
            std::regex columnNamesRegex(R"(RENAME\s+COLUMN\s+(\w+)\s+TO\s+(\w+))");
            if (std::regex_search(query, match, columnNamesRegex)) {
                q.alterColumn = {match[1], match[2]};
            }
        }
    }
    
    return q;
}
Query Parser::parseInsert(const std::string& query) {
    Query q;
    q.type = "INSERT";
    
    // Extract table name
    std::regex tableNameRegex(R"(INSERT\s+INTO\s+(\w+))");
    std::smatch match;
    if (std::regex_search(query, match, tableNameRegex)) {
        q.tableName = match[1];
    }
    
    // Extract values
    q.values = extractValues(query);
    
    return q;
}
Query Parser::parseSelect(const std::string& query) {
    Query q;
    q.type = "SELECT";
    
    // Check for DISTINCT or ALL
    if (query.find("SELECT DISTINCT") != std::string::npos) {
        q.distinct = true;
    } else if (query.find("SELECT ALL") != std::string::npos) {
        q.all = true;
    }
    
    // Extract select columns
    q.selectColumns = extractSelectColumns(query);
    
    // Extract FROM clause
    std::regex fromRegex(R"(FROM\s+(\w+))");
    std::smatch match;
    if (std::regex_search(query, match, fromRegex)) {
        q.tableName = match[1];
    }
    
    // Check for JOIN
    q.isJoin = false;
    if (query.find(" JOIN ") != std::string::npos) {
        q.isJoin = true;
        
        // Determine join type
        if (query.find("INNER JOIN") != std::string::npos) {
            q.joinType = "INNER";
        } else if (query.find("LEFT JOIN") != std::string::npos || query.find("LEFT OUTER JOIN") != std::string::npos) {
            q.joinType = "LEFT OUTER";
        } else if (query.find("RIGHT JOIN") != std::string::npos || query.find("RIGHT OUTER JOIN") != std::string::npos) {
            q.joinType = "RIGHT OUTER";
        } else if (query.find("FULL JOIN") != std::string::npos || query.find("FULL OUTER JOIN") != std::string::npos) {
            q.joinType = "FULL OUTER";
        } else if (query.find("NATURAL JOIN") != std::string::npos) {
            q.joinType = "NATURAL";
        } else {
            q.joinType = "INNER"; // Default
        }
        
        // Extract join table
        std::regex joinTableRegex;
        if (q.joinType == "NATURAL") {
            joinTableRegex = std::regex(R"(NATURAL\s+JOIN\s+(\w+))");
        } else {
            joinTableRegex = std::regex(R"(JOIN\s+(\w+))");
        }
        
        if (std::regex_search(query, match, joinTableRegex)) {
            q.joinTable = match[1];
        }
        
        // Extract join condition (for non-NATURAL joins)
        if (q.joinType != "NATURAL") {
            if (query.find(" ON ") != std::string::npos) {
                std::regex onRegex(R"(ON\s+(.*?)(?:\s+(?:WHERE|GROUP|ORDER|HAVING|LIMIT)|$))");
                if (std::regex_search(query, match, onRegex)) {
                    q.joinCondition = match[1];
                }
            } else if (query.find(" USING ") != std::string::npos) {
                std::regex usingRegex(R"(USING\s+\((.*?)\))");
                if (std::regex_search(query, match, usingRegex)) {
                    std::string columns = match[1];
                    std::istringstream colStream(columns);
                    std::string col;
                    while (std::getline(colStream, col, ',')) {
                        q.usingColumns.push_back(trim(col));
                    }
                }
            }
        }
    }
    
    // Extract WHERE clause
    q.condition = extractCondition(query);
    
    // Extract GROUP BY clause
    std::regex groupByRegex(R"(GROUP\s+BY\s+(.*?)(?:\s+(?:HAVING|ORDER|LIMIT)|$))");
    if (std::regex_search(query, match, groupByRegex)) {
        std::string groupByColumns = match[1];
        std::istringstream groupByStream(groupByColumns);
        std::string col;
        while (std::getline(groupByStream, col, ',')) {
            q.groupByColumns.push_back(trim(col));
        }
    }
    
    // Extract HAVING clause
    std::regex havingRegex(R"(HAVING\s+(.*?)(?:\s+(?:ORDER|LIMIT)|$))");
    if (std::regex_search(query, match, havingRegex)) {
        q.havingCondition = match[1];
    }
    
    // Extract ORDER BY clause
    std::regex orderByRegex(R"(ORDER\s+BY\s+(.*?)(?:\s+LIMIT|$))");
    if (std::regex_search(query, match, orderByRegex)) {
        std::string orderByColumns = match[1];
        std::istringstream orderByStream(orderByColumns);
        std::string col;
        while (std::getline(orderByStream, col, ',')) {
            q.orderByColumns.push_back(trim(col));
        }
    }
    
    // Check for set operations
    if (query.find(" UNION ") != std::string::npos) {
        q.setOperation = "UNION";
        auto parts = parseSetOperation(query);
        q.rightQuery = parts.second;
    } else if (query.find(" INTERSECT ") != std::string::npos) {
        q.setOperation = "INTERSECT";
        auto parts = parseSetOperation(query);
        q.rightQuery = parts.second;
    } else if (query.find(" EXCEPT ") != std::string::npos) {
        q.setOperation = "EXCEPT";
        auto parts = parseSetOperation(query);
        q.rightQuery = parts.second;
    }
    
    // Extract subqueries
    q.subqueries = extractSubqueries(query);
    
    return q;
}
Query Parser::parseUpdate(const std::string& query) {
    Query q;
    q.type = "UPDATE";
    
    // Extract table name
    std::regex tableNameRegex(R"(UPDATE\s+(\w+))");
    std::smatch match;
    if (std::regex_search(query, match, tableNameRegex)) {
        q.tableName = match[1];
    }
    
    // Extract SET updates
    q.updates = extractUpdates(query);
    
    // Extract WHERE clause
    q.condition = extractCondition(query);
    
    return q;
}
Query Parser::parseDelete(const std::string& query) {
    Query q;
    q.type = "DELETE";
    
    // Extract table name
    std::regex tableNameRegex(R"(FROM\s+(\w+))");
    std::smatch match;
    if (std::regex_search(query, match, tableNameRegex)) {
        q.tableName = match[1];
    }
    
    // Extract WHERE clause
    q.condition = extractCondition(query);
    
    return q;
}
Query Parser::parseGrant(const std::string& query) {
    Query q;
    q.type = "GRANT";
    
    // Extract privileges
    std::regex privRegex(R"(GRANT\s+((?:SELECT|INSERT|UPDATE|DELETE|ALL)(?:\s*,\s*(?:SELECT|INSERT|UPDATE|DELETE|ALL))*))");
    std::smatch match;
    if (std::regex_search(query, match, privRegex) && match.size() > 1) {
        q.privilege = match[1];
        
        // Split multiple privileges and process them individually
        std::string privStr = match[1];
        std::vector<std::string> privileges = split(privStr, ',');
        
        if (privileges.size() > 1) {
            q.multiplePrivileges = true;
            q.privileges = privileges;
        } else {
            q.privilege = trim(privStr);
        }
    }
    
    // Extract table name
    std::regex tableNameRegex(R"(ON\s+(\w+))");
    if (std::regex_search(query, match, tableNameRegex)) {
        q.tableName = match[1];
    }
    
    // Extract username
    std::regex userNameRegex(R"(TO\s+(\w+))");
    if (std::regex_search(query, match, userNameRegex)) {
        q.username = match[1];
    }
    
    return q;
}
Query Parser::parseRevoke(const std::string& query) {
    Query q;
    q.type = "REVOKE";
    
    // Extract privilege
    std::regex privRegex(R"(REVOKE\s+((?:SELECT|INSERT|UPDATE|DELETE|ALL)(?:\s*,\s*(?:SELECT|INSERT|UPDATE|DELETE|ALL))*))");
    std::smatch match;
    if (std::regex_search(query, match, privRegex)) {
        q.privilege = match[1];
    }
    
    // Extract table name
    std::regex tableNameRegex(R"(ON\s+(\w+))");
    if (std::regex_search(query, match, tableNameRegex)) {
        q.tableName = match[1];
    }
    
    // Extract username
    std::regex userNameRegex(R"(FROM\s+(\w+))");
    if (std::regex_search(query, match, userNameRegex)) {
        q.username = match[1];
    }
    
    return q;
}
std::vector<std::string> Parser::extractSelectColumns(const std::string& query) {
    std::vector<std::string> columns;
    
    // Find the SELECT clause
    size_t selectPos = query.find("SELECT");
    if (selectPos == std::string::npos) {
        return columns;
    }
    
    // Extract the columns part
    size_t fromPos = query.find("FROM", selectPos);
    if (fromPos == std::string::npos) {
        return columns;
    }
    
    std::string colsStr = query.substr(selectPos + 6, fromPos - selectPos - 6);
    colsStr = trim(colsStr);
    
    // Check for DISTINCT or ALL
    if (colsStr.find("DISTINCT") != std::string::npos) {
        colsStr = colsStr.substr(8);
    } else if (colsStr.find("ALL") != std::string::npos) {
        colsStr = colsStr.substr(3);
    }
    
    // Split comma-separated columns
    std::istringstream colStream(colsStr);
    std::string col;
    while (std::getline(colStream, col, ',')) {
        columns.push_back(trim(col));
    }
    
    return columns;
}
std::string Parser::extractCondition(const std::string& query) {
    std::string condition;
    
    // Check for WHERE clause
    size_t wherePos = query.find("WHERE");
    if (wherePos != std::string::npos) {
        size_t endPos = query.find_first_of(";", wherePos);
        if (endPos == std::string::npos) {
            endPos = query.length();
        }
        
        condition = query.substr(wherePos + 5, endPos - wherePos - 5);
        condition = trim(condition);
    }
    
    return condition;
}
std::vector<std::pair<std::string, std::string>> Parser::extractUpdates(const std::string& query) {
    std::vector<std::pair<std::string, std::string>> updates;
    
    // Find the SET clause
    size_t setPos = query.find("SET");
    if (setPos == std::string::npos) {
        return updates;
    }
    
    // Extract the updates part
    size_t wherePos = query.find("WHERE", setPos);
    if (wherePos == std::string::npos) {
        wherePos = query.find(";", setPos);
    }
    
    std::string updatesStr = query.substr(setPos + 3, wherePos - setPos - 3);
    updatesStr = trim(updatesStr);
    
    // Split into individual assignments
    std::istringstream updateStream(updatesStr);
    std::string assignment;
    while (std::getline(updateStream, assignment, ',')) {
        assignment = trim(assignment);
        size_t eqPos = assignment.find('=');
        if (eqPos != std::string::npos) {
            std::string col = trim(assignment.substr(0, eqPos));
            std::string val = trim(assignment.substr(eqPos + 1));
            updates.emplace_back(col, val);
        }
    }
    
    return updates;
}
std::pair<std::string, std::string> Parser::parseJoinCondition(const std::string& query) {
    std::pair<std::string, std::string> condition;
    
    // Find the ON clause
    size_t onPos = query.find("ON");
    if (onPos != std::string::npos) {
        size_t endPos = query.find_first_of(";", onPos);
        if (endPos == std::string::npos) {
            endPos = query.length();
        }
        
        condition.first = "ON";
        condition.second = query.substr(onPos + 2, endPos - onPos - 2);
        condition.second = trim(condition.second);
    }
    
    return condition;
}
std::vector<std::pair<std::string, std::string>> Parser::parseWithClause(const std::string& query) {
    return extractWithClauses(query);
}
std::pair<std::string, std::string> Parser::parseSetOperation(const std::string& query) {
    std::pair<std::string, std::string> parts;
    
    // Find the set operation (UNION, INTERSECT, EXCEPT)
    size_t unionPos = query.find(" UNION ");
    size_t intersectPos = query.find(" INTERSECT ");
    size_t exceptPos = query.find(" EXCEPT ");
    
    size_t opPos = std::string::npos;
    size_t opLen = 0;
    
    if (unionPos != std::string::npos) {
        opPos = unionPos;
        opLen = 7; // Length of " UNION "
    } else if (intersectPos != std::string::npos) {
        opPos = intersectPos;
        opLen = 11; // Length of " INTERSECT "
    } else if (exceptPos != std::string::npos) {
        opPos = exceptPos;
        opLen = 8; // Length of " EXCEPT "
    }
    
    if (opPos != std::string::npos) {
        parts.first = query.substr(0, opPos);
        parts.second = query.substr(opPos + opLen);
    }
    
    return parts;
}
std::vector<std::string> Parser::extractSubqueries(const std::string& query) {
    std::vector<std::string> subqueries;
    
    // Find all subqueries in parentheses
    std::regex subqueryRegex(R"(\(([^()]*)\))");
    std::string::const_iterator searchStart(query.cbegin());
    std::smatch match;
    
    while (std::regex_search(searchStart, query.cend(), match, subqueryRegex)) {
        subqueries.push_back(match[1]);
        searchStart = match.suffix().first;
    }
    
    return subqueries;
}