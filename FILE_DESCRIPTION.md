# Comprehensive File and Component Descriptions

This document provides detailed descriptions of each source file in the database system, explaining their purpose and functionality.

## Core Database Files

### Database.h / Database.cpp
**Purpose**: Main database management class that serves as the central coordination point for all database operations.

**Key Components**:
- Database creation and connection management
- Table and view management methods (create, drop, alter)
- Query execution methods (select, insert, update, delete)
- Transaction management
- User privilege management
- Schema information access

**Key Functions**:
- `createTable()` - Creates new tables with constraints
- `insertRecord()` - Inserts data into tables
- `selectRecords()` - Retrieves data with filtering and joins
- `updateRecords()` - Modifies existing data
- `deleteRecords()` - Removes data based on conditions
- `beginTransaction()` - Starts a transaction
- `commitTransaction()` - Commits a transaction
- `rollbackTransaction()` - Rolls back a transaction

### Table.h / Table.cpp
**Purpose**: Represents a database table and provides operations for manipulating table data and structure.

**Key Components**:
- Table schema definition (columns, types, constraints)
- Row storage and access
- Table-level query operations
- Constraint validation
- Data type enforcement
- Join operations implementation

**Key Functions**:
- `addRow()` - Adds a row to the table
- `addColumn()` - Adds a column to the table
- `selectRows()` - Filters and projects table data
- `updateRows()` - Updates rows matching conditions
- `deleteRows()` - Deletes rows matching conditions
- `addConstraint()` - Adds constraints to the table
- `validateConstraints()` - Validates that rows meet constraints
- Various join methods (`innerJoin()`, `leftOuterJoin()`, etc.)

### Parser.h / Parser.cpp
**Purpose**: Parses SQL statements into structured Query objects for execution.

**Key Components**:
- Lexical analysis of SQL statements
- Parsing of various SQL command types
- Command structure validation
- Query object construction

**Key Functions**:
- `parseQuery()` - Parses a SQL string into a Query object
- `parseCreateTable()` - Parses CREATE TABLE statements
- `parseSelect()` - Parses SELECT statements
- `parseInsert()` - Parses INSERT statements
- `parseUpdate()` - Parses UPDATE statements
- `parseDelete()` - Parses DELETE statements
- Extraction functions for columns, conditions, etc.

### ConditionParser.h / ConditionParser.cpp
**Purpose**: Specialized parser for SQL WHERE conditions and expressions.

**Key Components**:
- SQL expression parsing
- Condition tree construction
- Logical operator handling (AND, OR, NOT)
- Comparison operator processing
- Special condition handling (BETWEEN, IN, LIKE, etc.)

**Key Functions**:
- `parse()` - Parses a condition expression
- `tokenize()` - Breaks condition into tokens
- Logic for parsing various expression types
- Expression evaluation

## Data Management

### Aggregation.h / Aggregation.cpp
**Purpose**: Implements SQL aggregation functions.

**Key Components**:
- Statistical calculation functions
- NULL value handling
- Type conversion for aggregations

**Key Functions**:
- `computeMean()` - Calculates average
- `computeMin()` - Finds minimum value
- `computeMax()` - Finds maximum value
- `computeSum()` - Calculates sum
- `computeMedian()` - Finds median value
- `computeMode()` - Finds most common value
- `computeStdDev()` - Calculates standard deviation
- `computeCount()` - Counts values or rows

### ForeignKeyValidator.h / ForeignKeyValidator.cpp
**Purpose**: Manages foreign key constraint validation.

**Key Components**:
- Foreign key reference tracking
- Validation logic
- Singleton pattern implementation

**Key Functions**:
- `registerTable()` - Registers a table for FK validation
- `unregisterTable()` - Removes a table from validation
- `validateForeignKey()` - Validates a foreign key constraint

### Index.h / Index.cpp
**Purpose**: Implements database indexes for query optimization.

**Key Components**:
- Index creation and maintenance
- Index lookup functionality

**Key Functions**:
- `build()` - Builds an index on a column
- `lookup()` - Performs an index lookup

### Storage.h / Storage.cpp
**Purpose**: Manages persistence of database objects to disk.

**Key Components**:
- File I/O operations
- Data serialization and deserialization
- Database file format handling

**Key Functions**:
- `saveTableToFile()` - Persists a table to disk
- `loadTableFromFile()` - Loads a table from disk
- `saveDatabase()` - Saves entire database state
- `loadDatabase()` - Loads entire database state

## Metadata Management

### Catalog.h / Catalog.cpp
**Purpose**: Manages database metadata (schema information).

**Key Components**:
- Table metadata storage
- View metadata storage
- Index information
- Type definitions
- Privilege information

**Key Functions**:
- `addTable()` - Registers table metadata
- `addView()` - Registers view metadata
- `addIndex()` - Registers index metadata
- `showSchema()` - Displays schema information
- Various getters for metadata retrieval

## Transaction & Concurrency

### Transaction.h / Transaction.cpp
**Purpose**: Implements database transaction management.

**Key Components**:
- Transaction state management
- Begin/commit/rollback operations
- Table state logging for rollback

**Key Functions**:
- `begin()` - Starts a transaction
- `commit()` - Commits changes
- `rollback()` - Rolls back changes
- Table state saving and restoring methods

### Concurrency.h / Concurrency.cpp
**Purpose**: Manages concurrent access to database objects.

**Key Components**:
- Lock management
- Deadlock detection
- Wait-for graph construction

**Key Functions**:
- `acquireLock()` - Requests a lock on a resource
- `releaseAllLocks()` - Releases all locks held by a transaction
- `detectDeadlock()` - Detects deadlock situations

## User Management

### User.h / User.cpp
**Purpose**: Implements user management and authentication.

**Key Components**:
- User account storage
- Password management
- Privilege tracking

**Key Functions**:
- `authenticate()` - Verifies user credentials
- `grantPrivilege()` - Grants privileges to users
- `revokePrivilege()` - Revokes user privileges
- `hasPrivilege()` - Checks for privilege existence

## Utilities

### Utils.h / Utils.cpp
**Purpose**: Provides common utility functions used throughout the system.

**Key Components**:
- String manipulation functions
- Type conversion utilities
- Exception classes
- User-defined type registry

**Key Functions**:
- `toLowerCase()` - Case conversion for case-insensitive operations
- `toUpperCase()` - Case conversion for display
- `trim()` - String trimming utilities
- `split()` - String splitting
- Type validation and conversion functions
- Exception class definitions

## Main Entry Point

### Main.cpp
**Purpose**: Application entry point and command-line interface.

**Key Components**:
- Command-line parsing
- Interactive SQL shell
- Database instance creation

**Key Functions**:
- Command input and processing loop
- SQL command execution
- Result display
- Help command implementation

## Relationships Between Components

1. **Main Flow**: 
   - `main.cpp` creates a `Database` instance
   - SQL commands are processed by the `Parser`
   - Parsed commands are executed by the `Database` using `Table` operations
   - Results are returned to the user

2. **Transaction Flow**:
   - `Database` creates `Transaction` objects
   - `Transaction` manages table state changes
   - `Concurrency` handles locking during transactions
   - Commit/rollback operations are coordinated by `Database`

3. **Query Execution Flow**:
   - `Parser` converts SQL to query structure
   - `ConditionParser` handles WHERE clauses
   - `Database` delegates to `Table` for data operations
   - `Aggregation` computes summary functions
   - Results are collected and formatted

4. **Constraint Validation Flow**:
   - `Table` enforces constraints on data operations
   - `ForeignKeyValidator` handles referential integrity
   - Data type validation uses utilities from `Utils`

5. **Persistence Flow**:
   - `Storage` manages file I/O
   - `Database` coordinates save/load operations
   - `Table` state is serialized to disk
   - `Catalog` metadata is persisted

This structure creates a modular system where each component has clear responsibilities, making the codebase maintainable and extensible.