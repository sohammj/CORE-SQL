# CORE-SQL: In-Memory SQL Database System

A lightweight, fast, and feature-rich relational database management system implemented entirely in C++. This project provides a comprehensive SQL interface with support for ACID transactions, complex queries, referential integrity, and other essential database features.

## Features

- **Complete SQL Support**: Handles DDL (CREATE, ALTER, DROP), DML (SELECT, INSERT, UPDATE, DELETE), and DCL (GRANT, REVOKE) statements
- **ACID Transactions**: Ensures Atomicity, Consistency, Isolation, and Durability
- **Referential Integrity**: Full support for Foreign Key constraints with cascading actions
- **Complex Queries**: JOIN operations, subqueries, aggregations
- **Indexing**: B-tree index implementation for faster queries
- **Views**: Create and manage database views
- **Multi-user Access**: User management with privilege control
- **Data Types**: Support for common data types (INT, VARCHAR, FLOAT, DATE, etc.)
- **Constraints**: PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, NOT NULL

## Architecture

The system is built around these core components:

1. **Parser**: Processes SQL statements into an abstract syntax tree
2. **Query Execution Engine**: Executes parsed queries
3. **Storage Engine**: Manages data storage and retrieval
4. **Transaction Manager**: Handles transaction control
5. **Catalog Manager**: Maintains metadata about database objects
6. **Constraint Manager**: Enforces data integrity constraints
7. **Security Manager**: Manages user access and privileges

## Technology Stack

- C++17
- Standard Template Library (STL)
- No external dependencies or database libraries

## Getting Started

### Prerequisites

- C++17 compatible compiler (GCC 7+, Clang 5+, MSVC 19.14+)
- CMake 3.10 or higher (for building)
- 50MB minimum free disk space

### Building

```bash
# Clone the repository
git clone https://github.com/yourusername/cpp-database.git
cd cpp-database

# Build the project
mkdir build
cd build
cmake ..
make
```

### Running

```bash
# Start the database
./mydb
```

### Example Session

```sql
-- Start a session
CREATE TABLE employees (
    emp_id INT NOT NULL,
    emp_name VARCHAR(50) NOT NULL,
    dept_id INT,
    salary FLOAT,
    CONSTRAINT pk_emp PRIMARY KEY (emp_id)
);

-- Insert data
INSERT INTO employees VALUES (1, 'John Smith', 2, 75000);
INSERT INTO employees VALUES (2, 'Mary Johnson', 1, 65000);

-- Query data
SELECT * FROM employees WHERE salary > 70000;

-- Use transactions
BEGIN TRANSACTION;
UPDATE employees SET salary = salary * 1.1 WHERE dept_id = 2;
COMMIT;
```

## Database Schema Examples

### Sample Employee Management Schema

```sql
CREATE TABLE departments (
    dept_id INT NOT NULL,
    dept_name VARCHAR(50) NOT NULL,
    location VARCHAR(50),
    manager_id INT,
    CONSTRAINT pk_dept PRIMARY KEY (dept_id)
);

CREATE TABLE employees (
    emp_id INT NOT NULL,
    emp_name VARCHAR(50) NOT NULL,
    dept_id INT,
    salary FLOAT,
    hire_date DATE,
    CONSTRAINT pk_emp PRIMARY KEY (emp_id),
    CONSTRAINT fk_emp_dept FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);
```

### Sample Transportation System Schema

```sql
CREATE TABLE routes (
    route_id INT NOT NULL,
    origin VARCHAR(50),
    destination VARCHAR(50),
    distance FLOAT,
    CONSTRAINT pk_route PRIMARY KEY (route_id)
);

CREATE TABLE tickets (
    ticket_id INT NOT NULL,
    route_id INT,
    price FLOAT,
    purchase_date DATE,
    CONSTRAINT pk_ticket PRIMARY KEY (ticket_id),
    CONSTRAINT fk_ticket_route FOREIGN KEY (route_id) REFERENCES routes(route_id)
);
```

## Command Reference

The database supports these SQL command categories:

### Data Definition Language (DDL)

- `CREATE TABLE` - Create a new table
- `ALTER TABLE` - Modify an existing table (add/drop columns or constraints)
- `DROP TABLE` - Remove a table
- `CREATE INDEX` - Create an index on columns
- `DROP INDEX` - Remove an index
- `CREATE VIEW` - Create a logical view
- `DROP VIEW` - Remove a view

### Data Manipulation Language (DML)

- `SELECT` - Query data from tables
- `INSERT INTO` - Add new records
- `UPDATE` - Modify existing records
- `DELETE FROM` - Remove records
- `TRUNCATE TABLE` - Remove all records from a table

### Transaction Control

- `BEGIN TRANSACTION` - Start a transaction
- `COMMIT` - Commit changes
- `ROLLBACK` - Rollback changes

### Data Control Language (DCL)

- `GRANT` - Assign privileges to users
- `REVOKE` - Remove privileges from users
- `CREATE USER` - Create a new user
- `ALTER USER` - Modify user properties

### Utility Commands

- `DESCRIBE` - Show table schema
- `SHOW TABLES` - List all tables
- `SHOW VIEWS` - List all views
- `SHOW INDEXES` - List all indexes

## Performance Considerations

- The system is optimized for in-memory operations but supports disk persistence
- Query performance scales well up to ~100,000 rows per table
- Transaction throughput of ~1,000 transactions per second on modern hardware

## Limitations

- Maximum database size: Limited by available system memory and disk space
- Maximum table size: 2^32 rows (4.2 billion)
- Maximum columns per table: 256
- Maximum indexes per table: 64

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.


## Acknowledgements

- This project was developed as an educational exploration of database system internals
- Inspiration drawn from SQLite, PostgreSQL and other open-source databases
