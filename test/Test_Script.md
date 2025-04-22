# Test Script: Comprehensive Database Testing

This document provides a structured test script for validating all features of the C++ SQL Database Management System. Use this script to ensure your instance is working correctly or to systematically test new features.

## 1. Basic Setup and Schema Creation

```sql
-- Create base schema for testing
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
    age INT,
    gender VARCHAR(10),
    hire_date DATE,
    CONSTRAINT pk_emp PRIMARY KEY (emp_id),
    CONSTRAINT fk_emp_dept FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);

CREATE TABLE category_header (
    cat_code INT NOT NULL,
    category_description VARCHAR(100),
    capacity INT,
    CONSTRAINT pk_cat PRIMARY KEY (cat_code)
);

CREATE TABLE place_header (
    place_id INT NOT NULL,
    place_name VARCHAR(50) NOT NULL,
    place_address VARCHAR(100),
    bus_station VARCHAR(50),
    CONSTRAINT pk_place PRIMARY KEY (place_id)
);

CREATE TABLE route_header (
    route_id INT NOT NULL,
    route_no INT,
    cat_code INT,
    origin VARCHAR(50),
    destination VARCHAR(50),
    distance FLOAT,
    fare FLOAT,
    CONSTRAINT pk_route PRIMARY KEY (route_id),
    CONSTRAINT fk_route_cat FOREIGN KEY (cat_code) REFERENCES category_header(cat_code)
);

CREATE TABLE route_detail (
    route_id INT NOT NULL,
    place_id INT NOT NULL,
    nonstop CHAR(1),
    CONSTRAINT pk_route_detail PRIMARY KEY (route_id, place_id),
    CONSTRAINT fk_rd_route FOREIGN KEY (route_id) REFERENCES route_header(route_id),
    CONSTRAINT fk_rd_place FOREIGN KEY (place_id) REFERENCES place_header(place_id)
);

CREATE TABLE fleet_header (
    fleet_id INT NOT NULL,
    fleet_type VARCHAR(50),
    fleet_capacity INT,
    CONSTRAINT pk_fleet PRIMARY KEY (fleet_id)
);

CREATE TABLE ticket_header (
    ticket_id INT NOT NULL,
    fleet_id INT,
    route_id INT,
    dot DATE,
    no_adults INT,
    no_children INT,
    CONSTRAINT pk_ticket PRIMARY KEY (ticket_id),
    CONSTRAINT fk_ticket_fleet FOREIGN KEY (fleet_id) REFERENCES fleet_header(fleet_id),
    CONSTRAINT fk_ticket_route FOREIGN KEY (route_id) REFERENCES route_header(route_id)
);

CREATE TABLE ticket_detail (
    ticket_id INT NOT NULL,
    passenger_name VARCHAR(50),
    passenger_age INT,
    gender VARCHAR(10),
    fare FLOAT,
    CONSTRAINT fk_td_ticket FOREIGN KEY (ticket_id) REFERENCES ticket_header(ticket_id)
);

CREATE TABLE salesman (
    salesman_id INT NOT NULL,
    name VARCHAR(50),
    city VARCHAR(50),
    commission FLOAT,
    product VARCHAR(50),
    CONSTRAINT pk_salesman PRIMARY KEY (salesman_id)
);

-- Verify table creation
SHOW TABLES;
```

## 2. Data Insertion Tests

```sql
-- Insert department data
INSERT INTO departments VALUES (1, 'HR', 'New York', 100);
INSERT INTO departments VALUES (2, 'Engineering', 'San Francisco', 101);
INSERT INTO departments VALUES (3, 'Finance', 'Chicago', 102);
INSERT INTO departments VALUES (4, 'Marketing', 'Boston', 103);
INSERT INTO departments VALUES (5, 'Sales', 'Los Angeles', 104);

-- Insert employee data
INSERT INTO employees VALUES (1, 'John Smith', 2, 75000, 35, 'Male', '2015-06-15');
INSERT INTO employees VALUES (2, 'Mary Johnson', 1, 65000, 42, 'Female', '2010-03-22');
INSERT INTO employees VALUES (3, 'James Brown', 3, 55000, 28, 'Male', '2018-11-10');
INSERT INTO employees VALUES (4, 'Patricia Davis', 2, 72000, 31, 'Female', '2016-08-05');
INSERT INTO employees VALUES (5, 'Robert Miller', 4, 60000, 45, 'Male', '2012-01-30');
INSERT INTO employees VALUES (6, 'Linda Wilson', 5, 67000, 33, 'Female', '2014-12-12');
INSERT INTO employees VALUES (7, 'Michael Moore', 2, 80000, 38, 'Male', '2013-05-17');
INSERT INTO employees VALUES (8, 'Elizabeth Taylor', 1, 59000, 27, 'Female', '2019-03-08');
INSERT INTO employees VALUES (9, 'William Anderson', 3, 62000, 40, 'Male', '2011-07-22');
INSERT INTO employees VALUES (10, 'Jennifer Thomas', 5, 71000, 36, 'Female', '2017-09-14');

-- Insert category data
INSERT INTO category_header VALUES (1, 'Standard', 40);
INSERT INTO category_header VALUES (2, 'Deluxe', 35);
INSERT INTO category_header VALUES (3, 'Super Deluxe', 30);
INSERT INTO category_header VALUES (4, 'Sleeper', 25);

-- Insert place data
INSERT INTO place_header VALUES (1, 'Madras', 'Chennai Central', 'CMBT');
INSERT INTO place_header VALUES (2, 'Bangalore', 'Bangalore City', 'Majestic');
INSERT INTO place_header VALUES (3, 'Cochin', 'Ernakulam', 'KSRTC');
INSERT INTO place_header VALUES (4, 'Hyderabad', 'Hyderabad City', 'MGBS');
INSERT INTO place_header VALUES (5, 'Mumbai', 'Mumbai Central', 'Dadar');
INSERT INTO place_header VALUES (6, 'Delhi', 'New Delhi', 'ISBT');
INSERT INTO place_header VALUES (7, 'Kolkata', 'Howrah', 'Esplanade');
INSERT INTO place_header VALUES (8, 'Ahmedabad', 'City Center', 'Central');
INSERT INTO place_header VALUES (9, 'Pune', 'Shivaji Nagar', 'Swargate');
INSERT INTO place_header VALUES (10, 'Coimbatore', 'Gandhipuram', 'Ukkadam');

-- Insert route data
INSERT INTO route_header VALUES (101, 1001, 1, 'Madras', 'Bangalore', 350, 500);
INSERT INTO route_header VALUES (102, 1002, 2, 'Madras', 'Cochin', 700, 900);
INSERT INTO route_header VALUES (103, 1003, 3, 'Bangalore', 'Hyderabad', 570, 800);
INSERT INTO route_header VALUES (104, 1004, 4, 'Cochin', 'Mumbai', 1200, 1800);
INSERT INTO route_header VALUES (105, 1005, 1, 'Madras', 'Coimbatore', 500, 600);
INSERT INTO route_header VALUES (106, 1006, 2, 'Bangalore', 'Pune', 840, 1100);
INSERT INTO route_header VALUES (107, 1007, 3, 'Hyderabad', 'Delhi', 1500, 2200);

-- Insert route detail data
INSERT INTO route_detail VALUES (101, 1, 'S');
INSERT INTO route_detail VALUES (101, 2, 'N');
INSERT INTO route_detail VALUES (102, 1, 'S');
INSERT INTO route_detail VALUES (102, 3, 'N');
INSERT INTO route_detail VALUES (103, 2, 'S');
INSERT INTO route_detail VALUES (103, 4, 'N');
INSERT INTO route_detail VALUES (104, 3, 'S');
INSERT INTO route_detail VALUES (104, 5, 'N');
INSERT INTO route_detail VALUES (105, 1, 'S');
INSERT INTO route_detail VALUES (105, 10, 'N');

-- Insert fleet data
INSERT INTO fleet_header VALUES (201, 'Volvo AC', 45);
INSERT INTO fleet_header VALUES (202, 'Mercedes AC', 40);
INSERT INTO fleet_header VALUES (203, 'Ashok Leyland', 55);
INSERT INTO fleet_header VALUES (204, 'Tata Sleeper', 30);
INSERT INTO fleet_header VALUES (205, 'Volvo Sleeper', 28);

-- Insert ticket header data
INSERT INTO ticket_header VALUES (301, 201, 101, '2023-06-15', 2, 1);
INSERT INTO ticket_header VALUES (302, 202, 102, '2023-06-16', 1, 0);
INSERT INTO ticket_header VALUES (303, 203, 103, '2023-06-17', 2, 2);
INSERT INTO ticket_header VALUES (304, 204, 104, '2023-06-18', 1, 1);
INSERT INTO ticket_header VALUES (305, 205, 105, '2023-06-19', 3, 0);

-- Insert ticket detail data
INSERT INTO ticket_detail VALUES (301, 'Rajesh Kumar', 35, 'Male', 500);
INSERT INTO ticket_detail VALUES (301, 'Sunita Kumar', 32, 'Female', 500);
INSERT INTO ticket_detail VALUES (301, 'Rahul Kumar', 10, 'Male', 250);
INSERT INTO ticket_detail VALUES (302, 'Anil Sharma', 45, 'Male', 900);
INSERT INTO ticket_detail VALUES (303, 'Vijay Singh', 40, 'Male', 800);
INSERT INTO ticket_detail VALUES (303, 'Meena Singh', 38, 'Female', 800);
INSERT INTO ticket_detail VALUES (303, 'Ravi Singh', 12, 'Male', 400);
INSERT INTO ticket_detail VALUES (303, 'Sita Singh', 8, 'Female', 400);
INSERT INTO ticket_detail VALUES (304, 'Deepak Patel', 50, 'Male', 1800);
INSERT INTO ticket_detail VALUES (304, 'Anjali Patel', 14, 'Female', 900);
INSERT INTO ticket_detail VALUES (305, 'Ramesh Iyer', 55, 'Male', 600);
INSERT INTO ticket_detail VALUES (305, 'Sudha Iyer', 52, 'Female', 600);
INSERT INTO ticket_detail VALUES (305, 'Kiran Iyer', 28, 'Male', 600);

-- Insert salesman data
INSERT INTO salesman VALUES (5001, 'James Hoog', 'New York', 0.15, 'Food');
INSERT INTO salesman VALUES (5002, 'Nail Knite', 'Paris', 0.13, 'Electronics');
INSERT INTO salesman VALUES (5003, 'Larison', 'London', 0.12, 'Beverages');
INSERT INTO salesman VALUES (5004, 'Pit Alex', 'London', 0.11, 'Tickets');
INSERT INTO salesman VALUES (5005, 'McLyon', 'Paris', 0.14, 'Electronics');
INSERT INTO salesman VALUES (5006, 'Paul Adam', 'Rome', 0.13, 'Food');
INSERT INTO salesman VALUES (5007, 'John Smith', 'San Jose', 0.12, 'Tickets');

-- Verify data insertion
SELECT COUNT(*) FROM departments;
SELECT COUNT(*) FROM employees;
SELECT COUNT(*) FROM category_header;
SELECT COUNT(*) FROM place_header;
SELECT COUNT(*) FROM route_header;
```

## 3. Basic Query Tests

```sql
-- Describe table structure
DESCRIBE employees;

-- Basic SELECT statements
SELECT * FROM departments;
SELECT * FROM employees;
SELECT * FROM category_header;
SELECT * FROM route_header;

-- Filtered queries
SELECT * FROM employees WHERE salary > 70000;
SELECT * FROM route_header WHERE origin = 'Madras';
SELECT * FROM ticket_detail WHERE gender = 'Female';

-- Sorted queries
SELECT * FROM employees ORDER BY salary DESC;
SELECT * FROM route_header ORDER BY distance;

-- Filtered and sorted
SELECT * FROM employees WHERE dept_id = 2 ORDER BY salary DESC;
```

## 4. JOIN Tests

```sql
-- INNER JOIN
SELECT e.emp_name, d.dept_name 
FROM employees e 
INNER JOIN departments d ON e.dept_id = d.dept_id;



-- Join with filtering
SELECT e.emp_name, d.dept_name 
FROM employees e 
INNER JOIN departments d ON e.dept_id = d.dept_id
WHERE e.salary > 65000;
```

## 5. Aggregation Tests

```sql
-- Basic aggregations
SELECT COUNT(*) FROM employees;
SELECT AVG(salary) FROM employees;
SELECT MIN(salary), MAX(salary) FROM employees;
SELECT SUM(salary) FROM employees;

-- Grouped aggregations
SELECT dept_id, COUNT(*) as employee_count, AVG(salary) as avg_salary
FROM employees
GROUP BY dept_id;

-- Filtered aggregations
SELECT gender, COUNT(*) as count, AVG(salary) as avg_salary
FROM employees
WHERE age > 30
GROUP BY gender;

-- Having clause
SELECT dept_id, AVG(salary) as avg_salary
FROM employees
GROUP BY dept_id
HAVING AVG(salary) > 65000;
```

## 6. Subquery Tests

```sql
-- Scalar subquery
SELECT emp_name, salary FROM employees 
WHERE salary > (SELECT AVG(salary) FROM employees);





```

## 7. Transaction Tests

```sql
-- Start transaction
BEGIN TRANSACTION;

-- Make changes
UPDATE employees SET salary = salary * 1.1 WHERE dept_id = 2;

-- Verify changes
SELECT * FROM employees WHERE dept_id = 2;

-- Rollback
ROLLBACK;

-- Verify rollback
SELECT * FROM employees WHERE dept_id = 2;

-- Start new transaction
BEGIN TRANSACTION;

-- Make changes
UPDATE employees SET salary = salary * 1.1 WHERE dept_id = 2;

-- Commit changes
COMMIT;

-- Verify commit
SELECT * FROM employees WHERE dept_id = 2;
```

## 8. Schema Modification Tests

```sql

-- Test constraint
-- This should fail due to name uniqueness constraint
INSERT INTO salesman VALUES (5008, 'James Hoog', 'Chicago', 0.16, 'Beverages');

-- Drop constraint (if your system supports it)
ALTER TABLE salesman DROP CONSTRAINT uq_salesman_name;

-- Rename column (if your system supports it)
ALTER TABLE employees RENAME COLUMN email TO email_address;

-- Drop column (if your system supports it)
ALTER TABLE employees DROP COLUMN email_address;
```

## 9. View Tests

```sql
-- Create simple view
CREATE VIEW emp_dept_view AS
SELECT e.emp_id, e.emp_name, e.salary, d.dept_name
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id;

-- Query view
SELECT * FROM emp_dept_view;

-- Create filtered view
CREATE VIEW high_salary_view AS
SELECT * FROM employees WHERE salary > 70000;

-- Query filtered view
SELECT * FROM high_salary_view;

-- Create complex view
CREATE VIEW department_stats AS
SELECT d.dept_id, d.dept_name, COUNT(e.emp_id) as emp_count, 
       AVG(e.salary) as avg_salary, MAX(e.salary) as max_salary
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_id, d.dept_name;

-- Query complex view
SELECT * FROM department_stats;

-- Drop view
DROP VIEW high_salary_view;

-- Create new view
CREATE VIEW high_salary_view AS
SELECT emp_id, emp_name, salary, dept_id 
FROM employees 
WHERE salary > 65000
ORDER BY salary DESC;

-- Query recreated view
SELECT * FROM high_salary_view;
```

## 10. Index Tests

```sql
-- Create indexes
CREATE INDEX idx_emp_dept ON employees(dept_id);
CREATE INDEX idx_route_origin ON route_header(origin);

-- Show indexes
SHOW INDEXES;

-- Test query with index (should be faster)
SELECT * FROM employees WHERE dept_id = 2;
SELECT * FROM route_header WHERE origin = 'Madras';

-- Drop index
DROP INDEX idx_emp_dept;

-- Show indexes after drop
SHOW INDEXES;
```

## 11. User and Privilege Tests

```sql
-- Create user
CREATE USER test_user PASSWORD 'password123';

-- Grant privileges
GRANT SELECT ON employees TO test_user;
GRANT INSERT ON departments TO test_user;

-- Show user privileges
SHOW GRANTS FOR test_user;

-- Revoke privilege
REVOKE INSERT ON departments FROM test_user;

-- Show privileges after revoke
SHOW GRANTS FOR test_user;
```

## 12. Advanced Query Tests

```sql
-- CASE expression
SELECT emp_name, salary,
  CASE 
    WHEN salary > 75000 THEN 'High'
    WHEN salary > 60000 THEN 'Medium'
    ELSE 'Low'
  END as salary_category
FROM employees;

-- String functions
SELECT UPPER(emp_name) as upper_name, 
       LOWER(emp_name) as lower_name,
       LENGTH(emp_name) as name_length
FROM employees;

-- Date functions (if supported)
SELECT emp_name, hire_date,
       YEAR(hire_date) as hire_year,
       MONTH(hire_date) as hire_month
FROM employees;

-- Mathematical operations
SELECT emp_name, salary, 
       salary * 1.1 as increased_salary,
       salary / 12 as monthly_salary
FROM employees;

-- LIKE operator
SELECT * FROM employees WHERE emp_name LIKE 'J%';
SELECT * FROM route_header WHERE destination LIKE '%i%';

-- IN operator
SELECT * FROM employees WHERE dept_id IN (1, 3, 5);

-- BETWEEN operator
SELECT * FROM employees WHERE salary BETWEEN 60000 AND 75000;

-- NULL handling
SELECT * FROM employees WHERE email IS NULL;
```

## 13. Error Testing

```sql
-- Test primary key constraint violation
INSERT INTO departments VALUES (1, 'Duplicate', 'Error', 999);

-- Test foreign key constraint violation
INSERT INTO employees VALUES (100, 'Invalid', 999, 50000, 30, 'Male', '2020-01-01');

-- Test NOT NULL constraint violation
INSERT INTO departments VALUES (100, NULL, 'Error', 999);

-- Test data type validation
INSERT INTO employees VALUES (101, 'Invalid', 1, 'not-a-number', 30, 'Male', '2020-01-01');

-- Test syntax error
SLECT * FROM employees;
```

## 14. Performance Tests

```sql
-- Create a table with more rows for performance testing
CREATE TABLE performance_test (
    id INT NOT NULL,
    value VARCHAR(100),
    number FLOAT,
    CONSTRAINT pk_perf PRIMARY KEY (id)
);

-- Insert a large number of rows (adapt number based on system capabilities)
-- This would typically be done with a loop in a script
INSERT INTO performance_test VALUES (1, 'Test Value 1', 1.1);
INSERT INTO performance_test VALUES (2, 'Test Value 2', 2.2);
-- ... continue for desired number of rows

-- Run a query that should use index
SELECT * FROM performance_test WHERE id = 500;

-- Run a full table scan
SELECT * FROM performance_test WHERE value LIKE '%Value%';

-- Run a query with sorting
SELECT * FROM performance_test ORDER BY number DESC;

-- Run an aggregate query
SELECT AVG(number) FROM performance_test;
```

## 15. Cleanup

```sql
-- Drop all tables in reverse dependency order
DROP TABLE ticket_detail;
DROP TABLE ticket_header;
DROP TABLE route_detail;
DROP TABLE fleet_header;
DROP TABLE route_header;
DROP TABLE place_header;
DROP TABLE category_header;
DROP TABLE employees;
DROP TABLE departments;
DROP TABLE salesman;
DROP TABLE performance_test;

-- Drop all views
DROP VIEW emp_dept_view;
DROP VIEW high_salary_view;
DROP VIEW department_stats;

-- Drop all users
DROP USER test_user;

-- Verify cleanup
SHOW TABLES;
SHOW VIEWS;
```

## Expected Results

For each test section, verify that:

1. **Schema Creation**: All tables are created without errors
2. **Data Insertion**: All rows are inserted successfully
3. **Basic Queries**: Results match the expected output based on inserted data
4. **JOINs**: Join operations correctly combine related data
5. **Aggregations**: Mathematical functions produce correct results
6. **Subqueries**: Nested queries return expected results
7. **Transactions**: Changes are only persisted on COMMIT, and ROLLBACK works correctly
8. **Schema Modification**: Structure changes are applied correctly
9. **Views**: Views are created and query results match expectations
10. **Indexes**: Indexes are created and improve query performance
11. **User Privileges**: Access controls work as expected
12. **Advanced Queries**: Complex query operations produce correct results
13. **Error Testing**: Constraint violations and syntax errors are caught properly
14. **Performance**: System handles larger datasets efficiently
15. **Cleanup**: All objects are successfully removed

This comprehensive test script covers all major aspects of the database system functionality.