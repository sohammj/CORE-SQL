#!/bin/bash

# Code Cleanup Instructions for C++ SQL Database Project

# This script provides commands to remove debug statements from your codebase
# Run these commands in your project directory

echo "Removing debug output statements from code files..."

# 1. Remove Debug cout statements
find . -name "*.cpp" -type f -exec sed -i 's/std::cout << "Debug: .*$//g' {} \;
find . -name "*.cpp" -type f -exec sed -i 's/std::cout << "FKValidator: .*$//g' {} \;
find . -name "*.cpp" -type f -exec sed -i 's/std::cout << "Simple FK Validation: .*$//g' {} \;
find . -name "*.cpp" -type f -exec sed -i 's/std::cout << std::flush;$//g' {} \;

# 2. Remove std::flush calls 
find . -name "*.cpp" -type f -exec sed -i 's/std::cout << std::flush;//g' {} \;

# 3. Remove empty lines created by removing debug statements
find . -name "*.cpp" -type f -exec sed -i '/^\s*$/d' {} \;

# 4. Replace detailed error messages with simpler ones
find . -name "*.cpp" -type f -exec sed -i 's/std::cerr << "Error during.*$/std::cerr << "Error: " << e.what() << std::endl;/g' {} \;

# 5. Clean up Database.cpp specifically
sed -i 's/std::cout << "Inserting row into.*$/std::cout << "Inserting data..." << std::endl;/g' Database.cpp
sed -i 's/std::cout << "Debug: Row added successfully.*$/std::cout << "Row successfully inserted." << std::endl;/g' Database.cpp
sed -i 's/std::cout << "Debug: Finished processing all rows.*$//g' Database.cpp

# 6. Clean up Table.cpp
sed -i 's/Debug: addRow - Starting with.*$//g' Table.cpp
sed -i 's/Debug: addRow - Acquired mutex lock.*$//g' Table.cpp
sed -i 's/Debug: addRow - Enforcing data types.*$//g' Table.cpp
sed -i 's/Debug: addRow - Validating NOT NULL constraints.*$//g' Table.cpp
sed -i 's/Debug: addRow - Validating UNIQUE\/PK constraint:.*$//g' Table.cpp
sed -i 's/Debug: addRow - Validating FK constraint:.*$//g' Table.cpp
sed -i 's/Debug: addRow - All constraints passed, adding row.*$//g' Table.cpp
sed -i 's/Starting FK validation for.*$//g' Table.cpp

# 7. Clean up ForeignKeyValidator.cpp
sed -i 's/std::cout << "FKValidator: Starting validation for.*$//g' ForeignKeyValidator.cpp
sed -i 's/std::cout << "FKValidator: Acquired mutex lock.*$//g' ForeignKeyValidator.cpp
sed -i 's/std::cout << "FKValidator: Released mutex lock.*$//g' ForeignKeyValidator.cpp
sed -i 's/std::cout << "FKValidator: Looking for column.*$//g' ForeignKeyValidator.cpp
sed -i 's/std::cout << "FKValidator: Added FK value:.*$//g' ForeignKeyValidator.cpp
sed -i 's/std::cout << "FKValidator: Trying direct value check.*$//g' ForeignKeyValidator.cpp
sed -i 's/std::cout << "FKValidator: Match found via direct check.*$//g' ForeignKeyValidator.cpp
sed -i 's/std::cout << "FKValidator: No match found.*$//g' ForeignKeyValidator.cpp
sed -i 's/std::cout << "FKValidator: Checking.*$//g' ForeignKeyValidator.cpp
sed -i 's/std::cout << "FKValidator: Getting all rows.*$//g' ForeignKeyValidator.cpp

# 8. Clean up Transaction.cpp
sed -i 's/std::cout << "Debug: beginTransaction called.*$//g' Transaction.cpp
sed -i 's/std::cout << "Debug: commitTransaction called.*$//g' Transaction.cpp
sed -i 's/std::cout << "Debug: rollbackTransaction called.*$//g' Transaction.cpp

# 9. Remove all other debug output statements that might have been missed
find . -name "*.cpp" -type f -exec sed -i 's/^.*Debug:.*$//g' {} \;

# 10. Clean up any remaining FKValidator output 
find . -name "*.cpp" -type f -exec sed -i 's/^.*FKValidator:.*$//g' {} \;

echo "Code cleanup complete. Review files for any remaining debug statements."
echo "Remember to remove any other debug code that might not be caught by these patterns."

# Note: This script uses sed with the -i flag for in-place editing
# Make a backup of your code before running these commands
