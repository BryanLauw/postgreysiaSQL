import unittest
from QueryParser import QueryParser

class TestQueryParser(unittest.TestCase):
    def setUp(self):
        # Replace with the actual path
        self.parser = QueryParser("dfa.txt")

    def test_check_valid_syntax_valid_query(self):
        query = "SELECT * FROM employees WHERE salary >= 5000;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid SELECT query should pass syntax check")

    def test_check_valid_syntax_invalid_query(self):
        query = "SELECT FROM WHERE employees salary >= 5000;"
        self.assertFalse(self.parser.check_valid_syntax(query), "Invalid SELECT query should fail syntax check")

    def test_check_valid_syntax_with_join(self):
        query = "SELECT * FROM employees JOIN departments ON employees.dept_id = departments.id;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid JOIN query should pass syntax check")

    def test_check_valid_syntax_with_order_by(self):
        query = "SELECT name, age FROM students ORDER BY age DESC;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid ORDER BY query should pass syntax check")

    def test_check_valid_syntax_insert(self):
        query = "INSERT INTO students (id, name, age) VALUES (1, 'John Doe', 20);"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid INSERT query should pass syntax check")

    def test_check_valid_syntax_invalid_insert(self):
        query = "INSERT INTO students id, name, age VALUES (1, 'John Doe', 20);"
        self.assertFalse(self.parser.check_valid_syntax(query), "Invalid INSERT query should fail syntax check")

    def test_check_valid_syntax_create_table(self):
        query = "CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(50), age INT);"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid CREATE TABLE query should pass syntax check")

    def test_check_valid_syntax_drop_table(self):
        query = "DROP TABLE students;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid DROP TABLE query should pass syntax check")

    def test_check_valid_syntax_transaction(self):
        query = "BEGIN TRANSACTION;"
        self.assertTrue(self.parser.check_valid_syntax(query), "BEGIN TRANSACTION should pass syntax check")
        query = "COMMIT;"
        self.assertTrue(self.parser.check_valid_syntax(query), "COMMIT should pass syntax check")

if __name__ == "__main__":
    unittest.main()

# how to run the test:
# python -m unittest UnitTestQueryParser.py
