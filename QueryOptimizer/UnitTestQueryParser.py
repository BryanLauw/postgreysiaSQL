import unittest
from QueryParser import QueryParser

class TestQueryParser(unittest.TestCase):
    def setUp(self):
        # Replace with the actual path
        self.parser = QueryParser("dfa.txt")

    # SELECT Test Cases
    def test_valid_select(self):
        query = "SELECT name, age FROM students WHERE age > 20;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid SELECT should pass syntax check")

    def test_valid_select_all(self):
        query = "SELECT * FROM students;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid SELECT with * should pass syntax check")

    # UPDATE Test Cases
    def test_valid_update(self):
        query = "UPDATE employees SET salary = salary * 1.1 WHERE salary > 1000;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid UPDATE should pass syntax check")

    def test_invalid_update(self):
        query = "UPDATE employees salary = salary * 1.1 WHERE salary > 1000;"
        self.assertFalse(self.parser.check_valid_syntax(query), "Invalid UPDATE should fail syntax check")

    # FROM Test Cases
    def test_valid_from_multiple_tables(self):
        query = "SELECT * FROM table1, table2;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid FROM with multiple tables should pass")

    # AS Test Cases
    def test_valid_as_aliasing(self):
        query = "SELECT * FROM students AS s, courses AS c WHERE s.course_id = c.id;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid AS aliasing should pass syntax check")

    # JOIN Test Cases
    def test_valid_join_on(self):
        query = "SELECT * FROM employees JOIN departments ON employees.dept_id = departments.id;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid JOIN ON should pass syntax check")

    def test_valid_natural_join(self):
        query = "SELECT * FROM employees NATURAL JOIN departments;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid NATURAL JOIN should pass syntax check")

    # WHERE Test Cases
    def test_valid_where(self):
        query = "SELECT * FROM products WHERE price >= 100;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid WHERE should pass syntax check")

    # ORDER BY Test Cases
    def test_valid_order_by(self):
        query = "SELECT name FROM employees ORDER BY name DESC;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid ORDER BY should pass syntax check")

    # LIMIT Test Cases
    def test_valid_limit_query(self):
        query = "SELECT * FROM orders LIMIT 5;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid LIMIT should pass syntax check")

    # BEGIN TRANSACTION Test Cases
    def test_valid_begin_transaction(self):
        query = "BEGIN TRANSACTION;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid BEGIN TRANSACTION should pass syntax check")

    # COMMIT Test Cases
    def test_valid_commit(self):
        query = "COMMIT;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid COMMIT should pass syntax check")

    # DELETE Test Cases (BONUS)
    @unittest.skip("bonus")
    def test_valid_delete(self):
        query = "DELETE FROM employees WHERE department = 'RnD';"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid DELETE should pass syntax check")

    @unittest.skip("bonus")
    def test_invalid_delete(self):
        query = "DELETE employees WHERE department = 'RnD';"
        self.assertFalse(self.parser.check_valid_syntax(query), "Invalid DELETE should fail syntax check")

    # INSERT Test Cases (BONUS)
    @unittest.skip("bonus")
    def test_valid_insert(self):
        query = "INSERT INTO students (id, name, age) VALUES (1, 'John Doe', 20);"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid INSERT should pass syntax check")

    @unittest.skip("bonus")
    def test_invalid_insert(self):
        query = "INSERT students VALUES (1, 'John Doe');"
        self.assertFalse(self.parser.check_valid_syntax(query), "Invalid INSERT should fail syntax check")

    # CREATE TABLE Test Cases (BONUS)
    @unittest.skip("bonus")
    def test_valid_create_table(self):
        query = "CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(50), age INT);"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid CREATE TABLE should pass syntax check")

    @unittest.skip("bonus")
    def test_valid_create_table_with_foreign_key(self):
        query = "CREATE TABLE enrollments (id INT, student_id INT, FOREIGN KEY (student_id) REFERENCES students(id));"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid CREATE TABLE with foreign key should pass syntax check")

    # DROP TABLE Test Cases (BONUS)
    @unittest.skip("bonus")
    def test_valid_drop_table(self):
        query = "DROP TABLE students;"
        self.assertTrue(self.parser.check_valid_syntax(query), "Valid DROP TABLE should pass syntax check")

    @unittest.skip("bonus")
    def test_invalid_drop_table(self):
        query = "DROP TABLE;"
        self.assertFalse(self.parser.check_valid_syntax(query), "Invalid DROP TABLE should fail syntax check")

if __name__ == "__main__":
    unittest.main()

# how to run the test:
# python -m unittest -v UnitTestQueryParser.py
