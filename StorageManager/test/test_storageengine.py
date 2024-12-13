import os
import unittest
from StorageManager.classes import StorageEngine, DataRetrieval, DataWrite, DataDeletion, Condition

class TestStorageEngine(unittest.TestCase):
    def setUp(self):
        # # Ensure no leftover files
        # if os.path.exists("data.dat"):
        #     os.remove("data.dat")
        # if os.path.exists("indexes.dat"):
        #     os.remove("indexes.dat")

        self.storage = StorageEngine()
        
        # Create a test database and table
        self.storage.create_database("test_db")
        self.storage.create_table("test_db", "test_table", {
            "id": "INTEGER",
            "name": "VARCHAR(20)"
        },{
            "id": ["PRIMARY KEY"], 
            "name": []
        })

        # Insert some test data
        self.transaction_id = 1
        self.storage.insert_data("test_db", "test_table", {"id": 1, "name": "Alice"}, self.transaction_id)
        self.storage.insert_data("test_db", "test_table", {"id": 2, "name": "Bob"}, self.transaction_id)
        self.storage.insert_data("test_db", "test_table", {"id": 3, "name": "Charlie"}, self.transaction_id)
        self.storage.commit_buffer(self.transaction_id)

    def test_database_and_table_existence(self):
        databases = self.storage.get_database_names()
        self.assertIn("test_db", databases, "Database 'test_db' should exist.")

        tables = self.storage.get_tables_of_database("test_db")
        self.assertIn("test_table", tables, "Table 'test_table' should exist in 'test_db'.")

    def test_get_columns_of_table(self):
        columns = self.storage.get_columns_of_table("test_db", "test_table")
        self.assertIn("id", columns, "Column 'id' should exist in 'test_table'.")
        self.assertIn("name", columns, "Column 'name' should exist in 'test_table'.")

    def test_select_data(self):
        # Retrieve all data
        data_retrieval = DataRetrieval(["test_table"], ["id", "name"], [])
        result = self.storage.read_block(data_retrieval, "test_db", -1)
        self.assertEqual(result.rows_count, 3, "Should have 3 rows inserted.")
        rows = result.data
        self.assertIn({"id": 1, "name": "Alice"}, rows)
        self.assertIn({"id": 2, "name": "Bob"}, rows)
        self.assertIn({"id": 3, "name": "Charlie"}, rows)

    def test_conditions_selection(self):
        condition = Condition("id", "=", 2)
        data_retrieval = DataRetrieval(["test_table"], ["id", "name"], [condition])
        result = self.storage.read_block(data_retrieval, "test_db", -1)
        self.assertEqual(result.rows_count, 1, "Should return exactly one row.")
        self.assertEqual(result.data[0]["id"], 2, "Should return the row with id=2.")

    def test_update_data(self):
        condition = Condition("id", "=", 3)
        data_write = DataWrite(["test_table"], ["name"], [condition], ["Dave"])
        self.storage.write_block(data_write, "test_db", self.transaction_id)
        self.storage.commit_buffer(self.transaction_id)

        # Check the updated value
        condition = Condition("id", "=", 3)
        data_retrieval = DataRetrieval(["test_table"], ["id", "name"], [condition])
        result = self.storage.read_block(data_retrieval, "test_db", -1)
        self.assertEqual(result.rows_count, 1, "Should still have one row with id=3.")
        self.assertEqual(result.data[0]["name"], "Dave", "Name should be updated to 'Dave'.")

    def test_delete_data(self):
        # Delete the row where id=2
        condition = Condition("id", "=", 2)
        data_deletion = DataDeletion("test_table", [condition])
        self.storage.delete_block(data_deletion, "test_db", self.transaction_id)
        self.storage.commit_buffer(self.transaction_id)

        # Check that the row with id=2 is gone
        data_retrieval = DataRetrieval(["test_table"], ["id", "name"], [])
        result = self.storage.read_block(data_retrieval, "test_db", -1)
        self.assertEqual(result.rows_count, 2, "After deletion, should have 2 rows.")
        for row in result.data:
            self.assertNotEqual(row["id"], 2, "Row with id=2 should be deleted.")

if __name__ == '__main__':
    unittest.main()
