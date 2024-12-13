import os
import pickle
import unittest
from unittest.mock import patch, MagicMock
import sys
import os


from StorageManager.classes import StorageEngine, DataRetrieval, DataWrite, DataDeletion, Condition
from StorageManager.Bplus import BPlusTree
from StorageManager.Hash import HashTable

class TestStorageEngine(unittest.TestCase):
    def setUp(self):
        # Ensure no leftover files
        if os.path.exists("data.dat"):
            os.remove("data.dat")
        if os.path.exists("indexes.dat"):
            os.remove("indexes.dat")

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
        # Use a condition to retrieve only a certain row
        condition = Condition("id", "=", 2)
        data_retrieval = DataRetrieval(["test_table"], ["id", "name"], [condition])
        result = self.storage.read_block(data_retrieval, "test_db", -1)
        self.assertEqual(result.rows_count, 1, "Should return exactly one row.")
        self.assertEqual(result.data[0]["id"], 2, "Should return the row with id=2.")

    def test_update_data(self):
        # Update the row where id=3 to have name="Dave"
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

    def test_create_bplus_index(self):
        self.storage.initialize_index_structure("test_db", "test_table", "id")
        self.storage.set_index("test_db", "test_table", "id", self.transaction_id, "bplus")
        self.storage.commit_buffer(self.transaction_id)

        # Now test searching via B+Tree index if implemented
        indices = self.storage.search_bplus_index("test_db", "test_table", "id", self.transaction_id)
        # No specific key given yet (method above expects a key), let's try with a key:
        result = self.storage.search_bplus_index("test_db", "test_table", "id", self.transaction_id)
        # It's a bit unclear how search_bplus_index is invoked without a key, let's do a key-based test:
        res_for_key_1 = self.storage.search_bplus_index("test_db", "test_table", "id", self.transaction_id)
        # The method requires a key. Let's modify the code to:
        res_for_key_1 = self.storage.search_bplus_index("test_db", "test_table", "id", 1)
        self.assertIsNotNone(res_for_key_1, "Result for key=1 should not be None.")
        # The result should be something like (block_index, offset), just check type:
        self.assertTrue(isinstance(res_for_key_1, list), "Search result should be a list.")

    def test_create_hash_index(self):
        self.storage.initialize_index_structure("test_db", "test_table", "name")
        self.storage.set_index("test_db", "test_table", "name", self.transaction_id, "hash")
        self.storage.commit_buffer(self.transaction_id)

        # Test searching via hash index
        result_indices = self.storage.search_hash_index("test_db", "test_table", "name", "Alice", self.transaction_id)
        self.assertIsNotNone(result_indices, "Hash index search should return something for 'Alice'.")
        self.assertTrue(len(result_indices) > 0, "There should be at least one entry for 'Alice'.")

class TestBPlusTree(unittest.TestCase):
    def test_insert_and_search(self):
        tree = BPlusTree(order=4)
        values = [(10, "A"), (20, "B"), (5, "C")]
        for k, v in values:
            tree.insert(k, v)

        # Test search
        self.assertEqual(tree.search(10), "A", "Should find 'A' for key=10.")
        self.assertEqual(tree.search(20), "B", "Should find 'B' for key=20.")
        self.assertEqual(tree.search(5), "C", "Should find 'C' for key=5.")
        self.assertIsNone(tree.search(999), "Should return None for non-existent key.")

    def test_range_search(self):
        tree = BPlusTree(order=4)
        for key in [1,2,3,4,5,6,7]:
            tree.insert(key, key)
        result = tree.search_range(3,6)
        self.assertEqual(set(result), {3,4,5,6}, "Range search from 3 to 6 should return keys 3,4,5,6.")

class TestHashTable(unittest.TestCase):
    def test_insert_search_delete(self):
        hash_table = HashTable(size=10)
        hash_table.insert("Alice", (0,0))
        self.assertIn((0,0), hash_table.search("Alice"), "Should find (0,0) for 'Alice'.")
        hash_table.delete("Alice", (0,0))
        self.assertIsNone(hash_table.search("Alice"), "Should return None after deletion of 'Alice'.")

if __name__ == '__main__':
    unittest.main()
