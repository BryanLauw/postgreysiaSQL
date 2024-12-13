import unittest
from StorageManager.Bplus import BPlusTree
from StorageManager.Hash import HashTable

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
