from classes import StorageEngine

# Initialize the StorageEngine
storage_engine = StorageEngine()

# Mock Data Setup (no real values, just block and offset metadata)
storage_engine.blocks = {
    "database_name_1": {
        "table_name_1": {
            "columns": [
                {"name": "column_1", "type": "int"},
                {"name": "column_2", "type": "str"}
            ],
            "values": [
                [{"column_1": 1}, {"column_1": 2}],  # Block 1
                [{"column_1": 3}, {"column_1": 4}],  # Block 2
                [{"column_1": 5}, {"column_1": 6}],  # Block 3
                [{"column_1": 7}, {"column_1": 8}],  # Block 4
                [{"column_1": 9}, {"column_1": 10}],  # Block 5
            ]
        }
    }
}

# Mock Buffer for Transaction
transaction_id = 1
storage_engine.buffer = {
    transaction_id: storage_engine.blocks.copy()
}
storage_engine.buffer_index = {
    transaction_id: {
        "database_name_1": {
            "table_name_1": {
                "column_1": {
                    "bplus": None  # Placeholder for BPlus tree
                }
            }
        }
    }
}

# Create a BPlus index on 'column_1'
storage_engine.set_index("database_name_1", "table_name_1", "column_1", transaction_id, index_type="bplus")

# --- Testing Begins ---

# 1. Initial Search Tests
print("\n--- INITIAL SEARCH TESTS ---")
print(storage_engine.search_bplus_index("database_name_1", "table_name_1", "column_1", 5, transaction_id))  # Expected: (Block 3, Offset 0)
print(storage_engine.search_bplus_index("database_name_1", "table_name_1", "column_1", 10, transaction_id))  # Expected: (Block 5, Offset 1)

# 2. Range Search Tests
print("\n--- RANGE SEARCH TESTS ---")
print(storage_engine.search_bplus_index_range("database_name_1", "table_name_1", "column_1", transaction_id, 4, 8))  # Expected: [(Block 2, Offset 1), ..., (Block 4, Offset 1)]
print(storage_engine.search_bplus_index_range("database_name_1", "table_name_1", "column_1", transaction_id, 1, 10))  # Expected: All block/offset pairs

# 3. Insert New Key
print("\n--- INSERT TEST ---")
# Insert key 11 at a hypothetical Block 6, Offset 0
storage_engine.insert_bplus_index("database_name_1", "table_name_1", "column_1", 11, 6, 0, transaction_id)
print(storage_engine.search_bplus_index("database_name_1", "table_name_1", "column_1", 11, transaction_id))  # Expected: (Block 6, Offset 0)

# 4. Delete Key
print("\n--- DELETE TEST ---")
# Delete key 5
storage_engine.delete_bplus_index("database_name_1", "table_name_1", "column_1", 5, transaction_id)
print(storage_engine.search_bplus_index("database_name_1", "table_name_1", "column_1", 5, transaction_id))  # Expected: None (key 5 deleted)

# 5. Update Key
print("\n--- UPDATE TEST ---")
# Move key 11 to Block 6, Offset 1
storage_engine.update_bplus_index("database_name_1", "table_name_1", "column_1", 11, 6, 1, transaction_id)
print(storage_engine.search_bplus_index("database_name_1", "table_name_1", "column_1", 11, transaction_id))  # Expected: (Block 6, Offset 1)

# 6. Test Invalid Key Deletion
print("\n--- DELETE NON-EXISTENT KEY TEST ---")
# Attempt to delete key 99 (non-existent)
storage_engine.delete_bplus_index("database_name_1", "table_name_1", "column_1", 99, transaction_id)
print(storage_engine.search_bplus_index("database_name_1", "table_name_1", "column_1", 99, transaction_id))  # Expected: None (no key 99 exists)

# 7. Test Final Range Search
print("\n--- FINAL RANGE SEARCH TEST ---")
# Search keys from 1 to 11
print(storage_engine.search_bplus_index_range("database_name_1", "table_name_1", "column_1", transaction_id, 1, 11))  # Expected: Keys 1-11 (except 5)

# --- End of Testing ---
