from StorageManager.classes import StorageEngine

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
                    "hash": None  # Placeholder for BPlus tree
                }
            }
        }
    }
}

# Create a BPlus index on 'column_1'
storage_engine.set_index("database_name_1", "table_name_1", "column_1", transaction_id, index_type="hash")

# --- Testing Begins ---

# 1. Initial Search Tests
print("\n--- INITIAL SEARCH TESTS ---")
print(storage_engine.search_hash_index("database_name_1", "table_name_1", "column_1", 5, transaction_id))  # Expected: (Block 3, Offset 0)
print(storage_engine.search_hash_index("database_name_1", "table_name_1", "column_1", 10, transaction_id))  # Expected: (Block 5, Offset 1)

# 3. Insert New Key
print("\n--- INSERT TEST ---")
# Insert key 11 at a hypothetical Block 6, Offset 0
storage_engine.insert_hash_index("database_name_1", "table_name_1", "column_1", 11, 6, 0, transaction_id)
print(storage_engine.search_hash_index("database_name_1", "table_name_1", "column_1", 11, transaction_id))  # Expected: (Block 6, Offset 0)

# 4. Delete Key
print("\n--- DELETE TEST ---")
# Delete key 5
storage_engine.delete_hash_index("database_name_1", "table_name_1", "column_1", 5, (2,0),  transaction_id)
print(storage_engine.search_hash_index("database_name_1", "table_name_1", "column_1", 5, transaction_id))  # Expected: None (key 5 deleted)

# 5. Update Key
print("\n--- UPDATE TEST ---")
# Move key 11 to Block 6, Offset 1
storage_engine.update_key_hash_index("database_name_1", "table_name_1", "column_1", 11, 6, 1, transaction_id)
print(storage_engine.search_hash_index("database_name_1", "table_name_1", "column_1", 11, transaction_id))  # Expected: (Block 6, Offset 1)

# --- End of Testing ---
print("================== End of Testing. ================")
