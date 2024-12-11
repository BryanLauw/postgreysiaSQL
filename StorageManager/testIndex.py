from classes import StorageEngine
#  Expanded Mock Data
storage_engine = StorageEngine()

# Manually setting the expanded self.blocks structure
storage_engine.blocks = {
    "database_name_1": {
        "table_name_1": {
            "columns": [
                {"name": "column_1", "type": "int"},
                {"name": "column_2", "type": "str"}
            ],
            "values": [  # Simulating blocks
                [  # Block 1
                    {"column_1": 1, "column_2": "A"},
                    {"column_1": 2, "column_2": "B"}
                ],
                [  # Block 2
                    {"column_1": 3, "column_2": "C"},
                    {"column_1": 4, "column_2": "D"}
                ],
                [  # Block 3
                    {"column_1": 5, "column_2": "E"},
                    {"column_1": 6, "column_2": "F"}
                ],
                [  # Block 4
                    {"column_1": 7, "column_2": "G"},
                    {"column_1": 8, "column_2": "H"}
                ],
                [  # Block 5
                    {"column_1": 9, "column_2": "I"},
                    {"column_1": 10, "column_2": "J"}
                ]
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
                    "bplus": None
                }
            }
        }
    }
}

# Create a BPlus index on column_1
storage_engine.set_index("database_name_1", "table_name_1", "column_1", transaction_id, index_type="bplus")

# Search for specific keys
search_result_5 = storage_engine.search_bplus_index(
    "database_name_1", "table_name_1", "column_1", 5, transaction_id
)
print(f"Search result for key '5': {search_result_5}")

search_result_10 = storage_engine.search_bplus_index(
    "database_name_1", "table_name_1", "column_1", 10, transaction_id
)
print(f"Search result for key '10': {search_result_10}")

# Perform a range search
range_result_4_8 = storage_engine.search_bplus_index_range(
    "database_name_1", "table_name_1", "column_1", transaction_id, 4, 8
)
print(f"Range search result for keys between '4' and '8': {range_result_4_8}")

# Perform another range search for all rows
range_result_1_10 = storage_engine.search_bplus_index_range(
    "database_name_1", "table_name_1", "column_1", transaction_id, 1, 10
)
print(f"Range search result for keys between '1' and '10': {range_result_1_10}")

