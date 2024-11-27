from classes import StorageEngine

def create_database(storage):
    print("Creating database 'test_db'...")
    storage.create_database("test_db")

def create_table(storage):
    column_type = {"id": "INTEGER", "name": "TEXT", "age": "INTEGER"}
    print("Creating table 'users' with columns: id, name, age...")
    storage.create_table("test_db", "users", column_type)

def insert_data(storage):
    print("Inserting sample data into 'users' table...")
    storage.insert_data("test_db", "users", {"id": 1, "name": "John Doe", "age": 30})
    storage.insert_data("test_db", "users", {"id": 2, "name": "Jane Doe", "age": 25})
    storage.insert_data("test_db", "users", {"id": 3, "name": "Alice", "age": 25})
    storage.insert_data("test_db", "users", {"id": 4, "name": "Bob", "age": 22})
    storage.insert_data("test_db", "users", {"id": 5, "name": "Charlie", "age": 35})

def create_index(storage):
    print("Creating index on 'id' column...")
    storage.set_index("test_db", "users", "id")
    print("Creating index on 'age' column...")
    storage.set_index("test_db", "users", "age")

def search_with_primary_index(storage):
    print("Testing search with primary index on 'id'...")
    result = storage.search_with_index("test_db", "users", "id", 3)
    print("Search result (primary index on 'id' for key 3):", result)

def search_with_secondary_index(storage):
    print("Testing search with secondary index on 'age'...")
    result = storage.search_with_index("test_db", "users", "age", 25)
    print("Search result (secondary index on 'age' for key 25):", result)

def search_range_with_primary_index(storage):
    print("Testing search range with primary index on 'id' between 2 and 4...")
    result = storage.search_range_with_index("test_db", "users", "id", 2, 4)
    print("Search result (primary index on 'id' between 2 and 4):", result)

def search_range_with_secondary_index(storage):
    print("Testing search range with secondary index on 'age' between 25 and 30...")
    result = storage.search_range_with_index("test_db", "users", "age", 25, 30)
    print("Search result (secondary index on 'age' between 25 and 30):", result)

def search_on_non_indexed_column(storage):
    print("Testing search on a column without an index...")
    try:
        result = storage.search_with_index("test_db", "users", "name", "John Doe")
        print("Search result (on non-indexed 'name' column):", result)
    except ValueError as e:
        print("Error:", e)

def invalid_range_search(storage):
    print("Testing invalid range search (no matching values)...")
    result = storage.search_range_with_index("test_db", "users", "age", 40, 50)
    print("Search result (range on 'age' between 40 and 50):", result)

def test_storage_engine():
    storage = StorageEngine()

    create_database(storage)
    print()
    print()
    create_table(storage)
    print()
    print()
    insert_data(storage)
    print()
    print()
    create_index(storage)
    print()
    print()
    search_with_primary_index(storage)
    print()
    print()
    search_with_secondary_index(storage)
    print()
    print()
    search_range_with_primary_index(storage)
    print()
    print()
    search_range_with_secondary_index(storage)
    print()
    print()
    search_on_non_indexed_column(storage)
    print()
    print()
    invalid_range_search(storage)
    print()
    print()

test_storage_engine()
