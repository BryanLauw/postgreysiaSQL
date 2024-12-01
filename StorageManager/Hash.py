class HashTable:
    def __init__(self, size):
        self.size = size
        self.table = [[] for _ in range(size)]

    def hash_function(self, key):
        # simple modulo-based hash function
        return key % self.size

    def insert(self, key, value):
        # insert a key-value pair into the hash table
        index = self.hash_function(key)
        for pair in self.table[index]:
            if pair[0] == key:
                pair[1] = value  # update value of the key if the key alreay exists
                return
        self.table[index].append([key, value])  # add new key-value pair if the key don't exists

    def search(self, key):
        # search for a key in the hash table and return its value.
        index = self.hash_function(key)
        for pair in self.table[index]:
            if pair[0] == key:
                return pair[1]
        return None  # key not found

    def delete(self, key):
        # delete a key-value pair from the hsh table.
        index = self.hash_function(key)
        for i, pair in enumerate(self.table[index]):
            if pair[0] == key:
                del self.table[index][i]
                return True  # successfully deleted
        return False  # key not found
    
    def print_table(self):
        for i, bucket in enumerate(self.table):
            print(f"Bucket {i}: {bucket}")
    
def test_hash_table_with_visualization():
    print("Testing HashTable with Visualization...")

    # Create a hash table of size 10
    hash_table = HashTable(size=10)

    # Insert some key-value pairs
    hash_table.insert(15, "Alice")
    hash_table.insert(26, "Bob")
    hash_table.insert(35, "Charlie")
    hash_table.insert(5, "Eve") 
    print("\nHash table after insertion:")
    hash_table.print_table()

    # Delete a key
    hash_table.delete(5)
    print("\nHash table after deleting key 5:")
    hash_table.print_table()

    # Update a key
    hash_table.insert(15, "Updated Alice")
    print("\nHash table after updating key 15:")
    hash_table.print_table()

    # Test search functionality
    print("\nSearching for key 15:", hash_table.search(15))  # Expected: "Alice"
    print("Searching for key 25:", hash_table.search(26))  # Expected: "Bob"
    print("Searching for key 35:", hash_table.search(35))  # Expected: "Charlie"
    print("Searching for non-existing key 45:", hash_table.search(45))  # Expected: None

    print("\nTesting completed.")

# Run the test
if __name__ == "__main__":
    test_hash_table_with_visualization()