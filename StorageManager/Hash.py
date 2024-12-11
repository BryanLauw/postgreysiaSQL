class HashTable:
    def __init__(self, size):
        self.size = size
        self.table = [[] for _ in range(size)]

    def hash_function(self, key):
        if type(key) is int:
            return key % self.size
        else :
            # DJB2 algorithm for string hashing
            hash_value = 5381
            for char in key:
                hash_value = ((hash_value << 5) + hash_value) + ord(char)  # hash_value * 33 + ord(char)
            # print("hash_value of", key, "is", hash_value)
            return hash_value % self.size
    
    def insert(self, key, value):
        # insert a key-value pair into the hash table
        index = self.hash_function(key)
        # print(index)
        for map in self.table[index]:
            if map[0] == key:
                map[1][len(map[1])] = value
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

    def set_index(self, database_name: str, table_name: str, column: str, transaction_id:int,index_type="bplus") -> None:
        self.initialize_index_structure(database_name,table_name,column)

        table = self.blocks[database_name][table_name]
        if index_type == "bplus":
            bplus_tree = self.create_bplus_index(table, column)
            self.buffer_index[transaction_id][database_name][table_name][column]["bplus"] = bplus_tree
        elif index_type == "hash":
            hash_index = self.create_hash_index(table, column)
            self.buffer_index[transaction_id][database_name][table_name][column]["hash"] = hash_index
        else:
            raise ValueError("Invalid index type. Only 'bplus' and 'hash' are supported.")
        print(f"Index of type '{index_type}' created for column '{column}' in table '{table_name}'.")

    def create_hash_index(self, table: dict, column: str) :
        hash_index = HashTable(size=10)
        for block_index, block in enumerate(table["values"]):
            for offset, row in enumerate(block):
                if column not in row:
                    raise ValueError(f"Column '{column}' is missing in a row of the table.")
                key = row[column]
                hash_index.insert(key,(block_index,offset))
        return hash_index     

    
      
    
def test_hash_table_with_visualization():
    print("Testing HashTable with Visualization...")
    print("hasil hash", hash("alice"))

    # Create a hash table of size 10
    hash_table = HashTable(size=10)

    # Insert some key-value pairs
    hash_table.insert("Karen", "Alice")
    hash_table.insert("Physics", "Bob")
    hash_table.insert("Biology", "Charlie")
    hash_table.insert("Sicysph", "Eve") 
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
    