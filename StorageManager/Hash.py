class HashTable:
    def __init__(self, size):
        self.size = size
        self.table = [[] for _ in range(size)]

    def hash_function(self, key):
        if type(key) is int or type(key) is float:
            return round(key % self.size)
        else :
            # DJB2 algorithm for String Hashing
            hash_value = 5381
            for char in key:
                hash_value = ((hash_value << 5) + hash_value) + ord(char)  # hash_value * 33 + ord(char)
            # print("hash_value of", key, "is", hash_value)
        return hash_value % self.size
    
    def insert(self, key, value):
        # insert a key-value pair into the hash table
        index = self.hash_function(key)
        print("index", index)
        print("value inserted:",value)
        # print(index)
        for i, pair in enumerate(self.table[index]):
            if pair[0] == key:
                if pair[1].count(value) == 0:
                    self.table[index][i][1].append(value)
                    return
                else :
                    raise ValueError("Key-Value already exists in hash table.")
        self.table[index].append([key, [value]])

    def search(self, key):
        # search for a key in the hash table and return its value (list of value(s) which has (have) the searched key)
        print("self table =", self.table)
        print()
        index = self.hash_function(key)
        print(index)
        print(self.table[index])
        for pair in self.table[index]:
            print(pair)
            if pair[0] == key:
                return pair[1]
        print("Key not found in hash table.")  # key not found

    def delete(self, key, value):
        # delete a key-value pair from the hash table.
        bucket = self.hash_function(key)
        for i, pair in enumerate(self.table[bucket]):
            if pair[0] == key:
                if pair[1].count(value) > 0:
                    self.table[bucket][i][1].remove(value)
                if len(self.table[bucket][i][1]) == 0:
                    self.table[bucket].remove(pair)
                print("Delete success")
                return
        raise ValueError("Key not found in hash table.")  # key not found

    def delete_key_value(self, key, value):
        # delete a key-value pair from the hash table.
        bucket = self.hash_function(key)
        for i, pair in enumerate(self.table[bucket]):
            if pair[0] == key:
                if pair[1].count(value) > 0:
                    self.table[bucket][i][1].remove(value)
                if len(self.table[bucket][i][1]) == 0:
                    self.table[bucket].remove(pair)
                print("Delete success")
                return
        raise ValueError("Key not found in hash table.")  # key not found
    
    def delete_key(self, key):
        # delete a key from the hash table.
        bucket = self.hash_function(key)
        for i, pair in enumerate(self.table[bucket]):
            if pair[0] == key:
                self.table[bucket].remove(pair)
                print("Delete success")
                return
        raise ValueError("Key not found in hash table.")
    
    def print_table(self):
        for i, bucket in enumerate(self.table):
            print(f"Bucket {i}: {bucket}")      
    
def test_hash_table_with_visualization():
    print("Testing HashTable with Visualization...")

    # Create a hash table of size 10
    hash_table = HashTable(size=10)

    print("hasil hash 'alice' :", hash_table.hash_function("alice"))
    print("hasil hash 15 :", hash_table.hash_function(15))

    # Insert some key-value pairs
    hash_table.insert("Math",(1,1))
    hash_table.insert("Physics",(2,1))
    hash_table.insert("Physics",(3,5))
    hash_table.insert("Biology",(3,1))
    hash_table.insert("Sicysph",(5,2)) 
    print("\nHash table after insertion:")
    print("Hash table size :", hash_table.size)
    hash_table.print_table()
    

    # Delete a key
    # hash_table.delete(5)
    print("\nHash table after deleting key 5:")
    hash_table.print_table()

    # Update a key
    hash_table.insert(15, (8,7))
    print("\nHash table after updating key 15:")
    hash_table.print_table()

    # Test search functionality
    print("\nSearching for key 15:", hash_table.search("Biology"))  # Expected: "(3, 1)"
    # print("Searching for key 25:", hash_table.search(26))  # Expected: "Bob"
    # print("Searching for key 35:", hash_table.search(35))  # Expected: "Charlie"
    # print("Searching for non-existing key 45:", hash_table.search(45))  # Expected: None
    print("\nHash raw print:")
    print(hash_table.table)

    print("\nTesting completed.")

# Run the test
if __name__ == "__main__":
    test_hash_table_with_visualization()