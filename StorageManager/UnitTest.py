from classes import *

storageEngine = StorageEngine()
kondisinya = Condition("id", "=", 1)




tempDataRetriev = DataRetrieval(["users", "products"], ["id", "username"], [kondisinya])
a = storageEngine.read_block(tempDataRetriev, "database1")
print(a)

tempDataWrite = DataWrite(
    table="users",               # Hanya satu tabel
    column=["username"],         # Kolom yang akan diupdate
    conditions=[kondisinya],     # Kondisi update
    new_value=["updated_user"]   # Nilai baru untuk kolom username
)

# Operasi Write Block
b = storageEngine.write_block(tempDataWrite, "database1")
print("Jumlah baris yang diupdate:", b)
print("After Write", storageEngine.read_block(tempDataRetriev,'database1'))
# Debugging untuk melihat isi storage
storageEngine.debug()
# tempDataDelete = DataDeletion("users", [kondisinya])
# b = storageEngine.delete_block(tempDataDelete, "database1")
# print(b)
# storageEngine.debug()

# c = storageEngine.get_stats("database1","users")
# Statistic.print_statistics(c)