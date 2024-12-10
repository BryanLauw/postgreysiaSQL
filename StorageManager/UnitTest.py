from classes import *

storageEngine = StorageEngine()

# storageEngine.debug()

kondisinya = Condition("nama_user", "=", "Agus Maxwell")
temp = DataRetrieval(["users"], ["id_user", "nama_user"], [kondisinya])
print(storageEngine.read_block(temp, "database1", 1))
tempDataRetriev = DataRetrieval(["users", "products"], ["id_user", "nama_user"], [kondisinya])
a = storageEngine.read_block(tempDataRetriev, "database1", 3)
print(a)


print("\n\nskema edit: ")
tempDataWrite = DataWrite(
    table=["users"],               # Hanya satu tabel
    column=["nama_user"],         # Kolom yang akan diupdate
    conditions=[kondisinya],     # Kondisi update
    new_value=["Agus Minwell"]   # Nilai baru untuk kolom username
)
kondisinya = Condition("nama_user", "=", "Agus Maxwell")
temp = DataRetrieval(["users"], ["id_user", "nama_user"], [kondisinya])
print(storageEngine.read_block(temp, "database1", 1))
# # Operasi Write Block
storageEngine.write_block(tempDataWrite, "database1",1)

print(storageEngine.read_block(temp, "database1", 1))
print("disini masih tetap agus maxwell, karena belum dicommit")
storageEngine.commit_buffer(1)
print("disini udah dicommit : ", storageEngine.read_block(temp, "database1", 1), "(Agus Maxwell udah diganti jadi Agus Minwell)")

# ada 1 

print("\n\nskema dihapus: ")
kondisinya = Condition("nama_user", "=", "Agus Minwell")
temp = DataRetrieval(["users"], ["id_user", "nama_user"], [kondisinya])
print(storageEngine.read_block(temp, "database1", 1))
# # print("After Write", storageEngine.read_block(tempDataRetriev,'database1'))
# # Debugging untuk melihat isi storage
# storageEngine.debug()
tempDataDelete = DataDeletion("users", [kondisinya])
b = storageEngine.delete_block(tempDataDelete, "database1", 4)
print("tetapi ketika dicari kembali, datanya masih ada")
print(storageEngine.read_block(temp, "database1", 1))
print("belum dicommit soalnya")
print("nah kalo dicommit")
storageEngine.commit_buffer(4)
print(storageEngine.read_block(temp, "database1", 1))
# print(b)
# storageEngine.debug()

# c = storageEngine.get_stats("database1","users")
# Statistic.print_statistics(c)