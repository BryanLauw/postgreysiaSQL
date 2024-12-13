from StorageManager.classes import *

storageEngine = StorageEngine()

# GET TABLES AND COLUMNS OF DATABASE1
print(storageEngine.get_tables_and_columns_info("database1"))

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
temp = DataRetrieval(["users"], [], [kondisinya])
print(storageEngine.read_block(temp, "database1", 0))
# # Operasi Write Block
storageEngine.write_block(tempDataWrite, "database1",1)

print(storageEngine.read_block(temp, "database1", 0))
print("disini masih tetap agus maxwell, karena belum dicommit")
storageEngine.commit_buffer(1)
print("disini udah dicommit : ", storageEngine.read_block(temp, "database1", 0), "(Agus Maxwell udah diganti jadi Agus Minwell)")

# ada 1 

print("\n\nskema dihapus: ")
kondisinya = Condition("nama_user", "=", "Agus Minwell")
temp = DataRetrieval(["users"], ["id_user", "nama_user"], [kondisinya])
print(storageEngine.read_block(temp, "database1", 0))
# # print("After Write", storageEngine.read_block(tempDataRetriev,'database1'))
# # Debugging untuk melihat isi storage
# storageEngine.debug()
tempDataDelete = DataDeletion("users", [kondisinya])
b = storageEngine.delete_block(tempDataDelete, "database1", 4)
print("tetapi ketika dicari kembali, datanya masih ada")
print(storageEngine.read_block(temp, "database1", 0))
print("belum dicommit soalnya")
print("nah kalo dicommit")
storageEngine.commit_buffer(4)
print(storageEngine.read_block(temp, "database1", 0))
# print(b)
# storageEngine.debug()

# c = storageEngine.get_stats("database1","users")
# Statistic.print_statistics(c)

# --- NEW TABLE "USERS MEMBERSHIP" ---
# column_type = {"id_user" : "INTEGER", "membership_level" : "VARCHAR(20)", "registered_date" : "TEXT"}
# information = {"id_user" : ["UNIQUE", "FOREIGN KEY"]}

# storageEngine.create_table("database1", "users_membership", column_type, information)
# print("SUCCESS")

# data_insert = [{"id_user" : 1, "membership_level" : "Gold", "registered_date" : "2023-01-01"}
#                 ,{"id_user" : 2, "membership_level" : "Silver", "registered_date" : "2023-01-01"}
#                 ,{"id_user" : 3, "membership_level" : "Platinum", "registered_date" : "2023-01-01"}
#                 ,{"id_user" : 4, "membership_level" : "Platinum", "registered_date" : "2023-01-03"}
#                 ,{"id_user" : 5, "membership_level" : "Platinum", "registered_date" : "2023-01-03"}
#                 ,{"id_user" : 6, "membership_level" : "Platinum", "registered_date" : "2023-01-03"}
#                 ,{"id_user" : 7, "membership_level" : "Gold", "registered_date" : "2023-01-07"}
#                 ,{"id_user" : 8, "membership_level" : "Gold", "registered_date" : "2023-01-08"}
#                 ,{"id_user" : 9, "membership_level" : "Gold", "registered_date" : "2023-01-09"}
#                 ,{"id_user" : 10, "membership_level" : "Gold", "registered_date" : "2023-01-10"}
#                 ,{"id_user" : 11, "membership_level" : "Gold", "registered_date" : "2023-01-11"}
#                 ,{"id_user" : 12, "membership_level" : "Silver", "registered_date" : "2023-01-12"}
#                 ,{"id_user" : 13, "membership_level" : "Silver", "registered_date" : "2023-01-12"}
#                 ,{"id_user" : 14, "membership_level" : "Silver", "registered_date" : "2023-01-12"}
#                 ,{"id_user" : 15, "membership_level" : "Silver", "registered_date" : "2023-01-12"}
#                 ,{"id_user" : 16, "membership_level" : "Silver", "registered_date" : "2023-01-12"}
#                 ,{"id_user" : 17, "membership_level" : "Silver", "registered_date" : "2023-01-18"}
#                 ,{"id_user" : 18, "membership_level" : "Silver", "registered_date" : "2023-01-18"}
#                 ,{"id_user" : 19, "membership_level" : "Platinum", "registered_date" : "2023-01-19"}
#                 ,{"id_user" : 20, "membership_level" : "Platinum", "registered_date" : "2023-01-20"}
#                 ,{"id_user" : 21, "membership_level" : "Platinum", "registered_date" : "2023-01-21"}
#                 ,{"id_user" : 22, "membership_level" : "Platinum", "registered_date" : "2023-01-22"}
#                 ,{"id_user" : 23, "membership_level" : "Platinum", "registered_date" : "2023-01-22"}
#                 ,{"id_user" : 24, "membership_level" : "Gold", "registered_date" : "2023-01-24"}
#                 ,{"id_user" : 25, "membership_level" : "Gold", "registered_date" : "2023-01-24"}
#                 ,{"id_user" : 26, "membership_level" : "Gold", "registered_date" : "2023-01-24"}
#                 ,{"id_user" : 27, "membership_level" : "Gold", "registered_date" : "2023-01-28"}
#                 ,{"id_user" : 28, "membership_level" : "Gold", "registered_date" : "2023-01-28"}
#                 ,{"id_user" : 29, "membership_level" : "Gold", "registered_date" : "2023-01-28"}
#                 ,{"id_user" : 30, "membership_level" : "Gold", "registered_date" : "2023-01-30"}
#                 ,{"id_user" : 31, "membership_level" : "Gold", "registered_date" : "2023-01-31"}
#                 ,{"id_user" : 32, "membership_level" : "Gold", "registered_date" : "2023-02-02"}
#                 ,{"id_user" : 33, "membership_level" : "Platinum", "registered_date" : "2023-02-02"}
#                 ,{"id_user" : 34, "membership_level" : "Platinum", "registered_date" : "2023-02-02"}
#                 ,{"id_user" : 35, "membership_level" : "Platinum", "registered_date" : "2023-02-06"}
#                 ,{"id_user" : 36, "membership_level" : "Platinum", "registered_date" : "2023-02-06"}
#                 ,{"id_user" : 37, "membership_level" : "Platinum", "registered_date" : "2023-02-06"}
#                 ,{"id_user" : 38, "membership_level" : "Platinum", "registered_date" : "2023-02-06"}
#                 ,{"id_user" : 39, "membership_level" : "Platinum", "registered_date" : "2023-02-06"}
#                 ,{"id_user" : 40, "membership_level" : "Platinum", "registered_date" : "2023-02-09"}
#                 ,{"id_user" : 41, "membership_level" : "Platinum", "registered_date" : "2023-02-10"}
#                 ,{"id_user" : 42, "membership_level" : "Platinum", "registered_date" : "2023-02-11"}
#                 ,{"id_user" : 43, "membership_level" : "Platinum", "registered_date" : "2023-02-12"}
#                 ,{"id_user" : 44, "membership_level" : "Platinum", "registered_date" : "2023-02-13"}
#                 ,{"id_user" : 45, "membership_level" : "Platinum", "registered_date" : "2023-02-14"}
#                 ,{"id_user" : 46, "membership_level" : "Gold", "registered_date" : "2023-02-15"}
#                 ,{"id_user" : 47, "membership_level" : "Silver", "registered_date" : "2023-02-16"}
#                 ,{"id_user" : 48, "membership_level" : "Gold", "registered_date" : "2023-02-17"}
#                 ,{"id_user" : 49, "membership_level" : "Silver", "registered_date" : "2023-02-19"}
#                 ,{"id_user" : 50, "membership_level" : "Silver", "registered_date" : "2023-02-19"}]

# for data in data_insert :
#     storageEngine.insert_data("database1", "users_membership", data, 1)


# storageEngine.debug()