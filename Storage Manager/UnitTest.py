from classes import *

storageEngine = StorageEngine()
kondisinya = Condition("id", "=", 1)

tempDataRetriev = DataRetrieval(["users", "products"], ["id", "username"], [kondisinya])

a = storageEngine.read_block(tempDataRetriev, "database1")
print(a)

tempDataDelete = DataDeletion("users", [kondisinya])
b = storageEngine.delete_block(tempDataDelete, "database1")
print(b)
storageEngine.debug()