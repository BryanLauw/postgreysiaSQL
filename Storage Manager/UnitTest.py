from classes import *

storageEngine = StorageEngine()
kondisinya = Condition("id", "=", 1)

tempDataRetriev = DataRetrieval(["users", "products"], ["id", "username"], [kondisinya])

a = storageEngine.read_block(tempDataRetriev, "database1")
b = storageEngine.get_stats("database1","users")

print(a)
Statistic.print_statistics(b)