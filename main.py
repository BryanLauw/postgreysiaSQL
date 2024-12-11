import os,sys
cwd = os.getcwd()
sys.path.append(cwd + '/FailureRecovery/')
sys.path.append(cwd + '/ConcurrencyControlManager/')
sys.path.append(cwd + '/QueryOptimizer/')
sys.path.append(cwd + '/QueryProcessor/')
sys.path.append(cwd + '/StorageManager/')


from QueryProcessor.QueryProcessor import QueryProcessor

queryProcessor = QueryProcessor()

print("\033[92mWelcome to PostgreysiaSQL!\033[0m")
print(f"\033[94mYou can start using the SQL commands after the '>' prompt.\033[0m")

client_state = {
    "on_begin": False,
    "transactionId": None
}

while True:
    query = input("> ")
    if query == "exit":
        break
    # try:
    #     queryProcessor.execute_query(query)
    # except Exception as e:
    #     print(f"Error: {e}")
    queryProcessor.execute_query(query, client_state)