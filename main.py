from QueryProcessor.QueryProcessorClass import QueryProcessorClass

queryProcessor = QueryProcessorClass()

print("\033[92mWelcome to PostgreysiaSQL!\033[0m")
print(f"\033[94mYou can start using the SQL commands after the '>' prompt.\033[0m")

while True:
    query = input("> ")
    if query == "exit":
        break
    queryProcessor.execute_query(query)