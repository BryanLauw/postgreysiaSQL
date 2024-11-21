class QueryProcessor:
    def __init__(self):
        # qo = QueryOptimizer()
        # cc = ConcurrencyController()
        # sm = StorageManager()
        # rm = RecoveryManager()
        pass

    def execute_query(self, query : str):
        print("Executing query: " + query)
        if(query.upper() == "BEGIN" or query.upper() == "BEGIN TRANSACTION"):
            pass
        elif(query.upper() == "COMMIT" or query.upper() == "BEGIN TRANSACTION"):
            pass
        elif(query.upper() == "END TRANSACTION"):
            pass
        else:
            pass