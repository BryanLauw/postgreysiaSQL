from ConcurrencyControlManager.main import *
from QueryOptimizer.main import *

class QueryProcessor:
    def __init__(self):
        qo = QueryOptimizer()
        cc = ConcurrencyControlManager()
        # sm = StorageManager()
        # rm = RecoveryManager()
        pass

    def execute_query(self, query : str):
        queries = self.parse_query(query)
        for query in queries:
            if(query.upper() == "BEGIN" or query.upper() == "BEGIN TRANSACTION"):
                pass
            elif(query.upper() == "COMMIT" or query.upper() == "BEGIN TRANSACTION"):
                pass
            elif(query.upper() == "END TRANSACTION"):
                pass
            else:
                pass


    def parse_query(self, query : str):
        queries = query.split(';')
        return [q.strip() for q in queries if q.strip()]