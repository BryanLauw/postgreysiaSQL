from StorageManager.classes import *

class QueryCost:

    def __init__(self, storage_engine: StorageEngine, database: str):
        self.__storage_engine = storage_engine
        self.__database = database
    
    def where_equals(self, table: str, attribute: str) -> int:
        statistics = self.__storage_engine.get_stats(database_name=self.__database, table_name=table)
        return statistics.n_r // statistics.V_a_r[attribute]
    
    def where_not_equals(self, table: str, attribute: str) -> int:
        statistics = self.__storage_engine.get_stats(database_name=self.__database, table_name=table)
        return statistics.n_r - self.where_equals(table=table, attribute=attribute)
    
    def where_comparison(self, table: str, attribute: str) -> int:
        statistics = self.__storage_engine.get_stats(database_name=self.__database, table_name=table)
        # assume don't have min(A,r) and max(A,r)
        return statistics.n_r // 2
    

