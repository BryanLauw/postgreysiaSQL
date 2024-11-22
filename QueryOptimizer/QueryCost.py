from StorageManager.classes import *

class QueryCost:

    def __init__(self, storage_engine: StorageEngine, database: str):
        self.__storage_engine = storage_engine
        self.__database = database
    
    def where_equals(self, table: str, attribute: str) -> int: # returns number of tuples
        statistics = self.__storage_engine.get_stats(database_name=self.__database, table_name=table)
        return statistics.n_r // statistics.V_a_r[attribute]
    
    def where_not_equals(self, table: str, attribute: str) -> int: # returns number of tuples
        statistics = self.__storage_engine.get_stats(database_name=self.__database, table_name=table)
        return statistics.n_r - self.where_equals(table=table, attribute=attribute)
    
    def where_comparison(self, table: str) -> int: # returns number of tuples
        statistics = self.__storage_engine.get_stats(database_name=self.__database, table_name=table)
        # assume don't have min(A,r) and max(A,r)
        return statistics.n_r // 2
    
    def join(self, table1: str, table2: str) -> int: # returns number of tuples
        # assume no common attributes in join operation
        statistics1 = self.__storage_engine.get_stats(database_name=self.__database, table_name=table1)
        statistics2 = self.__storage_engine.get_stats(database_name=self.__database, table_name=table2)
        return statistics1.n_r * statistics2.n_r

    def select(self, table: str, attribute: str) -> int: # returns number of tuples
        statistics = self.__storage_engine.get_stats(database_name=self.__database, table_name=table)
        return statistics.V_a_r[attribute]
