from typing import Callable, Union
from math import prod
# from time import sleep
from StorageManager.classes import *
from QueryTree import *

class QueryCost:

    def __init__(self, get_stats: Callable[[str, str, int], Union[Statistic, Exception]], database: str):
        self.__get_stats = get_stats
        self.__database = database


    # Size Cost
    # Calculate the size cost of the query tree

    def get_cost_size(self, query_tree: QueryTree) -> Statistic:
        # print(f"cost for type: {query_tree.type}, val: {query_tree.val}")
        # sleep(1)
        if query_tree.type == "TABLE":
            return self.__get_stats(self.__database, query_tree.val)
        
        elif query_tree.type == "WHERE":
            if not query_tree.childs:
                # search the root of all the WHERE clause
                root_where = query_tree
                while root_where.get_next_sibling() is None:
                    root_where = root_where.parent
                
                node_to_check = root_where.get_next_sibling()
            else:
                node_to_check = query_tree.childs[0]
            
            if "OR" in query_tree.val:
                return self.get_cost_size(node_to_check)
            elif "=" in query_tree.val:
                statistic = self.get_cost_size(node_to_check)
                attribute = query_tree.val.split(" = ")[0].split(".")[1]
                return self.where_equals(statistic, attribute)
            elif "<>" in query_tree.val:
                statistic = self.get_cost_size(node_to_check)
                attribute = query_tree.val.split(" <> ")[0].split(".")[1]
                return self.where_not_equals(statistic, attribute)
            else:
                return self.where_comparison(self.get_cost_size(node_to_check))
        
        elif query_tree.type == "JOIN":
            statistic1 = self.get_cost_size(query_tree.childs[0])
            statistic2 = self.get_cost_size(query_tree.childs[1])
            attributes = {item.split('.')[0]: item.split('.')[1] for item in query_tree.val.split(" = ")}
            table1 = query_tree.childs[0].val.strip()
            table2 = query_tree.childs[1].val.strip()
            return self.join_on(statistic1, statistic2, attributes[table1], attributes[table2])

        elif query_tree.type == "NATURAL JOIN":
            statistic1 = self.get_cost_size(query_tree.childs[0])
            statistic2 = self.get_cost_size(query_tree.childs[1])
            attributes = query_tree.val
            return self.natural_join(statistic1, statistic2, attributes)
        
        elif query_tree.type == "SELECT": 
            statistic = self.get_cost_size(query_tree.childs[0])
            attributes = [item.split('.')[1] for item in query_tree.val]
            return self.select(statistic, attributes)

        else:
            return self.get_cost_size(query_tree.childs[0])
    
    def where_equals(self, statistic: Statistic, attribute: str) -> Statistic:
        n_r_result = statistic.n_r / statistic.V_a_r[attribute]

        V_a_r_result = {}
        for attribute in statistic.V_a_r:
            V_a_r_result[attribute] = min(statistic.V_a_r[attribute], n_r_result)
        
        result: Statistic = Statistic(n_r=n_r_result, V_a_r=V_a_r_result, b_r=None, l_r=None, f_r=None)
        return result

    def where_not_equals(self, statistic: Statistic, attribute: str) -> Statistic:
        n_r_result = statistic.n_r - self.where_equals(statistic, attribute).n_r
        
        V_a_r_result = {}
        for attribute in statistic.V_a_r:
            V_a_r_result[attribute] = min(statistic.V_a_r[attribute], n_r_result)
        
        result: Statistic = Statistic(n_r=n_r_result, V_a_r=V_a_r_result, b_r=None, l_r=None, f_r=None)
        return result

    def where_comparison(self, statistic: Statistic) -> Statistic:
        n_r_result = statistic.n_r // 2
        
        V_a_r_result = {}
        for attribute in statistic.V_a_r:
            V_a_r_result[attribute] = min(statistic.V_a_r[attribute], n_r_result)
        
        result: Statistic = Statistic(n_r=n_r_result, V_a_r=V_a_r_result, b_r=None, l_r=None, f_r=None)
        return result
    
    def cross_join(self, statistic1: Statistic, statistic2: Statistic) -> Statistic:
        n_r_result = statistic1.n_r * statistic2.n_r
        
        V_a_r_result = {}
        for attribute in statistic1.V_a_r:
            V_a_r_result[attribute] = min(statistic1.V_a_r[attribute], n_r_result)
        for attribute in statistic2.V_a_r:
            V_a_r_result[attribute] = min(statistic2.V_a_r[attribute], n_r_result)
        
        result: Statistic = Statistic(n_r=n_r_result, V_a_r=V_a_r_result, b_r=None, l_r=None, f_r=None)
        return result

    def join_on(self, statistic1: Statistic, statistic2: Statistic, attribute1: str, attribute2: str) -> Statistic:
        # print(f"statistic1: {statistic1}, statistic2: {statistic2}, attribute1: {attribute1}, attribute2: {attribute2}")
        n_r_result_1 = statistic1.n_r * statistic2.n_r // statistic2.V_a_r[attribute2]
        n_r_result_2 = statistic1.n_r * statistic2.n_r // statistic1.V_a_r[attribute1]
        n_r_result = min(n_r_result_1, n_r_result_2)
        
        V_a_r_result = {}
        for attribute in statistic1.V_a_r:
            V_a_r_result[attribute] = min(statistic1.V_a_r[attribute], n_r_result)
        for attribute in statistic2.V_a_r:
            V_a_r_result[attribute] = min(statistic2.V_a_r[attribute], n_r_result)
        
        result: Statistic = Statistic(n_r=n_r_result, V_a_r=V_a_r_result, b_r=None, l_r=None, f_r=None)
        return result
    
    def natural_join(self, statistic1: Statistic, statistic2: Statistic, attributes: list[str]) -> Statistic:
        V_a_r_2_all = prod([statistic2.V_a_r[attribute] for attribute in attributes])
        n_r_result_1 = statistic1.n_r * statistic2.n_r // V_a_r_2_all
        V_a_r_1_all = prod([statistic1.V_a_r[attribute] for attribute in attributes])
        n_r_result_2 = statistic1.n_r * statistic2.n_r // V_a_r_1_all
        n_r_result = min(n_r_result_1, n_r_result_2)
        
        V_a_r_result = {}
        for attribute in statistic1.V_a_r:
            V_a_r_result[attribute] = min(statistic1.V_a_r[attribute], n_r_result)
        for attribute in statistic2.V_a_r:
            V_a_r_result[attribute] = min(statistic2.V_a_r[attribute], n_r_result)
        
        result: Statistic = Statistic(n_r=n_r_result, V_a_r=V_a_r_result, b_r=None, l_r=None, f_r=None)
        return result

    def select(self, statistic: Statistic, attributes: list[str]) -> Statistic:
        n_r_result = 1
        for attribute in attributes:
            n_r_result *= statistic.V_a_r[attribute]

        V_a_r_result = {}
        for attribute in attributes:
            V_a_r_result[attribute] = statistic.V_a_r[attribute]
        
        result: Statistic = Statistic(n_r=n_r_result, V_a_r=V_a_r_result, b_r=None, l_r=None, f_r=None)
        return result
    

    # Time Cost
    # Calculate the time cost of the query tree
    # Time Cost dengan waktu block transfer = waktu seeks = 1
    
    def linearScan(self, table: str):
        statistics = self.__storage_engine.get_stats(database_name=self.__database, table_name=table)
        return statistics.b_r + 1
    
    #Asumsi tinggi pohon diketahui
    def indexScanKey(self, table: str, levelPohon): 
        statistics = self.__storage_engine.get_stats(database_name=self.__database, table_name=table)
        return 2*(levelPohon + 1)
    
    #Asumsi tinggi pohon diketahui
    def indexScanNonkey(self, table: str, levelPohon): 
        statistics = self.__storage_engine.get_stats(database_name=self.__database, table_name=table)
        return 2*(levelPohon) + statistics.b_r + 1
    
    def nestedLoopJoin(self, table1: str, table2: str) :
        statistics1 = self.__storage_engine.get_stats(database_name=self.__database, table_name=table1)
        statistics2 = self.__storage_engine.get_stats(database_name=self.__database, table_name=table2)
        return statistics1.n_r * statistics2.b_r +statistics1.b_r + statistics1.n_r +statistics1.b_r
        
    def blockNestedLoopJoin(self, table1: str, table2: str) :
        statistics1 = self.__storage_engine.get_stats(database_name=self.__database, table_name=table1)
        statistics2 = self.__storage_engine.get_stats(database_name=self.__database, table_name=table2)
        return statistics1.b_r * statistics2.b_r + statistics1.b_r + 2*statistics1.b_r
    
    # Asumsi kedua tabel sudah di-sort dan block buffer berukuran 1
    def mergeJoin(self, table1: str, table2: str) :
        statistics1 = self.__storage_engine.get_stats(database_name=self.__database, table_name=table1)
        statistics2 = self.__storage_engine.get_stats(database_name=self.__database, table_name=table2)
        return 2*(statistics1.b_r * statistics2.b_r)
    

