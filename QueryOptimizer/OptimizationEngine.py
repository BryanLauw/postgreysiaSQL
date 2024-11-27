import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from StorageManager.classes import Statistic, StorageEngine

from QueryParser import QueryParser
from QueryTree import ParsedQuery, QueryTree
from QueryHelper import *
from typing import Callable, Union
class OptimizationEngine:
    def __init__(self):
        self.QueryParser = QueryParser("dfa.txt")

    def parse_query(self, query: str,database_name: str, get_stats: Callable[[str, str, int], Union[Statistic, Exception]]) -> ParsedQuery:
        normalized_query = QueryHelper.remove_excessive_whitespace(
            QueryHelper.normalize_string(query).upper()
        )

        normalized_query = self.QueryParser.check_valid_syntax(normalized_query) 
        if(not normalized_query):
            return False
        
        query_components_value = self.QueryParser.get_components_values(normalized_query)

        if "FROM" in query_components_value:
            alias_map = QueryHelper.extract_table_aliases(query_components_value["FROM"])
            
            query_components_value["FROM"] = QueryHelper.remove_aliases(query_components_value["FROM"])
            
            query_components_value["FROM"] = [
                QueryHelper.rewrite_with_alias(attr, alias_map) for attr in query_components_value["FROM"]
            ]
            
            if "SELECT" in query_components_value:
                query_components_value["SELECT"] = [
                    QueryHelper.rewrite_with_alias(attr, alias_map) for attr in query_components_value["SELECT"]
                ]
            if "WHERE" in query_components_value:
                query_components_value["WHERE"] = QueryHelper.rewrite_with_alias(
                    query_components_value["WHERE"], alias_map
                )
                            
            table_arr = QueryHelper.extract_tables(query_components_value["FROM"])
        else:
            table_arr = query_components_value['UPDATE']
            
        attributes_arr = QueryHelper.extract_attributes(query_components_value)

        query_tree = self.__build_query_tree(query_components_value)
        return ParsedQuery(query_tree,normalized_query)
    
    def strip_alias(table: str) -> str:
        if " AS " in table:
            return table.split(" AS ")[0].strip()
        elif " " in table:
            return table.split()[0].strip()
        return table.strip()

    def __build_query_tree(self, components: dict) -> QueryTree:

        root = QueryTree(type="ROOT")
        top = root

        if "FROM" in components:
            from_tokens = components["FROM"]

        if "LIMIT" in components:
            limit_tree = QueryTree(type="LIMIT", val=components["LIMIT"])
            top.add_child(limit_tree)
            limit_tree.add_parent(top)
            top = limit_tree
        
        if "ORDER BY" in components:
            order_by_tree = QueryTree(type="ORDER BY", val=components["ORDER BY"])
            top.add_child(order_by_tree)
            order_by_tree.add_parent(top)
            top = order_by_tree
        
        if "SELECT" in components:
            select_tree = QueryTree(type="SELECT", val=components["SELECT"])
            top.add_child(select_tree)
            select_tree.add_parent(top)
            top = select_tree
                
        if "UPDATE" in components:
            where_tree = QueryTree(type="UPDATE", val=components["UPDATE"])
            top.add_child(where_tree)
            where_tree.add_parent(top)
            top = where_tree

        # if "DELETE" in components:
        #     where_tree = QueryTree(type="DELETE", val=components["DELETE"])
        #     top.add_child(where_tree)
        #     where_tree.add_parent(top)
        #     top = where_tree
        
        if "WHERE" in components:
            where_tree = QueryTree(type="WHERE", val=components["WHERE"])
            top.add_child(where_tree)
            where_tree.add_parent(top)
            top = where_tree
        
        if "FROM" in components:
            join_tree = QueryHelper.build_join_tree(components["FROM"])
            top.add_child(join_tree)
            join_tree.add_parent(top)

        return root

    def optimize_query(self, query: ParsedQuery) -> ParsedQuery:
        # Placeholder for future optimization logic
        pass

    def __get_cost(self, query: ParsedQuery) -> int:
        # Placeholder for query cost estimation
        pass



if __name__ == "__main__":
    optim = OptimizationEngine()
    storage = StorageEngine()

    # Test SELECT query with JOIN
    select_query = "SELECT s.id, s.b FROM students AS s JOIN st ON s.id=st.id WHERE x.a = 1 ORDER BY kok"
    parsed_query = optim.parse_query(select_query,"database_sample",storage.get_stats)
    print(parsed_query)

    # Test UPDATE query
    # update_query = "UPDATE employee SET salary = salary + 1.1 - 5 WHERE salary > 1000"
    # print(update_query)
    # parsed_update_query = optim.parse_query(update_query, "database_sample", storage.get_stats)
    # print(parsed_update_query)

    # #Test DELETE query
    # delete_query = "DELETE FROM employees WHERE salary < 3000"
    # print(delete_query)
    # parsed_delete_query = new.parse_query(delete_query)
    # print(parsed_delete_query)
    
    