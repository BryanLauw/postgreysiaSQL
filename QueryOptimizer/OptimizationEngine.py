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

        # Check Syntax
        normalized_query = self.QueryParser.check_valid_syntax(normalized_query) 

        query_components_value = self.QueryParser.get_components_values(normalized_query)
        comp_with_attr = "FROM" if "FROM" in query_components_value else "UPDATE"
        
        # Get list Tables and Aliases
        alias_map, table_arr = QueryHelper.extract_table_and_aliases(query_components_value[comp_with_attr])
        
        # Remove alias
        query_components_value[comp_with_attr] = QueryHelper.remove_aliases(query_components_value[comp_with_attr])
        
        # Validate wrong aliases
        QueryHelper.validate_aliases(query_components_value, alias_map)
        
        table_statistics = QueryHelper.validate_tables(table_arr,database_name,get_stats)
                
        QueryHelper.rewrite_components_alias(query_components_value,alias_map)
        
        print(query_components_value)
        # print(table_statistics)
        attributes_arr = QueryHelper.extract_attributes(query_components_value)
        # print(attributes_arr)
        
        QueryHelper.validate_attributes(attributes_arr,table_statistics)
        query_tree = self.__build_query_tree(query_components_value)
        return ParsedQuery(query_tree,normalized_query)

    def __build_query_tree(self, components: dict) -> QueryTree:
        query_type = next(iter(components))
        root = QueryTree(type="ROOT")
        top = root

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
            where_tree = QueryHelper.parse_where_clause(components["WHERE"])
            top.add_child(where_tree)
            where_tree.add_parent(top)
            if query_type == "SELECT":
                top = select_tree
            else:
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
    select_query = "SELECT s.id, t.product_id FROM users AS s JOIN products AS t ON s.id = t.product_id, s.id = t.product_id  WHERE s.id > 1 AND t.product_id = 2 OR t.product_id < 5 order by s.id ASC"
    print(select_query)
    parsed_query = optim.parse_query(select_query,"database1",storage.get_stats)
    print(parsed_query)

    try:
        invalid_query = "SELECT x.a FROM students AS s"
        print(invalid_query)
        parsed_query = optim.parse_query(invalid_query,"database1",storage.get_stats)
        print(parsed_query)
    except ValueError as e:
        print(e)

    where_clause = "students.a > 'aku' AND teacher.b = 'abc'"
    attribute_types = {
        "students.a": "integer",
        "teacher.b": "varchar",
    }

    try:
        QueryHelper.validate_comparisons(where_clause, attribute_types)
        print("All comparisons are valid!")
    except ValueError as e:
        print(f"Validation error: {e}")

    # # Test UPDATE query
    # update_query = "UPDATE employee SET salary = salary + 1.1 - 5 WHERE salary > 1000"
    # print(update_query)
    # parsed_update_query = optim.parse_query(update_query, "database_sample", storage.get_stats)
    # print(parsed_update_query)

    # #Test DELETE query
    # delete_query = "DELETE FROM employees WHERE salary < 3000"
    # print(delete_query)
    # parsed_delete_query = new.parse_query(delete_query)
    # print(parsed_delete_query)
