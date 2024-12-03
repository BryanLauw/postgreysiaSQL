import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from StorageManager.classes import Statistic, StorageEngine

from QueryParser import QueryParser
from QueryTree import ParsedQuery, QueryTree
from QueryHelper import *
from typing import Callable, Union
from QueryValidator import QueryValidator
from QueryCost import QueryCost
from queue import Queue
from QueryOptimizer import QueryOptimizer

class OptimizationEngine:
    def __init__(self, get_stats: Callable[[str, str, int], Union[Statistic, Exception]]):
        self.QueryParser = QueryParser("dfa.txt")
        self.QueryValidator = QueryValidator()
        self.QueryOptimizer = QueryOptimizer()
        self.get_stats = get_stats

    def parse_query(self, query: str,database_name: str) -> ParsedQuery:
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
        self.QueryValidator.validate_aliases(query_components_value, alias_map, table_arr)
        
        # Validate wrong tables
        self.QueryValidator.validate_tables(table_arr,database_name,self.get_stats)
                
        # Rewrite alias with direct table's name for simplicity
        QueryHelper.rewrite_components_alias(query_components_value,alias_map)
        
        # Get attributes and validate their existence
        self.QueryValidator.extract_and_validate_attributes(query_components_value, database_name,self.get_stats, table_arr)
        
        print(query_components_value)
        # Build the initial query evaluation plan tree
        query_tree = self.__build_query_tree(query_components_value,database_name)
        return ParsedQuery(query_tree,normalized_query)

    def __build_query_tree(self, components: dict, database_name: str) -> QueryTree:
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
            root.val = "SELECT"
            select_tree = QueryTree(type="SELECT", val=components["SELECT"])
            top.add_child(select_tree)
            select_tree.add_parent(top)
            top = select_tree
            
                
        if "UPDATE" in components:
            root.val = "UPDATE"
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
            # use this if you want to separate the childs
            where_tree = QueryHelper.parse_where_clause(components["WHERE"], top)
            top = where_tree

            # parsed_result = re.split(r'\sAND\s', components["WHERE"])
            # where_tree = QueryTree(type="WHERE", val=parsed_result)
            # top.add_child(where_tree)
            # where_tree.add_parent(top)
            # if query_type == "SELECT":
            #     top = select_tree
            # else:
            #     top = where_tree

        if "FROM" in components:
            join_tree = QueryHelper.build_join_tree(components["FROM"],database_name,self.get_stats)
            top.add_child(join_tree)
            join_tree.add_parent(top)

        return root

    def optimize_query(self, query: ParsedQuery):
        queue_nodes = Queue()
        queue_nodes.put(query.query_tree)
        while(not queue_nodes.empty()):
            current_node = queue_nodes.get()
            if(current_node.type == "TABLE"):
                continue
            for child in current_node.childs:
                queue_nodes.put(child)
                
            if self.QueryOptimizer.perform_operation(current_node):
                print(query)

    def get_cost(self, query: ParsedQuery, database_name: str) -> int:
        # implementasi sementara hanya menghitung size cost
        query_cost = QueryCost(self.get_stats, database_name)
        size_cost = query_cost.get_cost_size(query.query_tree).n_r
        return size_cost


if __name__ == "__main__":
    storage = StorageEngine()
    optim = OptimizationEngine(storage.get_stats)

    # Test SELECT query with JOIN
    select_query = "SELECT s.id, product_id FROM products AS t JOIN users as u ON u.id = products.product_id NATURAL JOIN users AS s WHERE s.id > 1 AND t.product_id = 2 OR t.product_id < 5 AND t.product_id = 10 order by s.id ASC"
    print("SELECT QUERY\n",select_query,end="\n\n")
    parsed_query = optim.parse_query(select_query,"database1")
    print(parsed_query)
    optim.optimize_query(parsed_query)
    # optim.optimize_query(parsed_query)
    print("EVALUATION PLAN TREE: \n",parsed_query)
    # cost_query = QueryCost(storage, "users")
    # print("COST = ", cost_query.get_cost(parsed_query.query_tree), "\n")

    try:
        invalid_query = "SELECT x.a FROM students AS s"
        print(invalid_query)
        parsed_query = optim.parse_query(invalid_query,"database1")
        print(parsed_query)
    except ValueError as e:
        print(e)

    # where_clause = "students.a > 'aku' AND teacher.b = 'abc'"
    # attribute_types = {
    #     "students.a": "integer",
    #     "teacher.b": "varchar",
    # }

    # try:
    #     QueryHelper.validate_comparisons(where_clause, attribute_types)
    #     print("All comparisons are valid!")
    # except ValueError as e:
    #     print(f"Validation error: {e}")

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
