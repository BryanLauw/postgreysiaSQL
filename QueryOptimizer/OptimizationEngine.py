import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from StorageManager.classes import Statistic, StorageEngine

from .QueryParser import QueryParser
from .QueryTree import ParsedQuery, QueryTree
from .QueryHelper import *
from typing import Callable, Union
from .QueryValidator import QueryValidator
from .QueryCost import QueryCost
from queue import Queue
from .QueryOptimizer import QueryOptimizer

class OptimizationEngine:
    def __init__(self, get_stats: Callable[[str, str, int], Union[Statistic, Exception]]):
        self.QueryParser = QueryParser("QueryOptimizer/dfa.txt")
        self.QueryValidator = QueryValidator()
        self.QueryOptimizer = QueryOptimizer()
        self.get_stats = get_stats

    def parse_query(self, query: str,database_name: str) -> ParsedQuery:
        normalized_query = QueryHelper.remove_excessive_whitespace(
            QueryHelper.normalize_string(query).lower()
        )
        
        normalized_query = self.QueryParser.transform_to_upper(normalized_query)
        
        # Check Syntax
        normalized_query = self.QueryParser.check_valid_syntax(normalized_query) 

        query_components_value = self.QueryParser.get_components_values(normalized_query)
        
        if "FROM" in query_components_value:
            comp_with_attr = "FROM"
            table_tokens = query_components_value["FROM"]
        elif "UPDATE" in query_components_value:
            comp_with_attr = "UPDATE"
            table_tokens = [query_components_value["UPDATE"]]
        else:
            splitted = query_components_value["INDEX"].split()
            table_tokens = [splitted[2]]
            
        # Get list Tables and Aliases
        alias_map, table_arr = QueryHelper.extract_table_and_aliases(table_tokens )
        
        if "CREATE" not in query_components_value:
            # Remove alias
            query_components_value[comp_with_attr] = QueryHelper.remove_aliases(query_components_value[comp_with_attr])
            self.QueryValidator.validate_aliases(query_components_value, alias_map, table_arr)
        
        # Validate wrong tables
        list_attributes = self.QueryValidator.validate_tables(table_arr,database_name,self.get_stats)
        
        if "SELECT" in query_components_value and query_components_value["SELECT"][0] == "*":
            query_components_value["SELECT"] = list_attributes
                
        # Rewrite alias with direct table's name for simplicity
        QueryHelper.rewrite_components_alias(query_components_value,alias_map)

        # Get attributes and validate their existence
        self.QueryValidator.extract_and_validate_attributes(query_components_value, database_name,self.get_stats, table_arr)

        # WHERE clause
        where_clause = query_components_value.get("WHERE", "")
        update_clause = query_components_value.get("UPDATE", "")
        from_clause = query_components_value.get("FROM", "")

        CO = ['<', '>', '=', '<=', '>=', '<>']

        if "WHERE" in query_components_value:
            attribute_types = self.QueryValidator.get_attribute_types(where_clause, database_name, table_arr)
            if(len(from_clause) == 1 and '.' not in where_clause):
                table_name = from_clause[0].strip()
                pattern = r'(\b\w+\b)\s*(' + '|'.join(map(re.escape, CO)) + r')'
                where_clause = re.sub(pattern, rf'{table_name}.\1 \2', where_clause)
                query_components_value["WHERE"] = where_clause
            if(next(iter(query_components_value), None) == "UPDATE" and '.' not in where_clause):
                table_name = update_clause.strip()
                pattern = r'(\b\w+\b)\s*(' + '|'.join(map(re.escape, CO)) + r')'
                where_clause = re.sub(pattern, rf'{table_name}.\1 \2', where_clause)
                query_components_value["WHERE"] = where_clause
            # Validate comparisons
            self.QueryValidator.validate_comparisons(where_clause, attribute_types)

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

        if "DELETE" in components:
            root.val = "DELETE"
            top = root
            where_tree = QueryTree(type="DELETE", val=components["FROM"][0])
            top.add_child(where_tree)
            where_tree.add_parent(top)
            top = where_tree
            
        if "UPDATE" in components:
            root.val = "UPDATE"
            where_tree = QueryTree(type="UPDATE", val=components["UPDATE"])
            top.add_child(where_tree)
            where_tree.add_parent(top)
            top = where_tree
            
        if "SET" in components:
            where_tree = QueryTree(type="SET", val=components["SET"])
            top.add_child(where_tree)
            where_tree.add_parent(top)
            top = where_tree

        if "INSERT" in components:
            where_tree = QueryTree(type="INSERT")
            top.add_child(where_tree)
            where_tree.add_parent(top)
            top = where_tree
            
        if "CREATE" in components:
            root.val = "CREATE"
            where_tree = QueryTree(type="CREATE", val=components["CREATE"])
            top.add_child(where_tree)
            where_tree.add_parent(top)
            top = where_tree
            
        if "INDEX" in components:
            where_tree = QueryTree(type="INDEX", val=components["INDEX"])
            top.add_child(where_tree)
            where_tree.add_parent(top)
            top = where_tree
            
        if "USING" in components:
            where_tree = QueryTree(type="USING", val=components["USING"])
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
            where_tree = QueryHelper.parse_where_clause(components["WHERE"], top, database_name)
            top = where_tree

        if "FROM" in components:
            if "DELETE" not in components:
                join_tree = QueryHelper.build_join_tree(components["FROM"],database_name,self.get_stats)
                top.add_child(join_tree)
                join_tree.add_parent(top)

        return root

    def optimize_query(self, query: ParsedQuery, database_name:str):
        list_nodes = {
            "JOIN": [],
            "SELECT": [],
            "WHERE": [],
        }
        queue_nodes = Queue()
        queue_nodes.put(query.query_tree)
        while not queue_nodes.empty():
            current_node = queue_nodes.get()
            if current_node.type in ["SELECT","NATURAL JOIN","JOIN","WHERE"]:
                if current_node.type == "NATURAL JOIN":
                    list_nodes["JOIN"].append(current_node)
                else:
                    list_nodes[current_node.type].append(current_node)
            
            for child in current_node.childs:
                queue_nodes.put(child)
            
        for node in list_nodes["WHERE"]:
            self.QueryOptimizer.pushing_selection(node)
                
        list_nodes["JOIN"].reverse()
        self.QueryOptimizer.reorder_join(list_nodes["JOIN"],database_name,self.get_stats)
        self.QueryOptimizer.determine_join_type(list_nodes["JOIN"],database_name,self.get_stats)

        for node in list_nodes["JOIN"]:
            if node.type == "JOIN":
                continue
            if len(node.val) == 0:
                self.QueryOptimizer.combine_selection_and_cartesian_product(node)

        if len(list_nodes["JOIN"]) > 0:
            for node in list_nodes["SELECT"] :
                self.QueryOptimizer.pushing_projection(node)

    def get_cost(self, query: ParsedQuery, database_name: str) -> int:
        # implementasi sementara hanya menghitung size cost
        query_cost = QueryCost(self.get_stats, database_name)
        return query_cost.calculate_size_cost(query.query_tree)

if __name__ == "__main__":
    storage = StorageEngine()
    optim = OptimizationEngine(storage.get_stats)

    while True:
        # try:
            query = input("Query: ")
            parsed_query = optim.parse_query(query,"database1")
            print("BEFORE TREE:")
            print(parsed_query)
            print(f"BEFORE COST = {optim.get_cost(parsed_query, 'database1')}")
            
            optim.optimize_query(parsed_query,"database1")
            print("AFTER TREE:")
            print(parsed_query)
            print(f"AFTER COST = {optim.get_cost(parsed_query, 'database1')}")
        # except Exception as e:
        #     print(f"Error: {e}")
    # Test SELECT query with JOIN
    # select_query = 'SELECT u.id_user FROM users AS u WHERE u.id_user > 1 OR u.nama_user = "A"'
    # select_query = 'select * from users JOIN products ON users.id_user=products.product_id JOIN orders ON orders.order_id = products.product_id AND users.id_user=products.product_id where users.id_user>1 order by users.id_user'
    # select_query = 'SELECT u.id_user FROM users AS u JOIN products AS p ON p.product_id = 1 JOIN orders AS o ON u.id_user = o.order_id'
    # create_index_query = 'CREATE INDEX nama_idx ON users(id) USING hash'
    # print("SELECT QUERY: ",select_query,end="\n\n")
    # parsed_query = optim.parse_query(select_query,"database1")
    # parsed_query = optim.parse_query(create_index_query,"database1")
    # optim.optimize_query(parsed_query)
    # print("EVALUATION PLAN TREE: \n",parsed_query)
    