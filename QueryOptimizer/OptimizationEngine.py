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
            QueryHelper.normalize_string(query).lower()
        )
        
        normalized_query = self.QueryParser.transform_to_upper(normalized_query)
        print(normalized_query)

        # Check Syntax
        normalized_query = self.QueryParser.check_valid_syntax(normalized_query) 

        query_components_value = self.QueryParser.get_components_values(normalized_query)
        comp_with_attr = "FROM" if "FROM" in query_components_value else "UPDATE"
        
        # Get list Tables and Aliases
        alias_map, table_arr = QueryHelper.extract_table_and_aliases(query_components_value[comp_with_attr] if comp_with_attr=="FROM" else [query_components_value[comp_with_attr]] )
        
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
        print("SINI ",query_components_value)

        # WHERE clause
        where_clause = query_components_value.get("WHERE", "")

        if next(iter(query_components_value), None) == "SELECT":
            attribute_types = get_attribute_types(where_clause, database_name, table_arr, storage)
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
        list_nodes = {
            "JOIN": [],
            "NATURAL JOIN":[],
            "SELECT": [],
            "WHERE": [],
            "ROOT": [],
        }
        
        queue_nodes = Queue()
        queue_nodes.put(query.query_tree)
        while not queue_nodes.empty():
            current_node = queue_nodes.get()
            if current_node.type in ["SELECT","NATURAL JOIN","JOIN","WHERE"]:
                list_nodes[current_node.type].append(current_node)
            
            for child in current_node.childs:
                queue_nodes.put(child)
        for node in list_nodes["NATURAL JOIN"]:
            self.QueryOptimizer.combine_selection_and_cartesian_product(node)
            
        for node in list_nodes["WHERE"]:
            self.QueryOptimizer.pushing_selection(node)
        
        self.QueryOptimizer.pushing_projection(query.query_tree.childs[0].childs[0])

    def get_cost(self, query: ParsedQuery, database_name: str) -> int:
        # implementasi sementara hanya menghitung size cost
        query_cost = QueryCost(self.get_stats, database_name)
        return query_cost.calculate_size_cost(query.query_tree)


def get_attribute_types(where_clause: str, database_name: str, table_arr: list[str], storage_engine: StorageEngine) -> dict:
    # Regex to find comparisons (attribute and literal pairs)
    comparison_pattern = r"([\w\.]+)\s*(=|<>|>|>=|<|<=)\s*([\w\.'\"]+)"
    matches = re.findall(comparison_pattern, where_clause)

    # Initialize the result dictionary
    attribute_types = {}

    # Process each match (attribute and literal)
    for left_attr, operator, right_attr in matches:
        for attr in [left_attr, right_attr]:
            # Skip literals
            if attr.isdigit() or attr.replace(".", "", 1).isdigit() or (attr.startswith(("'", '"')) and attr.endswith(("'", '"'))):
                continue

            if "." in attr:
                # Attribute is qualified with a table name
                table, column = attr.split(".")
                if table not in table_arr:
                    raise ValueError(f"Table {table} is not in the query context.")
            else:
                # Attribute is unqualified, disambiguate
                table, column = None, attr
                for tbl in table_arr:
                    table_data = storage_engine.blocks.get(database_name, {}).get(tbl, None)
                    if not table_data:
                        continue
                    columns = table_data["columns"]
                    if any(col["name"] == column for col in columns):
                        if table is not None:
                            raise ValueError(f"Ambiguous column {column} found in multiple tables.")
                        table = tbl
                if table is None:
                    raise ValueError(f"Column {column} does not exist in any table.")

            # Retrieve the column type
            table_data = storage_engine.blocks.get(database_name, {}).get(table, None)
            if not table_data:
                raise ValueError(f"Table {table} not found in database {database_name}.")
            columns = table_data["columns"]
            column_type = next((col["type"] for col in columns if col["name"] == column), None)
            if not column_type:
                raise ValueError(f"Column {column} does not exist in table {table}.")
            attribute_types[f"{table}.{column}"] = column_type.lower()

    return attribute_types


if __name__ == "__main__":
    storage = StorageEngine()
    optim = OptimizationEngine(storage.get_stats)

    # Test SELECT query with JOIN
    select_query = 'SELECT u.id, product_id FROM users AS u JOIN products AS t ON t.product_id = u.id  WHERE u.id > 1 AND t.product_id = 12 OR t.product_id < 5 AND t.product_id = 10 order by u.id ASC'
    # print("SELECT QUERY\n",select_query,end="\n\n")
    parsed_query = optim.parse_query(select_query,"database1")
    # print(parsed_query)
    optim.optimize_query(parsed_query)
    # optim.optimize_query(parsed_query)
    print("EVALUATION PLAN TREE: \n",parsed_query)
    
    # print(f"COST = {optim.get_cost(parsed_query, 'database1')}")

    # try:
    #     invalid_query = "SELECT x.a FROM students AS s"
    #     print(invalid_query)
    #     parsed_query = optim.parse_query(invalid_query,"database1")
    #     print(parsed_query)
    # except ValueError as e:
    #     print(e)

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

    # Test UPDATE query
    update_query = "UPDATE products SET product_id = product_id + 1.1 - 5 WHERE product_id > 1000"
    print(update_query)
    parsed_update_query = optim.parse_query(update_query, "database_sample")
    print(parsed_update_query)

    # #Test DELETE query
    # delete_query = "DELETE FROM employees WHERE salary < 3000"
    # print(delete_query)
    # parsed_delete_query = new.parse_query(delete_query)
    # print(parsed_delete_query)
