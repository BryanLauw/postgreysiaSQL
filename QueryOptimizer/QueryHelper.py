import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from StorageManager.classes import Statistic, StorageEngine
from QueryTree import ParsedQuery, QueryTree
import re
from typing import List, Union, Dict, Callable

class QueryHelper:
    @staticmethod
    def normalize_string(query: str):
        return query.replace("\t", "").replace("\n", "").replace("\r", "")
    
    @staticmethod
    def remove_excessive_whitespace(query: str):
        words = query.split()
        new_query = " ".join(words)
        return new_query
    
    @staticmethod
    def extract_table_and_aliases(from_tokens: list[str]) -> {dict,list[str]}:
        alias_map = {}
        attribute_arr = []
        for token in from_tokens:
            if token in ["JOIN","NATURAL JOIN",","]:
                continue
            
            splitted = token.split()
            attribute_arr.append(splitted[0])
            
            try:
                idx_AS = splitted.index("AS")
                alias_map[splitted[idx_AS+1]] = splitted[idx_AS-1]
            except ValueError:
                pass
        return alias_map, attribute_arr
    
    @staticmethod
    def remove_aliases(from_clause: list) -> list:
        def strip_alias(from_str: str) -> str:
            cleaned_str = re.sub(r"\s+as\s+\w+\s*", " ", from_str, flags=re.IGNORECASE)
            return cleaned_str

        return [strip_alias(token) if token.upper() not in ["JOIN", "ON", "NATURAL"] else token for token in from_clause]
    
    @staticmethod
    def rewrite_with_alias(expression: str, alias_map: dict) -> str:
        for alias, table in alias_map.items():
            pattern = rf"(?<!\w){re.escape(alias)}\."
            replacement = table + "."
            expression = re.sub(pattern, replacement, expression)
        return expression
    
    @staticmethod
    def rewrite_components_alias(query_components_value: Dict[str, Union[List[str],str]], alias_map: dict):
        for comp in query_components_value:
            if comp in ["SELECT","FROM"]:
                query_components_value[comp] = [
                    QueryHelper.rewrite_with_alias(attr, alias_map) for attr in query_components_value[comp]
                ]
            else:
                 query_components_value[comp] = QueryHelper.rewrite_with_alias(
                     query_components_value[comp], alias_map
                 )
    
    @staticmethod
    def parse_where_clause(where_clause: str, current_node: QueryTree) -> QueryTree:
        # Tokenize the WHERE clause into conditions split by AND
        parsed_result = re.split(r'\sAND\s', where_clause)

        # Dictionary to group conditions by table
        table_conditions = {}

        for parse in parsed_result:
            # If OR is present, handle it as a single condition
            if "OR" in parse:
                parse_node = QueryTree(type="WHERE", val=parse)
                current_node.add_child(parse_node)
                parse_node.add_parent(current_node)
            else:
                # Extract the table name from the condition
                match = re.match(r'(\w+)\.', parse)
                if match:
                    table_name = match.group(1)
                    # Group conditions by table name
                    if table_name not in table_conditions:
                        table_conditions[table_name] = []
                    table_conditions[table_name].append(parse)
        
        # Add grouped conditions to the tree
        for table, conditions in table_conditions.items():
            if len(conditions) > 1:
                # If multiple conditions for the same table, group them into an array
                parse_node = QueryTree(type="WHERE", val=conditions)
            else:
                # If only one condition, store it as a string
                parse_node = QueryTree(type="WHERE", val=conditions[0])
            current_node.add_child(parse_node)
            parse_node.add_parent(current_node)

        return current_node


    @staticmethod
    def gather_attributes(node: QueryTree, database_name: str, get_stats: Callable[[str, str, int], Union[Statistic, Exception]]):
        if(node.type =="TABLE"):
            return get_stats(database_name,node.val.strip().lower()).V_a_r.keys()
        
        if(node.type == "JOIN"):
            return QueryHelper.gather_attributes(node.childs[0],database_name,get_stats) | QueryHelper.gather_attributes(node.childs[1],database_name,get_stats)
        
        if(node.type == "SELECT"):
            return set(node.val)
        
        return QueryHelper.gather_attributes(node.childs[0],database_name,get_stats)
        
    @staticmethod
    def build_join_tree(from_tokens: list, database_name: str, get_stats: Callable[[str, str, int], Union[Statistic, Exception]]) -> QueryTree:
        first_table_node = QueryTree(type="TABLE", val=from_tokens.pop(0))

        if len(from_tokens) == 0:
            return first_table_node
        
        return QueryHelper.__recursive_build_join(first_table_node, from_tokens, database_name, get_stats)
    
    @staticmethod
    def __recursive_build_join(query_tree: QueryTree, join_tokens: list, database_name: str, get_stats: Callable[[str, str, int], Union[Statistic, Exception]]) -> QueryTree:
        join_type = join_tokens.pop(0)
        if(join_type == "NATURAL JOIN"):
            other_table = join_tokens.pop(0)
            natural_attributes = list(QueryHelper.gather_attributes(query_tree,database_name,get_stats) & get_stats(database_name,other_table.strip().lower()).V_a_r.keys())
            natural_attributes = [attr.upper() for attr in natural_attributes]
            join_node = QueryTree(type="JOIN", val=natural_attributes)
        else:
            value = join_tokens.pop(0).split(" ON ")
            other_table = value[0]
            join_node = QueryTree(type="JOIN", val=value[1])

        query_tree.add_parent(join_node)
        right_table_node = QueryTree(type="TABLE", val=other_table)
        right_table_node.add_parent(join_node)
        join_node.add_child(query_tree)
        join_node.add_child(right_table_node)
        
        if len(join_tokens) == 0:
            return join_node
        
        return QueryHelper.__recursive_build_join(query_tree=join_node, join_tokens=join_tokens, database_name=database_name,get_stats=get_stats)