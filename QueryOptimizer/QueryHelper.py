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
    def parse_where_clause(where_clause: str) -> QueryTree:
        # Tokenize the WHERE clause into conditions and operators
        tokens = re.split(r'(\bAND\b|\bOR\b)', where_clause)
        tokens = [token.strip() for token in tokens if token.strip()]

        def build_tree(tokens: list) -> QueryTree:
            if len(tokens) == 1:  # Base case: single condition
                return QueryTree(type="WHERE", val=tokens[0])

            # Find the lowest-precedence operator (OR > AND)
            if "OR" in tokens:
                operator_index = tokens.index("OR")
            elif "AND" in tokens:
                operator_index = tokens.index("AND")
            else:
                raise ValueError("Invalid WHERE clause")

            # Split into left and right parts
            left_tokens = tokens[:operator_index]
            right_tokens = tokens[operator_index + 1:]
            operator = tokens[operator_index]

            # Create the operator node
            operator_node = QueryTree(type="LOGIC", val=operator)

            # Recursively build left and right subtrees
            left_tree = build_tree(left_tokens)
            right_tree = build_tree(right_tokens)

            # Attach left and right subtrees to the operator node
            operator_node.add_child(left_tree)
            operator_node.add_child(right_tree)
            left_tree.add_parent(operator_node)
            right_tree.add_parent(operator_node)

            return operator_node

        return build_tree(tokens)
    
    @staticmethod
    def build_join_tree(from_tokens: list) -> QueryTree:
        first_table_node = QueryTree(type="TABLE", val=from_tokens.pop(0))

        if len(from_tokens) == 0:
            return first_table_node
        
        return QueryHelper.__build_explicit_join(first_table_node, from_tokens)
    
    @staticmethod
    def __build_explicit_join(query_tree: QueryTree, join_tokens: list) -> QueryTree:
        join_tokens.pop(0)
        value = join_tokens.pop(0).split(" ON ")

        join_node = QueryTree(type="JOIN", val=value[1])
        query_tree.add_parent(join_node)
        right_table_node = QueryTree(type="TABLE", val=value[0])
        right_table_node.add_parent(join_node)
        join_node.add_child(query_tree)
        join_node.add_child(right_table_node)
        
        if len(join_tokens) == 0:
            return join_node
        
        return QueryHelper.__build_explicit_join(query_tree=join_node, join_tokens=join_tokens)