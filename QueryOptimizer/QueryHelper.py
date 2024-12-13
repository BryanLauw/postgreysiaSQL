import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from StorageManager.classes import Statistic, StorageEngine
from .QueryTree import ParsedQuery, QueryTree
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
    def extract_table_and_aliases(table_tokens: list[str]) -> {dict, list[str]}:
        alias_map = {}
        attribute_arr = []
        
        defined_aliases = set()
        
        for token in table_tokens:
            if token in ["JOIN", "NATURAL JOIN", ","]:
                continue
            
            splitted = token.split()
            attribute_arr.append(splitted[0])
            defined_aliases.add(splitted[0])  
            
            try:
                idx_AS = splitted.index("AS")
                alias_map[splitted[idx_AS + 1]] = splitted[idx_AS - 1]
                defined_aliases.add(splitted[idx_AS + 1])
            except ValueError:
                pass
            
            if "ON" in token:
                splitted = token.split(" ON ")
                table_aliases_ON = re.findall(r"(\b\w+\b)\.", splitted[1])
                
                for table in table_aliases_ON:
                    if table not in defined_aliases:
                        raise ValueError(f"Alias or table '{table}' is used before being defined.")
        
        return alias_map, attribute_arr
    
    @staticmethod
    def get_tables_regex(val: str):
        table = re.findall(r'\b(\w+)\.(?=\w)', val)
        if not table:
            table = [val]
        return table
    
    @staticmethod
    def get_other_expression(expression, target):
        # print(expression," : " ,target)
        tokens = expression.split()
        operators = {"AND", "OR"}
        result = []
        current_operator = None

        for i, token in enumerate(tokens):
            if token in operators:
                current_operator = token
            elif token == target:
                before_operator = tokens[i - 1] if i > 0 and tokens[i - 1] in operators else None
                after_operator = tokens[i + 1] if i < len(tokens) - 1 and tokens[i + 1] in operators else None
                related_operator = before_operator or after_operator
            else:
                result.append(token)
        result = " ".join(result).replace(target, "").strip()
        return result, current_operator
    
    @staticmethod
    def get_tables_defined(node: QueryTree):
        if node.type == "TABLE":
            return [node.val]
        if node.type == "WHERE":
            return QueryHelper.get_tables_defined(node.childs[0])        
        # JOIN
        return QueryHelper.get_tables_defined(node.childs[0]) + QueryHelper.get_tables_defined(node.childs[1])
    
    @staticmethod
    def remove_aliases(from_clause: Union[list,str]) -> list:
        def strip_alias(from_str: str) -> str:
            cleaned_str = re.sub(r"\s+as\s+\w+\s*", " ", from_str, flags=re.IGNORECASE)
            return cleaned_str

        if(isinstance(from_clause, str)):
            return strip_alias(from_clause)
        
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
    def extract_table_and_column_from_condition(condition: str) -> tuple:
        match = re.match(r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*[=<>!]+\s*.+', condition)
        if match:
            table_name = match.group(1)  # Extract table name
            column_name = match.group(2)  # Extract column name
            return table_name, column_name
        return None, None
    
    @staticmethod
    def parse_where_clause(where_clause: str, current_node: QueryTree, database_name: str) -> QueryTree:
        storage_engine = StorageEngine()
        # print(storage_engine.retrieve_table_of_database(database_name))
        # Tokenize the WHERE clause into conditions
        parsed_result = re.split(r'\sAND\s', where_clause)
        print("parsed", parsed_result)

        for parse in parsed_result:
            if "OR" in parse:
                sub_conditions = re.split(r'\sOR\s', parse)
                for sub_condition in sub_conditions:
                    sub_condition = sub_condition.strip()
                    table_name, column = QueryHelper.extract_table_and_column_from_condition(sub_condition)
                    # print(table_name, column)
                    try:
                        if (storage_engine.is_hash_index_in_block(database_name, table_name, column) or 
                            storage_engine.is_bplus_index_in_block(database_name, table_name, column)):
                            method = "INDEX SCAN"
                    except Exception as e:
                        method = "FULL SCAN"
                parse_node = QueryTree(type="WHERE", val=parse, method=method)
                current_node.add_child(parse_node)
                parse_node.add_parent(current_node)
                current_node = parse_node
            else:
                parse = parse.strip()
                table_name, column = QueryHelper.extract_table_and_column_from_condition(parse)
                # print(table_name, column)
                try:
                    if (storage_engine.is_hash_index_in_block(database_name, table_name, column) or 
                        storage_engine.is_bplus_index_in_block(database_name, table_name, column)):
                        method = "INDEX SCAN"
                except Exception as e:
                    method = "FULL SCAN"
                parse_node = QueryTree(type="WHERE", val=parse, method=method)
                current_node.add_child(parse_node)
                parse_node.add_parent(current_node)
                current_node = parse_node
        return parse_node

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
        if(join_type in ["NATURAL JOIN",","]):
            other_table = join_tokens.pop(0)
            natural_attributes = list(QueryHelper.gather_attributes(query_tree,database_name,get_stats) & get_stats(database_name,other_table.strip().lower()).V_a_r.keys())
            natural_attributes = [attr for attr in natural_attributes]
            join_node = QueryTree(type="NATURAL JOIN", val=natural_attributes)
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