from QueryTree import ParsedQuery, QueryTree
import re
from typing import List, Union, Dict

class QueryHelper:
    @staticmethod
    def remove_aliases(from_clause: list) -> list:
        """
        Removes aliases from the FROM clause. Keeps only the table names.
        Handles both individual tables and join clauses in the form of a list.
        """
        def strip_alias(from_str: str) -> str:
            cleaned_str = re.sub(r"\s+as\s+\w+\s*", " ", from_str, flags=re.IGNORECASE)
            return cleaned_str

        # Process each token in the FROM clause
        return [strip_alias(token) if token.upper() not in ["JOIN", "ON", "NATURAL"] else token for token in from_clause]


    @staticmethod
    def rewrite_with_alias(expression: str, alias_map: dict) -> str:
        """
        Rewrite column references in the expression using table aliases.
        For example, 's.a' becomes 'students.a' based on alias_map {'s': 'students'}.
        """
        for alias, table in alias_map.items():
            pattern = rf"(?<!\w){re.escape(alias)}\."
            replacement = table + "."
            expression = re.sub(pattern, replacement, expression)
        return expression
    
    @staticmethod
    def remove_excessive_whitespace(query: str):
        words = query.split()
        new_query = " ".join(words)
        return new_query
    
    @staticmethod
    def extract_table_aliases(from_clause: List[str]) -> dict:
        alias_map = {}

        for token in from_clause:
            if " AS " in token:
                splitted = token.split(" AS ")
                splitted_len = len(splitted)
                for i in range(splitted_len-1):
                    table = splitted[i].split()[-1]
                    alias = splitted[i+1].split()[0]
                    alias_map[alias] = table
        return alias_map
    
    @staticmethod
    def extract_tables(from_clause: List[str]) -> List[str]:
        """ Get List of Tables in from clause. 

        Args:
            from_clause (List[str]): List of from clauses that doesnt have any aliases in it

        Returns:
            List[str]: List of Tables
        """        
        
        table_arr = []
        for token in from_clause:
            splitted = token.split()
            if("NATURAL JOIN" in token):
                if(splitted[0] == "NATURAL"):
                    table_arr.append(splitted[2])
                else:
                    table_arr.append(splitted[0])
                    table_arr.append(splitted[3])
                    
            elif "JOIN" in token or "," in token:
                if(splitted[0] == "JOIN" or splitted[0] == ","):
                    table_arr.append(splitted[1])
                else:
                    table_arr.append(splitted[0])
                    table_arr.append(splitted[2])
        return table_arr
    
    @staticmethod
    def extract_attributes(components_values: Dict[str, Union[List[str],str]]):
        attributes_arr = []
        for key in components_values:
            if key == 'UPDATE':
                attributes_arr.append(components_values[key])
            elif key == 'WHERE' or key == 'SET':
                splitted = components_values[key].split()
                for token in splitted:
                    if token.count('.')<=1 and token.replace('.','').isalpha():
                        attributes_arr.append(token)
            elif key == 'ORDER BY':
                attributes_arr.append(components_values[key].split()[0])
            elif key == 'FROM':
                for clause in components_values[key]:
                    tokens = clause.split()
                    if tokens[0] == 'NATURAL' or tokens[1] == 'NATURAL':
                        continue
                    try:
                        ON_idx = tokens.index('ON')
                        len_tokens = len(tokens)
                        for i in range(ON_idx+1,len_tokens):
                            if tokens[i].count('.')<=1 and tokens[i].replace('.','').isalpha():
                                attributes_arr.append(tokens[i])
                    except ValueError:
                        continue
            elif key == 'SELECT':
                attributes_arr.extend(components_values['SELECT'])
                
        return list(set(attributes_arr))
                    
    @staticmethod
    def normalize_string(query: str):
        return query.replace("\t", "").replace("\n", "").replace("\r", "")
    
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
