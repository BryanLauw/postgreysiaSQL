from QueryTree import ParsedQuery, QueryTree
import re
from typing import List, Union, Dict

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
            if token=="JOIN" or token=="NATURAL JOIN":
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

        # Process each token in the FROM clause
        return [strip_alias(token) if token.upper() not in ["JOIN", "ON", "NATURAL"] else token for token in from_clause]
    
    @staticmethod
    def rewrite_with_alias(expression: str, alias_map: dict) -> str:
        for alias, table in alias_map.items():
            pattern = rf"(?<!\w){re.escape(alias)}\."
            replacement = table + "."
            expression = re.sub(pattern, replacement, expression)
        return expression
    
    @staticmethod
    def validate_aliases(query_components: dict, alias_map: dict):
        def find_aliases(expression: str) -> set:
            aliases = set()
            tokens = expression.split()
            for token in tokens:
                if "." in token:  # Check for alias usage
                    alias = token.split(".")[0]
                    aliases.add(alias)
            return aliases

        used_aliases = set()

        if "SELECT" in query_components:
            for attr in query_components["SELECT"]:
                used_aliases.update(find_aliases(attr))
        if "WHERE" in query_components:
            used_aliases.update(find_aliases(query_components["WHERE"]))
        if "FROM" in query_components:
            for token in query_components["FROM"]:
                if "." in token:
                    used_aliases.update(find_aliases(token))

        # Find undefined aliases
        undefined_aliases = used_aliases - set(alias_map.keys())
        if undefined_aliases:
            raise ValueError(f"Undefined aliases detected: {', '.join(undefined_aliases)}")
    
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
    
    # Define supported types and compatible comparisons
    SUPPORTED_TYPES = {"integer", "float", "char", "varchar"}
    COMPATIBLE_TYPES = {
        "integer": {"integer", "float"},
        "float": {"integer", "float"},
        "char": {"char", "varchar"},
        "varchar": {"char", "varchar"},
    }

    @staticmethod
    def validate_comparisons(where_clause: str, attribute_types: dict):
        # Regex to find comparisons
        comparison_pattern = r"([\w\.]+)\s*(=|<>|>|>=|<|<=)\s*([\w\.'\"]+)"
        matches = re.findall(comparison_pattern, where_clause)

        for left_attr, operator, right_attr in matches:
            left_type = attribute_types.get(left_attr)
            right_type = attribute_types.get(right_attr)

            # Handle literal values
            if left_type is None:
                left_type = QueryHelper.infer_literal_type(left_attr)
            if right_type is None:
                right_type = QueryHelper.infer_literal_type(right_attr)

            # Check if both types are supported
            if left_type not in QueryHelper.SUPPORTED_TYPES or right_type not in QueryHelper.SUPPORTED_TYPES:
                raise ValueError(f"Unsupported data type(s) in comparison: {left_attr} ({left_type}) and {right_attr} ({right_type})")

            # Check compatibility of the types
            if right_type not in QueryHelper.COMPATIBLE_TYPES.get(left_type, {}):
                raise ValueError(f"Incompatible types in comparison: {left_attr} ({left_type}) and {right_attr} ({right_type})")

    @staticmethod
    def infer_literal_type(value: str) -> str:
        if value.isdigit():
            return "integer"
        try:
            float(value)
            return "float"
        except ValueError:
            pass
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            return "varchar"
        raise ValueError(f"Unknown literal type for value: {value}")