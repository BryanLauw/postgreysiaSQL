from QueryTree import ParsedQuery, QueryTree
import re
from typing import List, Union, Dict

class QueryHelper:
    @staticmethod
    def extract_table_aliases(from_tokens: list) -> dict:
        alias_map = {}
        for token in from_tokens:
            if " AS " in token:
                table, alias = map(str.strip, token.split(" AS "))
                alias_map[alias.split()[0]] = table
            elif " " in token:  # Implicit alias
                parts = token.split()
                alias_map[parts[1]] = parts[0]
        return alias_map
    
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
    def remove_excessive_whitespace(query: str):
        words = query.split()
        new_query = " ".join(words)
        return new_query
    
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
    def extract_SELECT(values: str):
        arr_attributes = [value.strip() for value in values.split(",")]
        return arr_attributes

    @staticmethod
    def extract_FROM(values: str):
        arr_joins = []
        values_parsed = values.split()
        element = ""
        i = 0
        while i < len(values_parsed):
            if values_parsed[i] == "JOIN":
                if element:
                    arr_joins.append(element.strip())
                arr_joins.append("JOIN")
                element = ""
            else:
                element += " " + values_parsed[i]
            i += 1

        if element:
            arr_joins.append(element.strip())

        return arr_joins


    @staticmethod
    def extract_WHERE(values: str):
        return values
    
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
    def extract_ORDERBY(values: str):
        return values.strip()

    @staticmethod
    def extract_LIMIT(values: str):
        return int(values)

    @staticmethod
    def extract_UPDATE(values: str):
        return values.strip()

    @staticmethod
    def extract_SET(values: str):
        return [value.strip() for value in values.split(",")]

    @staticmethod
    def extract_value(query: str, before: str, after: str):
        start = query.find(before) + len(before)
        if after == "":
            end = len(query)
        else:
            end = query.find(after)
        extracted = query[start:end]
        if before == "SELECT":
            extracted = QueryHelper.extract_SELECT(extracted)
        elif before == "FROM":
            extracted = QueryHelper.extract_FROM(extracted)
        elif before == "WHERE":
            extracted = QueryHelper.extract_WHERE(extracted)
        elif before == "ORDER BY":
            extracted = QueryHelper.extract_ORDERBY(extracted)
        elif before == "LIMIT":
            extracted = QueryHelper.extract_LIMIT(extracted)
        elif before == "UPDATE":
            extracted = QueryHelper.extract_UPDATE(extracted)
        elif before == "SET":
            extracted = QueryHelper.extract_SET(extracted)
        return extracted
    
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