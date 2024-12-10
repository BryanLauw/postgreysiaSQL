import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from StorageManager.classes import Statistic, StorageEngine
from QueryHelper import *
from typing import Callable, Union

class QueryValidator:
    # Define supported types and compatible comparisons
    SUPPORTED_TYPES = {"integer", "float", "char", "varchar"}
    COMPATIBLE_TYPES = {
        "integer": {"integer", "float"},
        "float": {"integer", "float"},
        "char": {"char", "varchar"},
        "varchar": {"char", "varchar"},
    }

    def validate_comparisons(self,where_clause: str, attribute_types: dict):
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

    def __infer_literal_type(self,value: str) -> str:
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
    
    def validate_aliases(self,query_components: dict, alias_map: dict, table_arr: List[str]):
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

        undefined_aliases = used_aliases - set(alias_map.keys()) - set(table_arr)
        if undefined_aliases:
            raise ValueError(f"Undefined aliases detected: {', '.join(undefined_aliases)}")
    
    def validate_tables(self,table_arr: list[str], database_name: str, get_stats: Callable[[str, str, int], Union[Statistic, Exception]]):
        for table in table_arr:
            get_stats(database_name,table)
    
    def validate_attribute(self,attribute: str,database_name: str, get_stats: Callable[[str, str, int], Union[Statistic, Exception]],table_arr: list[str]):
        if attribute in ["AND","OR"]:
            return attribute
        
        attr_with_table = ""
        if '.' in attribute:
            table, attr = attribute.split('.')
            if attr not in get_stats(database_name,table).V_a_r:
                raise ValueError(f"{attr} doesn't exist at table {table}")
            attr_with_table = attribute
        else:
            for table in table_arr:
                if attribute in get_stats(database_name,table).V_a_r:
                    if attr_with_table:
                        raise ValueError(f"Ambiguous attribute: {attribute}")
                    attr_with_table = f"{table}.{attribute}"
                    
        if not attr_with_table:
            raise ValueError(f"{attribute} doesn't exist!")
        return attr_with_table
    
    def extract_and_validate_attributes(self,components_values: Dict[str, Union[List[str],str]], database_name: str, get_stats: Callable[[str, str, int], Union[Statistic, Exception]],table_arr: list[str]):
        for key in (components_values):
            if key == 'UPDATE':
                continue
            elif key == 'WHERE' or key == 'SET':
                splitted = components_values[key].split()
                for index,token in enumerate(splitted):
                    if token.count('.')<=1 and token.replace('.','').isalpha():
                        splitted[index] = self.validate_attribute(token, database_name, get_stats, table_arr)
                components_values[key] = " ".join(splitted).strip()
                
            elif key == 'ORDER BY':
                attribute, order = components_values[key].split()
                components_values[key] = f"{self.validate_attribute(attribute, database_name, get_stats, table_arr)} {order}"
                
            elif key == 'FROM':
                for index,clause in enumerate(components_values[key]):
                    tokens = clause.split()
                    try:
                        ON_idx = tokens.index('ON')
                        len_tokens = len(tokens)
                        for i in range(ON_idx+1,len_tokens):
                            if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', tokens[i].replace('.', '')) and tokens[i].count('.') <= 1:
                                tokens[i] = self.validate_attribute(tokens[i], database_name, get_stats, table_arr)
                        components_values[key][index] = " ".join(tokens).strip()
                    except ValueError:
                        continue
                    
            elif key == 'SELECT':
                components_values[key] = [self.validate_attribute(attr, database_name, get_stats, table_arr) for attr in components_values[key]]
