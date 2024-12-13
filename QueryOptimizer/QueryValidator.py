import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from StorageManager.classes import Statistic, StorageEngine
from .QueryHelper import *
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

    def normalize_type(self, column_type: str) -> str:
        if column_type.startswith("varchar"):
            return "varchar"
        return column_type

    def get_attribute_types(self, where_clause: str, database_name: str, table_arr: list[str]) -> dict:
        storage_engine = StorageEngine()
        # Regex to find comparisons (attribute and literal pairs)
        comparison_pattern = r"([\w\.]+)\s*(=|<>|>|>=|<|<=)\s*([\w\.'\"]+)"
        matches = re.findall(comparison_pattern, where_clause)

        # Initialize the result dictionary
        attribute_types = {}

        # Process each match (attribute and literal)
        for left_attr, operator, right_attr in matches:
            for attr in [left_attr, right_attr]:
                # Skip literals
                if bool(re.fullmatch(r"[+-]?(\d*\.\d+|\d+\.?\d*)", attr)) or (attr.startswith(("'", '"')) or attr.endswith(("'", '"'))):
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


    def validate_comparisons(self, where_clause: str, attribute_types: dict):
        # Regex to find comparisons
        comparison_pattern = r"([\w\.]+)\s*(=|<>|>|>=|<|<=)\s*([\w\.'\"]+)"
        matches = re.findall(comparison_pattern, where_clause)

        for left_attr, operator, right_attr in matches:
            # Determine the type of the left attribute
            left_type = attribute_types.get(left_attr)
            if left_type is None:
                raise ValueError(f"Unknown attribute: {left_attr}")
            
            left_type = self.normalize_type(left_type)

            # Determine the type of the right attribute or literal
            if right_attr in attribute_types:  # It's an attribute
                right_type = attribute_types[right_attr]
            else:  # It's a literal
                if right_attr.isdigit():  # Check for numeric literals
                    right_type = "integer"
                elif right_attr.replace(".", "", 1).isdigit():  # Check for float literals
                    right_type = "float"
                else:  # Infer type for string literals
                    try:
                        right_type = self.__infer_literal_type(right_attr)
                    except ValueError:
                        raise ValueError(f"Unknown literal type for value: {right_attr}")
            
            right_type = self.normalize_type(right_type)

            # Check if both types are supported
            if left_type not in self.SUPPORTED_TYPES or right_type not in self.SUPPORTED_TYPES:
                raise ValueError(f"Unsupported data type(s) in comparison: {left_attr} ({left_type}) and {right_attr} ({right_type})")

            # Check compatibility of the types
            if right_type not in self.COMPATIBLE_TYPES.get(left_type, {}):
                raise ValueError(f"Incompatible types in comparison: {left_attr} ({left_type}) and {right_attr} ({right_type})")

    def __infer_literal_type(self, value: str) -> str:
        if value.isdigit():
            return "integer"
        try:
            float(value)
            return "float"
        except ValueError:
            pass
        if (value.startswith("'") or value.endswith("'")) or (value.startswith('"') or value.endswith('"')):
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
        filtered_aliases = {alias for alias in undefined_aliases if not alias.isdigit()}

        if filtered_aliases:
            raise ValueError(f"Undefined aliases detected: {', '.join(undefined_aliases)}")
    
    def validate_tables(self,table_arr: list[str], database_name: str, get_stats: Callable[[str, str, int], Union[Statistic, Exception]]):
        """validate tables and also gather all attributes accross all tables

        Args:
            table_arr (list[str])
            database_name (str)
            get_stats (Callable[[str, str, int], Union[Statistic, Exception]])

        Returns:
            result list[str]: list of all attributes from all tables 
        """
        result = []
        for table in table_arr:
            attributes = list(get_stats(database_name,table).V_a_r.keys())
            attributes = [f"{table}.{attr}" for attr in attributes]
            result += attributes
        return result
    
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
                            
                            # isAttribute
                            if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', tokens[i].replace('.', '')) and tokens[i].count('.') <= 1:
                                tokens[i] = self.validate_attribute(tokens[i], database_name, get_stats, table_arr)
                        components_values[key][index] = " ".join(tokens).strip()
                    except ValueError:
                        continue
            
            elif key == 'SELECT':
                components_values[key] = [self.validate_attribute(attr, database_name, get_stats, table_arr) for attr in components_values[key]]
                
            elif key == "INDEX":
                splitted = components_values[key].split()
                self.validate_attribute(splitted[4],database_name,get_stats,table_arr)
