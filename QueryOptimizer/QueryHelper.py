from QueryTree import ParsedQuery, QueryTree
import re

class QueryHelper:
    @staticmethod
    def extract_table_aliases(from_tokens: list) -> dict:
        """
        Extract table aliases from the FROM clause.
        Returns a dictionary mapping aliases to table names.
        """
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
            expression = expression.replace(alias + ".", table + ".")
        return expression
    
    @staticmethod
    def remove_excessive_whitespace(query: str):
        words = query.split()
        new_query = " ".join(words)
        return new_query

    @staticmethod
    def normalize_string(query: str):
        return query.replace("\t", "").replace("\n", "").replace("\r", "")
    
    @staticmethod
    def build_join_tree(from_tokens: list) -> QueryTree:
        """
        Build a query tree for the FROM clause, handling JOIN operations.
        Strips aliases like 'AS s' to keep only table names.
        """
        # Function to remove aliases from table names
        def strip_alias(table: str) -> str:
            if " AS " in table:
                return table.split(" AS ")[0].strip()
            elif " " in table:  # Implicit alias without 'AS'
                return table.split()[0].strip()
            return table.strip()

        # Process the first table
        first_table_node = QueryTree(type="TABLE", val=strip_alias(from_tokens.pop(0)))

        # If there's no JOIN, return the single table node
        if len(from_tokens) == 0:
            return first_table_node

        # Process JOINs recursively
        return QueryHelper.__build_explicit_join(first_table_node, from_tokens, strip_alias)

    @staticmethod
    def __build_explicit_join(query_tree: QueryTree, join_tokens: list, strip_alias) -> QueryTree:
        """
        Recursive function to build JOIN tree while stripping aliases.
        """
        join_tokens.pop(0)  # Remove "JOIN"
        value = join_tokens.pop(0).split(" ON ")

        # Remove aliases from the right table
        right_table = strip_alias(value[0])

        # Create the JOIN node
        join_node = QueryTree(type="JOIN", val=value[1])
        query_tree.add_parent(join_node)
        right_table_node = QueryTree(type="TABLE", val=right_table)
        right_table_node.add_parent(join_node)
        join_node.add_child(query_tree)
        join_node.add_child(right_table_node)

        # If no more JOINs, return the JOIN node
        if len(join_tokens) == 0:
            return join_node

        # Recursively process remaining JOINs
        return QueryHelper.__build_explicit_join(query_tree=join_node, join_tokens=join_tokens, strip_alias=strip_alias)
