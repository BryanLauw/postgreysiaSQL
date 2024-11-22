from QueryTree import ParsedQuery, QueryTree

class QueryHelper:

    @staticmethod
    def remove_excessive_whitespace(query: str):
        words = query.split()
        new_query = " ".join(words)
        return new_query
    
    @staticmethod
    def extract_table_aliases(from_clause: list) -> dict:
        alias_map = {}
        cleaned_from_clause = []

        for token in from_clause:
            if " ON " in token:
                token = token.split(" ON ")[0].strip()
            cleaned_from_clause.append(token)

        for token in cleaned_from_clause:
            if " AS " in token:
                table, alias = token.split(" AS ")
                alias_map[alias.strip()] = table.strip()
            elif " " in token and token.upper() not in ["JOIN", "ON"]:
                parts = token.split()
                if len(parts) == 2:
                    alias_map[parts[1].strip()] = parts[0].strip()

        print(f"Final alias map: {alias_map}")
        return alias_map



    @staticmethod
    def normalize_string(query: str):
        return query.replace("\t", "").replace("\n", "").replace("\r", "")
    
    @staticmethod
    def build_join_tree(from_tokens: list, alias_map: dict) -> QueryTree:
        """
        Build a query tree for the FROM clause, handling JOIN operations.
        Strips aliases like 'AS s' to keep only table names and rewrites JOIN conditions.
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
        return QueryHelper.__build_explicit_join(first_table_node, from_tokens, strip_alias, alias_map)


    @staticmethod
    def __build_explicit_join(query_tree: QueryTree, join_tokens: list, strip_alias, alias_map: dict) -> QueryTree:
        """
        Recursive function to build JOIN tree while stripping aliases and replacing alias references in conditions.
        """
        join_tokens.pop(0)  # Remove "JOIN"
        join_clause = join_tokens.pop(0)

        # Split the JOIN clause into table and ON condition
        if " ON " in join_clause:
            table_with_alias, condition = join_clause.split(" ON ", 1)
        else:
            raise ValueError(f"Malformed JOIN clause: Missing 'ON' condition in '{join_clause}'.")

        # Remove aliases from the right table
        right_table = strip_alias(table_with_alias.strip())

        # Rewrite the ON condition to replace aliases with full table names
        def rewrite_condition(cond: str, alias_map: dict) -> str:
            tokens = cond.split()
            rewritten_tokens = []
            for token in tokens:
                # Check if the token is an alias.column reference
                if "." in token:
                    alias, column = token.split(".", 1)
                    if alias in alias_map:
                        token = f"{alias_map[alias]}.{column}"  # Replace alias with table name
                rewritten_tokens.append(token)
            return " ".join(rewritten_tokens)

        rewritten_condition = rewrite_condition(condition.strip(), alias_map)

        # Create the JOIN node with the rewritten condition
        join_node = QueryTree(type="JOIN", val=rewritten_condition)
        query_tree.add_parent(join_node)
        right_table_node = QueryTree(type="TABLE", val=right_table)
        right_table_node.add_parent(join_node)
        join_node.add_child(query_tree)
        join_node.add_child(right_table_node)

        # If no more JOINs, return the JOIN node
        if len(join_tokens) == 0:
            return join_node

        # Recursively process remaining JOINs
        return QueryHelper.__build_explicit_join(query_tree=join_node, join_tokens=join_tokens, strip_alias=strip_alias, alias_map=alias_map)
