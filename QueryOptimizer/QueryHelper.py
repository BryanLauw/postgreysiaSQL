from QueryTree import ParsedQuery, QueryTree

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
                alias_map[alias] = table
            elif " " in token:  # Implicit alias
                parts = token.split()
                alias_map[parts[1]] = parts[0]
        return alias_map

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
