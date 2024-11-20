from QueryTree import ParsedQuery, QueryTree
from QueryHelper import *

class OptimizationEngine:
    def __init__(self):
        pass

    def parse_query(self, query: str) -> ParsedQuery:
        normalized_query = QueryHelper.remove_excessive_whitespace(
            QueryHelper.normalize_string(query).upper()
        )

        components = ["SELECT", "UPDATE", "DELETE", "FROM", "SET", "WHERE", "ORDER BY", "LIMIT"]

        query_components_value = {}

        i = 0
        while i < len(components):
            idx_first_comp = normalized_query.find(components[i])
            if idx_first_comp == -1:
                i += 1
                continue

            if i == len(components) - 1:  # Last component (LIMIT)
                query_components_value[components[i]] = QueryHelper.extract_value(
                    query, components[i], ""
                )
                break

            j = i + 1
            idx_second_comp = normalized_query.find(components[j])
            while idx_second_comp == -1 and j < len(components) - 1:
                j += 1
                idx_second_comp = normalized_query.find(components[j])

            query_components_value[components[i]] = QueryHelper.extract_value(
                query, components[i], "" if idx_second_comp == -1 else components[j]
            )

            i += 1

        query_tree = self.__build_query_tree(query_components_value)
        return ParsedQuery(query_tree, query)

    def __build_query_tree(self, components: dict) -> QueryTree:
        """
        Build a query tree with JOIN handling.
        """
        # Root node
        root = QueryTree(type_="QUERY", val="ROOT")

        for key, value in components.items():
            clause_node = QueryTree(type_=key, val=str(value))
            if key == "FROM" and isinstance(value, list):
                # Handle JOINs specifically
                current_table = None
                for item in value:
                    if item == "JOIN":
                        join_node = QueryTree(type_="JOIN", val="JOIN")
                        clause_node.add_child(join_node)
                    else:
                        if current_table is None:
                            current_table = QueryTree(type_="TABLE", val=item)
                            clause_node.add_child(current_table)
                        else:
                            current_table = QueryTree(type_="TABLE", val=item)
                            clause_node.add_child(current_table)
            else:
                clause_node.add_child(QueryTree(type_="VALUE", val=str(value)))

            root.add_child(clause_node)

        return root

    def optimize_query(self, query: ParsedQuery) -> ParsedQuery:
        # Placeholder for future optimization logic
        pass

    def __get_cost(self, query: ParsedQuery) -> int:
        # Placeholder for query cost estimation
        pass


if __name__ == "__main__":
    new = OptimizationEngine()

    # Test SELECT query with JOIN
    select_query = "SELECT a, b FROM students JOIN teacher ON students.id = teacher.id WHERE a > 1"
    print(select_query)
    parsed_query = new.parse_query(select_query)
    print(parsed_query)

    # Test UPDATE query
    update_query = "UPDATE employee SET salary = salary * 1.1 WHERE salary > 1000"
    print(update_query)
    parsed_update_query = new.parse_query(update_query)
    print(parsed_update_query)
