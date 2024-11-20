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

        print(query_components_value)
        return ParsedQuery(QueryTree("ROOT", "QUERY", []), query)

    def optimize_query(self, query: ParsedQuery) -> ParsedQuery:
        # Future optimization logic
        pass

    def __get_cost(self, query: ParsedQuery) -> int:
        # Placeholder for query cost estimation
        pass


if __name__ == "__main__":
    new = OptimizationEngine()

    # Test SELECT query
    select_query = "SELECT a, b, c FROM students JOIN teacher t ON students.id = t.id WHERE a=1 ORDER BY a DESC LIMIT 1"
    print(select_query)
    new.parse_query(select_query)

    # Test UPDATE query
    update_query = "UPDATE employee SET salary=1.05*salary WHERE salary > 1000"
    print(update_query)
    new.parse_query(update_query)
