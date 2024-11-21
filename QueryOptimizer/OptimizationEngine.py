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

        print(f"query_components_value: {query_components_value}") # testing

        query_tree = self.__build_query_tree(query_components_value)
        return ParsedQuery(query_tree, query)

    def __build_query_tree(self, components: dict) -> QueryTree:

        root = QueryTree(type="ROOT")
        top = root

        if "LIMIT" in components:
            limit_tree = QueryTree(type="LIMIT", val=components["LIMIT"])
            top.add_child(limit_tree)
            limit_tree.add_parent(top)
            top = limit_tree
        
        if "ORDER BY" in components:
            order_by_tree = QueryTree(type="ORDER BY", val=components["ORDER BY"])
            top.add_child(order_by_tree)
            order_by_tree.add_parent(top)
            top = order_by_tree
        
        if "SELECT" in components:
            for attribute in components['SELECT']:
                select_tree = QueryTree(type="SELECT", val=attribute)
                top.add_child(select_tree)
                select_tree.add_parent(top)
                top = select_tree
                
        if "UPDATE" in components:
            where_tree = QueryTree(type="UPDATE", val=components["UPDATE"])
            top.add_child(where_tree)
            where_tree.add_parent(top)
            top = where_tree

        # if "DELETE" in components:
        #     where_tree = QueryTree(type="DELETE", val=components["DELETE"])
        #     top.add_child(where_tree)
        #     where_tree.add_parent(top)
        #     top = where_tree
        
        if "WHERE" in components:
            where_tree = QueryTree(type="WHERE", val=components["WHERE"])
            top.add_child(where_tree)
            where_tree.add_parent(top)
            top = where_tree
        
        if "FROM" in components:
            join_tree = QueryHelper.build_join_tree(components["FROM"])
            top.add_child(join_tree)
            join_tree.add_parent(top)

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
    select_query = "SELECT a, b FROM students JOIN teacher ON students.id = teacher.id JOIN teacher ON students.id = teacher.id WHERE a > 1 ORDER BY apalah LIMIT 10"
    print(select_query)
    parsed_query = new.parse_query(select_query)
    print(parsed_query)

    # Test UPDATE query
    update_query = "UPDATE employee SET salary = salary * 1.1 WHERE salary > 1000"
    print(update_query)
    parsed_update_query = new.parse_query(update_query)
    print(parsed_update_query)

    # #Test DELETE query
    # delete_query = "DELETE FROM employees WHERE salary < 3000"
    # print(delete_query)
    # parsed_delete_query = new.parse_query(delete_query)
    # print(parsed_delete_query)
