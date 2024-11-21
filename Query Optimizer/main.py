from typing import List
import unittest

class ParsedQuery:
    def __init__(self, query_tree, query: str):
        self.query_tree = query_tree
        self.query = query

class QueryTree:
    def __init__(self, type_: str, val: str, childs: List['QueryTree'], parent: 'QueryTree' = None):
        self.type = type_
        self.val = val
        self.childs = childs
        self.parent = parent

class QueryOptimizer:
    def parse_query(self, query: str) -> ParsedQuery:
        tokens = self._tokenize(query)
        query_tree = self._parse_to_query_tree(tokens)
        return ParsedQuery(query_tree, query)

    def optimize_query(self, query: ParsedQuery) -> ParsedQuery:
        # Generate a set of possible query plans based on the equivalence rules
        query_plans = self._generate_query_plans(query.query_tree)

        # Calculate the cost of each query plan
        query_plan_costs = [(plan, self._get_cost(plan)) for plan in query_plans]

        # Use heuristic to choose the best plan with the lowest cost
        optimized_plan = min(query_plan_costs, key=lambda x: x[1])[0]

        return ParsedQuery(optimized_plan, query.query)

    def _get_cost(self, query_tree: QueryTree) -> int:
        return self._heuristic_cost(query_tree)

    def _tokenize(self, query: str) -> List[str]:
        tokens = query.strip().split()
        combined_tokens = []
        i = 0
        while i < len(tokens):
            token = tokens[i].upper()
            if token == "ORDER" and i + 1 < len(tokens) and tokens[i + 1].upper() == "BY":
                combined_tokens.append("ORDER BY")
                i += 2
            elif token == "BEGIN" and i + 1 < len(tokens) and tokens[i + 1].upper() == "TRANSACTION":
                combined_tokens.append("BEGIN TRANSACTION")
                i += 2
            elif token == "NATURAL" and i + 1 < len(tokens) and tokens[i + 1].upper() == "JOIN":
                combined_tokens.append("NATURAL JOIN")
                i += 2
            else:
                combined_tokens.append(tokens[i])
                i += 1
        return combined_tokens

    def _parse_to_query_tree(self, tokens: List[str]) -> QueryTree:
        root = QueryTree("root", "QUERY", [])
        current_node = root
        i = 0

        while i < len(tokens):
            token = tokens[i].upper()
            if token == "SELECT":
                select_node = QueryTree("select", tokens[i], [], current_node)
                current_node.childs.append(select_node)
                i += 1
                columns = []
                while i < len(tokens) and tokens[i].upper() not in ["FROM", "WHERE", "ORDER BY", "LIMIT"]:
                    if tokens[i] != ",":
                        columns.append(tokens[i])
                    i += 1
                select_node.childs.append(QueryTree("columns", " ".join(columns), []))
            elif token == "FROM":
                from_node = QueryTree("from", tokens[i], [], current_node)
                current_node.childs.append(from_node)
                i += 1
                table_name = tokens[i]
                from_node.childs.append(QueryTree("table", table_name, []))
                i += 1
                join_node = None
                while i < len(tokens) and tokens[i].upper() in ["JOIN", "ON"]:
                    if tokens[i].upper() == "JOIN":
                        if join_node is None:
                            join_node = QueryTree("join", "JOIN", [], from_node)
                            from_node.childs.append(join_node)
                        i += 1
                        join_table = tokens[i]
                        join_node.childs.append(QueryTree("table", join_table, []))
                    elif tokens[i].upper() == "ON":
                        i += 1
                        on_conditions = []
                        while i < len(tokens) and tokens[i].upper() not in ["WHERE", "ORDER BY", "LIMIT"]:
                            on_conditions.append(tokens[i])
                            i += 1
                        join_node.childs.append(QueryTree("on", " ".join(on_conditions), []))
                    i += 1
            elif token == "AS":
                as_node = QueryTree("alias", tokens[i], [], current_node)
                current_node.childs.append(as_node)
                i += 1
                alias_name = tokens[i]
                as_node.childs.append(QueryTree("alias_name", alias_name, []))
                i += 1
            elif token == "UPDATE":
                update_node = QueryTree("update", tokens[i], [], current_node)
                current_node.childs.append(update_node)
                i += 1
                table_name = tokens[i]
                update_node.childs.append(QueryTree("table", table_name, []))
                i += 1
                if tokens[i].upper() == "SET":
                    set_node = QueryTree("set", tokens[i], [], update_node)
                    update_node.childs.append(set_node)
                    i += 1
                    updates = []
                    while i < len(tokens) and tokens[i].upper() != "WHERE":
                        if tokens[i] != ",":
                            updates.append(tokens[i])
                        i += 1
                    set_node.childs.append(QueryTree("updates", " ".join(updates), []))
                if i < len(tokens) and tokens[i].upper() == "WHERE":
                    where_node = QueryTree("where", tokens[i], [], update_node)
                    update_node.childs.append(where_node)
                    i += 1
                    conditions = []
                    while i < len(tokens):
                        conditions.append(tokens[i])
                        i += 1
                    where_node.childs.append(QueryTree("conditions", " ".join(conditions), []))
            elif token == "WHERE":
                where_node = QueryTree("where", tokens[i], [], current_node)
                current_node.childs.append(where_node)
                i += 1
                conditions = []
                while i < len(tokens) and tokens[i].upper() not in ["ORDER BY", "LIMIT"]:
                    conditions.append(tokens[i])
                    i += 1
                where_node.childs.append(QueryTree("conditions", " ".join(conditions), []))
            elif token == "ORDER BY":
                orderby_node = QueryTree("orderby", tokens[i], [], current_node)
                current_node.childs.append(orderby_node)
                i += 1
                attribute = tokens[i]
                i += 1
                order = "ASC"
                if i < len(tokens) and tokens[i].upper() in ["ASC", "DESC"]:
                    order = tokens[i].upper()
                    i += 1
                orderby_node.childs.append(QueryTree("attribute", attribute, []))
                orderby_node.childs.append(QueryTree("order", order, []))
            elif token == "LIMIT":
                limit_node = QueryTree("limit", tokens[i], [], current_node)
                current_node.childs.append(limit_node)
                i += 1
                limit_value = tokens[i]
                limit_node.childs.append(QueryTree("value", limit_value, []))
                i += 1
            elif token == "BEGIN TRANSACTION":
                transaction_node = QueryTree("begin_transaction", tokens[i], [], current_node)
                current_node.childs.append(transaction_node)
                i += 1
            elif token == "COMMIT":
                commit_node = QueryTree("commit", tokens[i], [], current_node)
                current_node.childs.append(commit_node)
                i += 1
            else:
                i += 1

        return root

    def _generate_query_plans(self, query_tree: QueryTree) -> List[QueryTree]:
        # Generate alternative query plans using equivalence rules
        query_plans = [query_tree]  # Initially only one query plan available

        # Step 1: Apply equivalence rules (rules 1 to 8) to generate variations
        # Rule 1: Decompose conjunctive selection operations into individual selections
        new_plans = self._apply_decomposition_rules(query_tree)
        query_plans.extend(new_plans)

        # Add further steps to apply rules from the provided optimization rules
        return query_plans

    def _apply_decomposition_rules(self, query_tree: QueryTree) -> List[QueryTree]:
        # Implement decomposition of conjunctive selection into individual selection
        new_plan = QueryTree("decomposed", query_tree.val, query_tree.childs[:])
        return [new_plan]

    def _heuristic_cost(self, query_tree: QueryTree) -> int:
        # Implement a heuristic cost evaluation based on characteristics like number of joins, operations, etc.
        return len(query_tree.childs)  # Example: using the number of children nodes as a heuristic metric

# Example
optimizer = QueryOptimizer()
query = "SELECT * FROM table WHERE condition"
parsed_query = optimizer.parse_query(query)
optimized_query = optimizer.optimize_query(parsed_query)
print(optimized_query.query_tree.type)  # Output the type of optimized query tree

# Unit tests for parsing
class TestQueryParsing(unittest.TestCase):
    def setUp(self):
        self.optimizer = QueryOptimizer()

    def test_select_query(self):
        query = "SELECT name, age FROM employees WHERE age > 30 ORDER BY age DESC"
        parsed_query = self.optimizer.parse_query(query)
        self.assertEqual(parsed_query.query_tree.type, "root")
        self.assertEqual(parsed_query.query_tree.childs[0].type, "select")
        self.assertEqual(parsed_query.query_tree.childs[0].childs[0].val, "name, age")
        self.assertEqual(parsed_query.query_tree.childs[1].type, "from")
        self.assertEqual(parsed_query.query_tree.childs[1].childs[0].val, "employees")
        self.assertEqual(parsed_query.query_tree.childs[2].type, "where")
        self.assertEqual(parsed_query.query_tree.childs[2].childs[0].val, "age > 30")
        self.assertEqual(parsed_query.query_tree.childs[3].type, "orderby")
        self.assertEqual(parsed_query.query_tree.childs[3].childs[0].val, "age")
        self.assertEqual(parsed_query.query_tree.childs[3].childs[1].val, "DESC")

    def test_update_query(self):
        query = "UPDATE employees SET salary = salary * 1.1 WHERE department = 'Sales'"
        parsed_query = self.optimizer.parse_query(query)
        self.assertEqual(parsed_query.query_tree.type, "root")
        self.assertEqual(parsed_query.query_tree.childs[0].type, "update")
        self.assertEqual(parsed_query.query_tree.childs[0].childs[0].val, "employees")
        self.assertEqual(parsed_query.query_tree.childs[0].childs[1].type, "set")
        self.assertEqual(parsed_query.query_tree.childs[0].childs[1].childs[0].val, "salary = salary * 1.1")
        self.assertEqual(parsed_query.query_tree.childs[0].childs[2].type, "where")
        self.assertEqual(parsed_query.query_tree.childs[0].childs[2].childs[0].val, "department = 'Sales'")

    def test_join_query(self):
        query = "SELECT employees.name, departments.name FROM employees JOIN departments ON employees.dept_id = departments.id"
        parsed_query = self.optimizer.parse_query(query)
        self.assertEqual(parsed_query.query_tree.type, "root")
        self.assertEqual(parsed_query.query_tree.childs[0].type, "select")
        self.assertEqual(parsed_query.query_tree.childs[0].childs[0].val, "employees.name, departments.name")
        self.assertEqual(parsed_query.query_tree.childs[1].type, "from")
        self.assertEqual(parsed_query.query_tree.childs[1].childs[0].val, "employees")
        self.assertEqual(parsed_query.query_tree.childs[1].childs[1].type, "join")
        self.assertEqual(parsed_query.query_tree.childs[1].childs[1].childs[0].val, "departments")
        self.assertEqual(parsed_query.query_tree.childs[1].childs[1].childs[1].type, "on")
        self.assertEqual(parsed_query.query_tree.childs[1].childs[1].childs[1].val, "employees.dept_id = departments.id")

if __name__ == "__main__":
    unittest.main()