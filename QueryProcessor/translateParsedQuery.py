from typing import List, Union

class Condition:
    def __init__(self, column: str, operation: str, operand: Union[str, int]):
        self.column = column
        self.operation = operation
        self.operand = operand
    
    def __repr__(self):
        return f"Condition(column={self.column}, operation={self.operation}, operand={self.operand})"

class DataRetrieval:
    def __init__(self, table: str, columns: List[str], conditions: List[Condition]):
        self.table = table
        self.columns = columns
        self.conditions = conditions

    def __repr__(self):
        return f"DataRetrieval(table={self.table}, columns={self.columns}, conditions={self.conditions})"

class DataWrite:
    def __init__(self, table: str, column: List[str], conditions: List[Condition], new_value: List[str]):
        self.table = table
        self.column = column
        self.conditions = conditions
        self.new_value = new_value

    def __repr__(self):
        return f"DataWrite(table={self.table}, column={self.column}, conditions={self.conditions}, new_value={self.new_value})"

class DataDeletion:
    def __init__(self, table: str, conditions: List[Condition]):
        self.table = table
        self.conditions = conditions

    def __repr__(self):
        return f"DataDeletion(table={self.table}, conditions={self.conditions})"

class QueryTree:
    def __init__(self, type: str, val: str = None, childs: List['QueryTree'] = None, parent: 'QueryTree' = None):
        self.type = type
        self.val = val
        self.childs = childs if childs else []
        self.parent = parent

    def add_child(self, child: 'QueryTree') -> 'QueryTree':
        self.childs.append(child)
        return self

    def add_parent(self, parent: 'QueryTree') -> 'QueryTree':
        self.parent = parent
        return self

    def __repr__(self, level=0):
        ret = "\t" * level + f"({self.type}: {self.val})\n"
        for child in self.childs:
            ret += child.__repr__(level + 1)
        return ret

class ParsedQuery:
    def __init__(self, query_tree: QueryTree, query: str):
        self.query_tree = query_tree
        self.query = query

    def __repr__(self):
        return repr(self.query_tree)

class TranslateParsedQuery:
    @staticmethod
    def ParsedQueryToDataRetrieval(parsed_query: ParsedQuery) -> DataRetrieval:
        if parsed_query.query_tree.type == "JOIN":
            joined_tables = [
                child.val for child in parsed_query.query_tree.childs if child.type == "TABLE"
            ]
            table = joined_tables  
        else:
            table = parsed_query.query_tree.val  

        columns = [
            child.val for child in parsed_query.query_tree.childs if child.type == "COLUMN"
        ]
        conditions = [
            Condition(
                column=cond.childs[0].val,
                operation=cond.childs[1].val,
                operand=cond.childs[2].val
            )
            for cond in parsed_query.query_tree.childs if cond.type == "CONDITION"
        ]

        return DataRetrieval(table=table, columns=columns, conditions=conditions)
    
    @staticmethod
    def ParsedQueryToDataWrite(parsed_query: ParsedQuery) -> DataWrite:
        if parsed_query.query_tree.type == "JOIN":
            raise ValueError("DataWrite cannot be applied to JOIN operations.")

        table = parsed_query.query_tree.val

        columns = [
            child.val for child in parsed_query.query_tree.childs if child.type == "COLUMN"
        ]
        values = [
            child.val for child in parsed_query.query_tree.childs if child.type not in ["COLUMN", "CONDITION"]
        ]

        def infer_type(value: str):
            if value.startswith(("'", '"')) and value.endswith(("'", '"')):
                return value.strip("'\"")
            try:
                if '.' in value:
                    return float(value) 
                return int(value) 
            except ValueError:
                return value 

        new_values = [infer_type(value) for value in values]

        conditions = [
            Condition(
                column=cond.childs[0].val,
                operation=cond.childs[1].val,
                operand=cond.childs[2].val
            )
            for cond in parsed_query.query_tree.childs if cond.type == "CONDITION"
        ]

        return DataWrite(table=table, column=columns, conditions=conditions, new_value=new_values)

    @staticmethod
    def ParsedQueryToDataDeletion(parsed_query: ParsedQuery) -> DataDeletion:
        data_deletion = DataDeletion(
            table=parsed_query.query_tree.val,
            conditions=[
                Condition(
                    column=cond.childs[0].val,
                    operation=cond.childs[1].val,
                    operand=cond.childs[2].val
                )
                for cond in parsed_query.query_tree.childs if cond.type == "CONDITION"
            ]
        )
        return data_deletion

def display_test_results(retrieval=None, write=None, deletion=None):
    if retrieval:
        print("Retrieval Result:", retrieval)
    if write:
        print("Write Result:", write)
    if deletion:
        print("Deletion Result:", deletion)
    print("")

parsed_query = ParsedQuery(
    query_tree=QueryTree(
        type="TABLE",
        val="employee",
        childs=[
            QueryTree(type="COLUMN", val="name"),
            QueryTree(type="COLUMN", val="age"),
            QueryTree(
                type="CONDITION",
                val="WHERE",
                childs=[
                    QueryTree(type="COLUMN", val="age"),
                    QueryTree(type="OPERATION", val=">"),
                    QueryTree(type="VALUE", val="30"),
                ],
            ),
        ],
    ),
    query="SELECT name, age FROM employee WHERE age > 30",
)

retrieval_result = TranslateParsedQuery.ParsedQueryToDataRetrieval(parsed_query)

display_test_results(
    retrieval=retrieval_result,
)

parsed_query1 = ParsedQuery(
    query_tree=QueryTree(
        type="JOIN",
        val="INNER JOIN",
        childs=[
            QueryTree(type="TABLE", val="employee"),
            QueryTree(type="TABLE", val="department"),
            QueryTree(
                type="CONDITION",
                val="ON",
                childs=[
                    QueryTree(type="COLUMN", val="employee.department_id"),
                    QueryTree(type="OPERATION", val="="),
                    QueryTree(type="COLUMN", val="department.id"),
                ],
            ),
            QueryTree(type="COLUMN", val="employee.name"),
            QueryTree(type="COLUMN", val="department.location"),
        ],
    ),
    query="SELECT employee.name, department.location FROM employee INNER JOIN department ON employee.department_id = department.id",
)

retrieval_result1 = TranslateParsedQuery.ParsedQueryToDataRetrieval(parsed_query1)

display_test_results(
    retrieval=retrieval_result1,
)

parsed_query2 = ParsedQuery(
    query_tree=QueryTree(
        type="JOIN",
        val="JOIN",
        childs=[
            QueryTree(type="TABLE", val="student"),
            QueryTree(type="TABLE", val="course"),
            QueryTree(
                type="CONDITION",
                val="ON",
                childs=[
                    QueryTree(type="COLUMN", val="student.course_id"),
                    QueryTree(type="OPERATION", val="="),
                    QueryTree(type="COLUMN", val="course.id"),
                ],
            ),
            QueryTree(
                type="CONDITION",
                val="WHERE",
                childs=[
                    QueryTree(type="COLUMN", val="student.age"),
                    QueryTree(type="OPERATION", val=">"),
                    QueryTree(type="VALUE", val="20"),
                ],
            ),
            QueryTree(type="COLUMN", val="student.name"),
            QueryTree(type="COLUMN", val="course.title"),
        ],
    ),
    query="SELECT student.name, course.title FROM student LEFT JOIN course ON student.course_id = course.id WHERE student.age > 20",
)

retrieval_result2 = TranslateParsedQuery.ParsedQueryToDataRetrieval(parsed_query2)

display_test_results(
    retrieval=retrieval_result2,
)

parsed_query_update = ParsedQuery(
    query_tree=QueryTree(
        type="TABLE",
        val="employee",
        childs=[
            QueryTree(type="COLUMN", val="salary"),
            QueryTree(type="VALUE", val="10000"),
            QueryTree(
                type="CONDITION",
                val="WHERE",
                childs=[
                    QueryTree(type="COLUMN", val="department_id"),
                    QueryTree(type="OPERATION", val="="),
                    QueryTree(type="VALUE", val="5"),
                ],
            ),
        ],
    ),
    query="UPDATE employee SET salary = 10000 WHERE department_id = 5",
)

write_result_update = TranslateParsedQuery.ParsedQueryToDataWrite(parsed_query_update)

display_test_results(
    write=write_result_update,
)

parsed_query_insert = ParsedQuery(
    query_tree=QueryTree(
        type="TABLE",
        val="employee",
        childs=[
            QueryTree(type="COLUMN", val="id"),
            QueryTree(type="COLUMN", val="name"),
            QueryTree(type="COLUMN", val="department_id"),
            QueryTree(type="COLUMN", val="salary"),
            QueryTree(type="VALUE", val="101"),
            QueryTree(type="VALUE", val="'John Doe'"),
            QueryTree(type="VALUE", val="3"),
            QueryTree(type="VALUE", val="6000"),
        ],
    ),
    query="INSERT INTO employee (id, name, department_id, salary) VALUES (101, 'John Doe', 3, 6000)",
)

write_result_insert = TranslateParsedQuery.ParsedQueryToDataWrite(parsed_query_insert)

display_test_results(
    write=write_result_insert,
)

parsed_query_delete = ParsedQuery(
    query_tree=QueryTree(
        type="TABLE",
        val="employee",
        childs=[
            QueryTree(
                type="CONDITION",
                val="WHERE",
                childs=[
                    QueryTree(type="COLUMN", val="department_id"),
                    QueryTree(type="OPERATION", val="="),
                    QueryTree(type="VALUE", val="5"),
                ],
            )
        ],
    ),
    query="DELETE FROM employee WHERE department_id = 5",
)

deletion_result_delete = TranslateParsedQuery.ParsedQueryToDataDeletion(parsed_query_delete)

display_test_results(
    deletion=deletion_result_delete,
)