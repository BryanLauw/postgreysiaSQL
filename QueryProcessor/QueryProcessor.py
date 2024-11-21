from ..ConcurrencyControlManager.main import *
from ..QueryOptimizer.main import *
from ..FailureRecovery.failure_recovery import *
from ..StorageManager.classes import *

# temp class
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
    
class QueryProcessor:
    def __init__(self):
        self.current_transactionId = None
        self.parsedQuery = None
        self.qo = QueryOptimizer()
        self.cc = ConcurrencyControlManager()
        sm = StorageEngine()
        rm = FailureRecovery()
        pass

    def execute_query(self, query : str):
        tables = ["id", "name"]
        rows = [
            ['1', 'benfsjdjfdsfhuisdfosidfdjsopfjsduifbsdhyufdiogjdfbndifiodfuuiguirfhifhui'],
            ['2', 'tazki'],
            ['3', 'agil'],
            ['4', 'filbert'],
            ['5', 'bryan']
        ]
        
        queries = self.parse_query(query)
        for query in queries:
            print("Executing query: " + query)
            if(query.upper() == "BEGIN" or query.upper() == "BEGIN TRANSACTION"):
                self.current_transactionId = self.cc.begin_transaction()
                # self.rm.start_transaction(self.current_transactionId)
                
            elif(query.upper() == "COMMIT" or query.upper() == "BEGIN TRANSACTION"):
                self.cc.end_transaction(self.current_transactionId)
                self.current_transactionId = None
            
            elif(query.upper() == "END TRANSACTION"):
                self.cc.end_transaction(self.current_transactionId)
                self.current_transactionId = None

            elif(query.upper() == "PRINT"):
                self.printResult(tables, rows)
            
            else:
                self.parsedQuery = self.qo.parse_query(query)
    
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

    def printResult(self,column, data):
        # Determine the maximum width of each column
        column_widths = [max(len(row[i]) for row in data + [column]) for i in range(len(column))]

        # Function to format a row
        def format_row(row):
            return "| " + " | ".join(row[i].ljust(column_widths[i]) for i in range(len(row))) + " |"

        # Print the header
        print("+-" + "-+-".join("-" * width for width in column_widths) + "-+")
        print(format_row(column))
        print("+-" + "-+-".join("-" * width for width in column_widths) + "-+")

        # Print the data
        for row in data:
            print(format_row(row))

        # Print the bottom border
        print("+-" + "-+-".join("-" * width for width in column_widths) + "-+")


    def parse_query(self, query : str):
        queries = query.split(';')
        return [q.strip() for q in queries if q.strip()]