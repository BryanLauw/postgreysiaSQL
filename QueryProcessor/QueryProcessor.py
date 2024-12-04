from FailureRecovery.main_log_entry import LogEntry
from ..ConcurrencyControlManager.ConcurrencyControlManager import *
from ..QueryOptimizer.OptimizationEngine import *
from ..FailureRecovery import *
from StorageManager.classes import *
import re

import FailureRecovery.main as FailureRecovery

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
    def __init__(self, db_name: str):
        self.current_transactionId = None
        self.parsedQuery = None
        self.qo = OptimizationEngine()
        self.cc = ConcurrencyControlManager()
        self.sm = StorageEngine()
        self.rm = FailureRecovery()
        self.db_name = db_name
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
                self.rm.start_transaction(self.current_transactionId)
                
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

        if self.parsedQuery.query_tree.val == "UPDATE":
            write = self.ParsedQueryToDataWrite(self.parsedQuery)
            b = self.sm.write_block(write, self.db_name, self.current_transactionId)
    
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
        # Input: child (QueryTree with only where value)
        # Output: List of condition from child
        def filter_condition(child: QueryTree) -> List[Condition]:
            operator = r'(<=|>=|<>|<|>|=)'
            where_val = child.val
            match = re.split(operator, where_val, maxsplit=1)
            parts = [part.strip() for part in match]
            parts[2] = int(parts[2]) if parts[2].isdigit() else parts[2]
            temp = Condition(parts[0], parts[1], parts[2])
            if not child.childs:
                return [temp]
            else:
                return [temp] + filter_condition(child.childs[0])
        
        # Get table name
        table = parsed_query.query_tree.childs[0].val

        # Get new value
        new_value = parsed_query.query_tree.childs[0].childs[0].val
        match = re.split(r'=', new_value)
        columns = [match[0].strip()]
        new_value = [match[1].strip()]

        # Get all conditions
        conditions = filter_condition(parsed_update_query.query_tree.childs[0].childs[0].childs[0])

        return DataWrite([table], columns, conditions, new_value)

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
    
    def __handle_rollback(self, transaction_id: int):
        """
        Handle rollback when FailureRecovery calls rollback.

        Parameters:
            transaction_id (int): The ID of the transaction to rollback.
        """
        # Retrieve undo instructions from FailureRecovery
        undo_instructions = self.rm.rollback(transaction_id)

        # Reverse the changes made by the transaction
        for instruction in undo_instructions:
            object_value = instruction["object_value"]
            old_value = instruction["old_value"]
            db, table, column = self.__parse_object_value(object_value)
            # Revert the data to the old value using StorageManager
            self.sm.update_data(object_value, old_value) # TODO : Implement update_data in StorageManager

        # Release any locks held by the transaction
        self.cc.end_transaction(transaction_id)

    def __parse_object_value(self, object_value: str):
        """
        Parse the object value to extract table and column information.
        
        Parameters:
            object_value (str): The object value string.
        
        Returns:
            tuple: A tuple containing the table and column.
        """
        # Assuming object_value is a string in the format "{'nama_db':'db_name','nama_kolom':'table_a','primary_key':'column_a'}"
        matches = re.findall(r"'(\w+)':'([^']*)'", object_value)
        obj = dict(matches)
        table = obj['nama_kolom']
        column = obj['primary_key']
        db = obj['nama_db']
        return db, table, column
    
    def __removeAttribute(self, l: List) -> List[str]:
        # removing attribute from <table>.<attribute> for all element in list
        
        # Example:
        # __removeAttribute(['students.a', 'teacher.b']) = ['students', 'teacher']

        return [element.split('.')[0] for element in l]

    def __getTables(self,tree: QueryTree):
        # get all tables needed from Query (not ParsedQuery)
    
        # Example:
        # select_query = "SELECT s.a, t.b FROM students AS s JOIN teacher AS t ON s.id = t.id WHERE s.a > 1 AND t.b = 2 OR t.b < 5"
        # __getTables(select_query) = ['students', 'teacher']

        if tree.type.upper() == "SELECT": # This conditional is using "SELECT" because QueryTree does not have FROM type.
            return self.__removeAttribute(tree.val)
        elif len(tree.childs) == 0:
            return ""
        else:
            for child in tree.childs:
                return self.__getTables(child)

    def __makeCondition(self, tree: QueryTree) -> List[Condition]:
        # Make Condition from Query Tree
        cons = self.__getTables(tree)
        ret = []
        for con in cons:
            if "." in con[0]:
                column = con.split(".")[1]
            else:
                column = con
            temp = Condition(column, con[1], con[2])
            ret.append(temp)
        return ret

    def __getData(self, data_retrieval: DataRetrieval, database: str) -> dict|Exception:
        # fetches the required rows of data from the storage manager
        # and returns it as a dictionary

        # Example:
        # data_retrieval = DataRetrieval(table="students", 
        #                                columns=["a"], 
        #                                conditions=[Condition("a", ">", 1)])
        # database = "database1"
        # getData(data_retrieval, database) = {'a': [2, 3, 4, 5]}

        try:
            data = self.sm.read_block(data_retrieval, database)
            return data
        except Exception as e:
            return e
        
    def __get_filters_for_table(tree: QueryTree, table_name: str) -> List[tuple]:
        # contoh query SELECT students.name, students.age FROM students WHERE students.age > 20 AND students.grade = 'A';
        # contoh query tree
        # (root: QUERY)
        # (select: SELECT)
        #         (columns: students.name, students.age)
        # (from: FROM)
        #         (table: students)
        # (where: WHERE)
        #         (conditions: students.age > 20 AND students.grade = 'A';)

        # result [('students.age', '>', '20'), ('students.grade', '=', "'A';")]
        filters = []

        def traverse(node: QueryTree):
            if node.type in {"where", "on"}:
                for child in node.childs:
                    if child.type == "conditions":
                        filter_tokens = child.val.split()
                        i = 0
                        while i < len(filter_tokens):
                            if i + 2 < len(filter_tokens) and filter_tokens[i + 1] in {"=", "<>", ">", ">=", "<", "<="}:
                                column = filter_tokens[i]
                                operation = filter_tokens[i + 1]
                                operand = filter_tokens[i + 2]

                        
                                table_in_condition = column.split(".")[0]
                                if table_in_condition == table_name:
                                    filters.append((column, operation, operand))

                                
                                i += 3
                            else:
                        
                                i += 1

        
            for child in node.childs:
                traverse(child)

        traverse(tree)
        return filters
    
    # def delete_block(self, data_deletion:DataDeletion, database_name:str, transaction_id:int) -> int:
    def __deleteData(self, data_deletion: DataDeletion, database: str) -> int:
        # delete the required rows of data from the storage manager
        # and returns the number of rows deleted
        # data_deletion = DataDeletion(table="students", conditions=[Condition("a", ">", 1)])
        # database = "database1"
        # deleteData(data_deletion, database, transaction_id) = 4

        try:
            rows_deleted = self.sm.delete_block(data_deletion, database, self.current_transactionId)
            return rows_deleted
        except Exception as e:
            return e