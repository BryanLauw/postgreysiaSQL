import signal
from FailureRecovery.main_log_entry import LogEntry
from ConcurrencyControlManager.ConcurrencyControlManager import *
from QueryOptimizer.OptimizationEngine import *
from StorageManager.classes import *
from typing import Optional
import re
import atexit

import FailureRecovery.main as FailureRecovery
    
class QueryProcessor:
    # def __init__(self, db_name: str | None):
    def __init__(self):
        self.parsedQuery = None
        self.sm = StorageEngine()
        self.qo = OptimizationEngine(self.sm.get_stats)
        self.cc = ConcurrencyControlManager()
        self.rm = FailureRecovery.FailureRecovery()
        self.db_name = "database1" #SBD

        self.current_transactionId = self.cc.begin_transaction() #SBD
        self.rm.write_log_entry(self.current_transactionId, "START", None, None, None)

        # Register exit and signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGSEGV, self.signal_handler)

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
                self.rm.write_log_entry(self.current_transactionId, "START", None, None, None)
                # self.rm.start_transaction(self.current_transactionId)
                
            elif(query.upper() == "COMMIT" or query.upper() == "COMMIT TRANSACTION"):
                self.rm.write_log_entry(self.current_transactionId, "COMMIT", None, None, None)
                self.cc.end_transaction(self.current_transactionId)
                self.sm.commit_buffer(self.current_transactionId)
                self.sm.save()
                self.current_transactionId = None

            elif(query.upper() == "PRINT"):
                self.printResult(tables, rows)

            else:
                self.parsedQuery = self.qo.parse_query(query,'database1') #hardcode
                # try:
                #     # self.parsedQuery = self.qo.parse_query(query, self.db_name)
                #     self.parsedQuery = self.qo.parse_query(query, "database1")
                # except Exception as e:
                #     raise Exception(e)
                    
                if self.parsedQuery.query_tree.val == "UPDATE":
                    write = self.ParsedQueryToDataWrite(self.parsedQuery)
                    # b = self.sm.write_block(write, self.db_name, self.current_transactionId)
                    b = self.sm.write_block(write, "database1", self.current_transactionId)
                elif self.parsedQuery.query_tree.val == "SELECT":
                    data_ret:DataRetrieval = self.ParsedQueryToDataRetrieval(self.parsedQuery.query_tree)
                    temp = self.sm.read_block(data_ret,self.db_name,self.current_transactionId)
                    temp = self.__orderBy(temp, "id", True) # hardcode
                    result = self.printResult(temp)
                    return result

    def evaluateSelectTree(self, tree: QueryTree, select: list[str], where: str) -> list[dict]:
        if not tree.childs:
            if len(select) > 0 and len(where) > 0:
                cond = self.__makeCondition(where)
                dataRetriev = DataRetrieval(tree.val, select, cond)
                return self.__getData(dataRetriev, self.db_name)
            elif len(select) > 0:
                dataRetriev = DataRetrieval(tree.val, select, [])
                return self.__getData(dataRetriev, self.db_name)
            elif len(where) > 0:
                cond = self.__makeCondition(where)
                dataRetriev = DataRetrieval(tree.val, [], cond)
                return self.__getData(dataRetriev, self.db_name)
            else:
                dataRetriev = DataRetrieval(tree.val, [], [])
                return self.__getData(dataRetriev, self.db_name)
        else:
            if tree.type == "JOIN" or tree.type == "NATURAL JOIN":
                if tree.type == "JOIN":
                    temp = self.__joinOn(
                        "temp1", "temp2",
                        self.evaluateSelectTree(tree.childs[0], [], []),
                        self.evaluateSelectTree(tree.childs[1], [], []),
                        tree.val
                    )
                elif tree.type == "NATURAL JOIN":
                    temp = self.__naturalJoin(
                        "temp1", "temp2",
                        self.evaluateSelectTree(tree.childs[0], [], []),
                        self.evaluateSelectTree(tree.childs[1], [], []))
                    
                if len(select) > 0 and len(where) > 0:
                    temp = self.__filterSelect(temp, select)
                    temp = self.__filterWhere(temp, where)
                    return temp
                elif len(select) > 0:
                    temp = self.__filterSelect(temp, select)
                    return temp
                elif len(where) > 0:
                    temp = self.__filterWhere(temp, where)
                    return temp
                else:
                    return temp
            else:
                if tree.type == "SELECT":
                    select = tree.val
                elif tree.type == "WHERE":
                    where = tree.val
                for child in tree.childs:
                    return self.evaluateSelectTree(child, select, where)
                
    def __filterSelect(self, data: List[dict], select: list[str]) -> List[dict]:
        # filter select
        column = [col.split(".")[1] for col in select]
        return [{key: value for key, value in row.items() if key in column} for row in data]
    
    def __evalWhere(self, row:map, conds:list[Condition]):
        for cond in conds:
            if(cond.operation == "<>" and row[cond.column] != row[cond.operand]):
                return True
            elif(cond.operation == ">=" and row[cond.column] >= row[cond.operand]):
                return True
            elif(cond.operation == "<=" and row[cond.column] <= row[cond.operand]):
                return True
            elif(cond.operation == "=" and row[cond.column] == row[cond.operand]):
                return True
            elif(cond.operation == ">" and row[cond.column] > row[cond.operand]):
                return True
            elif(cond.operation == "<" and row[cond.column] < row[cond.operand]):
                return True
        return False
    
    def __filterWhere(self,data: list[map], where: str) -> list[map]:
        result = []
        cond = self.__makeCondition(where)
        for row in data:
            if self.__evalWhere(row,cond):
                result.append(row)
        return result
    
    def __makeCondition(self, where: str) -> List[Condition]:      
        eqs = where.split("OR")
        cond = [] 
        for eq in eqs:
            if("<>" in eq):
                temp = eq.split("<>")
                cond.append(Condition(temp[0].strip(),"<>",temp[1].strip()))
            elif(">=" in eq):
                temp = eq.split(">=")
                cond.append(Condition(temp[0].strip(),">=",temp[1].strip()))
            elif("<=" in eq):
                temp = eq.split("<=")
                cond.append(Condition(temp[0].strip(),"<=",temp[1].strip()))
            elif("=" in eq):
                temp = eq.split("=")
                cond.append(Condition(temp[0].strip(),"=",temp[1].strip()))
            elif(">" in eq):
                temp = eq.split(">")
                cond.append(Condition(temp[0].strip(),">",temp[1].strip()))
            elif("<" in eq):
                temp = eq.split("<")
                cond.append(Condition(temp[0].strip(),"<",temp[1].strip()))
        return cond

    def ParsedQueryToDataRetrieval(self,parsed_query: QueryTree) -> DataRetrieval:
        # if parsed_query.query_tree.type == "JOIN":
        #     joined_tables = [
        #         child.val for child in parsed_query.query_tree.childs if child.type == "TABLE"
        #     ]
        #     table = joined_tables  
        # else:
        #     table = parsed_query.query_tree.val  
        # columns = [
        #     child.val for child in parsed_query.query_tree.childs if child.type == "COLUMN"
        # ]
        # conditions = [
        #     Condition(
        #         column=cond.childs[0].val,
        #         operation=cond.childs[1].val,
        #         operand=cond.childs[2].val
        #     )
        #     for cond in parsed_query.query_tree.childs if cond.type == "CONDITION"
        # ]
        # print(parsed_query.query_tree)
        if parsed_query.type == "SELECT":
            tables = {}
            cols = {}
            # print(parsed_query.type)
            # print(parsed_query.val)
            for s in parsed_query.val:
                # print(s.split('/.'))
                tables[s.split(".")[0]] = 1
                cols[s.split(".")[1]] = 1    
            t = list(tables.keys())
            c = list(cols.keys())
            print(t)
            print(c)
            return DataRetrieval(tables=t, columns=c, conditions=[] )
        else:
            for child in parsed_query.childs:
                return self.ParsedQueryToDataRetrieval(child)
        # return DataRetrieval(table=table, columns=columns, conditions=conditions)

    def ParsedQueryToDataWrite(self, parsed_query: ParsedQuery) -> DataWrite:
        try:
            table = parsed_query.query_tree.childs[0].val
                
            # Validate write access
            response = self.cc.validate_object(table, self.current_transactionId, "write")
            if not response.allowed:
                raise Exception(f"Transaction {self.current_transactionId} cannot write to table {table}")
        
            # Input: child (QueryTree with only where value)
            # Output: List of condition from child
            def filter_condition(child: QueryTree) -> List[Condition]:
                operator = r'(<=|>=|<>|<|>|=)'
                where_val = child.val
                match = re.split(operator, where_val, maxsplit=1)
                parts = [part.strip() for part in match]
                parts[2] = int(parts[2]) if parts[2].isdigit() else parts[2]
                print("kondisi0 ", parts[0].split(".")[1])
                print("kondisi1 ", parts[1])
                print("kondisi2 ", parts[2])
                temp = Condition(parts[0].split(".")[1], parts[1], parts[2])
                if not child.childs:
                    return [temp]
                else:
                    return [temp] + filter_condition(child.childs[0])
            
            # Get table name
            table = parsed_query.query_tree.childs[0].val

            # Get new value
            new_value = parsed_query.query_tree.childs[0].childs[0].val
            match = re.split(r'=', new_value)
            columns = match[0].strip().split(".")[1]
            print("kolom ", columns)
            new_value = match[1].strip().replace('"', '')
            

            # Get all conditions
            conditions = filter_condition(parsed_query.query_tree.childs[0].childs[0].childs[0])
            
            print("kondisi ", conditions)
            return DataWrite([table], [columns], conditions, [new_value])
        except Exception as e:
            self.handle_rollback(self.current_transactionId)
            return e

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

    def printResult(self, data:map):
        if not data:
            print("No data to display.")
            return

        headers = list(data[0].keys())
        
        column_widths = [
            max(len(str(row.get(key, ""))) for row in data)
            for key in headers
        ]
        column_widths = [max(width, len(header)) for width, header in zip(column_widths, headers)]
        
        border = "+" + "+".join("-" * (width + 2) for width in column_widths) + "+"
        
        print(border)
        
        header_line = "|"
        for header, width in zip(headers, column_widths):
            header_line += f" {header:<{width}} |"
        print(header_line)
        
        print(border)
        
        data_lines = []
        for row in data:
            data_line = "|"
            for key, width in zip(headers, column_widths):
                value = str(row.get(key, ""))
                data_line += f" {value:<{width}} |"
            print(data_line)
            data_lines.append(data_line)

        print(border)
        
        result = "\n".join([border, header_line, border] + data_lines + [border])
        return result.strip()
        
    def parse_query(self, query : str):
        queries = query.split(';')
        return [q.strip() for q in queries if q.strip()]
    
    def handle_rollback(self, transaction_id: int):
        """
        Handle rollback when FailureRecovery calls rollback.

        Parameters:
            transaction_id (int): The ID of the transaction to rollback.
        """
        undo_list = self.rm.rollback(transaction_id)

        # Retrieve undo instructions from FailureRecovery
        for instruction in undo_list:
            object_value = instruction["object_value"]
            old_value = instruction["old_value"]
            db, table, column, key_column, key_value = self.__parse_object_value(object_value)

            conditions = []
            if key_column and key_value:
                conditions.append(Condition(
                    column=key_column,
                    operation="=",
                    operand=key_value
                ))

            data_write = DataWrite(
                table=table,
                column=[column],
                conditions=conditions,
                new_value=[old_value]
            )

            result = self.sm.write_block(data_write, db, self.current_transactionId)
            if isinstance(result, Exception):
                print(f"Error during rollback: {result}")
            else:
                print(f"Rollback successful, {result} rows updated.")

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
        db = obj['nama_db']
        table = obj['nama_tabel']
        column = obj['nama_kolom']
        key_column = obj.get('primary_key')
        key_value = obj.get('key_value')
        return db, table, column, key_column, key_value

    def getLeaf(self,tree:QueryTree):
        # Base case: if the node has no children, it is a leaf
        if not tree.childs:
            return [tree]
        
        # Recursive case: collect leaves from all children
        leaves = []
        for child in tree.childs:
            leaves.extend(self.getLeaf(child))
        return leaves

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
            for table in data_retrieval.table:
                response = self.cc.validate_object(table, database, self.current_transactionId)
                if not response.allowed:
                    raise Exception(f"Transaction {self.current_transactionId} cannot read table {table}")
            data = self.sm.read_block(data_retrieval, database)
            return data
        except Exception as e:
            self.handle_rollback(self.current_transactionId)
            return e
    
    def __transCond(self, tablename1:str, tablename2: str, cond: str) -> list:
        result = []
        eqs = cond.split("AND")  # Split on commas
        for eq in eqs:
            temp = eq.split("=") # [t1.a , t2.b]
            # print(temp)
            lhs = temp[0].split(".") # [t1,a]
            if(lhs[0]==tablename1):
                result.append([lhs[1].strip(),temp[1].split(".")[1].strip()])
            else: 
                result.append([temp[1].split(".")[1].strip(), lhs[1].strip()])
        return result
    
    def __joinOn(self,tablename1: str, tablename2:str, table1: List[map], table2: List[map], cond: str):
        result = []
        condList = self.__transCond(tablename1,tablename2,cond)
        # condList = [['key1','key2'],['key3','key4']]
        for r1 in table1:
            for r2 in table2:
                isValid = True
                for cond in condList: 
                    if r1[cond[0]] != r2[cond[1]]:
                        isValid = False
                        break
                if(isValid):
                    row = {}
                    for col in r1:
                        if (col in r2):
                            row[tablename1+"."+col] = r1[col]
                            row[tablename2+"."+col] = r2[col]
                        else:
                            row[col] = r1[col]
                    for col in r2:
                        if (col in r1):
                            pass
                        else:
                            row[col] = r2[col]
                    result.append(row)
        return result
    
    def __naturalJoin(self, tablename1: str, tablename2: str, table1: List[dict], table2: List[dict]) -> List[dict]:
        """
        input: 
        tablename1 = "table1"
        tablename2 = "table2"

        table1 = [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
            {"id": "3", "name": "Charlie"}
        ]

        table2 = [
            {"id": "1", "age": "25"},
            {"id": "2", "age": "30"},
            {"id": "4", "age": "35"}
        ]

        result = [
            {"id": "1", "name": "Alice", "age": "25"},
            {"id": "2", "name": "Bob", "age": "30"}
        ]
        """
        common_columns = list(set(table1[0].keys()) & set(table2[0].keys()))
        
        result = []

        for row1 in table1:
            for row2 in table2:
                if all(row1[col] == row2[col] for col in common_columns):
                    combined_row = {**row1, **{key: row2[key] for key in row2 if key not in common_columns}}
                    result.append(combined_row)

        return result   

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
        
    def __orderBy(self, data: List[dict], order_by: str, is_desc: bool) -> List[dict]:
        # order the data based on the given attribute
        # data = [
        #     {"id": "1", "name": "Alice"},
        #     {"id": "2", "name": "Bob"},
        #     {"id": "3", "name": "Charlie"}
        # ]
        # order_by = "name"
        # is_desc = False
        # orderBy(data, order_by, is_desc) = [
        #     {"id": "1", "name": "Alice"},
        #     {"id": "2", "name": "Bob"},
        #     {"id": "3", "name": "Charlie"}
        # ]

        if is_desc:
            return sorted(data, key=lambda x: x[order_by], reverse=True)
        else:
            return sorted(data, key=lambda x: x[order_by])

    def signal_handler(self, signum, frame):
        """
        Custom signal handler to handle SIGINT and SIGSEV.
        """
        self.cc.end_transaction(self.current_transactionId)
        self.sm.save()
        print("Bye!")
        self.rm.signal_handler(signum, frame)
        # Raise the original signal  to allow the program to terminate