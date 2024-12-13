import signal
from FailureRecovery.main_log_entry import LogEntry
from ConcurrencyControlManager.ConcurrencyControlManager import *
from QueryOptimizer.OptimizationEngine import *
from StorageManager.classes import *
import re
from typing import Tuple

import FailureRecovery.main as FailureRecovery
    
class QueryProcessor:
    def __init__(self, db_name: str):
        self.parsedQuery = None
        self.sm = StorageEngine()
        self.qo = OptimizationEngine(self.sm.get_stats)
        self.cc = ConcurrencyControlManager()
        self.rm = FailureRecovery.FailureRecovery()
        self.db_name = db_name #SBD
        # self.db_name = db_name

        self.current_transactionId = 0 #SBD
        self.rm.write_log_entry(self.current_transactionId, "START", None, None, None)

        # Register exit and signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGSEGV, self.signal_handler)

    def execute_query(self, query : str, client_state: dict):
        queries = self.parse_query(query)
        print("Executing query: " + query)
        transaction_id = client_state.get("transactionId")

        if(query.upper() == "BEGIN" or query.upper() == "BEGIN TRANSACTION"):
            if not client_state.get("on_begin", False):  # Begin only if not already in a transaction
                self.current_transactionId = self.cc.begin_transaction()
                self.rm.write_log_entry(self.current_transactionId, "START", None, None, None)
                client_state["transactionId"] = self.current_transactionId
                client_state["on_begin"] = True
            else:
                print("Transaction already started.")
            # self.rm.start_transaction(self.current_transactionId)
            
        elif(query.upper() == "COMMIT" or query.upper() == "COMMIT TRANSACTION"):
                transaction_id = client_state["transactionId"]
                print("transaction id for commit: ", transaction_id)
                self.rm.write_log_entry(transaction_id, "COMMIT", None, None, None)
                self.cc.end_transaction(transaction_id)
                self.sm.commit_buffer(transaction_id)
                self.sm.save()
                self.current_transactionId = None
                client_state["on_begin"] = False
                client_state["transactionId"] = None

        # elif(query.upper() == "PRINT"):
        #     self.printResult(tables, rows)

        else:
            retry = True
            while retry:
                try:
                    self.parsedQuery = self.qo.parse_query(query,self.db_name) #hardcode

                    if self.parsedQuery.query_tree.val == "UPDATE":
                        try:
                            if not client_state.get("on_begin", False):  
                                print("masuk")
                                self.current_transactionId = self.cc.begin_transaction()
                                client_state["transactionId"] = self.current_transactionId
                            write = self.ParsedQueryToDataWrite()
                            print("transaction id :", transaction_id)

                            # baca data lama
                            data_lama = self.sm.read_block(DataRetrieval(write.table, write.column, write.conditions), self.db_name, self.current_transactionId)
                            # cek concurrency control
                            transaction_id = client_state["transactionId"]
                            print("trans id real: ", transaction_id)
                            response = self.cc.validate_object(data_lama, transaction_id, "write")
                            print("response dr cc: ",response.allowed)
                            if not response.allowed:
                                print("Validation failed. Handling rollback.")
                                self.handle_rollback(transaction_id)
                                print("Retrying query after rollback.")
                                continue  
                            
                            # write data
                            self.sm.write_block(write, self.db_name, self.current_transactionId)
                            data_written = data_lama.get_data()[0].get(write.column[0])
                            object_value = f"{{'nama_db':'{self.db_name}','nama_tabel':'{write.table[0]}','nama_kolom':'{write.column[0]}','primary_key':'{write.conditions[0].column}'}}"
                            print(object_value)
                            self.rm.write_log_entry(self.current_transactionId, "DATA", object_value, data_written, write.new_value[0])
                            
                        except Exception as e:
                            self.handle_rollback(transaction_id)
                            print(e) 
                    elif self.parsedQuery.query_tree.val == "SELECT":
                        try:
                            if not client_state.get("on_begin", False):
                                self.current_transactionId = self.cc.begin_transaction()
                                client_state["transactionId"] = self.current_transactionId

                            transaction_id = client_state["transactionId"]
                            result = self.evaluateSelectTree(self.parsedQuery.query_tree,[],"", transaction_id)
                            ret_val = self.printResult(result)
                            print(f"Read {len(result)} row(s).")
                            return ret_val                            
                        
                        except Exception as e:
                            self.handle_rollback(transaction_id)
                            print(e)

                    elif self.parsedQuery.query_tree.val == "CREATE" and self.parsedQuery.query_tree.childs[0].val == "INDEX":
                        try:
                            index = self.ParsedQueryToSetIndex()
                            # TODO: nama index ada di index[3], belum tau mau dipake di mana
                            self.sm.set_index(self.db_name, index[0], index[1], self.current_transactionId, index[2])
                        except Exception as e:
                            print(e)
                    
                    retry = False
                except Exception as e:
                    print(f"Error during query execution: {e}. Rolling back.")
                    transaction_id = client_state["transactionId"]
                    self.handle_rollback(transaction_id)

                    # Restart transaction after rollback
                    if not client_state.get("on_begin", False):
                        transaction_id = self.cc.begin_transaction()
                        self.rm.write_log_entry(transaction_id, "START", None, None, None)
                        client_state["on_begin"] = True
    

    def  evaluateSelectTree(self, tree: QueryTree, select: list[str], where: str, transaction_id: int) -> list[dict]:
        if not tree.childs:
            condition = []
            if len(where) > 0:
                condition = self.__makeCondition(where)
            select = self.removeTablename(select)
            dataRetriev = DataRetrieval([tree.val], select, condition)
            try:
                temp = self.transformData(tree.val,self.__getData(dataRetriev, transaction_id))
            except Exception as e:
                raise(e)
            return temp
        else:
            if tree.type == "JOIN" or tree.type == "NATURAL JOIN":
                temp = []
                if tree.type == "JOIN":
                    temp = self.__joinOn(
                        self.evaluateSelectTree(tree.childs[0], [], [], transaction_id),
                        self.evaluateSelectTree(tree.childs[1], [], [], transaction_id),
                        tree.val
                    )
                elif tree.type == "NATURAL JOIN":
                    temp = self.__naturalJoin(
                        "temp1", "temp2",
                        self.evaluateSelectTree(tree.childs[0], [], [], transaction_id),
                        self.evaluateSelectTree(tree.childs[1], [], [], transaction_id)
                        )
                if len(select) > 0 and len(where) > 0:
                    temp = self.__filterSelect(temp, select)
                    temp = self.__filterWhere(temp, where)
                elif len(select) > 0:
                    temp = self.__filterSelect(temp, select)
                elif len(where) > 0:
                    temp = self.__filterWhere(temp, where)
                return temp
            elif tree.type == "ORDER BY":
                parse = tree.val.split()
                order_by_column = parse[0]
                is_asc = parse[1] == "ASC"
                return self.__orderBy(
                    self.evaluateSelectTree(tree.childs[0], select, where, transaction_id),
                    order_by_column,
                    is_asc
                )
            elif tree.type == "LIMIT":
                limit = int(tree.val)
                return self.evaluateSelectTree(tree.childs[0], select, where, transaction_id)[:limit]
            else:
                if tree.type == "SELECT":
                    select = tree.val
                elif tree.type == "WHERE":
                    where = tree.val
                for child in tree.childs:
                    return self.evaluateSelectTree(child, select, where, transaction_id)
    
    def removeTablename(self, data):
        result = []
        for col in data:
            result.append(col.split(".")[1])
        return result
    def addTablename(self, tablename, row):
        return {f"{tablename}.{key}": value for key, value in row.items()}
    def transformData(self, tablename, data):
        result = []
        for row in data:
            result.append(self.addTablename(tablename,row))
        return result
    
    def __filterSelect(self, data: List[dict], select: list[str]) -> List[dict]:
        return [{key: value for key, value in row.items() if key in select} for row in data]
    
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
        if parsed_query.type == "SELECT":
            tables = {}
            cols = {}
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

    def ParsedQueryToDataWrite(self) -> DataWrite:
        try:
            # Input: child (QueryTree with only where value)
            # Output: List of condition from child
            # Function to retreive update condition (only and)
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
            table = self.parsedQuery.query_tree.childs[0].val
                
            # Validate write access
            # response = self.cc.validate_object(table, self.current_transactionId, "write")
            # if not response.allowed:
            #     raise Exception(f"Transaction {self.current_transactionId} cannot write to table {table}")

            # Get new value
            new_value = self.parsedQuery.query_tree.childs[0].childs[0].val
            match = re.split(r'=', new_value)
            columns = match[0].strip()        
            new_value = match[1].strip().replace('"', '')
            

            # Get all conditions
            conditions = filter_condition(self.parsedQuery.query_tree.childs[0].childs[0].childs[0])
            return DataWrite([table], [columns], conditions, [new_value])
        except Exception as e:
            self.handle_rollback()
            return e

    def ParsedQueryToDataDeletion(self) -> DataDeletion:
        data_deletion = DataDeletion(
            table=self.parsedQuery.query_tree.val,
            conditions=[
                Condition(
                    column=cond.childs[0].val,
                    operation=cond.childs[1].val,
                    operand=cond.childs[2].val
                )
                for cond in self.parsedQuery.query_tree.childs if cond.type == "CONDITION"
            ]
        )
        return data_deletion

    def ParsedQueryToSetIndex(self) -> Tuple[str, str, str, str]:
        # Retreive index_name, table, and column
        main_query = self.parsedQuery.query_tree.childs[0].childs[0].val
        pattern = r"(\w+)\sON\s(\w+)\s\(\s(\w+)\s\)"
        match = re.match(pattern, main_query)
        if (match):
            nama_index, table, column = match.groups()

        # Retreive index_type
        index_type = self.parsedQuery.query_tree.childs[0].childs[0].childs[0].val
        return table, column, index_type, nama_index

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
    
    def handle_rollback(self, transaction_id):
        """
        Handle rollback when FailureRecovery calls rollback.

        Parameters:
            transaction_id (int): The ID of the transaction to rollback.
        """
        print("rollback transaction id: " ,transaction_id)
        undo_list = self.rm.write_log_entry(transaction_id, "ABORT", None, None, None)


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

            result = self.sm.write_block(data_write, db, transaction_id)
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
    
    def __getData(self, data_retrieval: DataRetrieval, transaction_id: int) -> dict|Exception:
        # fetches the required rows of data from the storage manager
        # and returns it as a dictionary

        # Example:
        # data_retrieval = DataRetrieval(table="students", 
        #                                columns=["a"], 
        #                                conditions=[Condition("a", ">", 1)])
        # database = "self.db_name"
        # getData(data_retrieval, database) = {'a': [2, 3, 4, 5]}

        data = self.sm.read_block(data_retrieval, self.db_name, self.current_transactionId)
        response = self.cc.validate_object(data, transaction_id, "read")
        if not response.allowed:
            print("Validation failed. Handling rollback.")
            print("Retrying query after rollback.")
            raise Exception(f"Cannot read {data_retrieval.table[0]}.")
        return data.data
    
    def __transCond(self, cond: str) -> list:
        result = [] 
        eqs = cond.split("AND")  # Split on commas
        for eq in eqs:
            temp = eq.split("=") # [t1.a , t2.b]
            result.append([temp[0].strip(),temp[1].strip()])
        return result

    def __joinOn(self, table1: list[map], table2: list[map], cond: str):
        result = []
        condList = self.__transCond(cond)      
        for r1 in table1:
            for r2 in table2:
                isValid = True
                for cond in condList:
                    # print(cond[0]) 
                    # print(cond[1]) 
                    if cond[0] in r1:
                        if r1[cond[0]] != r2[cond[1]]:
                            isValid = False
                            break
                    else:
                        if r1[cond[1]] != r2[cond[0]]:
                            isValid = False
                            break
                if(isValid):
                    result.append(r1 | r2) 
        # print(result)
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
        # database = "self.db_name"
        # deleteData(data_deletion, database, transaction_id) = 4

        try:
            rows_deleted = self.sm.delete_block(data_deletion, database, self.current_transactionId)
            return rows_deleted
        except Exception as e:
            return e
        
    def __orderBy(self, data: List[dict], order_by: str, is_asc: bool) -> List[dict]:
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

        if is_asc:
            return sorted(data, key=lambda x: x[order_by])
        else:
            return sorted(data, key=lambda x: x[order_by], reverse=True)

    def signal_handler(self, signum, frame):
        """
        Custom signal handler to handle SIGINT and SIGSEV.
        """
        print("masuk signal")
        self.cc.end_transaction(self.current_transactionId)
        self.sm.commit_buffer(self.current_transactionId)
        self.sm.save()
        print("Bye!")
        self.rm.signal_handler(signum, frame)
        # Raise the original signal  to allow the program to terminate