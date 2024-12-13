import signal
import re

from FailureRecovery.failure_recovery_log_entry import LogEntry
from ConcurrencyControlManager.ConcurrencyControlManager import *
from QueryOptimizer.OptimizationEngine import *
from StorageManager.classes import *
from typing import Tuple
from client_class import Client

import FailureRecovery.failure_recovery as FailureRecovery
    
class QueryProcessor:
    def __init__(self, db_name: str, clients: dict):
        self.parsedQuery = None
        self.sm = StorageEngine()
        self.qo = OptimizationEngine(self.sm.get_stats)
        self.cc = ConcurrencyControlManager()
        self.rm = FailureRecovery.FailureRecovery()
        self.db_name = db_name
        self.clients = clients

        self.current_transactionId = 0 #SBD
        self.rm.write_log_entry(self.current_transactionId, "START", None, None, None)

        # Register exit and signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGSEGV, self.signal_handler)

    def execute_query(self, query : str, client: Client) -> str:
        """ Execute a query and return the result.

        Args:
            query (str): The query to execute.
            client (Client): The client that is executing the query.

        Returns: str
        """
        print("Executing query: " + query)
        client_state = client.state
        
        if(query.upper() == "BEGIN" or query.upper() == "BEGIN TRANSACTION"):
            if not client_state.get("on_begin", False):  
                self.current_transactionId = self.cc.begin_transaction()
                self.rm.write_log_entry(self.current_transactionId, "START", None, None, None)
                client_state["transactionId"] = self.current_transactionId
                client_state["on_begin"] = True
            else:
                print("Transaction already started.")
            
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
        
        elif(query.upper() == "ROLLBACK" or query.upper() == "ROLLBACK TRANSACTION"):
                    transaction_id = client_state["transactionId"]
                    self.handle_rollback(transaction_id)
                    client_state["on_begin"] = False
                    client_state["transactionId"] = None

        else:
            retry = True
            while retry:
                try:
                    self.parsedQuery = self.qo.parse_query(query,self.db_name) 
                    if self.parsedQuery.query_tree.val == "UPDATE":
                        try:
                            if not client_state.get("on_begin", False):
                                self.current_transactionId = self.cc.begin_transaction()
                                client_state["transactionId"] = self.current_transactionId
                            write = self.ParsedQueryToDataWrite()
                            transaction_id = client_state["transactionId"]
                            print("transaction id :", transaction_id)

                            # baca data lama
                            data_lama = self.sm.read_block(DataRetrieval(write.table, write.column, write.conditions), self.db_name, transaction_id)
                            # cek concurrency control
                            print("trans id real: ", transaction_id)
                            response = self.cc.validate_object(data_lama, transaction_id, "write")
                            print("response dr cc: ",response.allowed)
                            if not response.allowed:
                                print("Validation failed. Handling rollback.")
                                run_directly = self.handle_rollback(transaction_id)
                                if not run_directly :
                                    with response.condition :
                                        response.condition.wait()
                                client_state["transactionId"] = self.cc.begin_transaction()
                                print("Retrying query after rollback.")
                                continue  
                            
                            # write data
                            print("trans id before write: " ,transaction_id)
                            data_written = data_lama.get_data()[0].get(write.column[0])
                            object_value = f"{{'nama_db':'{self.db_name}','nama_tabel':'{write.table[0]}','nama_kolom':'{write.column[0]}','primary_key':'{write.conditions[0].column}','primary_key_value':'{write.conditions[0].operand}'}}"
                            self.rm.write_log_entry(transaction_id, "DATA", object_value, data_written, write.new_value)
                            self.sm.write_block(write, self.db_name, transaction_id)
                            self.sm.commit_buffer(transaction_id)                            
                        except Exception as e:
                            self.handle_rollback(transaction_id)
                            print(e) 

                    elif self.parsedQuery.query_tree.val == "SELECT":
                        try:
                            if not client_state.get("on_begin", False):
                                self.current_transactionId = self.cc.begin_transaction()
                                client_state["transactionId"] = self.current_transactionId

                            transaction_id = client_state["transactionId"]
                            print("select", transaction_id)
                            result = self.evaluateSelectTree(self.parsedQuery.query_tree,[],"", transaction_id)
                            ret_val = self.printResult(result)
                            print(f"Read {len(result)} row(s).")
                            return ret_val                            
                        
                        except Exception as res:
                            run_directly = self.handle_rollback(transaction_id)
                            if not run_directly :
                                    with res.condition :
                                        res.condition.wait()
                            client_state["transactionId"] = self.cc.begin_transaction()

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
        """Evaluate the select tree and return the result.

        Args:
            tree (QueryTree): The tree to evaluate.
            select (list[str]): The columns to select.
            where (str): The where clause.
            transaction_id (int): The transaction ID.

        Returns:
            list[dict]: The result of the evaluation.
        """
        if not tree.childs: # leaf node
            condition = []
            if len(where) > 0:
                condition = self.__makeCondition(where)
            select = self.removeTablename(select)
            dataRetriev = DataRetrieval([tree.val], select, condition)
            
            temp = self.transformData(tree.val,self.__getData(dataRetriev, transaction_id))
            
            return temp
        else: # non-leaf node
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
                        self.evaluateSelectTree(tree.childs[0], [], [], transaction_id),
                        self.evaluateSelectTree(tree.childs[1], [], [], transaction_id),
                        tree.val
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
    
    def removeTablenameCond(self, conds: list[Condition]) -> None:
        """Remove the table name from the column in the conditions.

        Args:
            conds (list[Condition]): The conditions to remove the table name from.
        """
        for cond in conds:
            temp = cond.column.split(".")
            cond.column = temp[1]

    def removeTablename(self, data: list[map]) -> list[map]:
        """Remove the table name from the column in the data.

        Args:
            data (list[map]): The data to remove the table name from.

        Returns:
            list[map]: The data with the table name removed.
        """
        result = []
        for col in data:
            result.append(col.split(".")[1])
        return result
    
    def addTablename(self, tablename: str, row: map) -> map:
        """Add the table name to the column in the row.

        Args:
            tablename (str): The table name to add.
            row (map): The row to add the table name to.

        Returns:
            map: The row with the table name added.
        """
        return {f"{tablename}.{key}": value for key, value in row.items()}
    
    def transformData(self, tablename: str, data: list[map]) -> list[map]:
        """Transform the data by adding the table name to the column.

        Args:
            tablename (str): The table name to add.
            data (list[map]): The data to transform.

        Returns:
            list[map]: The transformed data.
        """
        result = []
        for row in data:
            result.append(self.addTablename(tablename,row))
        return result
    
    def __filterSelect(self, data: List[dict], select: list[str]) -> List[dict]:
        """Filter the data by selecting only the specified columns.

        Args:
            data (List[dict]): The data to filter.
            select (list[str]): The columns to select.

        Returns:
            List[dict]: The filtered data.
        """
        return [{key: value for key, value in row.items() if key in select} for row in data]
    
    def __evalWhere(self, row: map, conds: list[Condition]) -> bool:
        """Evaluate the where clause.

        Args:
            row (map): The row to evaluate the where clause on.
            conds (list[Condition]): The conditions to evaluate.

        Returns:
            bool: True if the row satisfies the conditions, False otherwise.
        """
        for cond in conds:
            if(cond.operand.is_integer()):
                if(cond.operation == "<>" and row[cond.column] != cond.operand):
                    return True
                elif(cond.operation == ">=" and row[cond.column] >= cond.operand):
                    return True
                elif(cond.operation == "<=" and row[cond.column] <= cond.operand):
                    return True
                elif(cond.operation == "=" and row[cond.column] == cond.operand):
                    return True
                elif(cond.operation == ">" and row[cond.column] > cond.operand):
                    return True
                elif(cond.operation == "<" and row[cond.column] < cond.operand):
                    return True
            else:
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
        """Filter the data by the where clause.

        Args:
            data (list[map]): The data to filter.
            where (str): The where clause.

        Returns:
            list[map]: The filtered data.
        """
        result = []
        cond = self.__makeCondition(where)
        for row in data:
            if self.__evalWhere(row,cond):
                result.append(row)
        return result
    
    def __makeCondition(self, where: str) -> List[Condition]:
        """Make the conditions from the where clause.

        Args:
            where (str): The where clause.

        Returns:
            List[Condition]: The conditions.
        """  
        eqs = where.split("OR")
        cond = [] 
        for eq in eqs:
            number_pattern = r"^-?\d+(\.\d+)?$"
            if("<>" in eq):
                temp = eq.split("<>")
                temp[1] = temp[1].strip()
                if(re.match(number_pattern,temp[1])):
                    temp[1] = int(temp[1])
                cond.append(Condition(temp[0].strip(),"<>",temp[1]))
            elif(">=" in eq):
                temp = eq.split(">=")
                temp[1] = temp[1].strip()
                if(re.match(number_pattern,temp[1])):
                    temp[1] = int(temp[1])
                cond.append(Condition(temp[0].strip(),">=",temp[1]))
            elif("<=" in eq):
                temp = eq.split("<=")
                temp[1] = temp[1].strip()
                if(re.match(number_pattern,temp[1])):
                    temp[1] = int(temp[1])
                cond.append(Condition(temp[0].strip(),"<=",temp[1]))
            elif("=" in eq):
                temp = eq.split("=")
                temp[1] = temp[1].strip()
                if(re.match(number_pattern,temp[1])):
                    temp[1] = int(temp[1])
                cond.append(Condition(temp[0].strip(),"=",temp[1]))
            elif(">" in eq):
                temp = eq.split(">")
                temp[1] = temp[1].strip()
                if(re.match(number_pattern,temp[1])):
                    temp[1] = int(temp[1])
                cond.append(Condition(temp[0].strip(),">",temp[1]))
            elif("<" in eq):
                temp = eq.split("<")
                temp[1] = temp[1].strip()
                if(re.match(number_pattern,temp[1])):
                    temp[1] = int(temp[1])
                cond.append(Condition(temp[0].strip(),"<",temp[1]))
        return cond

    def ParsedQueryToDataWrite(self) -> DataWrite:
        """Convert the parsed query to a DataWrite object.

        Returns:
            DataWrite: The DataWrite object.
        """
        try:
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
        """Convert the parsed query to a DataDeletion object.

        Returns:
            DataDeletion: The DataDeletion object.
        """
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
        """Convert the parsed query to a tuple containing the index name, table, column, and index type.

        Returns:
            Tuple[str, str, str, str]: The index name, table, column, and index type.
        """
        # Retreive index_name, table, and column
        main_query = self.parsedQuery.query_tree.childs[0].childs[0].val
        pattern = r"(\w+)\sON\s(\w+)\s\(\s(\w+)\s\)"
        match = re.match(pattern, main_query)
        if (match):
            nama_index, table, column = match.groups()

        # Retreive index_type
        index_type = self.parsedQuery.query_tree.childs[0].childs[0].childs[0].val
        return table, column, index_type, nama_index

    def printResult(self, data:list[map]) -> str:
        """Print the result of the query.

        Args:
            data (list[map]): The data to print.

        Returns:
            str: The printed data.
        """
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
        
    def parse_query(self, query : str) -> list[str]:
        """Parse the query.

        Args:
            query (str): The query to parse.

        Returns:
            list[str]: The parsed queries.
        """
        queries = query.split(';')
        return [q.strip() for q in queries if q.strip()]
    
    def handle_rollback(self, transaction_id) -> None:
        """ Handle rollback when FailureRecovery calls rollback.

        Args:
            transaction_id (int): The ID of the transaction to rollback.

        Returns:
            None
        """
        print("rollback transaction id: " ,transaction_id)
        undo_list = self.rm.write_log_entry(transaction_id, "ABORT", None, None, None)
        print (undo_list)


        # Retrieve undo instructions from FailureRecovery
        for instruction in undo_list['undo']:
            print(instruction)
            object_value = instruction["object_value"]
            old_value = instruction["old_value"]
            db, table, column, primary_key, primary_key_value = self.__parse_object_value(object_value)

            conditions = []
            if primary_key and primary_key_value:
                conditions.append(Condition(
                    column=primary_key,
                    operation="=",
                    operand=int(primary_key_value)
                ))

            data_write = DataWrite(
                table=[table],
                column=[column],
                conditions=conditions,
                new_value=[old_value]
            )

            result = self.sm.write_block(data_write, db, transaction_id)
            if isinstance(result, Exception):
                print(f"Error during rollback: {result}")
            else:
                print(f"Rollback successful, {result} rows updated.")

        return self.cc.end_transaction(transaction_id)

    def __parse_object_value(self, object_value: str) -> Tuple[str, str, str, str, str]:
        """Parse the object value to extract table and column information.
        
        Args:
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
        primary_key = obj.get('primary_key')
        primary_key_value = obj.get('primary_key_value')
        return db, table, column, primary_key, primary_key_value    
    
    def __getData(self, data_retrieval: DataRetrieval, transaction_id: int) -> list[dict]|Exception:
        """Get the data from the storage manager.

        Args:
            data_retrieval (DataRetrieval): The data retrieval object.
            transaction_id (int): The transaction ID.

        Raises:
            Exception: If the data retrieval fails.

        Returns:
            list[dict]|Exception: The data or an exception.
        """
        data = self.sm.read_block(data_retrieval, self.db_name, transaction_id)
        response = self.cc.validate_object(data, transaction_id, "read")
        if not response.allowed:
            print("Validation failed. Handling rollback.")
            print("Retrying query after rollback.")
            raise Exception(response)
        return data.data
    
    def __transCond(self, cond: str) -> list:
        """Transform the condition string to a list of conditions.

        Args:
            cond (str): The condition string.

        Returns:
            list: The list of conditions.
        """
        result = [] 
        eqs = cond.split("AND")  # Split on commas
        for eq in eqs:
            temp = eq.split("=") # [t1.a , t2.b]
            result.append([temp[0].strip(),temp[1].strip()])
        return result

    def __joinOn(self, table1: list[map], table2: list[map], cond: str) -> list[map]:
        """Join two tables on the specified condition.

        Args:
            table1 (list[map]): The first table to join.
            table2 (list[map]): The second table to join.
            cond (str): The condition to join on.

        Returns:
            list[map]: The joined table.
        """
        result = []
        condList = self.__transCond(cond)      
        for r1 in table1:
            for r2 in table2:
                isValid = True
                for cond in condList:
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
        return result
    
    def __naturalJoin(self, table1: list[dict], table2: list[dict], cols: list[str]) -> list[dict]:
        """Join two tables on the specified columns.

        Args:
            table1 (list[dict]): row of table1
            table2 (list[dict]): row of table2
            cols (list[str]): The columns to join on.

        Returns:
            list[dict]: The joined table.
        """
        joined_table = []
        col1 = list(table1[0].keys())
        name1 = col1[0].split(".")[0]
        col2 = list(table2[0].keys())
        name2 = col2[0].split(".")[0]
        for row1 in table1:
            for row2 in table2:
                # Check if rows match on all specified columns
                if all(row1[name1+"."+col] == row2[name2+"."+col] for col in cols):
                    # Merge rows, avoiding duplicate keys from table2
                    joined_row = {**row1, **{k: v for k, v in row2.items() if k not in row1}}
                    joined_table.append(joined_row)
        return joined_table
    
    def __orderBy(self, data: List[dict], order_by: str, is_asc: bool) -> List[dict]:
        """Order the data by the specified column.

        Args:
            data (List[dict]): The data to order.
            order_by (str): The column to order by.
            is_asc (bool): Whether to order in ascending order.

        Returns:
            List[dict]: The ordered data.
        """
        if is_asc:
            return sorted(data, key=lambda x: x[order_by])
        else:
            return sorted(data, key=lambda x: x[order_by], reverse=True)

    def signal_handler(self,client_id, signum, frame):
        """Custom signal handler to handle SIGINT and SIGSEV.

        Args:
            signum (int): The signal number.
            frame (frame): The frame object.

        Returns:
            None
        """
        print("Signal received. Handling transactions for all clients...")
        for client_id, client in self.clients.items():
            client_state = client.state
            print("client state: ", client_state)
            transaction_id = client_state.get("transactionId")
            print(f"Client {client_id} state: {client_state}") 
            if transaction_id is not None:
                print(f"Rolling back transaction {transaction_id} for client {client_id}.")
                self.cc.end_transaction(transaction_id)
                self.sm.commit_buffer(transaction_id)
                self.sm.save()
        self.rm.signal_handler(signum, frame)