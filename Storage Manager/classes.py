import pickle
import os

class Condition:
    valid_operations = ["=", "<>", ">", ">=", "<", "<=", "!"] # untuk sementara "!" berarti no operation
    def __init__(self, column:str, operation:str, operand:int|str) -> None:
        self.column = column
        if operation in Condition.valid_operations:
            self.operation = operation
        else:
            self.operation = "!"
        self.operand = operand

class DataRetrieval:
    def __init__(self, table:list[str], column:list[str], conditions:list[Condition]) -> None:
        self.table = table
        self.column = column
        self.conditions = conditions

class DataWrite:
    def __init__(self, table:list[str], column:list[str], conditions:list[Condition], new_value:object) -> None:
        self.table = table
        self.column = column
        self.conditions = conditions
        self.new_value = new_value

class DataDeletion:
    def __init__(self, table:str, conditions:list[Condition]) -> None:
        self.table = table
        self.conditions = conditions

class Statistic:
    def __init__(self, n_r:int, b_r:int, l_r:int, f_r:int, V_a_r:dict[str, int]) -> None:
        self.n_r = n_r
        self.b_r = b_r
        self.l_r = l_r
        self.f_r = f_r
        self.V_a_r = V_a_r

class StorageEngine:
    def __init__(self) -> None:
        try:
            if not (os.path.isfile("data.dat")):
                pickle.dump({}, open("data.dat", "wb"))
            self.blocks = pickle.load(open("data.dat", "rb"))
        except Exception as e:
            print(f"error, {str(e)}")

    def save(self) -> None:
        try:
            pickle.dump(self.blocks, open("data.dat", "wb"))
        except Exception as e:
            print(f"error, {str(e)}")

    def create_database(self, database_name:str) -> bool:
        if database_name in self.blocks:
            return Exception(f"Sudah ada database dengan nama {database_name}")
        self.blocks[database_name] = {}
        return True
    
    def create_table(self, database_name:str, table_name:str, column_type:dict[str, str]) -> bool|Exception:
        if database_name in self.blocks:
            if table_name not in self.blocks[database_name]:
                self.blocks[database_name][table_name] = {
                    "columns" : [{"name" : nama_col, "type" : tipe_col} for nama_col, tipe_col in column_type.items()],
                    "values" : []
                }
                return True
            return Exception(f"Sudah ada table dengan nama {table_name} di database {database_name}")
        return Exception(f"Tidak ada database dengan nama {database_name}")
    
    def insert_data(self, database_name:str, table_name:str, data_insert:dict) -> bool|Exception:
        if database_name in self.blocks:
            if table_name in self.blocks[database_name]:
                self.blocks[database_name][table_name]["values"].append(data_insert)
                return True
            return Exception(f"Tidak ada table dengan nama {table_name} di database {database_name}")
        return Exception(f"Tidak ada database dengan nama {database_name}")

    def read_block(self, data_retrieval:DataRetrieval, database_name:str) -> dict|Exception:
        if database_name not in self.blocks:
            return Exception(f"Tidak ada database dengan nama {database_name}")
        for tabel in data_retrieval.table:
            if tabel not in self.blocks[database_name]:
                return Exception(f"Tidak ada tabel dengan nama {tabel}")
        result = []
        temp_values = []
        
        if data_retrieval.condition:
            pass
        else:
            for block in self.blocks:
                for record in block.records:
                    pass

    def write_block(self, data_write:DataWrite):
        pass

    def delete_block(self, data_deletion:DataDeletion):
        pass
    
    def debug(self):
        print(self.blocks)