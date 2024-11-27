import pickle
import os
from Bplus import BPlusTree

class Condition:
    valid_operations = ["=", "<>", ">", ">=", "<", "<=", "!"] # untuk sementara "!" berarti no operation
    def __init__(self, column:str, operation:str, operand:int|str) -> None:
        self.column = column
        if operation in Condition.valid_operations:
            self.operation = operation
        else:
            self.operation = "!"
        self.operand = operand
    
    def evaluate(self, item:int|str):
        if self.operation == "=":
            return item == self.operand
        elif self.operation == "<>":
            return item != self.operand
        elif self.operation == ">":
            return item > self.operand
        elif self.operation == ">=":
            return item >= self.operand
        elif self.operation == "<":
            return item < self.operand
        else:
            return item <= self.operand

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

    @staticmethod
    def print_statistics(self):
        print(f"Number of tuples in relation r: {self.n_r}")
        print(f"Number of blocks containing tuples of r: {self.b_r}")
        print(f"Size of tuple of r : {self.l_r}")
        print(f"Blocking Factor : {self.b_r}")
        print(f"Number of distinct values that appear in r for attribute A: {self.V_a_r}")

class StorageEngine:
    def __init__(self) -> None:
        self.load()
        self.buffer = {}

    def load(self) -> None:
        try:
            if not (os.path.isfile("data.dat")):
                pickle.dump({}, open("data.dat", "wb"))
            self.blocks = pickle.load(open("data.dat", "rb"))
        except Exception as e:
            print(f"error, {str(e)}")

    def commit_buffer(self, transaction_id:int) -> None:
        try:
            self.blocks = self.buffer[transaction_id]
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
                    "values" : [],
                    "indexes" : {}
                }
                return True
            return Exception(f"Sudah ada table dengan nama {table_name} di database {database_name}")
        return Exception(f"Tidak ada database dengan nama {database_name}")
    
    def insert_data(self, database_name:str, table_name:str, data_insert:dict, transaction_id:int) -> bool|Exception:
        if database_name in self.blocks:
            if table_name in self.blocks[database_name]:
                self.blocks[database_name][table_name]["values"].append(data_insert)
                return True
            return Exception(f"Tidak ada table dengan nama {table_name} di database {database_name}")
        return Exception(f"Tidak ada database dengan nama {database_name}")

    def read_block(self, data_retrieval:DataRetrieval, database_name:str, transaction_id:int) -> dict|Exception:
        # error handling
        if database_name not in self.blocks:
            return Exception(f"Tidak ada database dengan nama {database_name}")
        for tabel in data_retrieval.table:
            if tabel not in self.blocks[database_name]:
                return Exception(f"Tidak ada tabel dengan nama {tabel}")
        column_tabel_query = []
        for tabel in data_retrieval.table:
            for kolom in self.blocks[database_name][tabel]["columns"]:
                column_tabel_query.append(kolom["name"])
        for kolom in data_retrieval.column:
            if kolom not in column_tabel_query:
                return Exception(f"Tidak ada kolom dengan nama {kolom}")
        if data_retrieval.conditions:
            for kondisi in data_retrieval.conditions:
                if kondisi.column not in column_tabel_query:
                    return Exception(f"Tidak ada kolom dengan nama {kondisi.column}")

        # di bawah ini, udah pasti tidak ada error dari input

        # cross terlebih dahulu dari tabel-tabel yang dipilih
        hasil_cross = self.blocks[database_name][data_retrieval.table[0]]["values"]
        for tabel_lainnya in data_retrieval.table[1:]:
            temp = self.blocks[database_name][tabel_lainnya]["values"]
            temp_hasil = []
            for row_hasil_cross in hasil_cross:
                for row_hasil_temp in temp:
                    temp_hasil.append({**row_hasil_cross, **row_hasil_temp})
            hasil_cross = temp_hasil

        # lalu hapus data dari hasil_cross yang tidak memenuhi kondisi
        hasil_operasi = []
        if data_retrieval.conditions:
            for kondisi in data_retrieval.conditions:
                for row in hasil_cross:
                    if kondisi.evaluate(row[kondisi.column]):
                        hasil_operasi.append(row)
        else:
            hasil_operasi = hasil_cross

        # lalu ambil hanya kolom yang diinginkan
        hasil_akhir = [{key: d[key] for key in data_retrieval.column if key in d} for d in hasil_operasi]

        # return akhir
        return hasil_akhir

    def write_block(self, data_write: DataWrite, database_name: str, transaction_id:int) -> int | Exception:
        if database_name not in self.blocks:
            return Exception(f"Tidak ada database dengan nama {database_name}")
        if data_write.table not in self.blocks[database_name]:
            return Exception(f"Tidak ada tabel dengan nama {data_write.table}")
        column_tabel_query = [col["name"] for col in self.blocks[database_name][data_write.table]["columns"]]
        if data_write.conditions:
            for kondisi in data_write.conditions:
                if kondisi.column not in column_tabel_query:
                    return Exception(f"Tidak ada kolom dengan nama {kondisi.column}")
        if not all(key in column_tabel_query for key in data_write.column):
            return Exception(f"Beberapa kolom yang akan diubah tidak ada di tabel {data_write.table}")

        # Tidak ada error, lanjutkan proses
        affected_rows = 0
        data_baru = []
        
        for row in self.blocks[database_name][data_write.table]["values"]:
            update_row = False
            if data_write.conditions:
                # Cek apakah row memenuhi semua kondisi
                update_row = all(kondisi.evaluate(row[kondisi.column]) for kondisi in data_write.conditions)
            else:
                # Jika tidak ada kondisi, semua baris akan diupdate
                update_row = True

            # Update nilai jika memenuhi kondisi
            if update_row:
                for col, value in zip(data_write.column, data_write.new_value):
                    row[col] = value
                affected_rows += 1

            data_baru.append(row)
        
        # Update data di tabel
        self.blocks[database_name][data_write.table]["values"] = data_baru
        self.save()
        print(f"Data berhasil diupdate, {affected_rows} baris diubah")
        return affected_rows


    def delete_block(self, data_deletion:DataDeletion, database_name:str, transaction_id:int) -> int:
        # error handling
        if database_name not in self.blocks:
            return Exception(f"Tidak ada database dengan nama {database_name}")  
        if data_deletion.table not in self.blocks[database_name]:
            return Exception(f"Tidak ada tabel dengan nama {data_deletion.table}")
        column_tabel_query = []
        for kolom in self.blocks[database_name][data_deletion.table]["columns"]:
            column_tabel_query.append(kolom["name"])
        if data_deletion.conditions:
            for kondisi in data_deletion.conditions:
                if kondisi.column not in column_tabel_query:
                    return Exception(f"Tidak ada kolom dengan nama {kondisi.column}")
                
        # seharusnya tidak ada error di sini
        data_baru = []
        affected_row = 0
        for kondisi in data_deletion.conditions:
            for row in self.blocks[database_name][data_deletion.table]["values"]:
                if not kondisi.evaluate(row[kondisi.column]):
                    data_baru.append(row)
                else:
                    affected_row += 1
            self.blocks[database_name][data_deletion.table]["values"] = data_baru
            data_baru = []
        print(f"Data berhasil dihapus, {affected_row} baris dihapus")

        return affected_row
    def get_stats(self, database_name:str , table_name: str, block_size=4096) -> Statistic | Exception:
        if database_name not in self.blocks:
            return Exception(f"Tidak ada database dengan nama {database_name}")
        if table_name not in self.blocks[database_name]:
            return Exception(f"Tidak ada table dengan nama {table_name}")
        
        table = self.blocks[database_name][table_name]
        rows = table["values"]
        columns = table["columns"]

        # 1. nr
        nr = len(rows)

        # 2. lr
        type_size = {
        "INTEGER": 4,  # byte integer
        "TEXT": 50,  #misal max string lenth 50 char
        "FLOAT" : 4
        }

        lr = sum(type_size.get(col["type"], 0) for col in columns)

        # 3. fr
        fr = block_size // lr if lr > 0 else 0

        # 4. number of blocks
        br = (nr + fr -1) // fr if fr > 0 else 0

        # 5. V(A,r)
        V_a_r = {}

        for col in columns:
            attribute = col["name"]
            V_a_r[attribute] = len(set(row[attribute] for row in rows if attribute in row))

        return Statistic(n_r=nr, b_r=br, l_r=lr, f_r=fr, V_a_r=V_a_r)
    
    def set_index(self, database_name: str, table_name: str, column: str) -> None:
        if database_name not in self.blocks:
            raise ValueError(f"Database '{database_name}' does not exist.")
        
        if table_name not in self.blocks[database_name]:
            raise ValueError(f"Table '{table_name}' does not exist in database '{database_name}'.")
        
        table = self.blocks[database_name][table_name]
        if not any(col["name"] == column for col in table["columns"]):
            raise ValueError(f"Column '{column}' does not exist in table '{table_name}'.")
        if "indexes" not in table:
            table["indexes"] = {}
        bplus_tree = BPlusTree(order=4)
        for row_index, row in enumerate(table["values"]):
            if column not in row:
                raise ValueError(f"Column '{column}' is missing in a row of table '{table_name}'.")
            
            key = row[column]  
            bplus_tree.insert(key, row_index) 
        table["indexes"][column] = bplus_tree
        print(f"B+ Tree index created for column '{column}' in table '{table_name}'.")

    def search_with_index(self, database_name: str, table_name: str, column: str, key) -> list :
        if database_name not in self.blocks:
            raise ValueError(f"Database '{database_name}' does not exist.")
        if table_name not in self.blocks[database_name]:
            raise ValueError(f"Table '{table_name}' does not exist in database '{database_name}'.")
        
        table = self.blocks[database_name][table_name]
        if "indexes" not in table or column not in table["indexes"]:
            raise ValueError(f"No index exists for column '{column}' in table '{table_name}'.")
        index = table["indexes"][column]
        result_indices = index.search(key)
        values = table["values"]
        if isinstance(result_indices, list): 
            return [values[i] for i in result_indices]
        elif result_indices is not None:  
            return [values[result_indices]]
        else:  
            return []
        
    def search_range_with_index(self, database_name:str, table_name: str, column: str, start, end) -> list:
        if database_name not in self.blocks:
            raise ValueError(f"Database '{database_name}' does not exist.")
        if table_name not in self.blocks[database_name]:
            raise ValueError(f"Table '{table_name}' does not exist in database '{database_name}'.")
        
        table = self.blocks[database_name][table_name]
        if "indexes" not in table or column not in table["indexes"]:
            raise ValueError(f"No index exists for column '{column}' in table '{table_name}'.")
        
        index = table["indexes"][column]
        result_indices = index.search_range(start, end)
        values = table["values"]
        return [values[i] for i in result_indices]

    def debug(self):
        print(self.blocks)