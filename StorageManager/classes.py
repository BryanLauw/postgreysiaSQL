import pickle
import os
import copy
from StorageManager.Bplus import BPlusTree
from StorageManager.Hash import HashTable
from QueryProcessor.Rows import Rows

class Condition:
    valid_operations = ["=", "<>", ">", ">=", "<", "<=", "!"] # untuk sementara "!" berarti no operation
    def __init__(self, column:str, operation:str, operand:int|str) -> None:
        self.column = column
        if operation not in Condition.valid_operations:
            operation = "!"
        self.operation = operation
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
    def __init__(self, tables:list[str], columns:list[str], conditions:list[Condition]) -> None:
        self.table = tables
        self.column = columns
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
        self.load_indexes()
        self.buffer = {}
        self.buffer_index = {}

    def get_database_names(self) -> list[str]:
        databases = []
        for database in self.blocks:
            databases.append(database)
        return databases
    
    def get_tables_of_database(self, database_name:str) -> list[str]:
        tables = []
        for table in self.blocks[database_name] :
            tables.append(table)
        return tables
    
    def get_columns_of_table(self, database_name:str, table_name:str) -> list[str]:
        columns = []
        for column in self.blocks[database_name][table_name]["columns"]:
            columns.append(column["name"])
        return columns
    
    def get_table_metadata(self, database_name:str, table_name:str) -> dict:
        return self.blocks[database_name][table_name]['columns']
    
    def load(self) -> None:
        try:
            if not (os.path.isfile("data.dat")):
                pickle.dump({}, open("data.dat", "wb"))
            self.blocks = pickle.load(open("data.dat", "rb"))
        except Exception as e:
            print(f"error, {str(e)}")

    def commit_buffer(self, transaction_id:int) -> None:
        """
        fungsi untuk commit transaction_id buat disave ke file utama
        """
        try:
            tempBlocks = self.buffer.get(transaction_id, [])
            if tempBlocks != []:
                self.blocks = tempBlocks
                self.buffer.pop(transaction_id)
            tempIndexes = self.buffer_index.get(transaction_id, [])
            if tempIndexes != []:
                self.indexes = tempIndexes
                self.buffer_index.pop(transaction_id)
        except Exception as e:
            print(f"error, {str(e)}")

    def load_indexes(self) -> None:
        try:
            if not os.path.isfile("indexes.dat"):
                pickle.dump({}, open("indexes.dat", "wb"))
            self.indexes = pickle.load(open("indexes.dat", "rb"))
        except Exception as e:
            print(f"Error initializing indexes: {str(e)}")
            self.indexes = {}

    def save(self) -> None:
        """
        bakal ngedump file utama di variabel ke file binary (data.dat)
        """
        try:
            pickle.dump(self.blocks, open("data.dat", "wb"))
        except Exception as e:
            print(f"error, {str(e)}")

    def save_indexes(self):
        try:
            pickle.dump(self.indexes, open("indexes.dat","wb"))
        except Exception as e:
             print(f"error, {str(e)}")

    def create_database(self, database_name:str) -> bool:
        """
        bikin database baru, tinggal masuking string aja, misal "database1"
        """
        if database_name in self.blocks:
            return Exception(f"Sudah ada database dengan nama {database_name}")
        self.blocks[database_name] = {}
        return True
    
    def create_table(self, database_name:str, table_name:str, column_type:dict[str, str], informasi_tambahan:dict[str, list[str]]) -> bool|Exception:
        """
        bikin tabel baru\n
        database_name tinggal string, misal "database1"\n
        table_name tinggal string, misal "id_user"\n
        column_type isinya dict[nama_column, tipe_column], misal {"id_user" : "INTEGER", "nama_user" : "VARCHAR(255)"} (tolong caps untuk tipenya, biar bisa diitung bytenya)\n
        informasi_tambahan misal {"id_user" : ["PRIMARY KEY", "UNIQUE"], "nama_user" : ["UNIQUE", "FOREIGN KEY"]} 
        """
        if database_name in self.blocks:
            if table_name not in self.blocks[database_name]:
                self.blocks[database_name][table_name] = {
                    "columns" : [{"name" : nama_col, "type" : tipe_col} for nama_col, tipe_col in column_type.items()],
                    "values" : [[]],
                } 
                for info in informasi_tambahan:
                    for i in range(len(self.blocks[database_name][table_name]["columns"])):
                        if self.blocks[database_name][table_name]["columns"][i]["name"] == info:
                            self.blocks[database_name][table_name]["columns"][i]["constraints"] = informasi_tambahan[info]
                # (STC) calculate max_record in 1 blocks not yet to be implemented
                # this is a PLACEHOLDER
                self.blocks[database_name][table_name]["max_record"] = 5
                return True
            return Exception(f"Sudah ada table dengan nama {table_name} di database {database_name}")
        return Exception(f"Tidak ada database dengan nama {database_name}")
    
    def insert_data(self, database_name:str, table_name:str, data_insert:dict, transaction_id:int) -> bool|Exception:
        """
        ngeinsert data baru. (Tidak menghandle duplicate data.)\n
        database_name tinggal string, misal "database1"\n
        table_name tinggal string, misal "id_user"\n
        data_insert tuh isinya kaya {"id_user" : 1, "nama_user" : "mas fuad"}
        """
        if database_name in self.blocks:
            if table_name in self.blocks[database_name]:
                self.buffer[transaction_id] = self.buffer.get(transaction_id, copy.deepcopy(self.blocks))
                temp = self.buffer[transaction_id][database_name][table_name]["values"]
                # (STC) harus ngisi record yang kosong juga (misal kosong di tengah2)
                if (len(temp[len(temp)-1]) >= self.buffer[transaction_id][database_name][table_name]["max_record"]): # blocks paling akhirnya penuh
                    self.buffer[transaction_id][database_name][table_name]["values"].append([data_insert])
                else:
                    self.buffer[transaction_id][database_name][table_name]["values"][len(temp)-1].append(data_insert)
                return True
            return Exception(f"Tidak ada table dengan nama {table_name} di database {database_name}")
        return Exception(f"Tidak ada database dengan nama {database_name}")

    def initialize_index_structure(self, database_name:str, table_name:str, column:str) -> None:
        if database_name not in self.indexes:
            self.indexes[database_name] = {}
        if table_name not in self.indexes[database_name]:
            self.indexes[database_name][table_name] = {}
        if column not in self.indexes[database_name][table_name]:
            self.indexes[database_name][table_name][column] = {}

    def read_block(self, data_retrieval:DataRetrieval, database_name:str, transaction_id:int) -> Rows|Exception:
        """
        Bakal ngeread block dan akan mereturn tipe bentukan Row (liat QueryProcessor/Rows.py)\n
        untuk argumennya silahkan liat tipe bentukan DataRetrieval di atas\n
        akan mencoba mereturn data hasil edit transaction_id, jika tidak ada, akan direturn data default.\n
        kalo mau ngambil data default, kasih transaction_id = -1 (atau angka apapun yang gaakan dipakai untuk transaction_id)
        """
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

        data_dibaca = self.buffer.get(transaction_id, copy.deepcopy(self.blocks))

        hasil_cross = []
        for blocks in data_dibaca[database_name][data_retrieval.table[0]]["values"]:
            for records in blocks:
                hasil_cross.append(records) 
        for tabel_lainnya in data_retrieval.table[1:]:
            temp = []
            for blocks in data_dibaca[database_name][tabel_lainnya]["values"]:
                for records in blocks:
                    temp.append(records)
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
        if data_retrieval.column:
            hasil_akhir = [{key: d[key] for key in data_retrieval.column if key in d} for d in hasil_operasi]
        else: 
            hasil_akhir = hasil_operasi
        # return akhir
        return Rows(hasil_akhir, len(hasil_akhir))

    def write_block(self, data_write: DataWrite, database_name: str, transaction_id: int) -> int | Exception:
        """
        Bakal ngewrite block yang masuk condition (operasinya AND) dan akan mereturn berapa row affected\n
        untuk argumennya silahkan liat tipe bentukan DataWrite di atas\n
        akan mencoba mengedit data hasil transaksi sebelumnya di transaction_id\n
        jika tidak ada, akan mengedit data default dan hasilnya disimpan di buffer transaction_id
        """
        if database_name not in self.blocks:
            return Exception(f"Tidak ada database dengan nama {database_name}")
        
        affected_rows_total = 0  # Total baris yang diubah

        for table in data_write.table:
            if table not in self.blocks[database_name]:
                return Exception(f"Tidak ada tabel dengan nama {table} di database {database_name}")
            
            column_tabel_query = [col["name"] for col in self.blocks[database_name][table]["columns"]]
            if data_write.conditions:
                for kondisi in data_write.conditions:
                    if kondisi.column not in column_tabel_query:
                        return Exception(f"Tidak ada kolom dengan nama {kondisi.column} di tabel {table}")
            if not all(key in column_tabel_query for key in data_write.column):
                return Exception(f"Beberapa kolom yang akan diubah tidak ada di tabel {table}")
            
            # Tidak ada error, lanjutkan proses untuk tabel ini
            affected_rows = 0
            data_baru = []
            tempData = self.buffer.get(transaction_id, copy.deepcopy(self.blocks))
            for block in tempData[database_name][table]["values"]:
                block_baru = []
                for record in block:
                    update_row = False
                    recordBaru = copy.deepcopy(record)
                    if data_write.conditions:
                        # Cek apakah row memenuhi semua kondisi
                        update_row = all(kondisi.evaluate(recordBaru[kondisi.column]) for kondisi in data_write.conditions)
                    else:
                        # Jika tidak ada kondisi, semua baris akan diupdate
                        update_row = True

                    # Update nilai jika memenuhi kondisi
                    if update_row:
                        for col, value in zip(data_write.column, data_write.new_value):
                            recordBaru[col] = value
                        affected_rows += 1

                    block_baru.append(recordBaru)
                data_baru.append(block_baru)

            tempData[database_name][table]["values"] = data_baru
            self.buffer[transaction_id] = tempData

            affected_rows_total += affected_rows  # Tambahkan jumlah baris yang diubah untuk tabel ini
        
        print(f"Data berhasil diupdate, total {affected_rows_total} baris diubah di semua tabel")
        return affected_rows_total


    def delete_block(self, data_deletion:DataDeletion, database_name:str, transaction_id:int) -> int:
        """
        Bakal ngedelete data yang masuk condition (operasinya AND) dan akan mereturn berapa row affected\n
        untuk argumennya silahkan liat tipe bentukan DataDeletion di atas\n
        akan mencoba mengdelete data hasil transaksi sebelumnya di transaction_id\n
        jika tidak ada, akan mengdelete data default dan hasilnya disimpan di buffer transaction_id
        """
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
        tempData = self.buffer.get(transaction_id, copy.deepcopy(self.blocks))
        for block in tempData[database_name][data_deletion.table]["values"]:
            block_baru = []
            for record in block:
                if data_deletion.conditions:
                    if not (all(kondisi.evaluate(record[kondisi.column]) for kondisi in data_deletion.conditions)):
                        # berarti tidak memenuhi kondisi, bakal dicopy
                        block_baru.append(record)
                    else:
                        affected_row += 1
                else:
                    block_baru.append(record)
            data_baru.append(block_baru)
        
        # self.buffer[transaction_id] = copy.deepcopy(self.blocks)
        tempData[database_name][data_deletion.table]["values"] = data_baru 
        self.buffer[transaction_id] = tempData
        print(f"Data berhasil dihapus, {affected_row} baris dihapus")
        return affected_row
    
    def get_stats(self, database_name:str , table_name: str, block_size=4096) -> Statistic | Exception:
        if database_name not in self.blocks:
            raise ValueError(f"Tidak ada database dengan nama {database_name}")
        if table_name not in self.blocks[database_name]:
            raise ValueError(f"Tidak ada table dengan nama {table_name}")
        
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
    
    def retrieve_table_of_database(self, database_name:str) -> dict:
        if database_name not in self.blocks:
            raise ValueError(f"Database '{database_name}' does not exist.")
        return self.blocks[database_name]
    
    def validate_column_buffer(self, database_name: str, table_name: str, column: str, trancaction_id:int) -> None:
        if database_name not in self.buffer[trancaction_id]:
            raise ValueError(f"Database '{database_name}' does not exist.")
        if table_name not in self.buffer[trancaction_id][database_name]:
            raise ValueError(f"Table '{table_name}' does not exist.")
        table = self.buffer[trancaction_id][database_name][table_name]
        if not any(col["name"] == column for col in table["columns"]):
            raise ValueError(f"Column '{column}' does not exist in table '{table_name}'.")
        
    def validate_column_buffer(self, database_name: str, table_name: str, column: str, trancaction_id:int) -> None:
        if database_name not in self.buffer[trancaction_id]:
            raise ValueError(f"Database '{database_name}' does not exist.")
        if table_name not in self.buffer[trancaction_id][database_name]:
            raise ValueError(f"Table '{table_name}' does not exist.")
        table = self.buffer[trancaction_id][database_name][table_name]
        if not any(col["name"] == column for col in table["columns"]):
            raise ValueError(f"Column '{column}' does not exist in table '{table_name}'.")
        
    def bplus_locator(self, database_name: str, table_name: str, column: str, transaction_id:int) -> None:
        self.validate_column_buffer(database_name, table_name, column, transaction_id)
        if self.is_bplus_index_in_buffer(database_name, table_name, column, transaction_id): # index adanya di buffer
            return self.buffer_index[transaction_id][database_name][table_name][column]["bplus"]
        elif self.is_bplus_index_in_block(database_name, table_name, column): # index adanya di block
            return self.indexes[database_name][table_name][column]["bplus"]
        else :
            raise ValueError("BPlus index not found")
        
    def hash_locator(self, database_name: str, table_name: str, column: str, transaction_id:int) -> None :
        self.validate_column_buffer(database_name, table_name, column, transaction_id)
        if self.is_hash_index_in_buffer(database_name, table_name, column, transaction_id):
            return self.buffer_index[transaction_id][database_name][table_name][column]["hash"]
        elif self.is_hash_index_in_block(database_name, table_name, column):
            return self.indexes[database_name][table_name][column]["hash"]
        else :
            raise ValueError("Hash index not found")

    def is_hash_index_in_buffer(self, database_name: str, table_name: str, column: str, transaction_id:int) -> bool:
        return self.buffer_index[transaction_id][database_name][table_name][column]["hash"] is not None
    
    def is_hash_index_in_block(self, database_name: str, table_name: str, column: str) -> bool:
        return self.indexes[database_name][table_name][column]["hash"] is not None
    
    def is_bplus_index_in_buffer(self, database_name: str, table_name: str, column: str, transaction_id:int) -> bool:
        return self.buffer_index[transaction_id][database_name][table_name][column]["bplus"] is not None
    
    def is_bplus_index_in_block(self, database_name: str, table_name: str, column: str) -> bool:
        return self.indexes[database_name][table_name][column]["bplus"] is not None

    # setindex ke buffer
    def set_index(self, database_name: str, table_name: str, column: str, transaction_id:int,index_type="bplus") -> None:
        self.initialize_index_structure(database_name,table_name,column)

        table = self.blocks[database_name][table_name]
        if index_type == "bplus":
            bplus_tree = self.create_bplus_index(table, column)
            self.buffer_index[transaction_id][database_name][table_name][column]["bplus"] = bplus_tree
        elif index_type == "hash":
            hash_index = self.create_hash_index(table, column)
            self.buffer_index[transaction_id][database_name][table_name][column]["hash"] = hash_index
        else:
            raise ValueError("Invalid index type. Only 'bplus' and 'hash' are supported.")
        print(f"Index of type '{index_type}' created for column '{column}' in table '{table_name}'.")

    def create_bplus_index(self, table : dict, column: str):
        bplus_tree = BPlusTree(order=4)
        for block_index, block in enumerate(table["values"]):
            for offset, row in enumerate(block):
                if column not in row:
                    raise ValueError(f"Column '{column}' is missing in a row of the table.")
                key = row[column]
                bplus_tree.insert(key,(block_index,offset))
        return bplus_tree
    
    # setelah insert delete
    def is_bplus_index_exist(self, database_name: str, table_name: str, column: str) -> bool:
        for transaction_id, dbs in self.buffer_index.items():
            if database_name not in dbs:
                continue
            if table_name not in dbs[database_name]:
                continue
            if column not in dbs[database_name][table_name]:
                continue
            if "bplus" in dbs[database_name][table_name][column]:
                if dbs[database_name][table_name][column]["bplus"] is not None:
                    return True
        return False
    
    def is_hash_index_exist(self, database_name: str, table_name: str, column: str) -> bool:
        for transaction_id, dbs in self.buffer_index.items():
            if database_name not in dbs:
                continue
            if table_name not in dbs[database_name]:
                continue
            if column not in dbs[database_name][table_name]:
                continue
            if "bplus" in dbs[database_name][table_name][column]:
                if dbs[database_name][table_name][column]["hash"] is not None:
                    return True
        return False
    
    def insert_bplus_index(self,database_name:str,table_name:str,column:str,key,block_index,offset,transaction_id : int):
        index : BPlusTree = self.buffer_index[transaction_id][database_name][table_name][column]['bplus']
        index.insert(key,(block_index,offset))

    def delete_bplus_index(self,database_name:str,table_name:str,column:str,key,transaction_id : int):
        index : BPlusTree = self.buffer_index[transaction_id][database_name][table_name][column]['bplus']
        index.delete(key)

    # panggil kalau yang diupdate search keynya
    def update_bplus_index(self,database_name:str,table_name:str,column:str,key,block_index,offset,transaction_id : int):
        self.delete_bplus_index(database_name,table_name,column,key,transaction_id)
        self.insert_bplus_index(database_name,table_name,column,key,block_index,offset,transaction_id)

    def search_bplus_index(self,database_name:str,table_name:str,column:str,key,transaction_id : int) -> list:
        self.validate_column_buffer(database_name,table_name,column,transaction_id)
        index : BPlusTree = self.buffer_index[transaction_id][database_name][table_name][column]['bplus']
        result_indices = index.search(key)
        return result_indices
    
    def search_bplus_index_range(self, database_name:str,table_name:str, column:str,  transaction_id:int,start,end) -> list:
        self.validate_column_buffer(database_name,table_name,column,transaction_id)
        index : BPlusTree = self.buffer_index[transaction_id][database_name][table_name][column]['bplus']
        result_indices = index.search_range(start, end)
        return result_indices
    
    def create_hash_index(self, table: dict, column: str):
        hash_index = HashTable(size=10)
        for block_index, block in enumerate(table["values"]):
            for offset, row in enumerate(block):
                if column not in row:
                    raise ValueError(f"Column '{column}' is missing in a row of the table.")
                key = row[column]
                hash_index.insert(key,(block_index,offset))
        return hash_index
    
    def insert_hash_tree(self, database_name:str, table_name:str, column:str, key, block_index, offset, transaction_id : int):
        index = self.hash_locator(database_name, table_name, column, transaction_id)
        index.insert(key, (block_index, offset))
        if index.search(key) != (block_index, offset):
            raise ValueError("Error in inserting to hash index")

    def search_hash_index(self,database_name:str,table_name:str,column:str,key,transaction_id : int):  
        index = self.hash_locator(database_name, table_name, column, transaction_id)
        result_indices = index.search(key)
        return result_indices

    def delete_hash_index(self,database_name:str,table_name:str,column:str,key,transaction_id : int):
        index = self.hash_locator(database_name, table_name, column, transaction_id)
        index.delete(key)

    def update_key_hash_index(self,database_name:str,table_name:str,column:str,key,block_index,offset,transaction_id : int):
        self.delete_hash_index(database_name,table_name,column,key,transaction_id)
        self.insert_hash_tree(database_name,table_name,column,key,block_index,offset,transaction_id)

    def debug(self):
        """cuma fungsi debug, literally ngeprint variabel"""
        print(self.blocks)

    def debug_indexes(self):
        """cuma fungsi debug, literally ngeprint variabel"""
        print(self.indexes)