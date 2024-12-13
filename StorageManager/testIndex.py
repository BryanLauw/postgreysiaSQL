from StorageManager.classes import StorageEngine

storage_engine = StorageEngine()

# GET TABLE STATISTICS
statistic = storage_engine.get_stats("database1", "users_membership")
print("================== Statistic for table 'users_membership' ==================")
print("nr", statistic.n_r)
print("br", statistic.b_r)
print("lr", statistic.l_r)
print("fr", statistic.f_r)
print("V[a,r]", statistic.V_a_r)
print("index info",statistic.col_index)
print("column data type info",statistic.col_data_type)
print("column bplus level info",statistic.col_bplus_tree_level)

# SEE ALL TABLES AND COLUMNS INSIDE THE PARTICULAR DATABASE
print("\nAll tables and columns info in database1")
print(storage_engine.get_tables_and_columns_info("database1"))
print()

# SEE WHAT'S INSIDE THE "indexes.dat" FILE
print("================== Index loaded from storage ==================")
storage_engine.debug_indexes()
print()

# GET READABLE INDEX STRUCTURE
print("================== Index structure for table 'users' ==================")
print("Column 'id_user'")
storage_engine.print_index_structure("database1", "users", "id_user", 1)
print()
print("================== Index structure for table 'products' ==================")
print("Column 'product_id'")
storage_engine.print_index_structure("database1", "products", "product_id", 1)
print()
print("================== Index structure for table 'orders' ==================")
print("Column 'order_id'")
storage_engine.print_index_structure("database1", "orders", "order_id", 1)
print()
print("================== Index structure for table 'users_membership' ==================")
print("Column 'id_user'")
storage_engine.print_index_structure("database1", "users_membership", "id_user", 1)
print()

# INSERT NEW KEY-VALUE PAIR INTO THE INDEX
print("================== Inserting new key-value pair into index ==================")
storage_engine.insert_key_value_to_index("database1", "users", "id_user", 100, 50, 20, 1)
storage_engine.commit_buffer(1)
storage_engine.print_index_structure("database1", "users", "id_user", 1)