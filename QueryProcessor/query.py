import re

class Query:
    def __init__(self, query):

        syntaxes = ["SELECT", "UPDATE", "AS", "FROM", "JOIN", "WHERE", "ORDER", "LIMIT", "BEGIN", "COMMIT", "END", "TRANSACTION", "DELETE", "INSERT", "VALUES", "INTO"]
        syntaxes_statement = ["SELECT", "UPDATE", "FROM", "WHERE", "ORDER", "LIMIT", "BEGIN", "COMMIT", "END", "DELETE", "INSERT"]
        
        # atribut
        self.statements = {} # statement
        self.rename_select = {} # rename map for select statement 
        self.rename_from = {} # rename map for from statement 
        
        # TODO

        # parse, trim, and uppercase all syntax
        temp_parsed_data = query.split()
        for i in range(len(temp_parsed_data)):
            if(temp_parsed_data[i].upper() in syntaxes):
                temp_parsed_data[i] = temp_parsed_data[i].upper()        

        # construct statements
        temp_statement = []
        for word in temp_parsed_data:
            if(word in syntaxes_statement):
                if temp_statement == []:
                    temp_statement.append(word)
                else:
                    self.statements[temp_statement[0]] = ' '.join(temp_statement)
                    temp_statement = [word]
            else:
                temp_statement.append(word)
        self.statements[temp_statement[0]] = ' '.join(temp_statement)
    
    def map_name_select(self,query):
        match = re.match(r"^\s*SELECT\s+(.+?)\s*$", query.strip(), re.IGNORECASE)
        if match:
            # Get the part of the query after SELECT
            columns_part = match.group(1).strip()
            columns = []
            
            # Handle '*' and individual column names with optional renaming
            if columns_part == '*':
                columns.append('*')
            else:
                # Split by commas and handle AS renaming
                for column in columns_part.split(','):
                    column = column.strip()
                    parts = column.split(' AS ')
                    if len(parts) == 2:  # It's a column with renaming
                        columns.append((parts[0].strip(), parts[1].strip()))
                    else:
                        columns.append((column, None))  # No renaming
            return columns
        return []
    

    # def isSelectValid(self, tables):
        # tables is table[]
        #
        # structure table
        # nameToAttr: map (str,str[])
        # select_pattern = r"""
        #     ^\s*SELECT\s+
        #     (
        #         \*|                                 # Match wildcard '*'
        #         (
                
        #             [a-zA-Z][a-zA-Z0-9_]+(\.[a-zA-Z][a-zA-Z0-9_]+)?  # Match <attribute> or <table>.<attribute>
        #             (\s+AS\s+[a-zA-Z][a-zA-Z0-9_]+)?         # Optional alias
        #             (\s*,\s*[a-zA-Z][a-zA-Z0-9_]+(\.[a-zA-Z][a-zA-Z0-9_]+)?(\s+AS\s+[a-zA-Z][a-zA-Z0-9_]+)?)*
        #         )
        #     )\s*$
        # """

        # if "SELECT" not in self.statements:
        #     raise Exception("Tidak ada Select Statement")
        
        # if re.match(select_pattern, self.statements["SELECT"], re.IGNORECASE | re.VERBOSE):
        #     temp_rename = self.map_name_select(self.statements["SELECT"])
        #     for atr in temp_rename:
        #         if len(atr) > 1:
        #             self.rename_select[atr[0]] = atr[1]
        #         # Handle <table>.<attribute> as valid
        #         if atr[0] != '*' and not (
        #             atr[0] in attributes or
        #             # any(atr[0].split(".")[0] in tables and atr[0].split(".")[1] in tables[atr[0].split(".")[0]])
        #         ):
        #             return False
        #     return True
        
        # return False

    def isSelectValid(self, attributes):
        # print(self.statements["SELECT"])
        # Update the regex pattern to include <table>.<attribute> format
        select_pattern = r"""
            ^\s*SELECT\s+
            (
                \*|                                 # Match wildcard '*'
                (
                
                    [a-zA-Z][a-zA-Z0-9_]+(\.[a-zA-Z][a-zA-Z0-9_]+)?  # Match <attribute> or <table>.<attribute>
                    (\s+AS\s+[a-zA-Z][a-zA-Z0-9_]+)?         # Optional alias
                    (\s*,\s*[a-zA-Z][a-zA-Z0-9_]+(\.[a-zA-Z][a-zA-Z0-9_]+)?(\s+AS\s+[a-zA-Z][a-zA-Z0-9_]+)?)*
                )
            )\s*$
        """

        if "SELECT" not in self.statements:
            raise Exception("Tidak ada Select Statement")
        
        if re.match(select_pattern, self.statements["SELECT"], re.IGNORECASE | re.VERBOSE):
            temp_rename = self.map_name_select(self.statements["SELECT"])
            for atr in temp_rename:
                if len(atr) > 1:
                    self.rename_select[atr[0]] = atr[1]
                # Handle <table>.<attribute> as valid
                if atr[0] != '*' and not (atr[0] in attributes):
                    if not (atr[0].split(".")[0] == "table" and atr[0].split(".")[1] in attributes):
                    # any(atr[0].split(".")[0] == "table" and atr[0].split(".")[1] in attributes)
                        return False
            return True
        
        return False


    def isBeginValid(self):
        if len(self.statements) == 1 and "BEGIN" in self.statements and (self.statements["BEGIN"] == "BEGIN" or self.statements["BEGIN"] == "BEGIN TRANSACTION"):
            return True
        return False
    
    def isCommitValid(self):
        if len(self.statements) == 1 and "COMMIT" in self.statements and (self.statements["COMMIT"] == "COMMIT"):
            return True
        return False
    
    def isEndValid(self):
        if len(self.statements) == 1 and "END" in self.statements and (self.statements["END"] == "END" or self.statements["END"] == "END TRANSACTION"):
            return True
        return False

    def isUpdateValid(self, valid_tables):
        if "WHERE" not in self.statements:
            print ("Error: Missing 'WHERE' clause. Avoid updating all rows accidentally.")
            return False


        update_pattern = r"^\s*UPDATE\s+(\w+)\s+SET\s+(.*?)\s*$"
        match = re.match(update_pattern, self.statements["UPDATE"], re.IGNORECASE)
        if not match:
            print ("Invalid UPDATE query")
            return False

        table_name = match.group(1)
        self.rename_from[table_name] = table_name
        set_clause = match.group(2)

        if table_name not in valid_tables:
            print (f"Table '{table_name}' does not exist")
            return False

        attributes = valid_tables[table_name]
        assignments = [assign.strip() for assign in set_clause.split(',')]

        for assignment in assignments:
            attr_match = re.match(r"(\w+)\s*=", assignment)
            if not attr_match:
                print ("Invalid SET clause")
                return False
            attr_name = attr_match.group(1)
            if attr_name not in attributes:
                print (f"Attribute '{attr_name}' does not exist in table '{table_name}'")
                return False
            
        try:
            self.isWhereValid(valid_tables)
        except Exception as e:
            print(e)
            return False   

        print("\033[92mSuccess: Your UPDATE query is valid.\033[0m")
        return True

    def isFromValid(self, attributes):
        # Check if there is a FROM statement
        if "FROM" not in self.statements:
            return Exception("There is no FROM Statement")

        # regex for FROM clause
        FROM = r'[Ff][Rr][Oo][Mm]'
        JOIN = r'[Jj][Oo][Ii][Nn]'
        NATURAL_JOIN = r'[Nn][Aa][Tt][Uu][Rr][Aa][Ll]\s+[Jj][Oo][Ii][Nn]'
        ON = r'[Oo][Nn]'
        AS = r'[Aa][Ss]'

        TABLE = ALIAS = r'[A-Za-z_][A-Za-z0-9_]*'
        COLUMNNAME = r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*'

        # TABLE((\s+AS)?\s+ALIAS)?
        TABLENAME = rf'{TABLE}((\s+{AS})?\s+{ALIAS})?'
        # JOIN\s+TABLENAME(\s+ON\s+COLUMNNAME\s*=\s*COLUMNNAME)?
        JOIN_CLAUSE = rf'{JOIN}\s+{TABLENAME}(\s+{ON}\s+{COLUMNNAME}\s*=\s*{COLUMNNAME})?'
        # NATURAL_JOIN\s+TABLENAME
        NATURAL_JOIN_CLAUSE = rf'{NATURAL_JOIN}\s+{TABLENAME}'
        # ^\s*FROM\s+TABLENAME(\s*,\s*TABLENAME|\s+JOIN|\s+NATURAL_JOIN)*$
        FROM_CLAUSE = rf'^\s*{FROM}\s+{TABLENAME}(\s*,\s*{TABLENAME}|\s+{JOIN_CLAUSE}|\s+{NATURAL_JOIN_CLAUSE})*\s*$'

        from_pattern = FROM_CLAUSE

        def validate_from_clause(sql_from_clause):
            return bool(re.match(from_pattern, sql_from_clause.strip()))

        # if the FROM clause is not valid        
        if not validate_from_clause(self.statements["FROM"]):
            return Exception("Invalid FROM Statement")
        
        # if the FROM clause is valid
        # check if the table exists or the column exists
        # map the renaming table into the dictionary
        token = self.statements["FROM"].replace(',',' , ').split()
        self.rename_from[token[1]] = token[1]
        i = 2
        while i < len(token):
            if token[i].upper() == "JOIN":
                if token[i+1].upper() == "NATURAL":
                    if token[i+2] in attributes:
                        self.rename_from[token[i+2]] = token[i+2]
                        i += 3
                    else:
                        return Exception("Table does not exist")
                else:
                    if token[i+1] in attributes:
                        self.rename_from[token[i+1]] = token[i+1]
                        i += 2
                    else:
                        return Exception("Table does not exist")
            elif token[i] == ',':
                if token[i+1] in attributes:
                    self.rename_from[token[i+1]] = token[i+1]
                    i += 2
                else:
                    return Exception("Table does not exist")
            elif token[i].upper() == "AS":
                self.rename_from[token[i+1]] = token[i-1]
                i += 2
            elif token[i].upper() == "ON":
                # NEED COLUMN NAME
                # if token[i+2] == "=":
                #     table1, column1 = token[i+1].split('.')
                #     table2, column2 = token[i+3].split('.')
                #     if (table1 in self.rename_from and table2 in self.rename_from):
                #         table1 = self.rename_from[table1]
                #         table2 = self.rename_from[table2]
                #         if column1 in table1 and column2 in table2:
                #             i += 4
                #         else:
                #             return Exception("Column does not exist")
                #     else:
                #         return Exception("Table does not exist")
                # else:
                #     return Exception("Did you mean: =")
                i += 4
            else:
                self.rename_from[token[i]] = token[i-1]
                i += 1
        return True

    def isWhereValid(self, valid_tables):
        where_statement = self.statements["WHERE"]
        if (len(self.rename_from) > 1):
            where_regex = r"^[Ww][Hh][Ee][Rr][Ee]\s+(\(*(\w+\.\w+)\s*(=|<>|>|<|>=|<=)\s*('[^']*'|\d+(\.\d+)?)(\)*))(\s+([Aa][Nn][Dd]|[Oo][Rr])\s+(\(*(\w+\.\w+)\s*(=|<>|>|<|>=|<=)\s*('[^']*'|\d+(\.\d+)?)(\)*)))*$"
        else:
            where_regex = r"^[Ww][Hh][Ee][Rr][Ee]\s+(\(*(\w+)\s*(=|<>|>|<|>=|<=)\s*('[^']*'|\d+(\.\d+)?)(\)*))(\s+([Aa][Nn][Dd]|[Oo][Rr])\s+(\(*(\w+)\s*(=|<>|>|<|>=|<=)\s*('[^']*'|\d+(\.\d+)?)(\)*)))*$"
        
        match = re.fullmatch(where_regex, where_statement)
        stack = 0
        flag = True

        # Check parentheses matching
        for char in where_statement:
            if char == '(':
                stack += 1
            elif char == ')':
                if stack == 0:
                    flag = False
                    break
                stack -= 1
        
        flag = flag and (stack == 0)
        if not match:
            raise Exception("Kesalahan sintaks pada klausa WHERE.")
        if not flag:
            raise Exception("Tanda kurung tidak seimbang.")
        
        if len(self.rename_from) > 1:
            table_groups = re.findall(r"((\w+\.\w+)\s*(=|<>|>|<|>=|<=)\s*('[^']*'|\d+(\.\d+)?))", where_statement)
            
        else:
            table_groups = re.findall(r"((\w+)\s*(=|<>|>|<|>=|<=)\s*('[^']*'|\d+(\.\d+)?))", where_statement)
        comparisons = [(group[1], group[3]) for group in table_groups]  # Extract attributes and RHS value

        for v in comparisons:
            v = v[0]
            if len(self.rename_from) > 1:
                table_alias, attr = v.split('.')
                table = self.rename_from.get(table_alias)
                if table == None:
                    raise Exception(f"Tidak ada {table_alias}.")
                
                if table not in valid_tables:
                    raise Exception(f"Nama tabel {table} tidak valid.")
                
                valid_attributes = valid_tables[table]
                if attr not in valid_tables.get(valid_attributes, []):
                    raise Exception(f"Atribut {attr} tidak valid pada tabel {table}.")
            else:
                attr = v
                first_key, first_value = list(self.rename_from.items())[0]
                valid_attributes = valid_tables[first_value]
                if attr not in valid_tables.get(valid_attributes, []):
                    raise Exception(f"Atribut {attr} tidak valid pada tabel {first_value}.")
        
        return True

    def isOrderValid(self):
        if "ORDER" not in self.statements:
            return True  

        order_pattern = r"ORDER\s+BY\s+[a-zA-Z0-9_]+(\s+(ASC|DESC))?"
        if re.match(order_pattern, self.statements["ORDER"], re.IGNORECASE):
            columns = self.statements["ORDER"].split("BY")[1].strip()
            if ',' in columns:  
                return False
            return True
        return False

    def isLimitValid(self):
        if "LIMIT" not in self.statements:
            return True  

        limit_pattern = r"LIMIT\s+\d+"
        if re.match(limit_pattern, self.statements["LIMIT"], re.IGNORECASE):
            if "ORDER" in self.statements:
                limit_index = list(self.statements.keys()).index("LIMIT")
                order_index = list(self.statements.keys()).index("ORDER")
                if limit_index < order_index:
                    return False  
           
            try:
                limit_value = int(self.statements["LIMIT"].split()[1].replace(";", "").strip())
                if limit_value < 0:
                    return False
            except ValueError:
                return False 
            return True
        return False
    
    def isDeleteValid(self, valid_tables):
        if "DELETE" not in self.statements or "FROM" not in self.statements:
            return "Error: Missing or incorrect 'DELETE FROM' clause."

        from_clause = self.statements["FROM"]
        table_name_match = re.match(r"FROM\s+(\w+)", from_clause, re.IGNORECASE)
        if not table_name_match:
            return "Error: Unable to identify the table name in your DELETE query."

        table_name = table_name_match.group(1)
        if table_name not in valid_tables:
            return f"Error: The table '{table_name}' does not exist in the database."

        if "WHERE" not in self.statements:
            return "Error: Missing 'WHERE' clause. Avoid deleting all rows accidentally."

        where_clause = self.statements["WHERE"]
        where_pattern = r"WHERE\s+([\w.]+)\s*(=|>|<|>=|<=|!=|<>)\s*.+"
        if not re.match(where_pattern, where_clause, re.IGNORECASE):
            return "Error: Invalid syntax in 'WHERE' clause."

        attribute_match = re.search(where_pattern, where_clause, re.IGNORECASE)
        if attribute_match:
            attribute = attribute_match.group(1)
            if table_name in valid_tables and attribute not in valid_tables[table_name]:
                return f"Error: The attribute '{attribute}' is not valid for the table '{table_name}'."

        if re.search(r"(AND|OR)", where_clause, re.IGNORECASE):
            return "Error: DELETE query must contain only one condition in the WHERE clause."

        return "Success: Your DELETE query is valid."
    
    def isInsertValid(self, valid_tables):
        if "INSERT" not in self.statements:
            return "Error: Missing 'INSERT' clause."

        pattern = r"INSERT\s+INTO\s+(\w+)\s*(\([^)]*\))?\s*VALUES\s*\(([^)]*)\)"
        match = re.match(pattern, self.statements["INSERT"], re.IGNORECASE)

        if not match:
            return "Error: Invalid syntax in the INSERT query."

        table_name = match.group(1)
        columns = match.group(2) or ""
        values = match.group(3) or ""

        if table_name not in valid_tables:
            return f"Error: The table '{table_name}' does not exist in the database."

        column_list = [col.strip() for col in columns.strip("()").split(",") if col.strip()]
        value_list = [val.strip() for val in values.split(",")]
        for column in column_list:
            if column not in valid_tables[table_name]:
                return f"Error: The column '{column}' is not valid for the table '{table_name}'."

        if len(column_list) != len(value_list):
            return f"Error: Mismatch between number of columns ({len(column_list)}) and values ({len(value_list)})."

        return "Success: Your INSERT query is valid."

    def debug_parse(self):
        print("Parsed Statements:")
        for key, value in self.statements.items():
            print(f"  {key}: {value}")
        print("Rename Map (SELECT):", self.rename_select)
        print("Rename Map (FROM):", self.rename_from)

    def printResult():
        pass
    def out(self):
        print(self.statements)


# testing
test_tables = ["id", "name", "data", "age","table.id"]
# test = Query("select id, name                      FROM data AS WHERE id=\"1\"")
queries = [
    "SELECT id", # True
    "SELECT id,", # False
    "SELECT id,name", # True
    "SELECT *", # True
    "SELECT test", # False
    "SELECT id,test", # False
    "SELECT name, age", # True
    "select id AS user_id, name", # True
    "SELECT id AS user_id, name, age", # True
    "SELECT id , test AS", # False
    "SELECT id AS , test", # False
    "SELECT id AS id2, test", # False
    "SELECT name AS full_name, age AS user_age", # True
    "SELECT *, id AS id2, test", # False
    "SELECT id, name AS user_name" # True
]
qs2 = [
    # "begin transaction",
    # "select id as id2",
    # "select id as id2, name",
    # "select id , name as boleh",
    # "select id , name as ",
    "select table.id, name as boleh ",
    "select table.asdas",
    "select name ",
    "select id,name ",
    "select *, name ",
    "select * ",
]

test_queries = [
        "SELECT name, age FROM employees ORDER BY age ASC LIMIT 10",  # Valid
        "SELECT * FROM employees ORDER BY name DESC LIMIT 5",  # Valid
        "SELECT id FROM employees ORDER BY salary",  # Valid (default ASC)
        "SELECT id FROM employees ORDER BY",  # Invalid ORDER BY missing column
        "SELECT id FROM employees LIMIT",  # Invalid LIMIT missing number
        "SELECT id FROM employees ORDER BY salary ASC LIMIT",  # Invalid LIMIT missing number
        "SELECT id FROM employees LIMIT 10 ORDER BY salary",  # Invalid ORDER
        "SELECT id FROM employees ORDER BY salary DESC LIMIT -5",  # Invalid LIMIT negative value
        "SELECT name, age FROM employees ORDER BY name, age LIMIT 10",  # Invalid multiple ORDER BY columns (unsupported here)
        "SELECT name, age FROM employees LIMIT 10 ORDER BY name",  # Invalid: LIMIT must follow ORDER BY
]

# test = Query(qs2[0])
# print(test.isBeginValid())

for q in qs2:
    test = Query(q)
    # test.out()
    # print(test.isFromValid(test_tables))
    print(test.isSelectValid(test_tables))
    # print("Columns and renaming:", test.rename_select)

# for query in test_queries:
#     test = Query(query)
#     print(f"Processing query: {query}")
#     test.debug_parse()  
#     print("ORDER BY Valid:", test.isOrderValid())
#     print("LIMIT Valid:", test.isLimitValid())

# if __name__ == "__main__":
#     valid_tables = {
#         "employee": ["id", "name", "department", "salary"],
#         "department": ["id", "name", "location"],
#         "project": ["id", "name", "deadline"],
#         "task": ["id", "status", "priority"]
#     }

#     delete_test_queries = [
#         "DELETE FROM employee WHERE department='RnD'",  # Valid
#         "DELETE FROM employee",  # Invalid: Missing WHERE clause
#         "DELETE FROM unknown_table WHERE department='RnD'",  # Invalid: Table does not exist
#         "DELETE FROM employee WHERE department='RnD' AND salary > 1000",  # Invalid: Multiple conditions
#         "DELETE employee WHERE department='RnD'",  # Invalid: Missing 'FROM'
#         "DELETE FROM employee WHERE department='RnD'",  # Invalid: Missing semicolon
#         "DELETE FROM department WHERE location='HQ'",  # Valid
#         "DELETE FROM task WHERE status='completed' OR priority='high'",  # Invalid: Multiple conditions
#         "DELETE FROM employee WHERE position='Manager'",  # Invalid: Invalid attribute
#     ]

#     insert_test_queries = [
#         "INSERT INTO employee (id, name, department, salary) VALUES (1, 'John Doe', 'RnD', 5000);",  # Valid
#         "INSERT INTO employee (id, name, department) VALUES (2, 'Jane Doe', 'HR', 4000);",  # Valid
#         "INSERT INTO unknown_table (id, name) VALUES (1, 'Unknown');",  # Invalid: Table does not exist
#         "INSERT INTO employee (id, name) VALUES (3);",  # Invalid: Mismatch between columns and values
#         "INSERT employee (id, name) VALUES (4, 'Test');",  # Invalid: Missing 'INTO'
#         "INSERT INTO employee (id, name department) VALUES (5, 'Error', 'RnD');",  # Invalid: Missing comma between columns
#         "INSERT INTO employee (id, name, department) VALUE (6, 'Missing', 'RnD');",  # Invalid: Typo 'VALUE' instead of 'VALUES'
#         "INSERT INTO employee VALUES (7, 'No Columns', 'RnD', 3000);",  # Invalid: Columns not specified
#         "INSERT INTO employee (id, name, department) VALUES ();",  # Invalid: Empty values
#     ]

    update_test_queries = [
        "UPDATE employee SET salary=6000 WHERE department='RnD'",  # Valid
        "UPDATE employee SET salary=6000",  # Invalid: Missing WHERE clause
        "UPDATE unknown_table SET location='HQ' WHERE name='IT'",  # Invalid: Table does not exist
        "UPDATE employee SET salary=6000 AND department='RnD'",  # Invalid: Missing WHERE keyword
        "UPDATE employee SET salary=6000 WHERE position='Manager'",  # Invalid: Invalid attribute
        "UPDATE employee SET salary=6000 WHERE department='RnD' AND salary > 5000",  # Invalid: Multiple conditions
    ]

#     # for query in delete_test_queries:
#     #     print(f"Processing query: {query}")
#     #     test = Query(query)
#     #     test.debug_parse() 
#     #     print(test.isDeleteValid(valid_tables))
#     #     print("-" * 80)

#     for query in insert_test_queries:
#         print(f"Processing query: {query}")
#         test = Query(query)
#         test.debug_parse()
#         print(test.isInsertValid(valid_tables))
#         print("-" * 80)
