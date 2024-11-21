import re

class Query:
    def __init__(self, query):

        syntaxes = ["SELECT", "UPDATE", "AS", "FROM", "JOIN", "WHERE", "ORDER", "LIMIT", "BEGIN", "COMMIT", "END", "TRANSACTION", "DELETE"]
        syntaxes_statement = ["SELECT", "UPDATE", "FROM", "WHERE", "ORDER", "LIMIT", "BEGIN", "COMMIT", "END", "DELETE"]
        
        # atribut
        self.statements = {} # statement
        self.rename_select = {} # rename map for select statement 
        self.rename_from = {} # rename map for from statement 
        # TODO

        # parse, trim, and uppercase all syntax
        temp_parsed_data = query.split(" ")
        for i in range(temp_parsed_data.count('')):
            temp_parsed_data.remove('')
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
    
    def isSelectValid(self, attributes):
        select_pattern = r"^\s*SELECT\s+(\*|([a-zA-Z0-9_]+(\s+AS\s+[a-zA-Z0-9_]+)?(\s*,\s*[a-zA-Z0-9_]+(\s+AS\s+[a-zA-Z0-9_]+)?)*)\s*)\s*$"

        if "SELECT" not in self.statements:
            return Exception("Tidak ada Select Statement")
        if re.match(select_pattern, self.statements["SELECT"], re.IGNORECASE):
            temp_rename = self.map_name_select(self.statements["SELECT"])
            for atr in temp_rename:
                if(len(atr) > 1):
                    self.rename_select[atr[0]] = atr[1]         
                if atr[0] != '*' and atr[0] not in attributes:
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

    def isUpdateValid():
        # TODO
        pass

    def isFromValid():
        # TODO
        pass

    def isWhereValid():
        # TODO
        pass

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

    def debug_parse(self):
        print("Parsed Statements:")
        for key, value in self.statements.items():
            print(f"  {key}: {value}")
        print("Rename Map (SELECT):", self.rename_select)
        print("Rename Map (FROM):", self.rename_from)


    def out(self):
        print(self.statements)


# testing
test_tables = ["id", "name", "data", "age"]
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
    "select id, name as boleh ",
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
# for q in qs2:
#     test = Query(q)
#     # test.out()
#     # print(test.isFromValid(test_tables))
#     print(test.isSelectValid(test_tables))
#     print("Columns and renaming:", test.rename_select)

# for query in test_queries:
#     test = Query(query)
#     print(f"Processing query: {query}")
#     test.debug_parse()  
#     print("ORDER BY Valid:", test.isOrderValid())
#     print("LIMIT Valid:", test.isLimitValid())

if __name__ == "__main__":
    valid_tables = {
        "employee": ["id", "name", "department", "salary"],
        "department": ["id", "name", "location"],
        "project": ["id", "name", "deadline"],
        "task": ["id", "status", "priority"]
    }

    delete_test_queries = [
        "DELETE FROM employee WHERE department='RnD'",  # Valid
        "DELETE FROM employee",  # Invalid: Missing WHERE clause
        "DELETE FROM unknown_table WHERE department='RnD'",  # Invalid: Table does not exist
        "DELETE FROM employee WHERE department='RnD' AND salary > 1000",  # Invalid: Multiple conditions
        "DELETE employee WHERE department='RnD'",  # Invalid: Missing 'FROM'
        "DELETE FROM employee WHERE department='RnD'",  # Invalid: Missing semicolon
        "DELETE FROM department WHERE location='HQ'",  # Valid
        "DELETE FROM task WHERE status='completed' OR priority='high'",  # Invalid: Multiple conditions
        "DELETE FROM employee WHERE position='Manager'",  # Invalid: Invalid attribute
    ]

    for query in delete_test_queries:
        print(f"Processing query: {query}")
        test = Query(query)
        test.debug_parse() 
        print(test.isDeleteValid(valid_tables))
        print("-" * 80)
