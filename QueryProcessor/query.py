import re

class Query:
    def __init__(self, query):

        syntaxes = ["SELECT","UPDATE","AS","FROM","JOIN","WHERE","ORDER","LIMIT","BEGIN","COMMIT","END","TRANSACTION"]
        syntaxes_statement = ["SELECT","UPDATE","FROM","WHERE","LIMIT","BEGIN","COMMIT","END"]
        
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

    def isLimitValid():
        # TODO
        pass

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
# test = Query(qs2[0])
# print(test.isBeginValid())
for q in qs2:
    test = Query(q)
    # test.out()
    # print(test.isFromValid(test_tables))
    print(test.isSelectValid(test_tables))
    print("Columns and renaming:", test.rename_select)