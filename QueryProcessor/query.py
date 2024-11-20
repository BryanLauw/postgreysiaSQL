class Query:
    def __init__(self, query):
        self.statements = {}
        temp_parsed_data = query.split(" ")
        syntaxes = ["SELECT","UPDATE","FROM","WHERE","LIMIT"]
        temp_statement = []
        rename_maps = {}
        for word in temp_parsed_data:
            if(word.upper() in syntaxes):
                if temp_statement == []:
                    temp_statement.append(word.upper())
                else:
                    # print(temp_statement)
                    self.statements[temp_statement[0].upper()] = ' '.join(temp_statement)
                    temp_statement = [word.upper()]
            else:
                temp_statement.append(word)
        self.statements[temp_statement[0].upper()] = ' '.join(temp_statement)
    def isFromValid(self, tables):
        temp_tables = []
        for word in self.statements["FROM"]:
            if(word != "JOIN" and word != "NATURAL" and word != "AS" and word != "ON" and word != "FROM"):
                temp_tables.append(word)
        for table in temp_tables:
            if table not in tables:
                return Exception("Tabel tidak ada")
        return True
    # def isSelectValid(self, attributes):

    def out(self):
        print(self.statements)

test_tables = ["id", "name", "data"]
test = Query("select id, name FROM data AS WHERE id=\"1\"")
test.out()
print(test.isFromValid(test_tables))