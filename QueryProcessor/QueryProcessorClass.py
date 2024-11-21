from ConcurrencyControlManager.main import *
from QueryOptimizer.main import *

class QueryProcessor:
    def __init__(self):
        self.current_transactionId = None
        self.parsedQuery = None
        self.qo = QueryOptimizer()
        self.cc = ConcurrencyControlManager()
        # sm = StorageManager()
        # rm = RecoveryManager()
        pass

    def execute_query(self, query : str):
        tables = ["id", "name"]
        rows = [
            ['1', 'benfsjdjfdsfhuisdfosidfdjsopfjsduifbsdhyufdiogjdfbndifiodfuuiguirfhifhui'],
            ['2', 'tazki'],
            ['3', 'agil'],
            ['4', 'filbert'],
            ['5', 'bryan']
        ]
        
        queries = self.parse_query(query)
        for query in queries:
            print("Executing query: " + query)
            if(query.upper() == "BEGIN" or query.upper() == "BEGIN TRANSACTION"):
                self.current_transactionId = self.cc.begin_transaction()
                
            elif(query.upper() == "COMMIT" or query.upper() == "BEGIN TRANSACTION"):
                self.cc.end_transaction(self.current_transactionId)
                self.current_transactionId = None
            
            elif(query.upper() == "END TRANSACTION"):
                self.cc.end_transaction(self.current_transactionId)
                self.current_transactionId = None

            elif(query.upper() == "PRINT"):
                self.printResult(tables, rows)
            
            else:
                self.parsedQuery = self.qo.parse_query(query)
    
    def ParsedQueryToDataRetrieval():
        pass

    def ParsedQueryToDataWrite():
        pass

    def ParsedQueryToDataDeletion():
        pass

    def printResult(self,column, data):
        # Determine the maximum width of each column
        column_widths = [max(len(row[i]) for row in data + [column]) for i in range(len(column))]

        # Function to format a row
        def format_row(row):
            return "| " + " | ".join(row[i].ljust(column_widths[i]) for i in range(len(row))) + " |"

        # Print the header
        print("+-" + "-+-".join("-" * width for width in column_widths) + "-+")
        print(format_row(column))
        print("+-" + "-+-".join("-" * width for width in column_widths) + "-+")

        # Print the data
        for row in data:
            print(format_row(row))

        # Print the bottom border
        print("+-" + "-+-".join("-" * width for width in column_widths) + "-+")


    def parse_query(self, query : str):
        queries = query.split(';')
        return [q.strip() for q in queries if q.strip()]