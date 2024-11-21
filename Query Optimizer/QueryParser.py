class QueryParser:    
    # Math Operator (MO)
    MO = ['+','/','-','*']
    
    # Comparison Operator (CO)
    CO = ['<','>','=','<=','>=','<>']
    
    # Valid Symbols
    symbols = [',', '(', ')']
    
    # Valid Keywords
    keywords = [
        'SELECT', 'DELETE', 'FROM', 'WHERE', 'JOIN', 'NATURAL', 'ON', 'ORDER', 
        'BY', 'LIMIT', 'UPDATE', 'SET', 'AS', 'DESC' , 'ASC'
    ]
    
    def __init__(self, dfa_path: str):
        self.start_state = ""
        self.final_states = []
        self.transitions = []
        
        with open(dfa_path, "r") as file:
            self.start_state = file.readline().strip()
            
            self.final_states = file.readline().split().pop(0)            
            
            while True:
                parsed = file.readline().split()
                if(not parsed):
                    break
                
                state = parsed[0]
                lenParsed = len(parsed)
                for i in range(1,lenParsed,2):
                    print(i, parsed[i])
                    self.transitions.append((state,parsed[i],parsed[i+1]))                
                
QueryParser("dfa.txt")