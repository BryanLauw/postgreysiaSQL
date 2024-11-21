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
        self.transitions = {}
        
        with open(dfa_path, "r") as file:
            self.start_state = file.readline().split()[1].strip()
            
            self.final_states = file.readline().split()
            self.final_states.pop(0)            
            
            while True:
                parsed = file.readline().split()
                if(not parsed):
                    break
                
                state = parsed[0]
                self.transitions[state] = []
                
                lenParsed = len(parsed)
                for i in range(1,lenParsed,2):
                    self.transitions[state].append((parsed[i],parsed[i+1])) 
    
    def tokenize_query(self,query: str):
        operators = self.CO + self.MO
        
        tokens = []
        buffer = ""
        i = 0

        while i < len(query):
            char = query[i]

            # Whitespace
            if char.isspace():
                if buffer:
                    tokens.append(buffer)
                    buffer = ""
                i += 1
                continue
            
            # Symbols
            if char in self.symbols:
                if buffer:
                    tokens.append(buffer)
                    buffer = ""
                tokens.append(char)
                i += 1
                continue

            # Two char Operators (<= , <> , >=)
            two_char_operator = query[i:i+2] 
            if two_char_operator in operators:
                if buffer:
                    tokens.append(buffer)
                    buffer = ""
                tokens.append(two_char_operator)
                i += 2
                continue

            # One char Operators
            if char in ''.join(operators):
                if buffer:
                    tokens.append(buffer)
                    buffer = ""
                tokens.append(char)
                i += 1
                continue

            # String
            if char == '"':
                if buffer:
                    tokens.append(buffer)
                    buffer = ""
                end_quote = query.find('"', i + 1)
                if end_quote == -1:
                    raise ValueError("Unterminated string in query")
                tokens.append(query[i:end_quote + 1])
                i = end_quote + 1
                continue

            buffer += char
            i += 1

        if buffer:
            tokens.append(buffer)

        final_tokens = []
        for token in tokens:
            if token.upper() in self.keywords:
                final_tokens.append(token.upper())
            else:
                final_tokens.append(token)

        return final_tokens
    
    def check_valid_syntax(self,query: str):
        tokens = self.tokenize_query(query)
        
        cur_state = self.start_state
        for token in tokens:
            next_state = ""
            cur_state_rules = self.transitions[cur_state]
            for rule in cur_state_rules:
                rule_token = rule[0]
                if((token == rule_token) or (rule_token == "<X>" and token.isalnum()) or
                   (rule_token == "<N>" and token.isnumeric()) or (rule_token == "<CO>" and token in self.CO) or
                   (rule_token == "<MO>" and token in self.MO)
                ):
                    next_state = rule[1]
                    break
                
            if not next_state:
                return False
            
            cur_state = next_state
        return cur_state in self.final_states
        
test = QueryParser("dfa.txt")
print(test.check_valid_syntax("SELECT a,b,     c FROM b as b NATURAL JOIN c AS WHERE a>1"))          