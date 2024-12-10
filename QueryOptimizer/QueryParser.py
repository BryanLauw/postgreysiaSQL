from QueryHelper import *

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
    
    # Main Components
    components = ["SELECT", "UPDATE", "DELETE", "FROM", "SET", "WHERE", "ORDER BY", "LIMIT"]
    
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
        for index,token in enumerate(tokens):
            next_state = ""
            cur_state_rules = self.transitions[cur_state]
            for rule in cur_state_rules:
                rule_token = rule[0]
                if((token == rule_token) or (rule_token == "<X>" and (re.match(r'^[A-Za-z_][A-Za-z0-9_]*$|^\d+(\.\d+)*$', token.replace('.', '')) and token.count('.') <= 1 or re.match(r'^".*"$', token))) or
                   ((rule_token == "<N>" or rule_token == "<X>") and token.isnumeric()) or (rule_token == "<CO>" and token in self.CO) or
                   (rule_token == "<MO>" and token in self.MO)
                ):
                    next_state = rule[1]
                    break
            if not next_state:
                start = index-2 if index-2>=0 else 0
                end = index+2 if index+2<len(tokens) else len(tokens)-1
                raise ValueError(f"Syntax error at: {tokens[start:end+1]}")
            
            cur_state = next_state
        
        if(cur_state not in self.final_states):
            raise ValueError(f"Query is incomplete!")
        
        return " ".join(tokens).strip()
    
    def extract_SELECT(self,values: str):
        arr_attributes = [value.strip() for value in values.split(",")]
        return arr_attributes
    
    def extract_FROM(self,values: str):
        arr_joins = []
        values_parsed = values.split()
        element = ""
        i = 0
        while i < len(values_parsed):
            if values_parsed[i] == "NATURAL" and values_parsed[i+1] == "JOIN":
                if element:
                    arr_joins.append(element.strip())
                arr_joins.append("NATURAL JOIN")
                element = ""
                i+=2
                continue
            elif values_parsed[i] in ["JOIN",","]:
                if element:
                    arr_joins.append(element.strip())
                arr_joins.append(values_parsed[i])
                element = ""
            else:
                element += " " + values_parsed[i]
            i += 1

        if element:
            arr_joins.append(element.strip())

        return arr_joins


    def extract_WHERE(self,values: str):
        return values.strip()

    def extract_ORDERBY(self,values: str):
        return values.strip()

    def extract_LIMIT(self,values: str):
        return int(values)

    def extract_UPDATE(self,values: str):
        return values.strip()

    def extract_SET(self,values: str):
        return values.strip()

    def extract_value(self,query: str, before: str, after: str):
        start = query.find(before) + len(before)
        if after == "":
            end = len(query)
        else:
            end = query.find(after)
        extracted = query[start:end]
        if before == "SELECT":
            extracted = self.extract_SELECT(extracted)
        elif before == "FROM":
            extracted = self.extract_FROM(extracted)
        elif before == "WHERE":
            extracted = self.extract_WHERE(extracted)
        elif before == "ORDER BY":
            extracted = self.extract_ORDERBY(extracted)
        elif before == "LIMIT":
            extracted = self.extract_LIMIT(extracted)
        elif before == "UPDATE":
            extracted = self.extract_UPDATE(extracted)
        elif before == "SET":
            extracted = self.extract_SET(extracted)
        return extracted
    
    def get_components_values(self,query: str):
        query_components_value = {}
        i = 0
        while i < len(self.components):
            idx_first_comp = query.find(self.components[i])
            if idx_first_comp == -1:
                i += 1
                continue

            if i == len(self.components) - 1:  # Last component (LIMIT)
                query_components_value[self.components[i]] = self.extract_value(
                    query, self.components[i], ""
                )
                break

            j = i + 1
            idx_second_comp = query.find(self.components[j])
            while idx_second_comp == -1 and j < len(self.components) - 1:
                j += 1
                idx_second_comp = query.find(self.components[j])

            query_components_value[self.components[i]] = self.extract_value(
                query, self.components[i], "" if idx_second_comp == -1 else self.components[j]
            )

            i += 1

        return query_components_value