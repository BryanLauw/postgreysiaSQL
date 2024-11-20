class QueryHelper:

    @staticmethod
    def remove_excessive_whitespace(query: str):
        words = query.split()
        new_query = " ".join(words)
        return new_query

    @staticmethod
    def normalize_string(query: str):
        return query.replace(" ","").replace("\t","").replace("\n","").replace("\r","")

    @staticmethod
    def extract_SELECT(values:str):
        arr_attributes = [value.strip() for value in values.split(',')]
        return arr_attributes
    
    @staticmethod
    def extract_FROM(values: str):
        arr_joins = []
        
        values_parsed = values.split()
        len_parsed = len(values_parsed)
        
        met_join = False
        element = ""
        i = 0
        while i < len_parsed:
            if(i+1 < len_parsed and values_parsed[i] == "NATURAL" and values_parsed[i+1] == "JOIN"):
                if(not met_join):
                    met_join = True
                    element += " NATURAL JOIN"
                else:
                    arr_joins.append(element.strip())
                    element = " NATURAL JOIN "
                    
                i += 2
                continue
            
            if (values_parsed[i] == "JOIN"):
                if(not met_join):
                    met_join = True
                else:
                    arr_joins.append(element.strip())
                    element = ""
            
            element += " " + values_parsed[i]
            
            i += 1
        
        if(element):
            arr_joins.append(element.strip())
        
        return arr_joins
    
    
            
    @staticmethod
    def extract_value(query:str, before: str,after: str):
        start = query.find(before) + len(before)
        if after == "":
            end = len(query)
        else:
            end = query.find(after)
        
        string_value = query[start:end]
    
print(QueryHelper.extract_FROM("a NATURAL JOIN b JOIN c ON a.id = b.id"))
        