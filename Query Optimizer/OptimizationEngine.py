from QueryTree import ParsedQuery, QueryTree
from QueryHelper import *

class OptimizationEngine:
    def __init__(self):
        pass
    
    def parse_query(self,query: str) -> ParsedQuery:
        normalized_query = QueryHelper.remove_excessive_whitespace(
            QueryHelper.normalize_string(query)
            .upper())
        
        components = ["SELECT","DELETE","FROM","UPDATE","SET","WHERE","ORDER BY","LIMIT"]
        
        # Map/Dictionary, untuk nampung value dari SELECT, FROM, dll
        # Cth: "SELECT" => "a,b"
        query_components_value = {}

        i = 0
        while(i < 8):
            idx_first_comp = normalized_query.find(components[i])
            if idx_first_comp == -1:
                i+=1
                continue
            
            if(i == 7):
                query_components_value[components[i]] = QueryHelper.extract_value(query,components[i], "")
                break
            
            j = i+1
            idx_second_comp = normalized_query.find(components[j])
            while(idx_second_comp == -1):
                if j == 5:
                    break
                j+=1
                idx_second_comp = query.find(components[j])

            # print(components[i],components[j])
            
            query_components_value[components[i]] = QueryHelper.extract_value(query,components[i],
                                                                              "" if idx_second_comp == -1
                                                                              else components[j])
            
            i += 1

        print(query_components_value)
        pass

    def optimize_query(query: ParsedQuery) -> ParsedQuery:
        pass

    def __get_cost(query: ParsedQuery)->int:
        pass

new = OptimizationEngine()

query = "SELECT a,b, c FROM students JOIN teacher t ON students.id = t.id WHERE a=1 ORDER BY a DESC LIMIT 1"
print(query)
new.parse_query(query)