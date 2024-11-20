from QueryTree import ParsedQuery, QueryTree
from QueryHelper import *

class OptimizationEngine:
    def __init__(self):
        pass
    
    def parse_query(self,query: str) -> ParsedQuery:
        query.upper()
        components = ["SELECT","DELETE","FROM","UPDATE","SET","WHERE","LIMIT","ORDERBY"]
        
        # Map/Dictionary, untuk nampung value dari SELECT, FROM, dll
        # Cth: "SELECT" => "a,b"
        query_components_value = {}

        i = 0
        while(i < 6):
            idx_first_comp = query.find(components[i])
            if idx_first_comp == -1:
                i+=1
                continue
            
            if(i == 5):
                query_components_value[components[i]] = QueryHelper.extract_value(query,components[i], "")
                break
            
            j = i+1
            idx_second_comp = query.find(components[j])
            while(idx_second_comp == -1):
                if j == 5:
                    break
                j+=1
                idx_second_comp = query.find(components[j])

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

new.parse_query("SELECT a,b, c FROM students WHERE a=1 ")