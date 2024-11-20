from typing import List

class QueryTree:
    def __init__(self,type_: str,val: str, childs: List['QueryTree'], parent: 'QueryTree' = None):
        self.type = type_
        self.val = val

class ParsedQuery:
    def __init__(self,query_tree,query: str):
        self.query_tree = query_tree
        self.query = query

    
