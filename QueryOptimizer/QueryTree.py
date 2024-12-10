from typing import List, Dict

class QueryTree:
    def __init__(self, type: str, val: str = None,method: str = "", childs: List['QueryTree'] = None, parent: 'QueryTree' = None):
        self.type = type
        self.val = val
        self.method = method
        self.childs = childs if childs else []
        self.parent = parent

    def add_child(self, child: 'QueryTree') -> 'QueryTree':
        self.childs.append(child)
    
    def add_parent(self, parent: 'QueryTree') -> 'QueryTree':
        self.parent = parent
    
    def get_next_sibling(self):
        if self.parent is None:
            return None # No parent, no sibling
        
        siblings = self.parent.childs
        current_index = siblings.index(self)
        if current_index < len(siblings) - 1:
            return siblings[current_index + 1] # Return the next sibling
        
        return None # No more siblings

    def __repr__(self, level=0):
        ret = "\t" * level + f"({self.type}: {self.val})\n"
        for child in self.childs:
            ret += child.__repr__(level + 1)
        return ret

class ParsedQuery:
    def __init__(self, query_tree: QueryTree, query: str):
        self.query_tree = query_tree
        self.query = query

    def __repr__(self):
        return repr(self.query_tree)
