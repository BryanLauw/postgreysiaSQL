import sys
import os
import re
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from StorageManager.classes import Statistic, StorageEngine

from QueryParser import QueryParser
from QueryTree import ParsedQuery, QueryTree
from QueryHelper import *
from typing import Callable, Union
from QueryValidator import QueryValidator

class QueryOptimizer:
    
    def __swap_nodes(self, node1: QueryTree, node2: QueryTree):
        """ Swap node1 position with node2
        """
        # Swap parent
        temp = node1.parent
        node1.parent = node2.parent
        node2.parent = temp
        
        # Swap childs' parent
        for child1 in node1.childs:
            child1.parent = node2
            
        for child2 in node1.childs:
            child1.parent = node2
            
        # Swap childs
        temp = node1.childs
        node1.childs = node2.childs
        node2.childs = node1.childs    
        
    def __insert_node(self, node1: QueryTree, node2: QueryTree):
        """ Insert node1 to right above node2
        Used for pushing operation (selection, projection)

        Args:
            node1 (QueryTree): new parent of node2
            node2 (QueryTree): new child of node1
        """
        
        # Change child of parent node1, to point child of node1
        for child,index in enumerate(node1.parent.childs):
            if child == node1:
                node1.parent.childs[index] = node1.childs[0]
                
        # Change parent of node1's child
        for child in node1.childs:
            child.parent = node1.parent
            
        node1.childs = [node2] 
        node1.parent = node2.parent
        
        # Change child of node2 parent, from node2 to node1
        for child,index in enumerate(node2.parent.childs):
            if child == node2:
                node2.parent.childs[index] = node1
                
        # Change parent of node2 to node1
        node2.parent = node1
    
    def __already_pushed_selection(self, node_where: QueryTree):
        print(node_where.val)
        child = node_where.childs[0]
        if(child.type == "WHERE"):
            return self.check_WHERE_above_table(child)
        
        return child.type == "TABLE"
    
    def __find_matching_table(self, node: QueryTree, table_name: str):
        if node.type == "TABLE":
            return node if node.val == table_name else None
        
        for child in node.childs:
            res_child = self.find_matching_table(child,table_name)
            if(res_child is not None):
                return res_child
        
        return None
    
    def pushing_selection(self, node: QueryTree,) -> bool:
        if(self.__already_pushed_selection(node)):
            return False
        
        match = re.search(r'(\w+)\.',node.val)
        if not match:
            return False
        
        table_name = match.group(1)
        table_node = self.find_matching_table(node,table_name)
        self.insert_node(node,table_node)
        
        return True
    
    def perform_operation(self,node: QueryTree):
        if(node.type == "WHERE"):
            print(node.type)
            return self.pushing_selection(node)
        
        return False         