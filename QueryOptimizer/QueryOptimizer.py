import sys
import os
import re
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from StorageManager.classes import Statistic, StorageEngine

from .QueryParser import QueryParser
from .QueryTree import ParsedQuery, QueryTree
from .QueryHelper import *
from typing import Callable, Union
from .QueryValidator import QueryValidator

class QueryOptimizer:

    def commutative_join(self, node: QueryTree, query_cost: Callable[[QueryTree], int]) -> bool:
        if node.type not in ["JOIN", "NATURAL JOIN"]:
            return False

        if len(node.childs) != 2:
            return False

        left_child, right_child = node.childs

        left_first_cost = query_cost(left_child)
        right_first_cost = query_cost(right_child)

        print(f"Evaluating commutative join for node: {node.type} {node.val}")
        print(f"Cost with left child ({left_child.val}) first: {left_first_cost}")
        print(f"Cost with right child ({right_child.val}) first: {right_first_cost}")

        if right_first_cost < left_first_cost:
            node.childs[0], node.childs[1] = right_child, left_child
            for child in node.childs:
                child.parent = node
            print(f"Swapped join order for node {node.type} {node.val} to improve cost.")
            return True

        print(f"No swap needed for node {node.type} {node.val}.")
        return False

    
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
        # print("NODE1: ",node1.type,node1.val, "\nNODE2: ",node2.type,node2.val)

        # Change child of parent node1, to point child of node1
        for index,child in enumerate(node1.parent.childs):
            if child == node1:
                node1.parent.childs[index] = node1.childs[0]
                
        # Change parent of node1's child
        for child in node1.childs:
            child.parent = node1.parent
            
        node1.childs = [node2] 
        node1.parent = node2.parent
        
        # Change child of node2 parent, from node2 to node1
        for index,child in enumerate(node2.parent.childs):
            if child == node2:
                node2.parent.childs[index] = node1
                
        # Change parent of node2 to node1
        node2.parent = node1
    
    def __already_pushed_selection(self, node_where: QueryTree):
        # print(node_where.val)
        child = node_where.childs[0]
        if(child.type == "WHERE"):
            # print("SINI ",child.val)
            return self.__already_pushed_selection(child)
        
        return child.type == "TABLE"
    
    def __find_matching_table(self, node: QueryTree, table_name: str):
        if node.type == "TABLE":
            return node if node.val.strip() == table_name.strip() else None
        
        for child in node.childs:
            res_child = self.__find_matching_table(child,table_name)
            if(res_child is not None):
                return res_child
        
        return None
    
    def pushing_selection(self, node: QueryTree,) -> bool:
        print("PUSHING SELECTION: ",node.val,node.type)
        if(self.__already_pushed_selection(node)):
            return False
        
        if "OR" in  node.val:
            return False
        
        match = re.search(r'(\w+)\.',node.val)
        if not match:
            return False
        
        table_name = match.group(1)
        # print("Table name: ",table_name)
        table_node = self.__find_matching_table(node,table_name)
        # print("Table node: ",table_node.val)
        self.__insert_node(node,table_node)
        
        return True
    
    def perform_operation(self, node: QueryTree, query_cost_calculator: Callable[[QueryTree], int]) -> bool:
        if node.type == "WHERE":
            return self.pushing_selection(node)
        elif node.type in ["JOIN", "NATURAL JOIN"]:
            return self.commutative_join(node, query_cost_calculator)
        return False       