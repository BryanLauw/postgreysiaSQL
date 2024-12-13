import sys
import os
import re
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from StorageManager.classes import Statistic, StorageEngine

from .QueryParser import QueryParser
from .QueryTree import ParsedQuery, QueryTree
from .QueryHelper import *
from .QueryCost import *
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
    
    # def __list_child_join(self,node_join: List[QueryTree]):
    #     childs = {}
    #     for node in node_join:
    #         for child in node.childs:
    #             if child.type not in ["JOIN", "NATURAL JOIN"]:
    #                 table = QueryHelper.get_tables_regex(child.val)[0]
    #                 childs[table] = child
    #     return childs
        
    # def reorder_join(self, node_join: List[QueryTree], database_name: str,get_stats: Callable[[str, str, int], Union[Statistic, Exception]]):
    #     parent_root_join = node_join[-1].parent
    #     childs = self.__list_child_join(node_join)
        
    #     table_iterators = {QueryHelper.get_tables_regex(node_join[0].childs[0].val)[0], QueryHelper.get_tables_regex(node_join[0].childs[1].val)[0]}
    #     joins_with_table = {0:list(table_iterators)}
    #     for i in range(1,len(node_join)):
    #         table_iterators.add(QueryHelper.get_tables_regex(node_join[i].childs[1].val)[0])
    #         if node_join[i].type == "JOIN":
    #             tables = QueryHelper.get_tables_regex(node_join[i].val)
    #             joins_with_table[i] = tables
    #         else: #NATURAL JOIN
    #             attributes = node_join[i].val
    #             for table in table_iterators:
    #                 if all(attr in get_stats(database_name,table) for attr in attributes):
    #                     joins_with_table[i] = [table,QueryHelper.get_tables_regex(node_join[i].childs[1].val)[0]]
    #                     break
        
    #     print(joins_with_table)
                
        # for node in node_join:
    
    
    def reorder_join(self, node_join: List[QueryTree], database_name: str,get_stats: Callable[[str, str, int], Union[Statistic, Exception]]):
        def perform_associative(child_node_join: QueryTree, isJoin: bool):
            # Index 0 atau 1
            parent_node_join = child_node_join.parent
            if parent_node_join.childs[0].compare(child_node_join):
                idx = False
            else:
                idx = True
            
            if child_node_join.type == "JOIN":
                result, operator = QueryHelper.get_other_expression(parent_node_join.val, child_node_join.val)
                child_node_join.val = parent_node_join.val
                parent_node_join.val = result
            
            child_node_join.parent = parent_node_join.parent
            parent_node_join.parent = child_node_join
            
            parent_node_join.childs[idx] = child_node_join.childs[not idx]
            child_node_join.childs[not idx] = parent_node_join
            child_node_join.parent.childs[idx] = child_node_join
        
        def perform_commutative(node_join: QueryTree):
            temp = node_join.childs[0]
            node_join.childs[0] = node_join.childs[1]
            node_join.childs[1] = temp
        
        def deep_copy_join_nodes(child_node_join: QueryTree, parent_node_join: QueryTree):
            copy_parent = parent_node_join.deep_copy()
            copy_node = copy_parent.childs[0] if child_node_join.compare(copy_parent.childs[0]) else copy_parent.childs[1]
            return copy_parent, copy_node
            
        query_cost = QueryCost(get_stats,database_name)
        for node in node_join:
            needCommutative = False
            needAssoc = False
            
            # Normal
            cost_normal = query_cost.calculate_size_cost(node.parent if node.parent.val in ["JOIN","NATURAL JOIN"] else node)
            smalles_cost = 9999
            
            # Komutatif
            parent_komut, node_komut = deep_copy_join_nodes(node, node.parent)
            # Swap child (komutatif)
            perform_commutative(node_komut)           
            
            cost_komutatif = query_cost.calculate_size_cost(node_komut.parent if parent_komut.val in ["JOIN","NATURAL JOIN"] else node_komut)
            if cost2 < smalles_cost:
                smalles_cost = cost_komutatif
                needCommutative = True
            
            # Associative (With commut and not)
            if node.parent.type in ["JOIN","NATURAL JOIN"] and node.type == node.parent.type:
                parent_assoc = node.parent.deep_copy()
                node_assoc = parent_assoc.childs[0] if node.compare(parent_assoc.childs[0]) else parent_assoc.childs[1]
                if node.type == "NATURAL JOIN":
                    # Normal
                    perform_associative(node_assoc)
                    cost_assoc = query_cost.calculate_size_cost(parent_assoc)
                    if cost_assoc < smalles_cost:
                        smalles_cost = cost_assoc
                        needAssoc = True
                        needCommutative = False
                    
                    # Comut
                    perform_associative(node_komut)
                    cost_comut_assoc = query_cost.calculate_size_cost(parent_komut)
                    if cost_comut_assoc < smalles_cost:
                        smalles_cost = cost_comut_assoc
                        needAssoc = True
                        needCommutative = True
                    
                else: 
                    if node.val in node.parent.val:
                        result, operator = QueryHelper.get_other_expression(node.parent.val, node.val)
                        if node_assoc.compare(parent_assoc.childs[0]):
                            idx = False
                        else:
                            idx = True
                            
                        tables = QueryHelper.get_tables_defined(node_assoc.childs[not idx]) + QueryHelper.get_tables_defined(parent_assoc.childs[not idx])
                        tables_in_join = QueryHelper.get_tables_regex(result)
                        
                        if all(table in tables for table in tables_in_join):
                            perform_associative(node_assoc)
                            
                            cost_assoc = query_cost.calculate_size_cost(parent_assoc)
                            if cost_assoc < smalles_cost:
                                smalles_cost = cost_assoc
                                needAssoc = True
                                needCommutative = False
                                
                            perform_associative(node_komut)
                            
                            cost_comut_assoc = query_cost.calculate_size_cost(parent_assoc)
                            if cost_comut_assoc < smalles_cost:
                                smalles_cost = cost_comut_assoc
                                needAssoc = True
                                needCommutative = True
            if needCommutative:
                perform_commutative(node)
            if needAssoc:
                perform_associative(node)
    
    def __already_pushed_selection(self, node_where: QueryTree):
        child = node_where.childs[0]
        if(child.type == "WHERE"):
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
        
        tables = re.findall(r'\b(\w+)\.',node.val)
        tables = list(set(tables))
        if len(tables) != 1:
            return False
        
        table_name = tables[0]
        table_node = self.__find_matching_table(node,table_name)
        self.__insert_node(node,table_node)
        
        return True    
    
    def get_table_column(self, data) : 
        matches = re.findall(r'\b[a-zA-Z_]+\.[a-zA-Z_]+\b', data)
        return matches
       
    def __split_projection(self, node_select : QueryTree, result:dict) : 
        for item in node_select.val:
            key = item.split('.')[0] 
            if key not in result:
                result[key] = []
            result[key].append(item)

    def __split_natural_join(self, node_natural : QueryTree, result:dict) : 
        table_left = self.__find_tables_from_children(node_natural.childs[0])
        # print(table_left)
        table_right = self.__find_tables_from_children(node_natural.childs[1])
        # print(table_right)

        for item in node_natural.val:
            value_left = table_left[0] + "." + item
            # print(value_left)
            if table_left[0] not in result:
                result[table_left[0]]= []
            if value_left.strip() not in result[table_left[0]] : 
                result[table_left[0]].append(value_left)
            
            value_right = table_right[0] + "." + item
            # print(value_right)
            if table_right[0] not in result:
                result[table_right[0]] = []
            if value_right.strip() not in result[table_right[0]]  : 
                result[table_right[0]].append(value_right[0])

    
    def __split_join(self, node_join : QueryTree, result:dict) :
        column = self.get_table_column(node_join.val)
        # print("KOLOM JOIN : ",column)
        for item in column:
            # print(item)
            key = item.split('.')[0] 
            if key not in result:
                result[key] = []
            if item.strip() not in result[key]  : 
                result[key].append(item)
    
    def __split_where(self, node_where : QueryTree, result:dict) :
        if "OR" in  node_where.val:
            value = self.get_table_column(node_where.val)
            key = value[0].split('.')[0] 
            if key not in result:
                result[key] = []
            if value[0].strip() not in result[key] : 
                result[key].append(value[0])

            key = value[1].split('.')[1] 
            if key not in result:
                result[key] = []
            if value[1].strip() not in result[key]: 
                result[key].append(value[1])
        else :
            value = node_where.val.split()[0] 
            key = value.split('.')[0] 
            if key not in result:
                result[key] = []
            if value.strip() not in result[key]: 
                result[key].append(value)
    
    def __do_pushing_projection(self, node_select:QueryTree, result:dict = [], index = 0) : 
        # print("REKURSIF: ",node_select.val,node_select.type)
        # print("RESULT: ",result)
        if node_select.type == "TABLE" :
            # parent = node_select.parent
            # print("PARENT : \n", parent)
            node_baru = QueryTree("SELECT", result[node_select.val.strip()])
            node_baru.parent = node_select.parent
            node_select.parent.childs[index] = node_baru
            node_baru.add_child(node_select)
            
        elif node_select.type == "WHERE" : 
            self.__split_where(node_select, result)
            self.__do_pushing_projection(node_select.childs[0], result)
        elif node_select.type == "JOIN" : 
            self.__split_join(node_select, result)
            self.__do_pushing_projection(node_select.childs[0], result, 0)
            self.__do_pushing_projection(node_select.childs[1], result, 1)
        elif node_select.type == "NATURAL JOIN" :
            self.__split_natural_join(node_select, result)
            self.__do_pushing_projection(node_select.childs[0], result, 0)
            self.__do_pushing_projection(node_select.childs[1], result, 1)
        else : 
            self.__do_pushing_projection(node_select.childs[0], result)

    def pushing_projection(self, node: QueryTree) :
        print("PUSHING PROJECTION: ",node.val,node.type)

        if node.type == "SELECT" : 
            # node_select:QueryTree = node
            result:dict = {}
            self.__split_projection(node, result)
            self.__do_pushing_projection(node, result) 
        
        return True
    
    def combine_selection_and_cartesian_product(self, node: QueryTree) -> bool:
        tables_from_children = self.__find_tables_from_children(node)
        
        node_to_combine = node.parent
        new_join_on_conditions = []
        is_combine_run = False
        while node_to_combine.type != "ROOT":
            if self.__is_where_combinable(node_to_combine, tables_from_children):
                is_combine_run = True
                new_join_on_conditions.append(node_to_combine.val)

                # remove the selection node
                parent = node_to_combine.parent
                child = node_to_combine.childs[0]
                parent.childs[0] = child
                child.parent = parent

            node_to_combine = node_to_combine.parent
        
        if is_combine_run:
            # change node type from cartesian product to join on
            node.type = "JOIN"
            node.val = " AND ".join(new_join_on_conditions)

        return is_combine_run

    def __find_tables_from_children(self, node: QueryTree) -> list:
        tables = []
        if node.type == "TABLE":
            tables.append(node.val.strip()) # remove trailing whitespaces
        
        for child in node.childs:
            tables.extend(self.__find_tables_from_children(child))
        
        return tables
    
    def __is_where_combinable(self, node: QueryTree, tables: list) -> bool:
        if node.type != "WHERE":
            return False
        
        tables_where = [item.split(".")[0] for item in node.val.split(" = ")]
        return all(table_where in tables for table_where in tables_where)
