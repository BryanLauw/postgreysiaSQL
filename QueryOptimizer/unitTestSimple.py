import unittest

from StorageManager.classes import Statistic, StorageEngine

from .QueryParser import QueryParser
from .QueryTree import ParsedQuery, QueryTree
from .QueryHelper import *
from typing import Callable, Union
from .QueryValidator import QueryValidator
from .QueryCost import QueryCost
from queue import Queue
from .QueryOptimizer import QueryOptimizer
from .OptimizationEngine import OptimizationEngine

class TestTree(unittest.TestCase):
    def setUp(self):
        storage = StorageEngine()
        self.optim = OptimizationEngine(storage.get_stats)
        select_query = 'select * from users where users.id_user>1 order by users.id_user'
        parsed_query = self.optim.parse_query(select_query,"database1")
        print(parsed_query)

        self.query = select_query
        # self.initTree = parsed_query.query_tree
        
        self.optim.optimize_query(parsed_query,"database1")
        tree = parsed_query.query_tree
        while True :
            tree = tree.childs[0]
            if tree.type == "TABLE":
                break
        
        self.optimTree = parsed_query.query_tree
        self.chosenleaf = tree
        self.query_cost = QueryCost(storage.get_stats,"database1")

    def testOptimTree(self) : 
        print("OPTIM : \n", self.optimTree)
        self.assertNotEqual(self.optimTree, None)

    def testPushingSelection(self) : 
        self.assertEqual(self.chosenleaf.parent.type, "WHERE")

    def testPushingProjection(self) :
        self.assertEqual(self.chosenleaf.val, "users")
        self.assertNotEqual(self.chosenleaf.parent.type, "SELECT")
    
if __name__ == "__main__":
    unittest.main()