import unittest

from StorageManager.classes import StorageEngine

from .QueryCost import QueryCost
from .OptimizationEngine import OptimizationEngine

class TestTree(unittest.TestCase):
    def setUp(self):
        storage = StorageEngine()
        self.optim = OptimizationEngine(storage.get_stats)
        select_query = '''
                    select users.id_user 
                    from users 
                    JOIN products ON users.id_user=products.product_id 
                    JOIN orders ON orders.order_id = products.product_id AND users.id_user=products.product_id 
                    where users.id_user>1 
                    order by users.id_user
                    '''
        parsed_query = self.optim.parse_query(select_query,"database1")

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
        self.assertEqual(self.chosenleaf.parent.parent.childs[1].childs[1].type, "WHERE")

    def testPushingProjection(self) :
        self.assertEqual(self.chosenleaf.val, "orders")
        self.assertEqual(self.chosenleaf.parent.type, "SELECT")
        self.assertEqual(self.chosenleaf.parent.val, ['orders.order_id'])
    
if __name__ == "__main__":
    unittest.main()