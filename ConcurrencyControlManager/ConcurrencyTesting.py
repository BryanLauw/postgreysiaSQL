from ConcurrencyControlManager import ConcurrencyControlManager, Row, Action

class ConcurrencyTester:
    def __init__(self):
        self.cm = ConcurrencyControlManager()
        
    def run_action(self, transaction_id: int, object: Row, action: Action):
        res = self.cm.validate_object(object, transaction_id, action)
        if res == False:
            raise Exception(f"Transaction {transaction_id} not allowed to {action} data")
            

    # read
    def r(self, id, obj): 
        self.run_action(id, obj, "read")
    
    # write
    def w(self, id, obj): 
        self.run_action(id, obj, "write")
        
    def run_test(self, test_func, test_name: str):
        print(f"\nRunning {test_name}")
        try:
            test_func()
            print("Allowed")
        except Exception as e:
            print(f"Not allowed: {str(e)}")

    def test_basic_read_write_conflict(self):
        """Test basic read-write conflict:
        t1 reads A, t2 tries to write A (should pass)"""
        a = Row()
        t1 = self.cm.begin_transaction()
        t2 = self.cm.begin_transaction()
        
        self.r(t1, a)  # t1 reads A
        self.w(t2, a)  # t2 write A should pass since t2 has higher timestamp

    def test_write_read_sequence(self):
        """Test write followed by read with higher timestamp:
        t1 writes A, t2 reads A (should pass)"""
        a = Row()
        t1 = self.cm.begin_transaction()
        t2 = self.cm.begin_transaction()
        
        self.w(t1, a)  # t1 writes A
        self.r(t2, a)  # t2 reads A should pass since t2 has higher timestamp

    def test_multiple_object_scenario(self):
        """Test complex scenario with multiple objects:
        t1 reads A, writes B
        t2 writes A, reads B
        Tests timestamp ordering across multiple objects"""
        a, b = Row(), Row()
        t1 = self.cm.begin_transaction()
        t2 = self.cm.begin_transaction()
        
        self.r(t1, a)  # t1 reads A
        self.w(t1, b)  # t1 writes B
        self.w(t2, a)  # t2 writes A 
        self.r(t2, b)  # t2 reads B should pass since t2 has higher timestamp

    def test_cascading_conflicts(self):
        """Test cascading conflicts across multiple objects:
        t1 reads A, B, C in sequence
        t2 tries to write A, B, C in reverse sequence"""
        a, b, c = Row(), Row(), Row()
        t1 = self.cm.begin_transaction()
        t2 = self.cm.begin_transaction()
        
        # t1 reads objects in sequence
        self.r(t1, a)
        self.r(t1, b)
        self.r(t1, c)
        
        # t2 tries to write in reverse order
        self.w(t2, c)
        self.w(t2, b)
        self.w(t2, a)

    def test_interleaved_operations(self):
        """Test interleaved read/write operations:
        t1 and t2 alternate between reading and writing different objects"""
        a, b, c = Row(), Row(), Row()
        t1 = self.cm.begin_transaction()
        t2 = self.cm.begin_transaction()
        
        self.r(t1, a)  # t1 reads A
        self.w(t2, b)  # t2 write B
        self.r(t1, b)  # t1 read B
        self.w(t2, c)  # t2 writes C
        self.w(t1, c)  # t1 write C , should fail since t1 has lower timestamp

    def test_cycle_prevention(self):
        """Test prevention of dependency cycles:
        t1: R(A) -> W(B)
        t2: R(B) -> W(C)
        t3: R(C) -> W(A)
        
        should create cycle t1 -> t2 -> t3 -> t1
        the timestamp ordering protocol should prevent this cycle
        """
        a, b, c = Row(), Row(), Row()
        t1 = self.cm.begin_transaction() 
        t2 = self.cm.begin_transaction()  
        t3 = self.cm.begin_transaction() 
        
        self.r(t1, a)  # t1 reads A
        self.r(t2, b)  # t2 reads B
        self.r(t3, c)  # t3 reads C
        
        self.w(t1, b)  # t1 write B
        self.w(t2, c)  # t2 write C
        self.w(t3, a)  # t3 writesA, should fail since it creates a cycle

def run_all_tests():
    tester = ConcurrencyTester()
    tests = [
        (tester.test_basic_read_write_conflict, "Basic Read-Write Conflict Test"),
        (tester.test_write_read_sequence, "Write-Read Sequence Test"),
        (tester.test_multiple_object_scenario, "Multiple Object Scenario Test"),
        (tester.test_cascading_conflicts, "Cascading Conflicts Test"),
        (tester.test_interleaved_operations, "Interleaved Operations Test"),
        (tester.test_cycle_prevention, "Cycle Prevention Test"),
    ]
    
    for test_func, test_name in tests:
        tester.run_test(test_func, test_name)

run_all_tests()