from ConcurrencyControlManager import ConcurrencyControlManager, Row, Action

class ConcurrencyTester:
    def __init__(self):
        self.cm = ConcurrencyControlManager()
        
    def run_action(self, transaction_id: int, object: Row, action: Action):
        res = self.cm.validate_object(object, transaction_id, action)
        if not res.allowed:
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
        self.r(t2, b)  # t2 write B
        self.r(t1, b)  # t1 read B
        self.w(t2, c)  # t2 writes C
        self.w(t1, c)  # t1 write C , should fail since t1 has lower timestamp

    def test_cycle(self):
        """Test prevention of dependency cycles:
        t1: R(A) -> W(B)
        t2: R(B) -> W(C)
        t3: R(C) -> W(A)
        
        This is completely fine in timestamp based concurrency :D
        """
        a, b, c = Row(), Row(), Row()
        t1 = self.cm.begin_transaction() 
        t2 = self.cm.begin_transaction()  
        t3 = self.cm.begin_transaction() 
        
        self.r(t1, a)  # t1 reads A
        self.w(t1, b)  # t1 write B
        self.r(t2, b)  # t2 reads B
        self.w(t2, c)  # t2 write C
        self.r(t3, c)  # t3 reads C
        self.w(t3, a)  # t3 write A

    def test_write_read_violation(self):
        """
        A transaction is not allowed to WRITE 
        something if it was READ by a younger transaction.
        """
        a = Row()
        
        t1 = self.cm.begin_transaction() 
        t2 = self.cm.begin_transaction()  
        
        self.r(t2, a)
        self.w(t1, a)
    

    def test_write_written_violation(self):
        """
        A transaction is not allowed to WRITE 
        something if it was WRITTEN by a younger transaction.
        """
        a = Row()
        
        t1 = self.cm.begin_transaction() 
        t2 = self.cm.begin_transaction()  
        
        self.w(t2, a)
        self.w(t1, a)
    
    def test_read_written_violation(self) :
        """
        A transaction is not allowed to READ something if it 
        was WRITTEN by a younger transaction
        """
        a = Row()
        
        t1 = self.cm.begin_transaction() 
        t2 = self.cm.begin_transaction()  
        
        self.w(t2, a)
        self.r(t1, a)
    
    def test_random_violation(self) :

        T1,T2,T3,T4 = [self.cm.begin_transaction() for _ in range(4)]
        A,B,C = [Row() for _ in range(3)]

        self.r(T1, A)   # T1 reads A
        self.w(T3, C)   # T3 writes C
        self.r(T2, B)   # T2 reads B
        self.r(T4, C)   # T4 reads C
        self.w(T4, A)   # T4 writes A
        self.w(T1, B)   # T1 writes B
        self.r(T4, B)   # T4 reads B

        # sixth operation is not allowed due to the third operation.

    def test_transaction_dependency(self):
        """
        Test varying read and write operations with different transactions
        """
        
        # Initialize rows
        a, b, c, d = Row(), Row(), Row(), Row()
        t1 = self.cm.begin_transaction()  
        t2 = self.cm.begin_transaction()  
        t3 = self.cm.begin_transaction()  
        
        # T1 reads A, writes B
        self.r(t1, a)
        self.w(t1, b)
        
        # T2 writes A, reads C
        self.w(t2, a)
        self.r(t2, c)
        
        # T3 reads B, writes D
        self.r(t3, b)
        self.w(t3, d)

        # Conflict testing:
        # T1's write to B should not conflict with T2 or T3.
        # T2's write to A should not conflict with T1 or T3.
        # T3 can safely read B since T1 is older.

    def test_cascading_writes_violation(self):
        """
        Transactions should not cascade their writes across objects causing conflicts.
        """
        
        # Initialize rows
        a, b = Row(), Row()

        t1 = self.cm.begin_transaction()  
        t2 = self.cm.begin_transaction()  

        self.r(t1, a)  # T1 reads A
        self.w(t2, a)  # T2 writes A
        self.w(t1, b)  # T1 writes B
        self.w(t1, a)  # T1 tries to write A 
        
        # Conflict testing:
        # T1 should fail because it attempts to write to A after T2 has written to A.
        
    def test_multiple_access_conflict(self):
        """
        Random order of read and write operations that leads to conflicts.
        """
        a, b, c = Row(), Row(), Row()
        
        t1 = self.cm.begin_transaction()  
        t2 = self.cm.begin_transaction()  
        t3 = self.cm.begin_transaction()  
        t4 = self.cm.begin_transaction()  

        self.r(t1, a)  # T1 reads A
        self.w(t2, a)  # T2 writes A
        self.w(t3, b)  # T3 writes B
        self.r(t4, b)  # T4 reads B
        self.w(t1, c)  # T1 writes C
        self.r(t2, b)  # T2 tries to reads B 
        
        # Conflict testing:
        # T2 tries to read B after T3 has already written to B, so this should cause a violation.

def run_all_tests():
    tester = ConcurrencyTester()
    tests = [
        (tester.test_basic_read_write_conflict, "Basic Read-Write Conflict Test"),
        (tester.test_write_read_sequence, "Write-Read Sequence Test"),
        (tester.test_multiple_object_scenario, "Multiple Object Scenario Test"),
        (tester.test_cascading_conflicts, "Cascading Conflicts Test"),
        (tester.test_interleaved_operations, "Interleaved Operations Test"),
        (tester.test_cycle, "Cycle Test"),
        (tester.test_write_read_violation, "Write Read Violation Test"),
        (tester.test_write_written_violation, "Write Written Violation Test"),
        (tester.test_read_written_violation, "Read Written Violation Test"),
        (tester.test_random_violation, "Random Violation Test"),
        (tester.test_transaction_dependency, "Transaction Dependency Test"),  
        (tester.test_cascading_writes_violation, "Cascading Write Test"),
        (tester.test_multiple_access_conflict, "Multiple Access Conflict Test"),
    ]
    
    for test_func, test_name in tests:
        tester.run_test(test_func, test_name)

run_all_tests()