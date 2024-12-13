from typing import Union
from threading import Lock, Thread
from ConcurrencyControlManager import ConcurrencyControlManager, Row, Action, Response
from time import sleep

type AtomicAction = tuple[Action, Row, int]
type Transaction = list[AtomicAction]

class ConcurrencyTester:
    def __init__(self):
        self.cm = ConcurrencyControlManager()
        self.log_mutex = Lock()
        
    def log(self, message: str):
        with self.log_mutex:
            print(message)

    def run_transaction(self, tr: Transaction, transaction_list: list[Transaction]):
        succeed = False
        
        while not succeed:
            ts = self.cm.begin_transaction() # get timestamp
            
            response: Union[Response, None] = None
            succeed = True
            
            for op in tr:
                response = self.cm.validate_object(op[1], ts, op[0])
                
                # If action not allowed, abort
                if not response.allowed:
                    succeed = False
                    self.log(f"{ts}: ABORT {transaction_list.index(tr)}")
                    break

                # If allowed, run that action
                self.log(f"{ts}: {transaction_list.index(tr)} {op[0]} {r.index(op[1])}")
                sleep(op[2] * 0.01)

            if response is not None and not succeed:
                with response.condition:
                    response.condition.wait()
            else:
                self.cm.end_transaction(ts)
                self.log(f"{ts}: COMMIT {transaction_list.index(tr)}")

    def run_scenario(self, scenario_name: str, transactions: list[Transaction]):
        print(f"\nRunning Scenario: {scenario_name}")
        
        threads: list[Thread] = []
        for tr in transactions:
            threads.append(Thread(target=self.run_transaction, args=[tr, transactions]))
        
        for thread in threads:
            thread.start()
            
        for thread in threads:
            thread.join()
        
        print(f"{scenario_name} Completed\n")

def run_test_scenarios():
    tester = ConcurrencyTester()
    
    global r
    r = [Row(), Row(), Row()]
    
    first_scenario =   [      
        [
            ("read", r[0], 1),
            ("write", r[1], 2),
            ("read", r[2], 1),
        ],
        [
            ("read", r[1], 1),
            ("write", r[2], 2),
            ("read", r[0], 1),
        ],
        [
            ("read", r[2], 1),
            ("write", r[0], 2),
            ("read", r[1], 1),
        ]
    ]
    
    second_scenario = [
        [
            ("write", r[0], 2),
            ("read", r[2], 0),
            ("read", r[1], 0),
            ("write", r[0], 1),
            ("read", r[1], 0),
        ],
        [
            ("read", r[0], 1),
            ("write", r[1], 2),
            ("read", r[0], 1),
            ("write", r[0], 2),
            ("read", r[2], 0),
        ],
        [
            ("read", r[1], 0),
            ("read", r[0], 1),
            ("write", r[2], 3),
            ("read", r[1], 0),
            ("write", r[2], 1),
        ],
        [
            ("read", r[0], 1),
            ("read", r[1], 0),
            ("write", r[1], 2),
            ("read", r[0], 1),
            ("write", r[2], 1),
        ]
    ]
    
    tester.run_scenario("First Scenario", first_scenario)
    tester.run_scenario("Second Scenario", second_scenario)

if __name__ == "__main__":
    run_test_scenarios()