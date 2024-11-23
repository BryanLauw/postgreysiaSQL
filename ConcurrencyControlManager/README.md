# Concurrency Control Manager

## Why this thing exist?
This module exist to make sure ACID property when operating database system. More specifically atomicity and isolation property. This include detecting when there is conflicted operation inside transaction that will break serializability, or deadlock.  

Responsibility for this module is **ONLY** to tell if an action allowed to run at that point in time or not. Query processor is the one who really responsible for which action run, this can be done concurrently with spawning multiple thread for each transaction. Before running action, Query processor will confirm to this module and decide to abort or continue based on the response. If the transaction need to be aborted, Query processor will wait for signal for this module to try rerunning the transaction.

## How this thing work?
Algorithm used in this module is timestamp scheduling. Every transaction has timestamp, and for every object inside database, exist read and write timestamp. This timestamp is from transaction timestamp in which the latest one doing the action. Every action allowed, unless:  

1. Write action, if the timestamp of the transaction that will be running is less than the write timestamp of that object. This transaction supposed to write earlier, but didn't get any chance to do it.  
2. Read action, if the timestamp of the transaction that will be running is greater than the write timestamp of that object. This transaction supposed to read earlier, but that object already overwritten by other transaction.  

Disallowed transaction will result in aborted transaction. All of aborted transaction will be rerun with newer timestamp to avoid starvation.  


## How to use this thing?
```python
# Create the instance
cc = ConcurrencyControlManager()

def run_transaction(operations):
    # Track whether transaction succeeded
    succeed = False

    while not succeed:
        # Get timestamp with begin_transaction
        ts = cc.begin_transaction()

        response = None
        succeed = True
        for op in operations:
            response = cc.validate(op.object, ts, op.action)

            # If action not allowed, abort
            if not v.allowed:
                succeed = False
                abort()
                break

            # If allowed, run that action
            run_action(op)

        # Wait for signal to rerun the transaction
        if not succeed:
            with response.condition:
                response.condition.wait()

threads = []
for t in transactions:
    threads.append(Thread(target=run_transaction, args=t))

for t in threads:
    t.join()
```
