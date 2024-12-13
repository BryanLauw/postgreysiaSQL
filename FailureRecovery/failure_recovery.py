# python built-ins
import time
from datetime import datetime
from typing import Optional, List, Set
import threading
import logging
import atexit
import signal
import random

# class from other files
# import QueryProcessor.QueryProcessor as QueryProcessor
from failure_recovery_checkpoint import CheckpointManager
from failure_recovery_recovery import Recovery
from failure_recovery_log_entry import LogEntry
from failure_recovery_recover_criteria import RecoverCriteria
from failure_recovery_threading_manager import ThreadingManager


class FailureRecovery:
    """
    A class for managing and recovering from system or transaction failures.

    Attributes:
        log_file(str): t
    """
    def __init__(self, fname:str="log_kel.log", buffer_size=1000, interval=300):
        """
        Constructor. Initialize the FailureRecovery Instance

        Params:
            fname(string): file log name
            buffer_size(int): Size for the log buffer
            checkpoint_interval(int): Do checkpoint for every `x` seconds concurrently 
        """
        # format for logger printed to console for debug
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        # init logger function from python. buat nge print doang dah
        self.logger = logging.getLogger(self.__class__.__name__)

        # init from param
        self.log_file = fname
        self.buffer_size = buffer_size
        
        # init set / list for keep track transaction.
        # Used in undo transaction list and active transaction for checkpoint 
        self.list_active_transaction: Set[str] = set()
        # init buffer
        self.buffer_log_entries: List[LogEntry] = []
        
        # initialize threading and checkpoint 
        # self.threading_manager = ThreadingManager(logger=self.logger)
        self.checkpoint_manager = CheckpointManager(fname, buffer_size, self.logger)
        # self.checkpoint_manager = CheckpointManager(fname, interval, buffer_size, self.logger)

        # init recovery
        self.recovery = Recovery(fname, self.logger, self.add_entry_to_buffer)

        # start checkpoint manager
        self.checkpoint_manager.start(self.get_buffer, self.get_active_transactions, self.clear_buffer)

        # Register exit and signal handler
        atexit.register(self._stop)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGSEGV, self.signal_handler)

    def get_buffer(self):
        """
        Callback to get current log buffer
        """
        return self.buffer_log_entries

    def add_entry_to_buffer(self, log_entry: LogEntry):
        """
        Callback to add new entry to buffer
        """
        self.buffer_log_entries.append(log_entry)

    def get_active_transactions(self):
        """
        Callback to get active transactions
        """
        return self.list_active_transaction

    def clear_buffer(self):
        """
        Callback to clear log buffer
        """
        self.buffer_log_entries.clear()

    def write_log_entry(self, transaction_id: int, event: str, object_value: Optional[str]=None, old_value: Optional[str]= None, new_value: Optional[str] = None):
        """
        Write a new log entry to the buffer.

        Parameters:
            transaction_id (int): The unique identifier for the operation.
            event (str): The type of event being logged (e.g., "START", "COMMIT", "ABORT", "DATA").
            object_value (Optional[str]): The value of the object being modified (if applicable).
            old_value (Optional[str]): The old value of the object (if applicable).
            new_value (Optional[str]): The new value of the object (if applicable).
        """
        # Wait if checkpoint is in progress
        # self.threading_manager.wait_for_pause()

        log_entry = LogEntry(
            timestamp=datetime.now(),
            transaction_id=transaction_id,
            event=event,
            object_value=object_value,
            old_value=old_value,
            new_value=new_value
        )

        recovery_result = None 

        if (event == "START"):
            # add when active
            self.list_active_transaction.add(transaction_id)

        elif(event == "COMMIT"):
            # remove when commited
            self.list_active_transaction.remove(transaction_id)

        elif(event == "ABORT"): # Simulate Normal Operation
            recovery_result = {
                "undo": self.rollback(log_entry)
            }

        elif(event == "ABORT SYSTEM"): # Simulate SYSTEM FAILURE ABORT
            crit = RecoverCriteria(log_entry.transaction_id)
            recovery_result = self.recover(log_entry, crit)

        self.buffer_log_entries.append(log_entry)

        if len(self.buffer_log_entries) >= self.buffer_size:
            self.checkpoint_manager.perform_checkpoint(self.buffer_log_entries, self.list_active_transaction)

        return recovery_result
    
    def rollback(self, log_entry: LogEntry):
        """
        Function to rollback , **DURING NORMAL OPERATION**
        """

        return self.recovery.rollback(self.buffer_log_entries, [log_entry.transaction_id])


    def recover(self, log_entry: LogEntry, criteria: RecoverCriteria):
        """
        Function to recover from specific entry in log file. 
        **FROM FAILURE**

        Params:
            criteria(RecoverCriteria): specific criteria to start the recovery point
        """
        
        # self.checkpoint_manager.perform_checkpoint()
        
        # redo and undo operations
        redo_instructions = self.recovery.redo(self.buffer_log_entries)
        undo_instructions = self.recovery.undo(self.buffer_log_entries)

        return {
            "redo": redo_instructions,
            "undo": undo_instructions
        }

    def _stop(self):
        """
        Stop the background checkpoint thread.
        """

        # perform final checkpoint
        self.checkpoint_manager.perform_checkpoint()
    
    def signal_handler(self, signum, frame):
        """
        Custom signal handler to handle SIGINT and SIGSEV.
        """
        self.logger.warning(f"Received signal {signum}. Stopping FailureRecovery and performing final checkpoint.")
        self._stop()
        
        # Raise the original signal to allow the program to terminate
        signal.signal(signum, signal.SIG_DFL)
        signal.raise_signal(signum)

# if __name__ == "__main__":

#     # Initiate main section of the program
#     log_file = "log_edbert.log" # log file name
#     recovery = FailureRecovery(log_file, 10, 5)

    