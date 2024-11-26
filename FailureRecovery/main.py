# python built-ins
import time
from datetime import datetime
from typing import Optional, List, Set
import threading
import logging
import atexit
import signal

# class from other files
from main_checkpoint import Checkpoint
from main_recovery import Recovery
from main_log_entry import LogEntry
from main_recover_criteria import RecoverCriteria


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
        # init from param
        self.log_file = fname
        self.buffer_size = buffer_size
        self.checkpoint_interval = interval
        
        # init logger function from python. buat nge print doang dah
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # init set / list for keep track transaction.
        # Used in undo transaction list and active transaction for checkpoint 
        self.list_active_transaction: Set[str] = set()

        # init buffer
        self.buffer_log_entries: List[LogEntry] = []
        
        # init threading
        self.checkpoint_thread = None
        self.stop_checkpoint = False
        
        # init checkpoint logger
        self.checkpoint = Checkpoint(fname, interval)
        # init recovery
        self.recovery = Recovery(fname, self.logger)
        
        # start the checkpoint threading
        # TODO: Uncomment once real testing needed, not simulated
        # self._start_checkpoint_thread()

        # Register the _stop method to be called when the program exits
        # TODO: Uncomment when running with thread
        # atexit.register(self._stop)
        
        # Register a custom signal handler to handle SIGINT and SIGSEV
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGSEGV, self.signal_handler)
        
    
    def _start_checkpoint_thread(self):
        """
        Start the background thread that periodically performs checkpoints.
        """
        self.checkpoint_thread = threading.Thread(target=self.checkpoint_worker)
        self.checkpoint_thread.daemon = True
        self.checkpoint_thread.start()

    def checkpoint_worker(self):
        """
        The worker function that runs in the background thread and performs checkpoints.
        """
        while not self.stop_checkpoint:
            # perform checkpoint
            self.checkpoint.perform_checkpoint(self.buffer_log_entries)
            # clear buffer after checkpoint
            self.buffer_log_entries.clear()
            self.logger.info(f"Checkpoint completed. Waiting for {self.checkpoint_interval} seconds before next checkpoint.")
            # simulate sleep for x seconds
            for _ in range(self.checkpoint_interval):
                if self.stop_checkpoint:
                    return
                time.sleep(1)
    
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

        log_entry = LogEntry(
            timestamp=datetime.now(),
            transaction_id=transaction_id,
            event=event,
            object_value=object_value,
            old_value=old_value,
            new_value=new_value
        )

        if (event == "START"):
            # add when active
            self.list_active_transaction.add(transaction_id)

        elif(event == "COMMIT"):
            # remove when commited
            self.list_active_transaction.remove(transaction_id)

        elif(event == "ABORT"): # Simulate Normal Operation
            self.rollback(log_entry)

        elif(event == "ABORT SYSTEM"): # Simulate SYSTEM FAILURE ABORT
            crit = RecoverCriteria(log_entry.transaction_id)
            self.recover(log_entry, crit)

        self.buffer_log_entries.append(log_entry)

        if len(self.buffer_log_entries) >= self.buffer_size:
            self.checkpoint.perform_checkpoint(self.buffer_log_entries)
            self.buffer_log_entries.clear()
    
    def rollback(self, log_entry: LogEntry):
        """
        Function to rollback , **DURING NORMAL OPERATION**
        """
        
        # TODO: Yang di rollback hanya dibuffer aja atau lgsg tulis stable storage ?

        self.recovery.rollback(self.buffer_log_entries, [log_entry.transaction_id])

    def recover(self, log_entry: LogEntry, criteria: RecoverCriteria):
        """
        Function to recover from specific entry in log file. 
        **FROM FAILURE**

        Params:
            criteria(RecoverCriteria): specific criteria to start the recovery point
        """
        # TODO: Tanya lebih lanjut terkait RECOVERY. Terutama bingung ini yang 
        # NORMAL FAILURE, ATAU SISTEM FAILURE

        # TODO: TAMBAHAN, ini tanya yang system failure teh pake recovercriteria?
        
        # redo
        self.recovery.redo(self.buffer_log_entries)

        # undo
        self.recovery.undo(self.buffer_log_entries)

    def _stop(self):
        """
        Stop the background checkpoint thread.
        """
        self.stop_checkpoint = True
        self.checkpoint_thread.join()
        self.checkpoint.perform_checkpoint(self.buffer_log_entries)
    
    def signal_handler(self, signum, frame):
        """
        Custom signal handler to handle SIGINT and SIGSEV.
        """
        self.logger.warning(f"Received signal {signum}. Stopping FailureRecovery and performing final checkpoint.")
        # TODO: UNCOMMENT When Run Threading
        # self._stop()
        
        # Raise the original signal to allow the program to terminate
        signal.signal(signum, signal.SIG_DFL)
        signal.raise_signal(signum)


if __name__ == "__main__":
    # format for logger printed to console for debug
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Initiate main section of the program
    log_file = "log_edbert.log" # log file name
    recovery = FailureRecovery(log_file, 10, 5)

    # ---------------------------------------------------------------- #
    #   SIMULATION - Only.                                             #
    #   Uncomment self._start_checkpoint_thread if run on real time    #
    # ---------------------------------------------------------------- #
    arr = [
        {"id": 0, "event": "START"},
        {"id": 0, "event": "DATA", "object_value": "B", "old_value": "2000", "new_value": "2050"},
        {"id": 1, "event": "START"},
        {"id": None, "event": "CHECKPOINT", "object_value": "{0,1}"}, # simulate checkpoint
        {"id": 1, "event": "DATA", "object_value": "C", "old_value": "700", "new_value": "600"},
        {"id": 1, "event": "COMMIT"},
        {"id": 2, "event": "START"},
        {"id": 2, "event": "DATA", "object_value": "A", "old_value": "500", "new_value": "400"},
        {"id": 0, "event": "ABORT"}, # NORMAL OPERATION ABORT
        # {"id": 0, "event": "DATA", "object_value": "B", "new_value": "2000"},
        {"id": 2, "event": "DATA", "object_value": "A", "new_value": "500"},
        {"id": 2, "event": "ABORT SYSTEM"}, # SIMULATE SYSTEM ABORT - testing aja. cape timing ctrl+c :V  Though Our Code handled SIGSEGV
    ]

    for x in arr:
        recovery.write_log_entry(
            x.get("id", ""),  # Default to an empty string if "id" is missing
            x.get("event", ""),  # Default to an empty string if "event" is missing
            x.get("object_value", ""),  # Default to an empty string if "object_value" is missing
            x.get("old_value", ""),  # Default to an empty string if "old_value" is missing
            x.get("new_value", "")  # Default to an empty string if "new_value" is missing
        )
        # break
        # time.sleep(1)

