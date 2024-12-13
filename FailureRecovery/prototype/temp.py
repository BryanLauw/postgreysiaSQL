# failure_recovery.py
import atexit
import signal
import logging
from datetime import datetime
from typing import Optional, List, Set

# Import necessary classes
from threading_manager import ThreadingManager
from checkpoint_manager import CheckpointManager
from main_log_entry import LogEntry
from main_recover_criteria import RecoverCriteria
from main_recovery import Recovery

class FailureRecovery:
    def __init__(self, 
                 fname: str = "log_kel.log", 
                 buffer_size: int = 1000, 
                 interval: int = 300):
        """
        Initialize FailureRecovery system
        
        Params:
            fname (str): Log file name
            buffer_size (int): Maximum log entries before checkpoint
            interval (int): Checkpoint interval in seconds
        """
        # Logging setup
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.__class__.__name__)

        # Configuration
        self.log_file = fname
        self.buffer_size = buffer_size

        # Transaction and buffer tracking
        self.list_active_transaction: Set[str] = set()
        self.buffer_log_entries: List[LogEntry] = []

        # Initialize threading and checkpoint managers
        self.threading_manager = ThreadingManager(logger=self.logger)
        self.checkpoint_manager = CheckpointManager(
            log_file=fname, 
            threading_manager=self.threading_manager,
            interval=interval,
            buffer_size=buffer_size,
            logger=self.logger
        )

        # Initialize recovery
        self.recovery = Recovery(fname, self.logger)

        # Start checkpoint manager
        self.checkpoint_manager.start(
            get_buffer_callback=self.get_buffer,
            get_active_transactions_callback=self.get_active_transactions,
            clear_buffer_callback=self.clear_buffer
        )

        # Register exit and signal handlers
        atexit.register(self.stop)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGSEGV, self.signal_handler)

    def get_buffer(self):
        """
        Callback to get current log buffer
        """
        return self.buffer_log_entries

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

    def write_log_entry(self, 
                         transaction_id: int, 
                         event: str, 
                         object_value: Optional[str] = None, 
                         old_value: Optional[str] = None, 
                         new_value: Optional[str] = None):
        """
        Write a new log entry with thread-safe controls
        """
        try:
            # Ensure thread-safe operation
            self.threading_manager.thread_safe_operation()
            
            # Create log entry
            log_entry = LogEntry(
                timestamp=datetime.now(),
                transaction_id=transaction_id,
                event=event,
                object_value=object_value,
                old_value=old_value,
                new_value=new_value
            )

            # Manage active transactions
            if event == "START":
                self.list_active_transaction.add(transaction_id)
            elif event == "COMMIT":
                self.list_active_transaction.remove(transaction_id)
            elif event == "ABORT":
                self.rollback(log_entry)
            elif event == "ABORT SYSTEM":
                criteria = RecoverCriteria(log_entry.transaction_id)
                self.recover(log_entry, criteria)

            # Add to buffer
            self.buffer_log_entries.append(log_entry)

            # Perform checkpoint if buffer is full
            if len(self.buffer_log_entries) >= self.buffer_size:
                self.checkpoint_manager.perform_checkpoint()
        
        finally:
            # Signal operation completion
            self.threading_manager.operation_complete()

    def rollback(self, log_entry: LogEntry):
        """
        Rollback a specific transaction
        """
        self.recovery.rollback(self.buffer_log_entries, [log_entry.transaction_id])

    def recover(self, log_entry: LogEntry, criteria: RecoverCriteria):
        """
        Recover from a system failure
        """
        # Perform checkpoint
        self.checkpoint_manager.perform_checkpoint()
        
        # Redo and undo operations
        self.recovery.redo(self.buffer_log_entries)
        self.recovery.undo(self.buffer_log_entries)

    def stop(self):
        """
        Stop all background threads and perform final checkpoint
        """
        self.checkpoint_manager.stop()
        
        # Perform final checkpoint
        self.checkpoint_manager.perform_checkpoint()

    def signal_handler(self, signum, frame):
        """
        Handle system signals
        """
        self.logger.warning(f"Received signal {signum}. Stopping FailureRecovery.")
        self.stop()
        
        # Raise the original signal to allow program termination
        signal.signal(signum, signal.SIG_DFL)
        signal.raise_signal(signum)

# Main execution (similar to your original script)
if __name__ == "__main__":
    log_file = "log_edbert.log"
    recovery = FailureRecovery(log_file, 10, 5)

    # Your existing simulation code here
    arr = [
        {"id": 0, "event": "START"},
        {"id": 0, "event": "DATA", "object_value": "B", "old_value": "2000", "new_value": "2050"},
        # ... rest of your simulation array
    ]

    for x in arr:
        recovery.write_log_entry(
            x.get("id", ""),
            x.get("event", ""),
            x.get("object_value", ""),
            x.get("old_value", ""),
            x.get("new_value", "")
        )
    time.sleep(1)