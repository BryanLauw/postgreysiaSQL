# python built-ins
from datetime import datetime
from typing import Optional, List
from dataclasses import dataclass
import threading
import logging

@dataclass
class LogEntry:
    """
    Represents a single entry in the log. (object)

    Attributes:
        timestamp (datetime): The datetime when the log entry was created.
        operation_id (int): The unique identifier for the operation.
        event (str): The type of event being logged (e.g., "START", "COMMIT", "ABORT", "Object Value").
        object_value (Optional[str]): The value of the object being modified (if applicable).
        old_value (Optional[str]): The old value of the object (if applicable).
        new_value (Optional[str]): The new value of the object (if applicable).
    """
    timestamp: datetime
    operation_id: int
    event: str
    object_value: Optional[str] = None
    old_value: Optional[str] = None
    new_value: Optional[str] = None


class Checkpoint:
    def __init__(self, log_file:str="log_kel.log", interval: int = 10):
        """
        Params:
            log_file (str): The name of the log file.
            checkpoint_lock (threading.Lock): A lock to ensure only one checkpoint is performed at a time.
        """
        # input from param
        self.log_file = log_file
        self.checkpoint_lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)

    def perform_checkpoint(self, buffer_log_entries: List[LogEntry]):
        """
        Perform a checkpoint by writing the log entries in the buffer to the log file.

        This method also holds all transactions/operations until the checkpoint is completed,
        to ensure data consistency.
        """
        with self.checkpoint_lock:
            self.hold_transactions()

            with open(self.log_file, 'a') as f:
                for entry in buffer_log_entries:
                    f.write(f"{entry.timestamp.isoformat(timespec='seconds')},{entry.transaction_id},{entry.event},{entry.object_value or ''},{entry.old_value or ''},{entry.new_value or ''}\n")
            self.logger.info(f"Checkpoint written to {self.log_file}")

            self.release_transactions()
    
    def hold_transactions(self):
        """
        Hold all ongoing transactions/operations until the checkpoint is completed.
        """
        # TODO: NEED MORE DISCUSSION
        pass

    def release_transactions(self):
        """
        Release the held transactions/operations after the checkpoint is completed.

        This method should be implemented to accommodate the specific requirements of your application.
        """
        # TODO: NEED MORE DISCUSSION
        pass

