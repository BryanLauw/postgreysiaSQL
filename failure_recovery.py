from datetime import datetime
from dataclasses import dataclass
from typing import Optional, Union, Literal
import time
import threading

@dataclass
class RecoverCriteria:
    transaction_id: Optional[int] = None
    timestamp: Optional[datetime] = None

StatusType = Union[Literal["commit"], Literal["abort"]]

@dataclass
class LogEntry:
    transaction_id: Optional[int]
    timestamp: datetime
    log_type: str
    is_ended: bool
    status: Optional[str]
    table: Optional[str]
    column: Optional[str]
    row: Optional[str]
    old_value: Optional[str]
    new_value: Optional[str]

    @classmethod
    def from_dict(cls, log_dict: dict):
        """
        Create a LogEntry object from a dictionary
        
        :param log_dict: Dictionary containing log entry information
        :return: LogEntry object
        """
        # Convert timestamp string to datetime object
        timestamp = datetime.strptime(log_dict['timestamp'], "%Y-%m-%d %H:%M:%S") \
            if log_dict['timestamp'] else None
        
        return cls(
            transaction_id=log_dict.get('transaction_id'),
            timestamp=timestamp,
            log_type=log_dict.get('log_type'),
            is_ended=log_dict.get('is_ended', False),
            status=log_dict.get('status'),
            table=log_dict.get('table'),
            column=log_dict.get('column'),
            row=log_dict.get('row'),
            old_value=log_dict.get('old_value'),
            new_value=log_dict.get('new_value')
        )

class FailureRecovery:
    def __init__(self, buffer_size=1000, interval=300):
        self.interval = interval
        self.buffer = []
        self.buffer_size = buffer_size
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.checkpoint_thread = threading.Thread(target=self._periodic_checkpoint, args=(self.interval,))
        self.checkpoint_thread.daemon = True
        self.checkpoint_thread.start()
        self.log_file = "log.txt"
        # ASUMSI INI STATE DARI QUERY PROCESSING
        self.current_state = {}

    def start_transaction(self, transaction_id: int):
        log_entry = {
            "transaction_id": transaction_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "log_type": "transaction_start",
            "is_ended": False,
            "status": None,
            "table": None,
            "column": None,
            "row": None,
            "old_value": None,
            "new_value": None,
        }
        with self.lock:
            self.buffer.append(log_entry)
            if len(self.buffer) >= self.buffer_size + 10:
                print("Buffer full")
                self.save_checkpoint()

    def end_transaction(self, transaction_id: int, status: StatusType):
        if status not in ["commit", "abort"]:
            raise ValueError("Invalid status!")
        log_entry = {
            "transaction_id": transaction_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "log_type": "transaction_end",
            "is_ended": True,
            "status": status,
            "table": None,
            "column": None,
            "row": None,
            "old_value": None,
            "new_value": None,
        }
        with self.lock:
            self.buffer.append(log_entry)
            if len(self.buffer) >= self.buffer_size + 10:
                print("Buffer full")
                self.save_checkpoint()

    def write_log(self, transaction_id: int, timestamp: datetime, table: str, column: str, row: int, old_value: str, new_value: str):
        log_entry = {
            "transaction_id": transaction_id,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "log_type": "data_change",
            "is_ended": False,
            "status": None,
            "table": table,
            "column": column,
            "row": row,
            "old_value": old_value,
            "new_value": new_value,
        }
        with self.lock:
            self.buffer.append(log_entry)
            if len(self.buffer) >= self.buffer_size + 10:
                print("Buffer full")
                self.save_checkpoint()

    def save_checkpoint(self):
        print("Save")
        self._write_to_txt()

    def redo():
        pass

    # Example usage
    def _load_log_entries(self) -> list[LogEntry]:
        """
        Load log entries from the log file and parse them into LogEntry objects
        
        :return: List of LogEntry objects
        """
        try:
            with open(self.log_file, 'r') as f:
                # Assuming log entries are stored as dictionaries (e.g., in JSON format)
                log_dicts = [eval(line.strip()) for line in f]  # Replace with json.loads() in real-world scenario
                log_entries = [LogEntry.from_dict(log_dict) for log_dict in log_dicts]
                return log_entries
        except FileNotFoundError:
            return []
    
    def execute_recovery_query(self, log_entry: dict):
        """
        Execute a recovery query based on the log entry
        
        :param log_entry: Log entry to be recovered
        """
        operation = log_entry.get('operation')
        key = log_entry.get('key')
        value = log_entry.get('value')
        
        if operation == 'INSERT':
            # Undo an insert by removing the key
            if key in self.current_state:
                del self.current_state[key]
        elif operation == 'UPDATE':
            # Revert to the previous value
            if 'previous_value' in log_entry:
                self.current_state[key] = log_entry['previous_value']
        elif operation == 'DELETE':
            # Restore a deleted entry
            self.current_state[key] = value

    def _meets_recovery_criteria(log_entry: LogEntry, criteria: RecoverCriteria):
        """
        Check if a log entry meets the recovery criteria
        
        :param log_entry: LogEntry object to check
        :param criteria: RecoverCriteria to match against
        :return: Boolean indicating if criteria are met
        """
        # Check transaction_id if specified in criteria
        if criteria.transaction_id is not None and \
        log_entry.transaction_id != criteria.transaction_id:
            return False
        
        # Check timestamp if specified in criteria
        if criteria.timestamp is not None and \
        log_entry.timestamp != criteria.timestamp:
            return False
        
        return True

    def recover(self, criteria: RecoverCriteria):

        # first read all log entries
        log_entries = self._load_log_entries()

        # reverse from latest as first item
        log_entries.reverse()

        for log_entry in log_entries:
            # Check recovery criteria
            if not self._meets_recovery_criteria(log_entry, criteria):
                break
            
            # Execute recovery query
            self.execute_recovery_query(log_entry)
        
        print("Recovery process completed.")
        print("Recovered database state:", self.query_processor.current_state)

    def _write_to_txt(self):
        try:
            with self.lock:
                if not self.buffer:
                    return

                with open(self.log_file, "a") as logfile:
                    for log_entry in self.buffer:
                        log_entry_str = ",".join([
                            str(log_entry.get("transaction_id", "")),
                            log_entry.get("timestamp", ""),
                            log_entry.get("log_type", ""),
                            str(log_entry.get("status", "")),
                            str(log_entry.get("table", "")),
                            str(log_entry.get("column", "")),
                            str(log_entry.get("row", "")),
                            str(log_entry.get("old_value", "")),
                            str(log_entry.get("new_value", "")),
                        ]) + "\n"
                        logfile.write(log_entry_str)
                self.buffer.clear()

        except Exception as e:
            print(e)

    def _periodic_checkpoint(self, interval: int):
        while not self.stop_event.is_set():
            time.sleep(interval)
            try:
                print("Periodic")
                self.save_checkpoint()
            except Exception as e:
                print(e)

    def stop(self):
        print("Stop")
        self.stop_event.set()
        self.checkpoint_thread.join()
        self.save_checkpoint()

if __name__ == "__main__":

    recovery = FailureRecovery(buffer_size=10, interval=10)

    recovery.start_transaction(1)
    for i in range(20):
        recovery.write_log(
            transaction_id=i,
            timestamp=datetime.now(),
            table="users",
            column="name",
            row=i,
            old_value=f"old_name_{i}",
            new_value=f"new_name_{i}",
        )
        print("Write log")
        time.sleep(2)

    recovery.end_transaction(1, "commit")

