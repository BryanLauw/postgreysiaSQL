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
        self.undo_log = []

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

    def undo(self):
        n_baris = 5 # asumsi yang diundo 5 baris
        try:
            log_entries = self._load_log_entries()
            for _ in range (min(n_baris, len(log_entries))):
                self.undo_log.append(log_entries.pop())


            with open(self.log_file, 'w') as f:
                for entry in log_entries:
                    f.write(entry + '\n')
            print("UNDO success")
            print(self.undo_log)
        except Exception as e:
            print(f"Error during undo: {e}")
    
    def redo(self):
        try:
            if not self.undo_log:
                print("Tidak ada log untuk redo.")
                return

            with open(self.log_file, 'a') as f:
                for entry in reversed(self.undo_log):
                    f.write(entry + '\n')

            print("REDO success")
        except Exception as e:
            print(f"Error during redo: {e}")
    


    def _load_log_entries(self) -> list:
        """
        Load log entries from the log file
        
        :return: List of log entries
        """
        try:
            with open(self.log_file, 'r') as f:
                log_entries = [line.strip() for line in f]  # Read each line and remove whitespace
                return log_entries
        except FileNotFoundError:
            return []

    def recover(self, criteria: RecoverCriteria):

        # first read all log entries
        log_entries = self._load_log_entries()

        # reverse from latest as first item
        log_entries.reverse()

        

        pass

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
    print("\nPerforming Undo Operation:")
    recovery.undo()
    recovery.redo()
