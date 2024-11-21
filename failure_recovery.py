from datetime import datetime
from dataclasses import dataclass
from typing import Optional
import time
import threading


@dataclass
class RecoverCriteria:
    transaction_id: Optional[int] = None
    timestamp: Optional[datetime] = None


class FailureRecovery:
    def __init__(self, buffer_size=1000, interval=60):
        self.interval = interval
        self.buffer = []
        self.buffer_size = buffer_size
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.checkpoint_thread = threading.Thread(target=self._periodic_checkpoint, args=(self.interval,))
        self.checkpoint_thread.daemon = True
        self.checkpoint_thread.start()

    def write_log(
        self, transaction_id: int, timestamp: datetime, table: str, column: str, row: int, old_value: str, new_value: str
    ):
        log_entry = {
            "transaction_id": transaction_id,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "table": table,
            "column": column,
            "row": row,
            "old_value": old_value,
            "new_value": new_value,
        }
        with self.lock:
            self.buffer.append(log_entry)
            if len(self.buffer) >= self.buffer_size:
                print("Buffer full")
                self.save_checkpoint()

    def save_checkpoint(self):
        print("Save")
        self._write_to_txt()

    def _write_to_txt(self):
        try:
            with self.lock:
                if not self.buffer:
                    return

                with open("log.txt", "a") as logfile:
                    for log_entry in self.buffer:
                        log_entry_str = ",".join(
                            [
                                str(log_entry["transaction_id"]),
                                log_entry["timestamp"],
                                log_entry["table"],
                                log_entry["column"],
                                str(log_entry["row"]),
                                log_entry["old_value"],
                                log_entry["new_value"],
                            ]
                        ) + "\n"
                        logfile.write(log_entry_str)
                self.buffer.clear()
        except Exception as e:
            print(e)

    def _periodic_checkpoint(self, interval: int = 60):
        while not self.stop_event.is_set():
            time.sleep(interval)
            print("Periodik")
            self.save_checkpoint()

    def stop(self):
        print("Stop")
        self.stop_event.set()
        self.checkpoint_thread.join()


if __name__ == "__main__":
    recovery = FailureRecovery()

    try:
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
            print("write log")
            time.sleep(2)
    finally:
        recovery.stop()
