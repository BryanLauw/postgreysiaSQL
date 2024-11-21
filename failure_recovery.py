from datetime import datetime

class FailureRecovery:

    def __init__(self):
        self.log = []

    def write_log(self, transaction_id: int, timestamp: datetime, table: str, column: str, row: int, old_value: str, new_value: str):
        log_entry = {
            "transaction_id": transaction_id,
            "timestamp": timestamp,
            "table": table,
            "column": column,
            "row": row,
            "old_value": old_value,
            "new_value": new_value,
        }
        self.log.append(log_entry)

    def save_checkpoint(self):
        self.write_to_txt()

    def write_to_txt(self):
        try:
            with open('log.txt', 'a') as logfile:
                for log_entry in self.log:
                    log_entry_str = ",".join([
                        str(log_entry["transaction_id"]),
                        log_entry["timestamp"],
                        log_entry["table"],
                        log_entry["column"],
                        str(log_entry["row"]),
                        log_entry["old_value"],
                        log_entry["new_value"]
                    ]) + "\n"
                    logfile.write(log_entry_str)
                self.log.clear()  
        except Exception as e:
            raise e

    def recover(self):
        pass

