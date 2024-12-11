from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List
import csv
import logging

@dataclass
class RecoverCriteria:
    transaction_id: Optional[int] = None
    timestamp: Optional[datetime] = None

@dataclass
class LogEntry:
    transaction_id: int
    timestamp: datetime
    operation: str
    status: Optional[str]
    table: Optional[str]
    column: Optional[str]
    row: Optional[int]
    old_value: Optional[str]
    new_value: Optional[str]

    @classmethod
    def from_list(cls, log_data: List[str]):
        """
        Create a LogEntry object from a list of log data
        
        :param log_data: List containing log entry information
        :return: LogEntry object
        """
        # Ensure the log data has the correct number of elements
        if len(log_data) < 9:
            raise ValueError(f"Insufficient log data: {log_data}")
        
        return cls(
            transaction_id=int(log_data[0]),
            timestamp=datetime.strptime(log_data[1], "%Y-%m-%d %H:%M:%S"),
            operation=log_data[2],  # Add the missing operation argument
            status=log_data[3] if log_data[3] != 'None' else None,
            table=log_data[4] if log_data[4] != 'None' else None,
            column=log_data[5] if log_data[5] != 'None' else None,
            row=int(log_data[6]) if log_data[6] != 'None' else None,
            old_value=log_data[7] if log_data[7] != 'None' else None,
            new_value=log_data[8] if log_data[8] != 'None' else None
        )

class FailureRecovery:
    def __init__(self, buffer_size=1000, interval=300):
        self.log_file = "log_edbert_example.log"
        self.current_state = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def load_log_entries_from_csv(self, file_path: str) -> List[LogEntry]:
        """
        Load log entries from a CSV file
        
        :param file_path: Path to the log file
        :return: List of LogEntry objects
        """
        log_entries = []
        try:
            with open(file_path, 'r') as f:
                csv_reader = csv.reader(f)
                for row in csv_reader:
                    try:
                        log_entry = LogEntry.from_list(row)
                        log_entries.append(log_entry)
                    except ValueError as e:
                        logging.error(f"Skipping invalid log entry: {row}")
        except FileNotFoundError:
            logging.error(f"Log file not found: {file_path}")
        
        return log_entries
    
    def execute_recovery_query(self, log_entry: LogEntry):
        """
        Execute a recovery query based on the log entry
        
        :param log_entry: Log entry to be recovered
        """
        try:
            # Validate required fields
            if not log_entry.table or log_entry.row is None:
                self.logger.error(f"Invalid log entry: missing table or row. Entry: {log_entry}")
                return False
            
            # Logging for traceability
            self.logger.info(f"Recovering operation: {log_entry.operation} for {log_entry.table}.{log_entry.row}")
            
            # Ensure the table exists in current state
            if log_entry.table not in self.current_state:
                self.current_state[log_entry.table] = {}
            
            if log_entry.operation == 'INSERT':
                # Undo an insert by removing the row
                if log_entry.row in self.current_state[log_entry.table]:
                    del self.current_state[log_entry.table][log_entry.row]
                    self.logger.debug(f"Removed row {log_entry.row} from {log_entry.table}")
            
            elif log_entry.operation == 'UPDATE':
                # Revert to the previous value
                if log_entry.old_value is not None:
                    self.current_state[log_entry.table][log_entry.row] = log_entry.old_value
                    self.logger.debug(f"Reverted {log_entry.table}.{log_entry.row} to: {log_entry.old_value}")
                else:
                    self.logger.error(f"Cannot recover UPDATE: no old value for {log_entry.table}.{log_entry.row}")
                    return False
            
            elif log_entry.operation == 'DELETE':
                # Restore a deleted entry
                if log_entry.old_value is not None:
                    self.current_state[log_entry.table][log_entry.row] = log_entry.old_value
                    self.logger.debug(f"Restored deleted row {log_entry.row} in {log_entry.table} with value: {log_entry.old_value}")
                else:
                    self.logger.error(f"Cannot recover DELETE: no value for {log_entry.table}.{log_entry.row}")
                    return False
            
            else:
                # Unknown operation or transaction_start
                if log_entry.operation != 'transaction_start':
                    self.logger.warning(f"Unhandled operation: {log_entry.operation}")
                return True
            
            return True
        
        except Exception as e:
            self.logger.error(f"Error during recovery: {str(e)}", exc_info=True)
            return False

    def _meets_recovery_criteria(self, log_entry: LogEntry, criteria: RecoverCriteria) -> bool:
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
        """
        Recover database state based on given criteria
        
        :param criteria: Recovery criteria
        """
        # first read all log entries
        log_entries = self.load_log_entries_from_csv(self.log_file)

        # reverse from latest as first item
        log_entries.reverse()

        for log_entry in log_entries:
            # Check recovery criteria
            if self._meets_recovery_criteria(log_entry, criteria):
                break
            
            # Execute recovery query
            self.execute_recovery_query(log_entry)
        
        print("Recovery process completed.")
        print("Recovered database state:", self.current_state)

if __name__ == '__main__':
    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    recovery = FailureRecovery(buffer_size=5, interval=10)

    # USE edbert_example.log
    # transaction_id, timestamp, log_type, is_ended, status, table, column, row, old_value, new_value
    a = RecoverCriteria(transaction_id=1, timestamp=datetime(2024, 11, 21, 20, 17, 5))
    print(a)
    recovery.recover(a)