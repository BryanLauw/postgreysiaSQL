from main_log_entry import LogEntry
from typing import Optional, List, Set, Union
from datetime import datetime
import logging
import re
import ast

class Recovery:
    """
    Class to handle recovery
    """
    def __init__(self, log_file: str, logger: logging.Logger):
        """
        Constructor. Initialize the Recovery Instance

        Params:
            log_file(str): the log file name
            logger(logging.Logger): the log function to print to terminal
        """
        # init from param
        self.log_file = log_file
        self.logger = logger

    def rollback(self, buffer_log_entries: List[LogEntry], log_entry: LogEntry):
        
        # start checking from buffer
        temp = buffer_log_entries[::-1] # reverse list
        # print(temp, len(temp))
        # find the start
        isFound = False
        for x in temp:
            if (x.transaction_id == log_entry.transaction_id):
                if x.event == "START":
                    # stop since we already found it
                    isFound = True
                    break
                print(x)
                self._reverse_query_executor(x)
        

        # if not found, then check in the log
        if isFound:
            return
        
        # not found. Read from file log
        temp = self._load_log_entries()
        
        for x in temp:
            if (x.transaction_id == log_entry.transaction_id):
                if x.event == "START":
                    # stop since we already found it
                    isFound = True
                    break
                print(x)
                self._reverse_query_executor(x)

    def _write_log_entry(self, entry: LogEntry):
        """
        Write a new log entry to the log file.
        """
        try:
            with open(self.log_file, 'a') as f:
                f.write(f"EDBERT {entry.timestamp.isoformat(timespec='seconds')},{entry.transaction_id},{entry.event},{entry.object_value or ''},{entry.new_value or ''}\n")
        except FileNotFoundError:
            return []

    def _reverse_query_executor(self, log_entry: LogEntry):
        
        # add compensation log
        self._write_log_entry(log_entry)

        # TODO: SEND TO ????
        # soalnya failure recovery harus kerjasama, suruh edit data kan :VVVV

        self.logger.info("SEND to ??? REVERSED Query")

    def _load_log_entries(self) -> List[LogEntry]:
        """
        Load log entries from the log file then parse it.
        """
        try:
            temp: List[LogEntry] = []
            with open(self.log_file, 'r') as f:
                lines = [line.strip() for line in f if line.strip()]  # Read each line and remove whitespace
                
                for line in lines:
                    # parse each line first

                    log_entry = self._parse_line_to_log_entry(line)

                    temp.append(log_entry)
            return temp
        except FileNotFoundError:
            return []
    
    def _parse_line_to_log_entry(line: str) -> LogEntry:
        # First, extract the list if it exists
        list_match = re.search(r'\[.*?\]', log_line)
        list_str = list_match.group(0) if list_match else None
        
        # Remove the list from the original line for splitting
        if list_str:
            log_line = log_line.replace(list_str, 'LIST_PLACEHOLDER')
        
        # Split the modified line by comma
        parts = log_line.split(',')
        
        # Ensure we have at least 3 parts
        if len(parts) < 3:
            raise ValueError(f"Invalid log entry format: {log_line}")
        
        # Parse timestamp
        timestamp = datetime.fromisoformat(parts[0].replace('T', ' '))
        
        # Parse transaction ID (handle None case)
        transaction_id = int(parts[1]) if parts[1] and parts[1] != 'None' else None
        
        # Get event type
        event = parts[2]
        
        # Initialize optional values
        object_value: Optional[Union[str, List]] = None
        old_value = None
        new_value = None
        
        # Parse the list if it exists
        if list_str:
            try:
                object_value = ast.literal_eval(list_str)
            except (ValueError, SyntaxError):
                object_value = list_str
        elif len(parts) > 3 and parts[3] != 'LIST_PLACEHOLDER':
            object_value = parts[3]
        
        # Parse old and new values
        if len(parts) > 4 and parts[4]:
            old_value = parts[4]
        
        if len(parts) > 5 and parts[5]:
            new_value = parts[5].strip()
        
        # Case when compensation log read
        if (parts[4] and not parts[5]):
            old_value = None
            new_value = parts[4]
        
        return LogEntry(
            timestamp=timestamp,
            transaction_id=transaction_id,
            event=event,
            object_value=object_value,
            old_value=old_value,
            new_value=new_value
        )
