# python built-ins
from datetime import datetime
from typing import Optional, List, Set
from dataclasses import dataclass
from main_log_entry import LogEntry
import time
import threading
import logging

from main_threading_manager import ThreadingManager

class CheckpointManager:
    def __init__(self, log_file:str="log_kel.log", threading_manager: ThreadingManager = None, interval: int = 10, buffer_size: int = 10, logger=None):
        """
        Params:
            log_file (str): The name of the log file.
            checkpoint_lock (threading.Lock): A lock to ensure only one checkpoint is performed at a time.
        """
        # input from param
        self.log_file = log_file
        self.checkpoint_interval = interval
        
        # init for thread
        self.threading_manager = threading_manager
        self.checkpoint_thread = None
        self.stop_checkpoint = False

        # init logger
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def start(self, get_buffer_callback, get_active_transaction_callback, clear_buffer_callback):
        """
        Start the background thread that periodically performs checkpoints.

        Params:
            get_buffer_callback (callable): Function to get current log buffer
            get_active_transactions_callback (callable): Function to get active transactions
            clear_buffer_callback (callable): Function to clear the buffer
        """
        self.get_buffer = get_buffer_callback
        self.get_active_transactions = get_active_transaction_callback
        self.clear_buffer = clear_buffer_callback

        self.checkpoint_thread = threading.Thread(target=self.checkpoint_worker)
        self.checkpoint_thread.daemon = True
        self.checkpoint_thread.start()

    def checkpoint_worker(self):
        """
        The worker function that runs in the background thread and performs checkpoints.
        """

        next_checkpoint_time = time.time() + self.checkpoint_interval
        while not self.stop_checkpoint:
            # sleep time
            sleep_time = max(0, next_checkpoint_time - time.time())
            # simulate sleep for 1 seconds
            if self.stop_checkpoint:
                return
            time.sleep(1)
            time.sleep(min(1, sleep_time))
            
            if time.time() >= next_checkpoint_time:
                # perform checkpoint
                buffer = self.get_buffer()
                if buffer:
                    self.perform_checkpoint()

                self.logger.info(f"Checkpoint completed. Waiting for {self.checkpoint_interval} seconds before next checkpoint.")
                
                next_checkpoint_time = time.time() + self.checkpoint_interval

    def perform_checkpoint(self):
        """
        Perform a checkpoint by writing the log entries in the buffer to the log file.

        This method also holds all transactions/operations until the checkpoint is completed,
        to ensure data consistency.
        """
        # Pause ALL THREAD
        self.threading_manager.pause_operations()

        try:
            # get current buffer and active transaction
            buffer = self.get_buffer()
            active_transactions = self.get_active_transactions()

            with self.threading_manager.checkpoint_lock:

                with open(self.log_file, 'a') as f:
                    for entry in buffer:
                        f.write(f"{entry.database_name},{entry.table},{entry.column},{entry.row},{entry.timestamp.isoformat(timespec='seconds')},{entry.transaction_id},{entry.event},{entry.object_value or ''},{entry.old_value or ''},{entry.new_value or ''}\n")
                    
                    # checkpoint marker
                    f.write(f",,,,{datetime.now().isoformat(timespec='seconds')},,CHECKPOINT,{self.get_active_transactions()}\n")
                
                self.logger.info(f"Checkpoint written to {self.log_file}")

                self.clear_buffer()

        except Exception as e:
            self.logger.error(f"Checkpoint failed: {e}")
        
        finally:
            # Resume all threads
            self.threading_manager.resume_operations()
    
    def stop(self):
        """
        Stop the background checkpoint thread.
        """
        self.stop_checkpoint = True
        if self.checkpoint_thread:
            self.checkpoint_thread.join()