# checkpoint_manager.py
import threading
import time
from datetime import datetime
import logging
from typing import List, Set

class CheckpointManager:
    def __init__(self, 
                 log_file: str, 
                 threading_manager,
                 interval: int = 300, 
                 buffer_size: int = 1000, 
                 logger=None):
        """
        Initialize Checkpoint Manager
        
        Params:
            log_file (str): Path to the log file
            threading_manager (ThreadingManager): Thread management instance
            interval (int): Checkpoint interval in seconds
            buffer_size (int): Maximum number of log entries before checkpoint
            logger (logging.Logger, optional): Logger for tracking checkpoint events
        """
        # Configuration
        self.log_file = log_file
        self.checkpoint_interval = interval
        self.buffer_size = buffer_size

        # Threading management
        self.threading_manager = threading_manager
        self.checkpoint_thread = None
        self.stop_checkpoint = False

        # Logging
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def start(self, 
              get_buffer_callback, 
              get_active_transactions_callback, 
              clear_buffer_callback):
        """
        Start the checkpoint thread
        
        Params:
            get_buffer_callback (callable): Function to get current log buffer
            get_active_transactions_callback (callable): Function to get active transactions
            clear_buffer_callback (callable): Function to clear the buffer
        """
        self.get_buffer = get_buffer_callback
        self.get_active_transactions = get_active_transactions_callback
        self.clear_buffer = clear_buffer_callback

        self.checkpoint_thread = threading.Thread(target=self._checkpoint_worker)
        self.checkpoint_thread.daemon = True
        self.checkpoint_thread.start()

    def _checkpoint_worker(self):
        """
        Background thread for periodic checkpointing
        """
        while not self.stop_checkpoint:
            # Wait for the specified interval
            for _ in range(self.checkpoint_interval):
                if self.stop_checkpoint:
                    return
                time.sleep(1)
            
            # Perform checkpoint if buffer is not empty
            buffer = self.get_buffer()
            if buffer:
                self.perform_checkpoint()

    def perform_checkpoint(self):
        """
        Perform checkpoint operation
        Ensures data consistency by pausing and synchronizing operations
        """
        # Pause operations
        self.threading_manager.pause_operations()

        try:
            # Wait for operations to complete
            if not self.threading_manager.wait_for_operations_complete():
                self.logger.warning("Checkpoint could not be performed due to ongoing operations")
                return

            # Get current buffer and active transactions
            buffer = self.get_buffer()
            active_transactions = self.get_active_transactions()

            # Perform checkpoint with lock
            with self.threading_manager.checkpoint_lock:
                # Write to log file
                with open(self.log_file, 'a') as f:
                    for entry in buffer:
                        f.write(f"{entry.database_name},{entry.table},{entry.column},{entry.row},"
                                f"{entry.timestamp.isoformat(timespec='seconds')},"
                                f"{entry.transaction_id},{entry.event},"
                                f"{entry.object_value or ''},"
                                f"{entry.old_value or ''},"
                                f"{entry.new_value or ''}\n")
                    
                    # Write checkpoint marker
                    f.write(f",,,,{datetime.now().isoformat(timespec='seconds')},,CHECKPOINT,"
                            f"{active_transactions}\n")
                
                self.logger.info(f"Checkpoint written to {self.log_file}")
                
                # Clear the buffer
                self.clear_buffer()
        
        except Exception as e:
            self.logger.error(f"Checkpoint failed: {e}")
        
        finally:
            # Resume operations
            self.threading_manager.resume_operations()

    def stop(self):
        """
        Stop the checkpoint thread
        """
        self.stop_checkpoint = True
        if self.checkpoint_thread:
            self.checkpoint_thread.join()