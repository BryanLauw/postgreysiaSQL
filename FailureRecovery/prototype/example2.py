import logging
from typing import Dict, Any

class RecoveryExecutor:
    def __init__(self, current_state: Dict[str, Any]):
        """
        Initialize the recovery executor with the current state
        
        :param current_state: Current state dictionary to be modified
        """
        self.current_state = current_state
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def execute_recovery_query(self, log_entry: Dict[str, Any]) -> bool:
        """
        Execute a recovery query based on the log entry
        
        :param log_entry: Log entry to be recovered
        :return: Boolean indicating if recovery was successful
        """
        try:
            # Validate required keys
            if not all(key in log_entry for key in ['operation', 'key']):
                self.logger.error(f"Invalid log entry: missing required keys. Entry: {log_entry}")
                return False
            
            operation = log_entry.get('operation')
            key = log_entry.get('key')
            value = log_entry.get('value')
            
            # Logging for traceability
            self.logger.info(f"Recovering operation: {operation} for key: {key}")
            
            if operation == 'INSERT':
                # Undo an insert by removing the key
                if key in self.current_state:
                    del self.current_state[key]
                    self.logger.debug(f"Removed key {key} from current state")
                else:
                    self.logger.warning(f"Key {key} not found in current state during INSERT recovery")
            
            elif operation == 'UPDATE':
                # Revert to the previous value
                if 'previous_value' in log_entry:
                    self.current_state[key] = log_entry['previous_value']
                    self.logger.debug(f"Reverted key {key} to previous value: {log_entry['previous_value']}")
                else:
                    self.logger.error(f"Cannot recover UPDATE for key {key}: previous_value missing")
                    return False
            
            elif operation == 'DELETE':
                # Restore a deleted entry
                if value is not None:
                    self.current_state[key] = value
                    self.logger.debug(f"Restored deleted key {key} with value: {value}")
                else:
                    self.logger.error(f"Cannot recover DELETE for key {key}: value missing")
                    return False
            
            else:
                # Unknown operation
                self.logger.error(f"Unknown recovery operation: {operation}")
                return False
            
            return True
        
        except Exception as e:
            self.logger.error(f"Error during recovery: {str(e)}", exc_info=True)
            return False

# Example usage
def setup_logging():
    """
    Configure logging for the recovery executor
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

# Demonstration of usage
def main():
    # Setup logging
    setup_logging()
    
    # Initial state
    current_state = {
        'user1': 'John Doe',
        'user2': 'Jane Smith'
    }
    
    # Create recovery executor
    recovery_executor = RecoveryExecutor(current_state)
    
    # Example log entries
    log_entries = [
        {
            'operation': 'INSERT',
            'key': 'user3',
            'value': 'Bob Johnson'
        },
        {
            'operation': 'UPDATE',
            'key': 'user1',
            'previous_value': 'John Doe',
            'value': 'John Updated'
        },
        {
            'operation': 'DELETE',
            'key': 'user2',
            'value': 'Jane Smith'
        }
    ]
    
    # Execute recovery for each log entry
    for log_entry in log_entries:
        success = recovery_executor.execute_recovery_query(log_entry)
        print(f"Recovery {'successful' if success else 'failed'} for entry: {log_entry}")
    
    # Print final state
    print("\nFinal State:", current_state)

if __name__ == '__main__':
    main()