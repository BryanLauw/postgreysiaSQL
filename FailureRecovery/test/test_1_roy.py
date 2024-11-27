from ..main import FailureRecovery
import random
import time

if __name__ == "__main__":
    # Initiate main section of the program
    log_file = "log_M.log" # log file name
    recovery = FailureRecovery(log_file, 10, 5)

    # ---------------------------------------------------------------- #
    #   SIMULATION - Only.                                             #
    #   Uncomment self._start_checkpoint_thread if run on real time    #
    # ---------------------------------------------------------------- #
    arr_new = []

    # Generate new transactions starting from id 4
    for i in range(0, 60):
        # Start event
        arr_new.append({"id": i, "event": "START"})
        
        # Random DATA event with object_value, old_value, new_value
        object_value = random.choice(['A', 'B', 'C', 'D', 'E', 'F', 'G'])
        old_value = random.randint(100, 1000)
        new_value = random.randint(100, 1000)
        arr_new.append({"id": i, "event": "DATA", "object_value": object_value, 
                        "old_value": str(old_value), "new_value": str(new_value)})
        
        # Random COMMIT or ABORT event
        if random.choice(['COMMIT', 'ABORT']) == 'COMMIT':
            arr_new.append({"id": i, "event": "COMMIT"})
        else:
            arr_new.append({"id": i, "event": "ABORT"})
        
        # Simulate random ABORT SYSTEM event occasionally
        if random.random() < 0.05:  # 5% chance of system abort
            arr_new.append({"id": i, "event": "ABORT SYSTEM"})


    for x in arr_new:
        recovery.write_log_entry(
            x.get("id", ""),  # Default to an empty string if "id" is missing
            x.get("event", ""),  # Default to an empty string if "event" is missing
            x.get("object_value", ""),  # Default to an empty string if "object_value" is missing
            x.get("old_value", ""),  # Default to an empty string if "old_value" is missing
            x.get("new_value", "")  # Default to an empty string if "new_value" is missing
        )
        # break
        time.sleep(1)