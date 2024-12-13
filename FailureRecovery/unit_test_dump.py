import random
import json

if __name__ == "__main__":

    logs_dump = []

    for i in range(0, 60):

        logs_dump.append({"id": i, "event": "START"})
        
        object_value = random.choice(['A', 'B', 'C', 'D', 'E', 'F', 'G'])
        old_value = random.randint(100, 1000)
        new_value = random.randint(100, 1000)
        logs_dump.append({"id": i, "event": "DATA", "object_value": object_value, 
                        "old_value": str(old_value), "new_value": str(new_value)})
        
        if random.choice(['COMMIT', 'ABORT']) == 'COMMIT':
            logs_dump.append({"id": i, "event": "COMMIT"})
        else:
            logs_dump.append({"id": i, "event": "ABORT"})
        
        if random.random() < 0.05:
            logs_dump.append({"id": i, "event": "ABORT SYSTEM"})

    with open("logs_dump.txt", "w") as file:
        for log in logs_dump:
            file.write(json.dumps(log) + "\n")