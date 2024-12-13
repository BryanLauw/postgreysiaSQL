import re
from FailureRecovery.failure_recovery import FailureRecovery
from FailureRecovery.failure_recovery_log_entry import LogEntry
from datetime import datetime

filename = "./../unit_test_rollback.txt"
f = open(filename, 'w')
f.close()

failure_recovery = FailureRecovery(fname=filename, buffer_size=5)

test_data = [
    {"transaction_id": 1, "event": "START"},
    {"transaction_id": 2, "event": "START"},
    {"transaction_id": 3, "event": "START"},
    {"transaction_id": 1, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key": "db1_kolom1_pk_1", "primary_key_value": "bebas"}, "old_value": "1", "new_value": "2"},
    {"transaction_id": 1, "event": "COMMIT"},
    {"transaction_id": 3, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key": "db1_kolom1_pk_2", "primary_key_value": "bebas"}, "old_value": "2", "new_value": "3"},
]

expected_undo = [
    {"transaction_id": 3, "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key": "db1_kolom1_pk_2", "primary_key_value": "bebas"}, "old_value": "2"},
]

expected_txt = [
    r".*,1,START,,,",
    r".*,2,START,,,",
    r".*,3,START,,,",
    r".*,1,DATA,\{'nama_db': 'test_db_1', 'nama_kolom': 'db1_kolom1', 'primary_key': 'db1_kolom1_pk_1', 'primary_key_value': 'bebas'\},1,2",
    r".*,1,COMMIT,,,",
    r".*,CHECKPOINT,\{2, 3\}",
    r".*,3,DATA,\{'nama_db': 'test_db_1', 'nama_kolom': 'db1_kolom1', 'primary_key': 'db1_kolom1_pk_2', 'primary_key_value': 'bebas'\},2,3",
    r".*,CHECKPOINT,\{2, 3\}",
    r".*,3,DATA,\{'nama_db': 'test_db_1', 'nama_kolom': 'db1_kolom1', 'primary_key': 'db1_kolom1_pk_2', 'primary_key_value': 'bebas'\},2,",
    r".*,CHECKPOINT,\{2, 3\}",
]

for data in test_data:
    failure_recovery.write_log_entry(**data)

log_entry_rollback = LogEntry(datetime.now(), 3, "ABORT") 

rollback_res = failure_recovery.rollback(log_entry=log_entry_rollback)

print("[--------------]")
print(rollback_res)
print(expected_undo)

print(f"Undo test result: {rollback_res == expected_undo}")

with open(filename, "r") as log_file:
    log_content = [line.strip() for line in log_file.readlines()]

regex_match = all(
    re.match(expected_line, actual_line)
    for expected_line, actual_line in zip(expected_txt, log_content)
)

print(f"Txt test result: {regex_match}")
