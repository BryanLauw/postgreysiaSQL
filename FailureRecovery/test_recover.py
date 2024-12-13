import re
from failure_recovery import FailureRecovery

filename = "unit_test_recover.txt"
f = open(filename, 'w')
f.close()

failure_recovery = FailureRecovery(fname=filename, buffer_size=5)

test_data = [
    {"transaction_id": 1, "event": "START"},
    {"transaction_id": 2, "event": "START"},
    {"transaction_id": 3, "event": "START"},
    {"transaction_id": 1, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key": "db1_kolom1_pk_1"}, "old_value": "1", "new_value": "2"},
    {"transaction_id": 1, "event": "COMMIT"},
    {"transaction_id": 3, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key": "db1_kolom1_pk_2"}, "old_value": "2", "new_value": "3"},
]

expected_redo = [
    {"transaction_id": 3, "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key": "db1_kolom1_pk_2"}, "new_value": "3"},
]

expected_undo = [
    {"transaction_id": 3, "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key": "db1_kolom1_pk_2"}, "old_value": "2"},
]

expected_txt = [
    r".*,1,START,,,",
    r".*,2,START,,,",
    r".*,3,START,,,",
    r".*,1,DATA,\{'nama_db': 'test_db_1', 'nama_kolom': 'db1_kolom1', 'primary_key': 'db1_kolom1_pk_1'\},1,2",
    r".*,1,COMMIT,,,",
    r".*,CHECKPOINT,\{2, 3\}",
    r".*,3,DATA,\{'nama_db': 'test_db_1', 'nama_kolom': 'db1_kolom1', 'primary_key': 'db1_kolom1_pk_2'\},2,3",
    r".*,CHECKPOINT,\{2, 3\}",
    r".*,3,DATA,\{'nama_db': 'test_db_1', 'nama_kolom': 'db1_kolom1', 'primary_key': 'db1_kolom1_pk_2'\},2,",
    r".*,CHECKPOINT,\{2, 3\}",
]

for data in test_data:
    failure_recovery.write_log_entry(**data)

recover_res = failure_recovery.recover()

print(f"Redo test result: {recover_res['redo'] == expected_redo}")
print(f"Undo test result: {recover_res['undo'] == expected_undo}")

with open(filename, "r") as log_file:
    log_content = [line.strip() for line in log_file.readlines()]

regex_match = all(
    re.match(expected_line, actual_line)
    for expected_line, actual_line in zip(expected_txt, log_content)
)

print(f"Txt test result: {regex_match}")
