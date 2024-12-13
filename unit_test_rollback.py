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
    {"transaction_id": 4, "event": "START"},
    {"transaction_id": 1, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key": "db1_kolom1_pk_1", "primary_key_value": "value_1"}, "old_value": "1", "new_value": "2"},
    {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_2", "nama_kolom": "db2_kolom1", "primary_key": "db2_kolom1_pk_1", "primary_key_value": "value_2"}, "old_value": "3", "new_value": "4"},
    {"transaction_id": 3, "event": "DATA", "object_value": {"nama_db": "test_db_3", "nama_kolom": "db3_kolom1", "primary_key": "db3_kolom1_pk_1", "primary_key_value": "value_3"}, "old_value": "5", "new_value": "6"},
    {"transaction_id": 4, "event": "DATA", "object_value": {"nama_db": "test_db_4", "nama_kolom": "db4_kolom1", "primary_key": "db4_kolom1_pk_1", "primary_key_value": "value_4"}, "old_value": "7", "new_value": "8"},
    {"transaction_id": 1, "event": "COMMIT"},
    {"transaction_id": 3, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom2", "primary_key": "db1_kolom2_pk_2", "primary_key_value": "value_5"}, "old_value": "9", "new_value": "10"},
    {"transaction_id": 4, "event": "DATA", "object_value": {"nama_db": "test_db_4", "nama_kolom": "db4_kolom2", "primary_key": "db4_kolom2_pk_2", "primary_key_value": "value_6"}, "old_value": "11", "new_value": "12"},
    {"transaction_id": 4, "event": "DATA", "object_value": {"nama_db": "test_db_3", "nama_kolom": "db3_kolom2", "primary_key": "db3_kolom2_pk_3", "primary_key_value": "value_7"}, "old_value": "13", "new_value": "14"},
    {"transaction_id": 1, "event": "START"},
    {"transaction_id": 5, "event": "START"},
    {"transaction_id": 5, "event": "DATA", "object_value": {"nama_db": "test_db_5", "nama_kolom": "db5_kolom1", "primary_key": "db5_kolom1_pk_1", "primary_key_value": "value_8"}, "old_value": "15", "new_value": "16"},
    {"transaction_id": 5, "event": "DATA", "object_value": {"nama_db": "test_db_5", "nama_kolom": "db5_kolom2", "primary_key": "db5_kolom2_pk_2", "primary_key_value": "value_9"}, "old_value": "17", "new_value": "18"},
    {"transaction_id": 1, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom3", "primary_key": "db1_kolom3_pk_3", "primary_key_value": "value_10"}, "old_value": "19", "new_value": "20"},
    {"transaction_id": 5, "event": "DATA", "object_value": {"nama_db": "test_db_5", "nama_kolom": "db5_kolom3", "primary_key": "db5_kolom3_pk_3", "primary_key_value": "value_11"}, "old_value": "21", "new_value": "22"},
    {"transaction_id": 5, "event": "COMMIT"},
    {"transaction_id": 6, "event": "START"},
    {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_6", "nama_kolom": "db6_kolom1", "primary_key": "db6_kolom1_pk_1", "primary_key_value": "value_12"}, "old_value": "23", "new_value": "24"},
    {"transaction_id": 6, "event": "COMMIT"}
]


expected_undo = [
    {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_6", "nama_kolom": "db6_kolom1", "primary_key": "db6_kolom1_pk_1", "primary_key_value": "value_12"}, "old_value": "23"},
    {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_2", "nama_kolom": "db2_kolom1", "primary_key": "db2_kolom1_pk_1", "primary_key_value": "value_2"}, "old_value": "3"},
]

# expected_txt = [
#     r".*,1,START,,,",
#     r".*,2,START,,,",
#     r".*,3,START,,,",
#     r".*,1,DATA,\{'nama_db': 'test_db_1', 'nama_kolom': 'db1_kolom1', 'primary_key': 'db1_kolom1_pk_1', 'primary_key_value': 'bebas'\},1,2",
#     r".*,1,COMMIT,,,",
#     r".*,CHECKPOINT,\{2, 3\}",
#     r".*,3,DATA,\{'nama_db': 'test_db_1', 'nama_kolom': 'db1_kolom1', 'primary_key': 'db1_kolom1_pk_2', 'primary_key_value': 'bebas'\},2,3",
#     r".*,CHECKPOINT,\{2, 3\}",
#     r".*,3,DATA,\{'nama_db': 'test_db_1', 'nama_kolom': 'db1_kolom1', 'primary_key': 'db1_kolom1_pk_2', 'primary_key_value': 'bebas'\},2,",
#     r".*,CHECKPOINT,\{2, 3\}",
# ]

for data in test_data:
    failure_recovery.write_log_entry(**data)

log_entry_rollback = LogEntry(datetime.now(), 2, "ABORT") 

rollback_res = failure_recovery.rollback(log_entry=log_entry_rollback)

# print("[--------------]")
for i in rollback_res:
    print(i)
for r in expected_undo:
    print(r)
# print(rollback_res)
# print(expected_undo)

print(f"Undo test result: {rollback_res == expected_undo}")

# with open(filename, "r") as log_file:
#     log_content = [line.strip() for line in log_file.readlines()]

# regex_match = all(
#     re.match(expected_line, actual_line)
#     for expected_line, actual_line in zip(expected_txt, log_content)
# )

# print(f"Txt test result: {regex_match}")
