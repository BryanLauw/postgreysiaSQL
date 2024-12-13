from datetime import datetime
import re
import unittest
from FailureRecovery.failure_recovery import FailureRecovery
from FailureRecovery.failure_recovery_log_entry import LogEntry

class TestFailureRecovery(unittest.TestCase):

    def setUp(self):

        self.filename = "test_db_2.txt"
        self.buffer_limit = 5 

        f = open(self.filename, 'w')
        f.close()

        self.failure_recovery = FailureRecovery(self.filename, buffer_size=self.buffer_limit)

    def test_write_log_entry(self):
    
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
            {"transaction_id": 3, "event": "COMMIT"},
            {"transaction_id": 4, "event": "DATA", "object_value": {"nama_db": "test_db_3", "nama_kolom": "db3_kolom2", "primary_key": "db3_kolom2_pk_3", "primary_key_value": "value_7"}, "old_value": "13", "new_value": "14"},
            {"transaction_id": 1, "event": "START"},
            {"transaction_id": 5, "event": "START"},
            {"transaction_id": 5, "event": "DATA", "object_value": {"nama_db": "test_db_5", "nama_kolom": "db5_kolom1", "primary_key": "db5_kolom1_pk_1", "primary_key_value": "value_8"}, "old_value": "15", "new_value": "16"},
            {"transaction_id": 5, "event": "DATA", "object_value": {"nama_db": "test_db_5", "nama_kolom": "db5_kolom2", "primary_key": "db5_kolom2_pk_2", "primary_key_value": "value_9"}, "old_value": "17", "new_value": "18"},
            {"transaction_id": 1, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom3", "primary_key": "db1_kolom3_pk_3", "primary_key_value": "value_10"}, "old_value": "19", "new_value": "20"},
            {"transaction_id": 5, "event": "DATA", "object_value": {"nama_db": "test_db_5", "nama_kolom": "db5_kolom3", "primary_key": "db5_kolom3_pk_3", "primary_key_value": "value_11"}, "old_value": "21", "new_value": "22"},
            {"transaction_id": 5, "event": "COMMIT"},
            {"transaction_id": 6, "event": "START"},
            {"transaction_id": 6, "event": "DATA", "object_value": {"nama_db": "test_db_6", "nama_kolom": "db6_kolom1", "primary_key": "db6_kolom1_pk_1", "primary_key_value": "value_12"}, "old_value": "23", "new_value": "24"},
            {"transaction_id": 6, "event": "COMMIT"}
        ]

        for data in test_data:
            self.failure_recovery.write_log_entry(**data)

        self.assertIn(2, self.failure_recovery.list_active_transaction)
        self.assertIn(4, self.failure_recovery.list_active_transaction)

        for i, data in enumerate(test_data):
            if i <= len(test_data) // self.buffer_limit * self.buffer_limit:
                with open(self.filename, "r") as log_file:
                    flushed_entries = log_file.readlines()
                    self.assertTrue(any(str(data["transaction_id"]) in entry for entry in flushed_entries))
            else:
                buffer_index = i % self.buffer_limit
                entry = self.failure_recovery.buffer_log_entries[buffer_index]
                self.assertEqual(entry.transaction_id, data["transaction_id"])
    
    def test_rollback(self):

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
            {"transaction_id": 2, "object_value": {"nama_db": "test_db_6", "nama_kolom": "db6_kolom1", "primary_key": "db6_kolom1_pk_1", "primary_key_value": "value_12"}, "old_value": "23"},
            {"transaction_id": 2, "object_value": {"nama_db": "test_db_2", "nama_kolom": "db2_kolom1", "primary_key": "db2_kolom1_pk_1", "primary_key_value": "value_2"}, "old_value": "3"},
        ]

        for data in test_data:
            self.failure_recovery.write_log_entry(**data)

        log_entry_rollback = LogEntry(datetime.now(), 2, "ABORT") 

        rollback_res = self.failure_recovery.rollback(log_entry=log_entry_rollback)

        self.assertEqual(rollback_res, expected_undo)


    def tearDown(self):
        self.failure_recovery._stop()

if __name__ == "__main__":
    unittest.main()
