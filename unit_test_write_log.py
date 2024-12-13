from datetime import datetime
import re
import unittest
from unittest.mock import MagicMock
from FailureRecovery.failure_recovery import FailureRecovery
from FailureRecovery.failure_recovery_log_entry import LogEntry

class TestFailureRecovery(unittest.TestCase):

    def setUp(self):
        f = open('test_db_1.txt', 'w')
        f.close()

        self.failure_recovery = FailureRecovery("test_db_1.txt", buffer_size=5)

        self.failure_recovery.recovery.rollback = MagicMock(return_value="Rollback Successful")
        self.failure_recovery.recovery.redo = MagicMock(return_value="Redo Instructions")
        self.failure_recovery.recovery.undo = MagicMock(return_value="Undo Instructions")

    def test_write_log_entry(self):
    
        test_data = [
            {"transaction_id": 1, "event": "START"},
            {"transaction_id": 2, "event": "START"},
            {"transaction_id": 3, "event": "START"},
            {"transaction_id": 1, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_1", "primary_key_value": "bebas"}, "old_value": "1", "new_value": "2"},
            {"transaction_id": 2, "event": "COMMIT"},
            {"transaction_id": 1, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_1", "primary_key_value": "bebas"}, "old_value": "1", "new_value": "2"}, 
        ]

        buffer_limit = 5 
        log_file_path = "test_db_1.txt"

        for data in test_data:
            self.failure_recovery.write_log_entry(**data)

        self.assertIn(3, self.failure_recovery.list_active_transaction)
        self.assertIn(1, self.failure_recovery.list_active_transaction)

        for i, data in enumerate(test_data):
            if i <= len(test_data) // buffer_limit * buffer_limit:
                with open(log_file_path, "r") as log_file:
                    flushed_entries = log_file.readlines()
                    self.assertTrue(any(str(data["transaction_id"]) in entry for entry in flushed_entries))
            else:
                buffer_index = i % buffer_limit
                entry = self.failure_recovery.buffer_log_entries[buffer_index]
                self.assertEqual(entry.transaction_id, data["transaction_id"])
    
    def test_rollback(self):

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

        with open(filename, "r") as log_file:
            log_content = [line.strip() for line in log_file.readlines()]

        regex_match = all(
            re.match(expected_line, actual_line)
            for expected_line, actual_line in zip(expected_txt, log_content)
        )

        self.assertEqual(rollback_res, expected_undo)
        self.assertEqual(regex_match, True)

    def recover(self):
        filename = "./../unit_test_recover.txt"
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

        expected_redo = [
            {"transaction_id": 3, "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key": "db1_kolom1_pk_2", "primary_key_value": "bebas"}, "new_value": "3"},
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

        recover_res = failure_recovery.recover()

        with open(filename, "r") as log_file:
            log_content = [line.strip() for line in log_file.readlines()]

        regex_match = all(
            re.match(expected_line, actual_line)
            for expected_line, actual_line in zip(expected_txt, log_content)
        )

        self.assertEqual(recover_res["undo"], expected_undo)
        self.assertEqual(recover_res["redo"], expected_redo)
        self.assertEqual(regex_match, True)

    def tearDown(self):
        self.failure_recovery._stop()

if __name__ == "__main__":
    unittest.main()
