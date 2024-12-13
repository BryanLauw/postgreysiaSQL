import unittest
from unittest.mock import MagicMock
from failure_recovery_log_entry import LogEntry
from failure_recovery_recover_criteria import RecoverCriteria
from failure_recovery import FailureRecovery

class TestFailureRecovery(unittest.TestCase):

    def setUp(self):
        f = open('test_db_1.txt', 'w')
        f.close()

        self.failure_recovery = FailureRecovery("test_db_1.txt", buffer_size=5)

        self.failure_recovery.recovery.rollback = MagicMock(return_value="Rollback Successful")
        self.failure_recovery.recovery.redo = MagicMock(return_value="Redo Instructions")
        self.failure_recovery.recovery.undo = MagicMock(return_value="Undo Instructions")

    # def test_write_log_entry(self):
    
    #     test_data = [
    #         {"transaction_id": 1, "event": "START"},
    #         {"transaction_id": 2, "event": "START"},
    #         {"transaction_id": 3, "event": "START"},
    #         {"transaction_id": 1, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_1"}, "old_value": "1", "new_value": "2"},
    #         {"transaction_id": 1, "event": "COMMIT"},
    #         {"transaction_id": 3, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_2"}, "old_value": "2", "new_value": "3"},
    #         {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_3"}, "old_value": "4", "new_value": "5"},
    #         {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_3"}, "old_value": "4", "new_value": "5"},
    #         {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_3"}, "old_value": "4", "new_value": "5"},
    #         {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_3"}, "old_value": "4", "new_value": "5"},
    #         {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_3"}, "old_value": "4", "new_value": "5"},
    #         {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_3"}, "old_value": "4", "new_value": "5"},
    #         {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_3"}, "old_value": "4", "new_value": "5"},
    #         {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_3"}, "old_value": "4", "new_value": "5"},
    #         {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_3"}, "old_value": "4", "new_value": "5"},
    #         {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_3"}, "old_value": "4", "new_value": "5"},
    #         {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_3"}, "old_value": "4", "new_value": "5"},
    #     ]

    #     buffer_limit = 5 
    #     log_file_path = "test_db_1.txt"

    #     for data in test_data:
    #         self.failure_recovery.write_log_entry(**data)

    #     self.assertIn(3, self.failure_recovery.list_active_transaction)
    #     self.assertIn(2, self.failure_recovery.list_active_transaction)

    #     for i, data in enumerate(test_data):
    #         if i <= len(test_data) // buffer_limit * buffer_limit:
    #             with open(log_file_path, "r") as log_file:
    #                 flushed_entries = log_file.readlines()
    #                 self.assertTrue(any(str(data["transaction_id"]) in entry for entry in flushed_entries))
    #         else:
    #             buffer_index = i % buffer_limit
    #             entry = self.failure_recovery.buffer_log_entries[buffer_index]
    #             self.assertEqual(entry.transaction_id, data["transaction_id"])
        
    # def test_rollback(self):

    #     self.failure_recovery.buffer_log_entries == [
    #         {"transaction_id": 1, "event": "START"},
    #         {"transaction_id": 2, "event": "START"},
    #         {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_1"}, "old_value": "1", "new_value": "2"},
    #         {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_2"}, "old_value": "2", "new_value": "3"}
    #     ]

    #     log_entry = LogEntry(
    #         timestamp=datetime.now(),
    #         transaction_id=1,
    #         event="ABORT",
    #         object_value="test_object",
    #         old_value="old_value",
    #         new_value="new_value"
    #     )

    #     result = self.failure_recovery.rollback(log_entry)

    #     self.failure_recovery.recovery.rollback.assert_called_with(self.failure_recovery.buffer_log_entries, [log_entry.transaction_id])

    #     self.assertEqual(result, "Rollback Successful")

    def test_recover(self):

        test_data = [
            {"transaction_id": 1, "event": "START"},
            {"transaction_id": 2, "event": "START"},
            {"transaction_id": 3, "event": "START"},
            {"transaction_id": 1, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_1"}, "old_value": "1", "new_value": "2"},
            {"transaction_id": 1, "event": "COMMIT"},
            {"transaction_id": 3, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_2"}, "old_value": "2", "new_value": "3"},
        ]

        expected_redo = [
            {"transaction_id": 3, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_2"}, "old_value": "2", "new_value": "3"},
        ]

        expected_undo = [
            {"transaction_id": 3, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_2"}, "old_value": "", "new_value": "2"},
        ]

        # buffer_limit = 5
        for data in test_data:
            self.failure_recovery.write_log_entry(**data)

        recover_res = self.failure_recovery.recover()

        self.assertEqual(recover_res["redo"], expected_redo)
        self.assertEqual(recover_res["undo"], expected_undo)

    def tearDown(self):
        self.failure_recovery._stop()

if __name__ == "__main__":
    unittest.main()
