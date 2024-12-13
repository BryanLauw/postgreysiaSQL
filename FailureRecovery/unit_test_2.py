import unittest
from unittest.mock import MagicMock
from datetime import datetime
from failure_recovery_log_entry import LogEntry
from failure_recovery_recover_criteria import RecoverCriteria
from failure_recovery import FailureRecovery

class TestFailureRecovery(unittest.TestCase):

    def setUp(self):
        self.failure_recovery = FailureRecovery("test_db_1.log", buffer_size=10, interval=5)

        self.failure_recovery.checkpoint_manager.perform_checkpoint = MagicMock()
        self.failure_recovery.recovery.rollback = MagicMock(return_value="Rollback Successful")
        self.failure_recovery.recovery.redo = MagicMock(return_value="Redo Instructions")
        self.failure_recovery.recovery.undo = MagicMock(return_value="Undo Instructions")

    def test_write_log_entry(self):

        test_data = [
            {"transaction_id": 1, "event": "START"},
            {"transaction_id": 2, "event": "START"},
            {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_1"}, "old_value": "1", "new_value": "2"},
            {"transaction_id": 2, "event": "DATA", "object_value": {"nama_db": "test_db_1", "nama_kolom": "db1_kolom1", "primary_key":"db1_kolom1_pk_2"}, "old_value": "2", "new_value": "3"}
        ]

        for data in test_data:
            self.failure_recovery.write_log_entry(
                **data
            )

        self.assertEqual(len(self.failure_recovery.buffer_log_entries), len(test_data))
        for i, data in enumerate(test_data):
            self.assertEqual(self.failure_recovery.buffer_log_entries[i].transaction_id, data["transaction_id"])
            self.assertEqual(self.failure_recovery.buffer_log_entries[i].event, data["event"])

        self.assertIn(1, self.failure_recovery.list_active_transaction)
        self.assertIn(2, self.failure_recovery.list_active_transaction)

    def test_rollback(self):

        log_entry = LogEntry(
            timestamp=datetime.now(),
            transaction_id=1,
            event="ABORT",
            object_value="test_object",
            old_value="old_value",
            new_value="new_value"
        )

        result = self.failure_recovery.rollback(log_entry)

        self.failure_recovery.recovery.rollback.assert_called_with(self.failure_recovery.buffer_log_entries, [log_entry.transaction_id])

        self.assertEqual(result, "Rollback Successful")

    def test_recover(self):

        log_entry = LogEntry(
            timestamp=datetime.now(),
            transaction_id=1,
            event="ABORT SYSTEM",
            object_value="test_object",
            old_value="old_value",
            new_value="new_value"
        )

        criteria = RecoverCriteria(transaction_id=log_entry.transaction_id)

        result = self.failure_recovery.recover(log_entry, criteria)

        self.failure_recovery.recovery.redo.assert_called_with(self.failure_recovery.buffer_log_entries)
        self.failure_recovery.recovery.undo.assert_called_with(self.failure_recovery.buffer_log_entries)

        self.assertEqual(result["redo"], "Redo Instructions")
        self.assertEqual(result["undo"], "Undo Instructions")

    def tearDown(self):
        self.failure_recovery._stop()

if __name__ == "__main__":
    unittest.main()
