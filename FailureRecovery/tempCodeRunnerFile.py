  # def test_rollback(self):

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

    # def test_recover(self):

    #     log_entry = LogEntry(
    #         timestamp=datetime.now(),
    #         transaction_id=1,
    #         event="ABORT SYSTEM",
    #         object_value="test_object",
    #         old_value="old_value",
    #         new_value="new_value"
    #     )

    #     criteria = RecoverCriteria(transaction_id=log_entry.transaction_id)

    #     result = self.failure_recovery.recover(log_entry, criteria)

    #     self.failure_recovery.recovery.redo.assert_called_with(self.failure_recovery.buffer_log_entries)
    #     self.failure_recovery.recovery.undo.assert_called_with(self.failure_recovery.buffer_log_entries)

    #     self.assertEqual(result["redo"], "Redo Instructions")
    #     self.assertEqual(result["undo"], "Undo Instructions")
