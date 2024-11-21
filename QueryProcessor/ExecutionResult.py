import datetime
from typing import Union

from QueryProcessor.Rows import Rows

class ExecutionResult:
    def __init__(self, transaction_id: int, timestamp: datetime, message: str, data: Union[Rows, int], query: str):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.message = message
        self.data = data
        self.query = query