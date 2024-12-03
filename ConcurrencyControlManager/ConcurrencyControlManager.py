from typing import Any, Dict, List, Literal, Union
from threading import Lock, Condition

type Action = Union[Literal["write"], Literal["read"]]


class Row:
    data: List[Any]
    rows_count: int


class Response:
    allowed: bool
    transaction_id: int
    condition: Condition


class ConcurrencyControlManager:
    last_transaction: int
    timestamp: Dict[Row, Dict[Action, int]] = {}
    mutex: Lock = Lock()
    condition: Condition = Condition()

    def __init__(self) -> None:
        self.last_transaction = 0

    def begin_transaction(self) -> int:
        with self.mutex:
            tmp = self.last_transaction
            self.last_transaction += 1
            return tmp

    def log_object(self, object: Row, transaction_id: int):
        print(object, transaction_id)

    def __get_timestamp__(self, object: Row):
        if not object in self.timestamp:
            self.timestamp[object] = {"write": 0, "read": 0}
        return self.timestamp[object]

    def __set_timestamp__(self, object: Row, timestamp: Dict[Action, int]):
        self.__get_timestamp__(object)
        self.timestamp[object] = timestamp

    def validate_object(
        self, object: Row, transaction_id: int, action: Action
    ) -> Response:
        with self.mutex:
            result = Response()
            result.allowed = True
            result.transaction_id = transaction_id
            result.condition = self.condition
            current_timestamp = transaction_id
            timestamp = self.__get_timestamp__(object)

            if action == "write":
                max_timestamp = max(timestamp["read"], timestamp["write"])
                if current_timestamp < max_timestamp:
                    result.allowed = False
                timestamp["write"] = current_timestamp
            elif action == "read":
                if timestamp["write"] > current_timestamp:
                    result.allowed = False
                timestamp["read"] = current_timestamp

            return result

    def end_transaction(self, transaction_id: int):
        with self.mutex:
            with self.condition:
                self.condition.notify_all()
                transaction_id
