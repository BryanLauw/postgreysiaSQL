from typing import Any, Dict, List, Literal, Union
from threading import Lock, Condition

type Action = Union[Literal["write"], Literal["read"]]


class Response:
    allowed: bool
    transaction_id: int
    condition: Condition


class ConcurrencyControlManager:
    last_transaction: int
    timestamp: Dict[Any, Dict[Action, int]] = {}
    mutex: Lock = Lock()
    condition: Condition = Condition()
    active_transaction: set[int]

    def __init__(self) -> None:
        self.last_transaction = 1
        self.active_transaction = set()

    def begin_transaction(self) -> int:
        with self.mutex:
            tmp = self.last_transaction
            self.active_transaction.add(tmp)
            self.last_transaction += 1
        return tmp

    def log_object(self, object: Any, transaction_id: int):
        print(object, transaction_id)

    def __get_timestamp__(self, object: Any):
        if not object in self.timestamp:
            self.timestamp[object] = {"write": 0, "read": 0}
        return self.timestamp[object]

    def __set_timestamp__(self, object: Any, timestamp: Dict[Action, int]):
        self.__get_timestamp__(object)
        self.timestamp[object] = timestamp

    def validate_object(
        self, object: Any, transaction_id: int, action: Action
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
            run_directly = False
            self.active_transaction.remove(transaction_id)
            with self.condition:
                self.condition.notify()

            if len(self.active_transaction) <= 1:
                run_directly = True
        return run_directly
