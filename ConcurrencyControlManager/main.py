from typing import Any, Dict, List, Literal, Union

type Action = Union[Literal["write"], Literal["read"]]


class Row:
    data: List[Any]
    rows_count: int


class Response:
    allowed: bool
    transaction_id: int


class ConcurrencyControlManager:
    last_transaction: int
    timestamp: Dict[Row, Dict[Action, int]] = {}

    def __init__(self) -> None:
        self.last_transaction = 0

    def begin_transaction(self) -> int:
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
        result = Response()
        result.transaction_id = transaction_id
        current_timestamp = transaction_id
        timestamp = self.__get_timestamp__(object)

        if action == "write":
            max_timestamp = max(timestamp["read"], timestamp["write"])
            if current_timestamp < max_timestamp:
                result = False
                return result
            timestamp["write"] = current_timestamp
        elif action == "read":
            if timestamp["write"] > current_timestamp:
                result = False
                return result
            timestamp["read"] = current_timestamp

        result.allowed = True
        return result

    def end_transaction(self, transaction_id: int):
        transaction_id
