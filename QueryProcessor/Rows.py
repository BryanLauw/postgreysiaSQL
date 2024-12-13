from typing import List, Generic, TypeVar

T = TypeVar('T')

class Rows(Generic[T]):
    def __init__(self, data: List[T], rows_count: int, identifier: str):
        self.data = data
        self.rows_count = rows_count
        self.identifier = identifier

    def get_data(self):
        return self.data

    def __hash__(self):
        return self.identifier

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()
