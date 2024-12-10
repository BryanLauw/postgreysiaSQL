from typing import List

def filterSelect(data: List[dict], select: list[str]) -> List[dict]:
    # filter select
    column = [col.split(".")[1] for col in select]
    return [{key: value for key, value in row.items() if key in column} for row in data]

data = [
    {"id": 1, "name": "Alice", "age": 24},
    {"id": 2, "name": "Bob", "age": 25},
    {"id": 3, "name": "Charlie", "age": 26}
]

select = ["uid.id", "uid.name"]
print(filterSelect(data, select))