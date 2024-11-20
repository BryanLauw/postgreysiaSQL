import re

class UpdateSetDFA:
    def __init__(self, table_names, table_attributes):
        self.table_names = table_names
        self.table_attributes = table_attributes
        self.pattern = r"^\s*UPDATE\s+(\w+)\s+SET\s+(.*?)\s*$"

    def is_valid(self, query):
        match = re.match(self.pattern, query, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            attr_name = match.group(2)
            if table_name in self.table_names:
                try:
                    self._are_attributes_valid(table_name, attr_name)
                    return True
                except Exception as e:
                    raise e
            else:
                raise Exception(f"Table '{table_name}' does not exist")
        else:
            raise Exception("Invalid UPDATE query")

    def _are_attributes_valid(self, table_name, set_clause):
        attributes = self.table_attributes.get(table_name, [])
        set_parts = set_clause.split(',')
        for part in set_parts:
            attr = part.split('=')[0].strip()
            if attr not in attributes:
                raise Exception(f"Attribute '{attr}' does not exist in table '{table_name}'")
        return True

# * Cara pakenya
# table_names = ['users', 'orders']
# table_attributes = {
#     'users': ['id', 'name', 'email'],
#     'orders': ['id', 'user_id', 'amount']
# }
# test = Query("UPDATE orders SET user_id = 'test' WHERE email = 1")
# updateDFA = UpdateDFA(table_names, table_attributes)

# dfa = UpdateSetDFA(table_names, table_attributes)
# try:
#     if dfa.is_valid(self.query):
#         print ("Update query is valid")
#         return True
# except Exception as e:
#     print(f"Error: {e}")
#     return False
