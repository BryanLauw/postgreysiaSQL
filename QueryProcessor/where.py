import re

class Where(object):
    def __init__(self, multipleFrom: str, table_names, table_attr, debug = False):
        if (multipleFrom == ""):
            self.__regex = r"^[Ww][Hh][Ee][Rr][Ee]\s+(\(*(\w+\.\w+)\s*(=|<>|>|<|>=|<=)\s*('[^']*'|\d+(\.\d+)?)(\)*))(\s+([Aa][Nn][Dd]|[Oo][Rr])\s+(\(*(\w+\.\w+)\s*(=|<>|>|<|>=|<=)\s*('[^']*'|\d+(\.\d+)?)(\)*)))*$"
        else:
            self.__regex = r"^[Ww][Hh][Ee][Rr][Ee]\s+(\(*(\w+)\s*(=|<>|>|<|>=|<=)\s*('[^']*'|\d+(\.\d+)?)(\)*))(\s+([Aa][Nn][Dd]|[Oo][Rr])\s+(\(*(\w+)\s*(=|<>|>|<|>=|<=)\s*('[^']*'|\d+(\.\d+)?)(\)*)))*$"
        self.__table_names = table_names
        self.__table_attr = table_attr
        self.__debug = debug
        self.__multiple = multipleFrom

    def cekQuery(self, string) -> bool:
        match = re.fullmatch(self.__regex, string)
        stack = 0
        flag = True

        # Check parentheses matching
        for char in string:
            if char == '(':
                stack += 1
            elif char == ')':
                if stack == 0:
                    flag = False
                    break
                stack -= 1

        flag = flag and (stack == 0)
        if not match:
            raise Exception("Query does not match the required syntax.")
        if not flag:
            raise Exception("Unbalanced parentheses in the query.")

        if self.__multiple == "":
            table_groups = re.findall(r"((\w+\.\w+)\s*(=|<>|>|<|>=|<=)\s*('[^']*'|\d+(\.\d+)?))", string)
            comparisons = [(group[1], group[3]) for group in table_groups]  # Extract 'table.attr' and RHS value
        else:
            table_groups = re.findall(r"((\w+)\s*(=|<>|>|<|>=|<=)\s*('[^']*'|\d+(\.\d+)?))", string)
            comparisons = [(group[1], group[3]) for group in table_groups]  # Extract 'attr' and RHS value

        # Validate attributes
        for attr in comparisons:
            attr = attr[0]
            if self.__multiple == "":
                table, attr = attr.split(".")
                if table not in self.__table_names:
                    raise Exception(f"Invalid table name: {table}")
                if attr not in self.__table_attr.get(table, []):
                    raise Exception(f"Invalid attribute '{attr}' in table '{table}'.")
            else:
                attr = attr
                if attr not in self.__table_attr.get(self.__multiple, []):
                    raise Exception(f"Invalid attribute '{attr}' in table '{self.__multiple}'.")

        # Determine types of RHS values
        value_types = []
        for _, value in comparisons:
            if value.startswith("'") and value.endswith("'"):  # String type
                value_types.append('string')
            elif '.' in value:  # Float type
                value_types.append('float')
            elif value.isdigit():  # Integer type
                value_types.append('integer')
            else:  # Unknown type
                value_types.append('unknown')

        # Debugging Output
        if self.__debug:
            print("Debugging Info:")
            print(f"Matched attributes: {comparisons}")
            print(f"Comparison RHS values and types: {list(zip(comparisons, value_types))}")
            print(f"Query is valid.")

        return True
        
table_names = ['users', 'orders']
table_attributes = {
    'users': ['id', 'name', 'email'],
    'orders': ['id', 'user_id', 'amount']
}

tc1 = Where("", table_names, table_attributes, True)
tc1.cekQuery("WHERE users.id >= 10 AND orders.amount < 'abc'")

tc2 = Where("users", table_names, table_attributes, True) 
tc2.cekQuery("WHERE id >= 10 AND name = 'abc'")

# TEST
# tc1 = Where("WHERE as.attr >= 10 OR av.attr = 'abc'")
# print(tc1.cekQuery())  # Expected output: True

# tc2 = Where("WHERE as.attr < 10 OR av.attr = 'abc' AND x.at <> 'aaa'")
# print(tc2.cekQuery())  # Expected output: True

# tc3 = Where("WHERE as.attr > 10 OR av.attr = 'abc' AND x.at <= 2 OR v.vvv < 3")
# print(tc3.cekQuery())  # Expected output: True

# tc4 = Where("WHERE ((as.attr > 10 OR av.attr = 'abc') AND (x.at <= 2 OR v.vvv < 3))")
# print(tc4.cekQuery())  # Expected output: True

# tc5 = Where("WHERE (as.attr > 10 OR av.attr = 'abc') AND (x.at <= 2 OR v.vvv < 3))")
# print(tc5.cekQuery())  # Expected output: False (Unmatched parentheses)

# tc6 = Where("WHERE (as.attr > 10 OR av.attr = 'abc') AND (x.at <= 2 OR v.vvv < 3)")
# print(tc6.cekQuery())  # Expected output: True

# tc7 = Where("WHERE as.attr = 10 OR av.attr = 'abc' AND x.at <= 2 OR v.vvv < 3 AND x.abc = 1")
# print(tc7.cekQuery())  # Expected output: True

# tc8 = Where("WHERE att_r = 10 AND axr <= 3")
# print(tc8.cekQuery())  # Expected output: True