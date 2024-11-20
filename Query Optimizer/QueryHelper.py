class QueryHelper:

    @staticmethod
    def remove_excessive_whitespace(query: str):
        words = query.split()
        new_query = " ".join(words)
        return new_query

    @staticmethod
    def normalize_string(query: str):
        return query.replace("\t", "").replace("\n", "").replace("\r", "")

    @staticmethod
    def extract_SELECT(values: str):
        arr_attributes = [value.strip() for value in values.split(",")]
        return arr_attributes

    @staticmethod
    def extract_FROM(values: str):
        arr_joins = []
        values_parsed = values.split()
        len_parsed = len(values_parsed)
        met_join = False
        element = ""
        i = 0
        while i < len_parsed:
            if i + 1 < len_parsed and values_parsed[i] == "NATURAL" and values_parsed[i + 1] == "JOIN":
                if not met_join:
                    met_join = True
                    element += " NATURAL JOIN"
                else:
                    arr_joins.append(element.strip())
                    element = " NATURAL JOIN "
                i += 2
                continue
            if values_parsed[i] == "JOIN":
                if not met_join:
                    met_join = True
                else:
                    arr_joins.append(element.strip())
                    element = ""
            element += " " + values_parsed[i]
            i += 1
        if element:
            arr_joins.append(element.strip())
        return arr_joins

    @staticmethod
    def extract_WHERE(values: str):
        return values.replace(" ", "")

    @staticmethod
    def extract_ORDERBY(values: str):
        return values.strip()

    @staticmethod
    def extract_LIMIT(values: str):
        return int(values)

    @staticmethod
    def extract_UPDATE(values: str):
        return values.strip()

    @staticmethod
    def extract_SET(values: str):
        return [value.strip() for value in values.split(",")]

    @staticmethod
    def extract_value(query: str, before: str, after: str):
        start = query.find(before) + len(before)
        if after == "":
            end = len(query)
        else:
            end = query.find(after)
        extracted = query[start:end]
        if before == "SELECT":
            extracted = QueryHelper.extract_SELECT(extracted)
        elif before == "FROM":
            extracted = QueryHelper.extract_FROM(extracted)
        elif before == "WHERE":
            extracted = QueryHelper.extract_WHERE(extracted)
        elif before == "ORDER BY":
            extracted = QueryHelper.extract_ORDERBY(extracted)
        elif before == "LIMIT":
            extracted = QueryHelper.extract_LIMIT(extracted)
        elif before == "UPDATE":
            extracted = QueryHelper.extract_UPDATE(extracted)
        elif before == "SET":
            extracted = QueryHelper.extract_SET(extracted)
        return extracted
