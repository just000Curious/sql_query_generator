from pypika import Query, Table

class QueryGenerator:

    def __init__(self, table_name):
        self.table = Table(table_name)
        self.query = Query.from_(self.table)

    def select(self, columns):
        fields = [getattr(self.table, col) for col in columns]
        self.query = self.query.select(*fields)
        return self

    def where(self, column, operator, value):

        field = getattr(self.table, column)

        if operator == "=":
            condition = field == value
        elif operator == ">":
            condition = field > value
        elif operator == "<":
            condition = field < value
        else:
            raise ValueError("Unsupported operator")

        self.query = self.query.where(condition)
        return self

    def group_by(self, columns):
        fields = [getattr(self.table, col) for col in columns]
        self.query = self.query.groupby(*fields)
        return self

    def having(self, column, operator, value):

        field = getattr(self.table, column)

        if operator == ">":
            condition = field > value
        elif operator == "<":
            condition = field < value
        elif operator == "=":
            condition = field == value
        else:
            raise ValueError("Unsupported operator")

        self.query = self.query.having(condition)
        return self

    def limit(self, number):
        self.query = self.query.limit(number)
        return self

    def build(self):
        return str(self.query)
