from pypika import Query, Table, Field, Order, functions as fn
from typing import List, Dict, Optional, Union, Any
import pandas as pd


class QueryGenerator:
    def __init__(self, table_name: str, schema_name: Optional[str] = None, alias: Optional[str] = None):
        """
        Initialize query generator for a specific table
        """
        if schema_name:
            self.table = Table(f'{schema_name}.{table_name}')
        else:
            self.table = Table(table_name)

        if alias:
            self.table = self.table.as_(alias)

        self.query = Query.from_(self.table)
        self.selected_columns = []
        self.conditions = []
        self.group_by_cols = []
        self.having_conditions = []
        self.order_by_cols = []
        self.limit_val = None
        self.offset_val = None
        self.ctes = []
        self.cte_name = None

    # -------------------------
    # SELECT OPERATIONS
    # -------------------------

    def select(self, columns: Union[str, List[str]]):
        """Select specific columns"""
        if isinstance(columns, str):
            columns = [columns]

        fields = []
        for col in columns:
            if '*' in col:
                fields.append(Field('*'))
            else:
                fields.append(getattr(self.table, col))

        self.query = self.query.select(*fields)
        self.selected_columns = columns
        return self

    def select_all(self):
        """Select all columns"""
        self.query = self.query.select('*')
        self.selected_columns = ['*']
        return self

    def select_distinct(self, columns: Union[str, List[str]]):
        """Select distinct values"""
        if isinstance(columns, str):
            columns = [columns]

        fields = [getattr(self.table, col) for col in columns]
        self.query = self.query.select(*fields).distinct()
        self.selected_columns = columns
        return self

    def select_with_alias(self, column: str, alias: str):
        """Select column with alias"""
        field = getattr(self.table, column).as_(alias)
        self.query = self.query.select(field)
        self.selected_columns.append(f"{column} AS {alias}")
        return self

    # -------------------------
    # AGGREGATE FUNCTIONS
    # -------------------------

    def count(self, column: str = '*', alias: str = 'count'):
        """Add COUNT aggregate"""
        if column == '*':
            field = fn.Count('*').as_(alias)
        else:
            field = fn.Count(getattr(self.table, column)).as_(alias)
        self.query = self.query.select(field)
        self.selected_columns.append(f"COUNT({column}) AS {alias}")
        return self

    def sum(self, column: str, alias: str = 'sum'):
        """Add SUM aggregate"""
        field = fn.Sum(getattr(self.table, column)).as_(alias)
        self.query = self.query.select(field)
        self.selected_columns.append(f"SUM({column}) AS {alias}")
        return self

    def avg(self, column: str, alias: str = 'avg'):
        """Add AVG aggregate"""
        field = fn.Avg(getattr(self.table, column)).as_(alias)
        self.query = self.query.select(field)
        self.selected_columns.append(f"AVG({column}) AS {alias}")
        return self

    def min(self, column: str, alias: str = 'min'):
        """Add MIN aggregate"""
        field = fn.Min(getattr(self.table, column)).as_(alias)
        self.query = self.query.select(field)
        self.selected_columns.append(f"MIN({column}) AS {alias}")
        return self

    def max(self, column: str, alias: str = 'max'):
        """Add MAX aggregate"""
        field = fn.Max(getattr(self.table, column)).as_(alias)
        self.query = self.query.select(field)
        self.selected_columns.append(f"MAX({column}) AS {alias}")
        return self

    # -------------------------
    # WHERE CONDITIONS
    # -------------------------

    def where(self, column: str, operator: str, value: Any):
        """Add WHERE condition"""
        field = getattr(self.table, column)

        condition_map = {
            '=': field == value,
            '!=': field != value,
            '>': field > value,
            '>=': field >= value,
            '<': field < value,
            '<=': field <= value,
            'LIKE': field.like(value),
            'NOT LIKE': field.not_like(value),
            'IN': field.isin(value) if isinstance(value, (list, tuple)) else field == value,
            'NOT IN': field.notin(value) if isinstance(value, (list, tuple)) else field != value,
            'IS NULL': field.isnull(),
            'IS NOT NULL': field.notnull()
        }

        if operator.upper() in condition_map:
            condition = condition_map[operator.upper()]
        else:
            raise ValueError(f"Unsupported operator: {operator}")

        self.query = self.query.where(condition)
        self.conditions.append((column, operator, value))
        return self

    def where_between(self, column: str, start: Any, end: Any):
        """Add BETWEEN condition"""
        field = getattr(self.table, column)
        condition = field[start:end]
        self.query = self.query.where(condition)
        self.conditions.append((column, 'BETWEEN', (start, end)))
        return self

    def where_in(self, column: str, values: List[Any]):
        """Add IN condition"""
        return self.where(column, 'IN', values)

    # -------------------------
    # GROUP BY / HAVING
    # -------------------------

    def group_by(self, columns: Union[str, List[str]]):
        """Add GROUP BY clause"""
        if isinstance(columns, str):
            columns = [columns]

        fields = [getattr(self.table, col) for col in columns]
        self.query = self.query.groupby(*fields)
        self.group_by_cols = columns
        return self

    def having(self, column: str, operator: str, value: Any):
        """Add HAVING condition"""
        field = getattr(self.table, column)

        condition_map = {
            '>': field > value,
            '>=': field >= value,
            '<': field < value,
            '<=': field <= value,
            '=': field == value,
            '!=': field != value
        }

        if operator in condition_map:
            condition = condition_map[operator]
        else:
            raise ValueError(f"Unsupported operator for HAVING: {operator}")

        self.query = self.query.having(condition)
        self.having_conditions.append((column, operator, value))
        return self

    # -------------------------
    # ORDER BY / LIMIT
    # -------------------------

    def order_by(self, column: str, direction: str = 'ASC'):
        """Add ORDER BY clause"""
        field = getattr(self.table, column)
        if direction.upper() == 'DESC':
            self.query = self.query.orderby(field, order=Order.desc)
        else:
            self.query = self.query.orderby(field)

        self.order_by_cols.append((column, direction))
        return self

    def limit(self, number: int, offset: int = 0):
        """Add LIMIT and OFFSET"""
        self.query = self.query.limit(number)
        self.limit_val = number

        if offset > 0:
            self.query = self.query.offset(offset)
            self.offset_val = offset

        return self

    # -------------------------
    # CTE OPERATIONS
    # -------------------------

    def as_cte(self, cte_name: str):
        """Convert current query to a CTE"""
        self.cte_name = cte_name
        return self

    def with_cte(self, cte_name: str, cte_query):
        """Add a CTE to the current query"""
        if isinstance(cte_query, QueryGenerator):
            cte_table = Table(cte_name)
            self.ctes.append((cte_name, cte_query.build()))
            # Reference the CTE in the main query
            self.table = cte_table
            self.query = Query.with_(cte_query.build(), cte_name).from_(cte_table)
        return self

    # -------------------------
    # QUERY BUILDING
    # -------------------------

    def build(self) -> str:
        """Build and return the SQL query string"""
        query_str = str(self.query)

        # Add CTEs if present
        if self.ctes:
            cte_str = "WITH "
            for i, (name, subquery) in enumerate(self.ctes):
                if i > 0:
                    cte_str += ", "
                cte_str += f"{name} AS (\n{subquery}\n)"
            query_str = cte_str + "\n" + query_str

        return query_str

    def get_metadata(self) -> Dict:
        """Get query metadata for debugging"""
        return {
            'selected_columns': self.selected_columns,
            'conditions': self.conditions,
            'group_by': self.group_by_cols,
            'having': self.having_conditions,
            'order_by': self.order_by_cols,
            'limit': self.limit_val,
            'offset': self.offset_val,
            'ctes': [name for name, _ in self.ctes]
        }