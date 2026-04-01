from pypika import Query, Table, Field, Order, functions as fn
from pypika.terms import Criterion, Term
from typing import List, Dict, Optional, Union, Any
import pandas as pd
import re


class RawExpression(Term):
    """
    Allows raw SQL expressions to be used in queries without escaping
    """

    def __init__(self, expression: str, alias: str = None):
        super().__init__(alias)
        self.expression = expression

    def get_sql(self, **kwargs) -> str:
        sql = self.expression
        if self.alias:
            sql += f" AS {self.alias}"
        return sql


class QueryGenerator:
    """
    Core Query Builder Module - Constructs base SQL query structure
    """

    def __init__(self, table_name: str, schema_name: Optional[str] = None, alias: Optional[str] = None):
        """
        Initialize query generator for a specific table
        """
        self.table_name = table_name
        self.schema_name = schema_name

        # Create table reference
        if schema_name:
            full_name = f"{schema_name}.{table_name}"
            self.table = Table(full_name)
        else:
            self.table = Table(table_name)

        if alias:
            self.table = self.table.as_(alias)
            self.alias = alias
        else:
            self.alias = table_name

        # Initialize query
        self.query = Query.from_(self.table)

        # Store query components for metadata
        self.selected_columns = []
        self.conditions = []
        self.group_by_cols = []
        self.having_conditions = []
        self.order_by_cols = []
        self.limit_val = None
        self.offset_val = None
        self.is_distinct = False

    def _get_field(self, column: str) -> Field:
        """Get field with proper table reference - FIX: avoid double prefix"""
        if '.' in column:
            parts = column.split('.')
            if len(parts) == 2:
                table_alias, col_name = parts
                # If the table_alias matches our alias, use the table reference
                if table_alias == self.alias:
                    return getattr(self.table, col_name)
                else:
                    # Create a table reference for this alias
                    temp_table = Table(self.table_name).as_(table_alias)
                    return getattr(temp_table, col_name)
        # No dot or just column name
        return getattr(self.table, column)

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
                field = self._get_field(col)
                fields.append(field)

        self.query = self.query.select(*fields)
        self.selected_columns = columns
        return self

    def select_all(self):
        """Select all columns from the table"""
        self.query = self.query.select('*')
        self.selected_columns = ['*']
        return self

    def select_with_alias(self, column: str, alias: str):
        """Select column with alias"""
        field = self._get_field(column).as_(alias)
        self.query = self.query.select(field)
        self.selected_columns.append(f"{column} AS {alias}")
        return self

    def select_raw(self, expression: str, alias: str = None) -> 'QueryGenerator':
        """Add a raw SQL expression to SELECT clause"""
        raw = RawExpression(expression, alias)
        self.query = self.query.select(raw)

        if alias:
            self.selected_columns.append(f"{expression} AS {alias}")
        else:
            self.selected_columns.append(expression)

        return self

    # -------------------------
    # AGGREGATE FUNCTIONS
    # -------------------------

    def count(self, column: str = '*', alias: str = 'count'):
        """Add COUNT aggregate"""
        if column == '*':
            field = fn.Count('*').as_(alias)
        else:
            field = fn.Count(self._get_field(column)).as_(alias)
        self.query = self.query.select(field)
        self.selected_columns.append(f"COUNT({column}) AS {alias}")
        return self

    def sum(self, column: str, alias: str = 'sum'):
        """Add SUM aggregate"""
        field = fn.Sum(self._get_field(column)).as_(alias)
        self.query = self.query.select(field)
        self.selected_columns.append(f"SUM({column}) AS {alias}")
        return self

    def avg(self, column: str, alias: str = 'avg'):
        """Add AVG aggregate"""
        field = fn.Avg(self._get_field(column)).as_(alias)
        self.query = self.query.select(field)
        self.selected_columns.append(f"AVG({column}) AS {alias}")
        return self

    def min(self, column: str, alias: str = 'min'):
        """Add MIN aggregate"""
        field = fn.Min(self._get_field(column)).as_(alias)
        self.query = self.query.select(field)
        self.selected_columns.append(f"MIN({column}) AS {alias}")
        return self

    def max(self, column: str, alias: str = 'max'):
        """Add MAX aggregate"""
        field = fn.Max(self._get_field(column)).as_(alias)
        self.query = self.query.select(field)
        self.selected_columns.append(f"MAX({column}) AS {alias}")
        return self

    # -------------------------
    # WHERE CONDITIONS
    # -------------------------

    def where(self, column: str, operator: str, value: Any):
        """
        Add WHERE condition
        IMPORTANT: Pass raw values (without quotes). The builder will handle quoting.
        """
        field = self._get_field(column)

        # Pass the raw value to _build_condition - it will handle quoting
        condition = self._build_condition(field, operator, value)
        self.query = self.query.where(condition)
        self.conditions.append((column, operator, value))
        return self

    def where_raw(self, condition: str) -> 'QueryGenerator':
        """Add a raw SQL condition to WHERE clause"""
        self.conditions.append(('raw', 'RAW', condition))
        self.query = self.query.where(condition)
        return self

    def _build_condition(self, field, operator: str, value: Any) -> Criterion:
        """
        Build a condition based on operator
        Handles proper quoting of values
        """
        op_upper = operator.upper()

        # For IN and NOT IN with lists
        if op_upper in ['IN', 'NOT IN'] and isinstance(value, (list, tuple)):
            # Format each value in the list
            formatted_values = []
            for v in value:
                formatted_values.append(self._format_value(v))

            if op_upper == 'IN':
                return field.isin(formatted_values)
            else:
                return field.notin(formatted_values)

        # For single values
        formatted_value = self._format_value(value)

        if op_upper == '=':
            return field == formatted_value
        elif op_upper == '!=' or op_upper == '<>':
            return field != formatted_value
        elif op_upper == '>':
            return field > formatted_value
        elif op_upper == '>=':
            return field >= formatted_value
        elif op_upper == '<':
            return field < formatted_value
        elif op_upper == '<=':
            return field <= formatted_value
        elif op_upper == 'LIKE':
            return field.like(formatted_value)
        elif op_upper == 'NOT LIKE':
            return field.not_like(formatted_value)
        elif op_upper == 'IS NULL':
            return field.isnull()
        elif op_upper == 'IS NOT NULL':
            return field.notnull()
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def _format_value(self, value: Any) -> Any:
        """
        Format a value for SQL query
        - Strings get quoted with single quotes
        - Numbers remain unquoted
        - None becomes NULL
        """
        if value is None:
            return 'NULL'

        if isinstance(value, str):
            # Remove any existing quotes first
            cleaned = value.strip("'").strip('"')

            # Check if it's a number (numeric string)
            try:
                # Try to convert to float - if successful, it's numeric
                float(cleaned)
                # It's a numeric string - return as number (no quotes)
                return cleaned
            except ValueError:
                # It's a text string - add quotes
                # Also escape single quotes inside the string
                escaped = cleaned.replace("'", "''")
                return f"'{escaped}'"

        if isinstance(value, (int, float)):
            return value

        if isinstance(value, (datetime, date)):
            return f"'{value.strftime('%Y-%m-%d')}'"

        # For any other type, convert to string and quote
        return f"'{str(value)}'"

    def where_between(self, column: str, start: Any, end: Any):
        """Add BETWEEN condition"""
        field = self._get_field(column)
        formatted_start = self._format_value(start)
        formatted_end = self._format_value(end)
        condition = field.between(formatted_start, formatted_end)
        self.query = self.query.where(condition)
        self.conditions.append((column, 'BETWEEN', (start, end)))
        return self

    # -------------------------
    # GROUP BY / ORDER BY / LIMIT
    # -------------------------

    def group_by(self, columns: Union[str, List[str]]):
        """Add GROUP BY clause"""
        if isinstance(columns, str):
            columns = [columns]

        fields = []
        for col in columns:
            # Check if column contains table alias
            if '.' in col:
                # Extract the column name (strip table alias)
                col_name = col.split('.')[-1] if '.' in col else col
                fields.append(self._get_field(col_name))
            else:
                fields.append(self._get_field(col))

        self.query = self.query.groupby(*fields)
        self.group_by_cols = columns
        return self

    def order_by(self, column: str, direction: str = 'ASC'):
        """Add ORDER BY clause"""
        field = self._get_field(column)

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

    def build(self) -> str:
        """Build and return the SQL query string"""
        return str(self.query)

    def get_metadata(self) -> Dict:
        """Get query metadata for debugging and validation"""
        return {
            'table': self.table_name,
            'schema': self.schema_name,
            'alias': self.alias,
            'selected_columns': self.selected_columns,
            'conditions': self.conditions,
            'group_by': self.group_by_cols,
            'order_by': self.order_by_cols,
            'limit': self.limit_val,
            'offset': self.offset_val,
            'is_distinct': self.is_distinct
        }


# Add import for datetime at the top of the file
from datetime import datetime, date