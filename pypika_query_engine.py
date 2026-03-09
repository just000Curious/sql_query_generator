from pypika import Query, Table, Field, Order, functions as fn
from pypika.terms import Criterion
from typing import List, Dict, Optional, Union, Any
import pandas as pd


class QueryGenerator:
    """
    Core Query Builder Module - Constructs base SQL query structure
    Handles SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
    Does NOT handle joins (delegated to JoinBuilder)
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

        # CTE support
        self.ctes = []
        self.cte_name = None

        # Query type flags
        self.is_distinct = False

    # -------------------------
    # SELECT OPERATIONS
    # -------------------------

    def select(self, columns: Union[str, List[str]]):
        """
        Select specific columns

        Args:
            columns: Single column name or list of column names
        """
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
        """Select all columns from the table"""
        self.query = self.query.select('*')
        self.selected_columns = ['*']
        return self

    def select_distinct(self, columns: Union[str, List[str]]):
        """Select distinct values"""
        self.is_distinct = True

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

    def select_expression(self, expression: str, alias: str):
        """
        Select a custom expression

        Args:
            expression: SQL expression string
            alias: Alias for the expression
        """
        # Note: pypika doesn't directly support raw SQL expressions
        # This is a placeholder - would need custom implementation
        self.selected_columns.append(f"{expression} AS {alias}")
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

    def count_distinct(self, column: str, alias: str = 'count_distinct'):
        """Add COUNT DISTINCT aggregate"""
        field = fn.Count(getattr(self.table, column)).distinct().as_(alias)
        self.query = self.query.select(field)
        self.selected_columns.append(f"COUNT(DISTINCT {column}) AS {alias}")
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
        """
        Add WHERE condition

        Args:
            column: Column name
            operator: Comparison operator (=, !=, >, <, >=, <=, LIKE, IN, IS NULL, etc.)
            value: Value to compare against
        """
        field = getattr(self.table, column)

        condition = self._build_condition(field, operator, value)
        self.query = self.query.where(condition)
        self.conditions.append((column, operator, value))
        return self

    def _build_condition(self, field, operator: str, value: Any) -> Criterion:
        """Build a condition based on operator"""
        op_upper = operator.upper()

        if op_upper == '=':
            return field == value
        elif op_upper == '!=' or op_upper == '<>':
            return field != value
        elif op_upper == '>':
            return field > value
        elif op_upper == '>=':
            return field >= value
        elif op_upper == '<':
            return field < value
        elif op_upper == '<=':
            return field <= value
        elif op_upper == 'LIKE':
            return field.like(value)
        elif op_upper == 'NOT LIKE':
            return field.not_like(value)
        elif op_upper == 'IN':
            if isinstance(value, (list, tuple)):
                return field.isin(value)
            else:
                return field == value
        elif op_upper == 'NOT IN':
            if isinstance(value, (list, tuple)):
                return field.notin(value)
            else:
                return field != value
        elif op_upper == 'IS NULL':
            return field.isnull()
        elif op_upper == 'IS NOT NULL':
            return field.notnull()
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def where_between(self, column: str, start: Any, end: Any):
        """Add BETWEEN condition"""
        field = getattr(self.table, column)
        condition = field.between(start, end)
        self.query = self.query.where(condition)
        self.conditions.append((column, 'BETWEEN', (start, end)))
        return self

    def where_in(self, column: str, values: List[Any]):
        """Add IN condition"""
        return self.where(column, 'IN', values)

    def where_not_in(self, column: str, values: List[Any]):
        """Add NOT IN condition"""
        return self.where(column, 'NOT IN', values)

    def where_like(self, column: str, pattern: str):
        """Add LIKE condition"""
        return self.where(column, 'LIKE', pattern)

    def where_null(self, column: str):
        """Add IS NULL condition"""
        return self.where(column, 'IS NULL', None)

    def where_not_null(self, column: str):
        """Add IS NOT NULL condition"""
        return self.where(column, 'IS NOT NULL', None)

    def where_and(self, *conditions):
        """Add multiple conditions with AND"""
        # This is a simplified version - would need to be enhanced
        for condition in conditions:
            if isinstance(condition, tuple) and len(condition) == 3:
                self.where(condition[0], condition[1], condition[2])
        return self

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
        """Add HAVING condition (for aggregates)"""
        field = getattr(self.table, column)
        condition = self._build_condition(field, operator, value)
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
        """
        Mark this query as a CTE (to be used in WITH clause)
        This doesn't modify the query but stores metadata
        """
        self.cte_name = cte_name
        return self

    def with_cte(self, cte_name: str, cte_query):
        """
        Add a CTE to this query

        Args:
            cte_name: Name of the CTE
            cte_query: QueryGenerator instance for the CTE
        """
        if isinstance(cte_query, QueryGenerator):
            self.ctes.append({
                'name': cte_name,
                'query': cte_query
            })
        return self

    # -------------------------
    # QUERY BUILDING
    # -------------------------

    def build(self) -> str:
        """Build and return the SQL query string"""
        query_str = str(self.query)

        # Add CTEs if present
        if self.ctes:
            cte_parts = []
            for cte in self.ctes:
                cte_parts.append(f"{cte['name']} AS (\n{cte['query'].build()}\n)")

            # Insert CTEs at the beginning
            cte_section = "WITH " + ",\n".join(cte_parts)
            query_str = cte_section + "\n" + query_str

        return query_str

    def get_metadata(self) -> Dict:
        """Get query metadata for debugging and validation"""
        return {
            'table': self.table_name,
            'schema': self.schema_name,
            'alias': self.alias,
            'selected_columns': self.selected_columns,
            'conditions': self.conditions,
            'group_by': self.group_by_cols,
            'having': self.having_conditions,
            'order_by': self.order_by_cols,
            'limit': self.limit_val,
            'offset': self.offset_val,
            'is_distinct': self.is_distinct,
            'ctes': [cte['name'] for cte in self.ctes],
            'cte_name': self.cte_name
        }

    def validate(self, db_info) -> List[str]:
        """
        Validate query against database schema

        Args:
            db_info: DBInfo instance with schema information

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Check if table exists
        if not db_info.table_exists(self.table_name, self.schema_name):
            errors.append(f"Table '{self.table_name}' does not exist in schema")
            return errors

        # Check columns
        for col in self.selected_columns:
            if col == '*':
                continue
            if ' AS ' in col:
                col = col.split(' AS ')[0]
            if '(' in col and ')' in col:
                # Aggregate function, extract column name
                match = re.search(r'\(([^)]+)\)', col)
                if match:
                    col = match.group(1)

            if not db_info.column_exists(self.table_name, col, self.schema_name):
                errors.append(f"Column '{col}' does not exist in table '{self.table_name}'")

        # Check GROUP BY columns
        for col in self.group_by_cols:
            if not db_info.column_exists(self.table_name, col, self.schema_name):
                errors.append(f"GROUP BY column '{col}' does not exist in table '{self.table_name}'")

        return errors