"""
fixed_generator.py - Fixed Query Generator with proper string handling
"""

from typing import List, Dict, Optional, Any


class FixedQueryGenerator:
    """
    Fixed Query Generator - Correctly handles value formatting
    """

    def __init__(self, table_name: str, schema_name: Optional[str] = None, alias: Optional[str] = None):
        self.table_name = table_name
        self.schema_name = schema_name
        self.alias = alias or table_name

        # Build table reference
        if schema_name:
            self.table_ref = f"{schema_name}.{table_name}"
        else:
            self.table_ref = table_name

        if alias:
            self.table_ref += f" AS {alias}"

        # Query components
        self.selected_columns = []
        self.where_conditions = []
        self.group_by_cols = []
        self.order_by_cols = []
        self.limit_val = None
        self.offset_val = None

    def _format_value(self, value: Any) -> str:
        """
        Properly format values for SQL - THIS IS THE KEY FIX
        """
        if value is None:
            return "NULL"

        if isinstance(value, str):
            # Remove any existing quotes
            cleaned = value.strip("'").strip('"')

            # Check if it's a number
            try:
                float(cleaned)
                # It's numeric - return without quotes
                return cleaned
            except ValueError:
                # It's text - return with quotes (escape internal quotes)
                escaped = cleaned.replace("'", "''")
                return f"'{escaped}'"

        if isinstance(value, (int, float)):
            return str(value)

        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"

        # Default: convert to string and quote
        return f"'{str(value)}'"

    def select(self, columns: List[str]):
        """Select specific columns"""
        if isinstance(columns, str):
            self.selected_columns = [columns]
        else:
            self.selected_columns = columns
        return self

    def select_all(self):
        """Select all columns"""
        self.selected_columns = ["*"]
        return self

    def select_with_alias(self, column: str, alias: str):
        """Select column with alias"""
        self.selected_columns.append(f"{column} AS {alias}")
        return self

    def where(self, column: str, operator: str, value: Any):
        """Add WHERE condition"""
        # Format the value properly
        formatted_value = self._format_value(value)
        self.where_conditions.append({
            'column': column,
            'operator': operator,
            'value': formatted_value
        })
        return self

    def where_between(self, column: str, start: Any, end: Any):
        """Add BETWEEN condition"""
        formatted_start = self._format_value(start)
        formatted_end = self._format_value(end)
        self.where_conditions.append({
            'column': column,
            'operator': 'BETWEEN',
            'value': f"{formatted_start} AND {formatted_end}"
        })
        return self

    def group_by(self, columns: List[str]):
        """Add GROUP BY clause"""
        if isinstance(columns, str):
            self.group_by_cols = [columns]
        else:
            self.group_by_cols = columns
        return self

    def order_by(self, column: str, direction: str = 'ASC'):
        """Add ORDER BY clause"""
        self.order_by_cols.append({'column': column, 'direction': direction})
        return self

    def limit(self, number: int, offset: int = 0):
        """Add LIMIT and OFFSET"""
        self.limit_val = number
        self.offset_val = offset
        return self

    def build(self) -> str:
        """Build the SQL query"""
        parts = []

        # SELECT clause
        if self.selected_columns:
            select_clause = "SELECT " + ", ".join(self.selected_columns)
        else:
            select_clause = "SELECT *"
        parts.append(select_clause)

        # FROM clause
        from_clause = f"FROM {self.table_ref}"
        parts.append(from_clause)

        # WHERE clause
        if self.where_conditions:
            where_parts = []
            for cond in self.where_conditions:
                if cond['operator'].upper() == 'BETWEEN':
                    where_parts.append(f"{cond['column']} BETWEEN {cond['value']}")
                else:
                    where_parts.append(f"{cond['column']} {cond['operator']} {cond['value']}")
            parts.append("WHERE " + " AND ".join(where_parts))

        # GROUP BY clause
        if self.group_by_cols:
            parts.append("GROUP BY " + ", ".join(self.group_by_cols))

        # ORDER BY clause
        if self.order_by_cols:
            order_parts = [f"{o['column']} {o['direction']}" for o in self.order_by_cols]
            parts.append("ORDER BY " + ", ".join(order_parts))

        # LIMIT clause
        if self.limit_val:
            limit_clause = f"LIMIT {self.limit_val}"
            if self.offset_val:
                limit_clause += f" OFFSET {self.offset_val}"
            parts.append(limit_clause)

        # Fixed: Use proper newline character
        return "\n".join(parts)

    def get_metadata(self) -> Dict:
        """Get query metadata"""
        return {
            'table': self.table_name,
            'schema': self.schema_name,
            'alias': self.alias,
            'selected_columns': self.selected_columns,
            'conditions': self.where_conditions,
            'group_by': self.group_by_cols,
            'order_by': self.order_by_cols,
            'limit': self.limit_val,
            'offset': self.offset_val
        }