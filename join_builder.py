from pypika import Query, Table, Field, functions as fn
from typing import List, Dict, Optional, Union, Any
import pandas as pd

from db_information import DBInfo
from pypika_query_engine import QueryGenerator
from temporary_table import TemporaryTable


class JoinBuilder:
    """
    Join Builder Module - Constructs JOIN clauses using relationship data
    from DBInfo. Automatically detects relationships and builds join paths.
    """

    def __init__(self, db_info: DBInfo):
        """
        Initialize JoinBuilder with database schema information

        Args:
            db_info: DBInfo instance with parsed schema
        """
        self.db_info = db_info
        self.join_path = []
        self.selected_columns = []
        self.conditions = []

        # Table references with aliases
        self.table_aliases = {}

        # CTE support
        self.ctes = []

        # Temporary tables
        self.temp_tables = []

        # Query components
        self.group_by_cols = []
        self.order_by_cols = []
        self.limit_val = None
        self.offset_val = None

    # -------------------------
    # TABLE MANAGEMENT
    # -------------------------

    def add_table(self, table_name: str, schema_name: Optional[str] = None,
                  alias: Optional[str] = None) -> 'JoinBuilder':
        """
        Add a table to the join path

        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)
            alias: Table alias (optional)
        """
        # Generate alias if not provided
        if not alias:
            alias = table_name

        # Store alias mapping
        self.table_aliases[alias] = {
            'table': table_name,
            'schema': schema_name,
            'alias': alias
        }

        # Add to join path
        self.join_path.append({
            'type': 'table',
            'table': table_name,
            'schema': schema_name,
            'alias': alias
        })

        return self

    def add_join_manual(self, from_table: str, to_table: str,
                        from_column: str, to_column: str,
                        from_schema: Optional[str] = None,
                        to_schema: Optional[str] = None,
                        from_alias: Optional[str] = None,
                        to_alias: Optional[str] = None,
                        join_type: str = 'INNER JOIN'):
        """
        Manually add a join condition

        Args:
            from_table: Source table name
            to_table: Target table name
            from_column: Source column name
            to_column: Target column name
            from_schema: Source schema (optional)
            to_schema: Target schema (optional)
            from_alias: Source alias (optional)
            to_alias: Target alias (optional)
            join_type: Type of join (INNER JOIN, LEFT JOIN, RIGHT JOIN)
        """
        # Determine aliases
        if not from_alias:
            from_alias = from_table
        if not to_alias:
            to_alias = to_table

        # Add to join path
        self.join_path.append({
            'type': 'join',
            'from_table': from_table,
            'from_schema': from_schema,
            'from_alias': from_alias,
            'from_column': from_column,
            'to_table': to_table,
            'to_schema': to_schema,
            'to_alias': to_alias,
            'to_column': to_column,
            'join_type': join_type
        })

        return self

    def auto_join(self, tables: List[Dict]) -> 'JoinBuilder':
        """
        Automatically build join path for multiple tables

        Args:
            tables: List of dicts with keys:
                   - table: table name
                   - schema: schema name (optional)
                   - alias: alias (optional)

        Raises:
            ValueError: If no join path can be found
        """
        if len(tables) < 2:
            raise ValueError("Need at least 2 tables to build joins")

        # Convert to list of (table, schema) tuples for path finding
        table_tuples = []
        table_info = {}

        for i, t in enumerate(tables):
            table = t['table']
            schema = t.get('schema')
            alias = t.get('alias', table)

            table_tuples.append((table, schema))
            table_info[alias] = {
                'table': table,
                'schema': schema,
                'alias': alias
            }

            # Store alias mapping
            self.table_aliases[alias] = table_info[alias]

            # Add table to join path
            self.join_path.append({
                'type': 'table',
                'table': table,
                'schema': schema,
                'alias': alias
            })

        # Find join path
        path = self.db_info.find_join_path(table_tuples)

        if not path:
            raise ValueError(f"Could not find join path between tables: {[t['table'] for t in tables]}")

        # Replace the table entries with join entries
        # Keep first table, replace others with joins
        join_entries = []

        for i, step in enumerate(path):
            join_entries.append({
                'type': 'join',
                'from_table': step['from_table'],
                'from_schema': step.get('from_schema'),
                'from_alias': step.get('from_alias', step['from_table']),
                'from_column': step['from_column'],
                'to_table': step['to_table'],
                'to_schema': step.get('to_schema'),
                'to_alias': step.get('to_alias', step['to_table']),
                'to_column': step['to_column'],
                'join_type': step.get('join_type', 'INNER JOIN'),
                'relationship_type': step.get('relationship_type', 'forward')
            })

        # Replace join path (first table + joins)
        self.join_path = [self.join_path[0]] + join_entries

        return self

    # -------------------------
    # COLUMN SELECTION
    # -------------------------

    def select(self, columns: List[Dict]) -> 'JoinBuilder':
        """
        Select columns from joined tables

        Args:
            columns: List of dicts with keys:
                    - table: table name or alias
                    - column: column name
                    - alias: output alias (optional)
        """
        self.selected_columns = columns
        return self

    def select_all_from(self, table_alias: str):
        """Select all columns from a specific table"""
        self.selected_columns.append({
            'table': table_alias,
            'column': '*',
            'alias': None
        })
        return self

    def select_expression(self, expression: str, alias: str):
        """Select a custom expression"""
        self.selected_columns.append({
            'type': 'expression',
            'expression': expression,
            'alias': alias
        })
        return self

    def count(self, table_alias: str, column: str = '*', alias: str = 'count'):
        """Add COUNT aggregate"""
        self.selected_columns.append({
            'type': 'aggregate',
            'function': 'COUNT',
            'table': table_alias,
            'column': column,
            'alias': alias
        })
        return self

    def sum(self, table_alias: str, column: str, alias: str = 'sum'):
        """Add SUM aggregate"""
        self.selected_columns.append({
            'type': 'aggregate',
            'function': 'SUM',
            'table': table_alias,
            'column': column,
            'alias': alias
        })
        return self

    def avg(self, table_alias: str, column: str, alias: str = 'avg'):
        """Add AVG aggregate"""
        self.selected_columns.append({
            'type': 'aggregate',
            'function': 'AVG',
            'table': table_alias,
            'column': column,
            'alias': alias
        })
        return self

    # -------------------------
    # CONDITIONS
    # -------------------------

    def where(self, table_alias: str, column: str, operator: str, value: Any):
        """Add WHERE condition"""
        self.conditions.append({
            'table': table_alias,
            'column': column,
            'operator': operator,
            'value': value
        })
        return self

    def where_between(self, table_alias: str, column: str, start: Any, end: Any):
        """Add BETWEEN condition"""
        self.conditions.append({
            'table': table_alias,
            'column': column,
            'operator': 'BETWEEN',
            'value': (start, end)
        })
        return self

    def where_in(self, table_alias: str, column: str, values: List[Any]):
        """Add IN condition"""
        self.conditions.append({
            'table': table_alias,
            'column': column,
            'operator': 'IN',
            'value': values
        })
        return self

    def where_null(self, table_alias: str, column: str):
        """Add IS NULL condition"""
        self.conditions.append({
            'table': table_alias,
            'column': column,
            'operator': 'IS NULL',
            'value': None
        })
        return self

    # -------------------------
    # GROUP BY / ORDER BY / LIMIT
    # -------------------------

    def group_by(self, columns: List[Dict]) -> 'JoinBuilder':
        """
        Add GROUP BY clause

        Args:
            columns: List of dicts with keys:
                    - table: table alias
                    - column: column name
        """
        self.group_by_cols = columns
        return self

    def order_by(self, table_alias: str, column: str, direction: str = 'ASC') -> 'JoinBuilder':
        """Add ORDER BY clause"""
        self.order_by_cols.append({
            'table': table_alias,
            'column': column,
            'direction': direction
        })
        return self

    def limit(self, number: int, offset: int = 0) -> 'JoinBuilder':
        """Add LIMIT and OFFSET"""
        self.limit_val = number
        self.offset_val = offset
        return self

    # -------------------------
    # TEMPORARY TABLE CREATION
    # -------------------------

    def create_temp_table(self, name: str) -> 'TemporaryTable':
        """Create a temporary table from the current join"""
        temp_table = TemporaryTable(name, self)
        self.temp_tables.append(temp_table)
        return temp_table

    def save_as_temp(self, name: str, engine=None):
        """Save current join result as a temporary table"""
        query = self.build()
        temp_table = TemporaryTable(name, self)
        temp_table.create(query, engine)
        return temp_table

    # -------------------------
    # CTE ARCHITECTURE
    # -------------------------

    def with_cte(self, name: str, query_generator: QueryGenerator) -> 'JoinBuilder':
        """Add a CTE to the query"""
        self.ctes.append({
            'name': name,
            'query': query_generator
        })
        return self

    # -------------------------
    # QUERY ASSEMBLER - Builds 70% of final query
    # -------------------------

    def _build_table_reference(self, table_info: Dict) -> str:
        """Build table reference with optional schema and alias"""
        parts = []

        if table_info.get('schema'):
            parts.append(table_info['schema'])
            parts.append('.')

        parts.append(table_info['table'])

        table_ref = ''.join(parts)

        if table_info.get('alias') and table_info['alias'] != table_info['table']:
            table_ref += f" AS {table_info['alias']}"

        return table_ref

    def _build_column_reference(self, table_alias: str, column: str) -> str:
        """Build column reference with table alias"""
        return f"{table_alias}.{column}"

    def _build_select_clause(self) -> str:
        """Build the SELECT clause"""
        if not self.selected_columns:
            return "SELECT *"

        select_parts = ["SELECT"]

        if self._has_aggregates():
            # Will be handled by actual query builder
            pass

        columns = []
        for col_info in self.selected_columns:
            if col_info.get('type') == 'expression':
                expr = col_info['expression']
                if col_info.get('alias'):
                    expr += f" AS {col_info['alias']}"
                columns.append(expr)

            elif col_info.get('type') == 'aggregate':
                func = col_info['function']
                table = col_info['table']
                col = col_info['column']
                alias = col_info.get('alias', f"{func}_{col}")

                if col == '*':
                    columns.append(f"{func}(*) AS {alias}")
                else:
                    columns.append(f"{func}({table}.{col}) AS {alias}")

            else:
                # Regular column
                table = col_info['table']
                col = col_info['column']

                if col == '*':
                    columns.append(f"{table}.*")
                elif col_info.get('alias'):
                    columns.append(f"{table}.{col} AS {col_info['alias']}")
                else:
                    columns.append(f"{table}.{col}")

        return "SELECT " + ", ".join(columns)

    def _has_aggregates(self) -> bool:
        """Check if query contains aggregate functions"""
        for col in self.selected_columns:
            if col.get('type') == 'aggregate':
                return True
        return False

    def _build_from_clause(self) -> str:
        """Build the FROM clause with joins"""
        if not self.join_path:
            return ""

        first = self.join_path[0]
        from_clause = f"FROM {self._build_table_reference(first)}"

        # Add joins
        for item in self.join_path[1:]:
            if item['type'] == 'join':
                join_type = item.get('join_type', 'INNER JOIN')
                from_alias = item.get('from_alias', item['from_table'])
                to_ref = self._build_table_reference({
                    'table': item['to_table'],
                    'schema': item.get('to_schema'),
                    'alias': item.get('to_alias', item['to_table'])
                })

                join_condition = f"{from_alias}.{item['from_column']} = {item.get('to_alias', item['to_table'])}.{item['to_column']}"
                from_clause += f"\n{join_type} {to_ref} ON {join_condition}"

        return from_clause

    def _build_where_clause(self) -> str:
        """Build the WHERE clause"""
        if not self.conditions:
            return ""

        where_parts = []
        for cond in self.conditions:
            table = cond['table']
            col = cond['column']
            operator = cond['operator']
            value = cond['value']

            if operator.upper() == 'BETWEEN':
                where_parts.append(f"{table}.{col} BETWEEN {value[0]} AND {value[1]}")
            elif operator.upper() == 'IN':
                value_str = ", ".join([str(v) for v in value])
                where_parts.append(f"{table}.{col} IN ({value_str})")
            elif operator.upper() == 'IS NULL':
                where_parts.append(f"{table}.{col} IS NULL")
            elif operator.upper() == 'IS NOT NULL':
                where_parts.append(f"{table}.{col} IS NOT NULL")
            elif operator.upper() == 'LIKE':
                where_parts.append(f"{table}.{col} LIKE '{value}'")
            else:
                where_parts.append(f"{table}.{col} {operator} {value}")

        return "WHERE " + " AND ".join(where_parts)

    def _build_group_by_clause(self) -> str:
        """Build the GROUP BY clause"""
        if not self.group_by_cols:
            return ""

        group_parts = [f"{g['table']}.{g['column']}" for g in self.group_by_cols]
        return "GROUP BY " + ", ".join(group_parts)

    def _build_order_by_clause(self) -> str:
        """Build the ORDER BY clause"""
        if not self.order_by_cols:
            return ""

        order_parts = []
        for order in self.order_by_cols:
            direction = order.get('direction', 'ASC')
            order_parts.append(f"{order['table']}.{order['column']} {direction}")

        return "ORDER BY " + ", ".join(order_parts)

    def _build_limit_clause(self) -> str:
        """Build the LIMIT clause"""
        if not self.limit_val:
            return ""

        limit_clause = f"LIMIT {self.limit_val}"
        if self.offset_val:
            limit_clause += f" OFFSET {self.offset_val}"

        return limit_clause

    def build(self) -> str:
        """
        Build the SQL query (approximately 70% complete)
        This is the Query Assembler step in the flowchart
        """
        query_parts = []

        # SELECT clause
        query_parts.append(self._build_select_clause())

        # FROM clause with joins
        query_parts.append(self._build_from_clause())

        # WHERE clause
        where_clause = self._build_where_clause()
        if where_clause:
            query_parts.append(where_clause)

        # GROUP BY clause
        group_clause = self._build_group_by_clause()
        if group_clause:
            query_parts.append(group_clause)

        # ORDER BY clause
        order_clause = self._build_order_by_clause()
        if order_clause:
            query_parts.append(order_clause)

        # LIMIT clause
        limit_clause = self._build_limit_clause()
        if limit_clause:
            query_parts.append(limit_clause)

        return "\n".join(query_parts)

    def build_with_ctes(self) -> str:
        """
        Build query with CTEs (final query)
        This is the CTE Builder step in the flowchart
        """
        if not self.ctes:
            return self.build()

        cte_parts = []
        for cte in self.ctes:
            cte_parts.append(f"{cte['name']} AS (\n{cte['query'].build()}\n)")

        main_query = self.build()

        return "WITH\n" + ",\n".join(cte_parts) + "\n\n" + main_query

    def build_final(self) -> str:
        """Alias for build_with_ctes"""
        return self.build_with_ctes()

    def preview(self) -> Dict:
        """Preview the join configuration"""
        return {
            'join_path': self.join_path,
            'table_aliases': self.table_aliases,
            'selected_columns': self.selected_columns,
            'conditions': self.conditions,
            'group_by': self.group_by_cols,
            'order_by': self.order_by_cols,
            'limit': self.limit_val,
            'offset': self.offset_val,
            'temp_tables': [t.name for t in self.temp_tables],
            'ctes': [c['name'] for c in self.ctes]
        }


# ========== WRAPPER FUNCTIONS FOR COMPATIBILITY ==========
# These functions make the module compatible with the test script

_join_builder_instance = None
_default_db_info = None


def _get_default_db_info():
    """Get or create a default DBInfo instance"""
    global _default_db_info
    if _default_db_info is None:
        _default_db_info = DBInfo()
    return _default_db_info


def _get_join_builder_instance():
    """Get or create the join builder instance"""
    global _join_builder_instance
    if _join_builder_instance is None:
        _join_builder_instance = JoinBuilder(_get_default_db_info())
    return _join_builder_instance


def build_join(table1, table2, join_type="INNER JOIN", condition=None):
    """
    Wrapper function to build a JOIN
    Compatible with test.py

    Args:
        table1: First table name or alias
        table2: Second table name or alias
        join_type: Type of join (INNER, LEFT, RIGHT, etc.)
        condition: Join condition (optional)

    Returns:
        The JoinBuilder instance
    """
    builder = _get_join_builder_instance()

    # Parse join_type to ensure it has the "JOIN" suffix if needed
    if "JOIN" not in join_type.upper():
        join_type = f"{join_type} JOIN"

    # If condition is provided, parse it
    if condition and '=' in condition:
        # Simple parsing of condition like "table1.id = table2.user_id"
        parts = condition.split('=')
        left = parts[0].strip()
        right = parts[1].strip()

        # Extract table and column from left side
        if '.' in left:
            from_alias, from_col = left.split('.')
        else:
            from_alias = table1
            from_col = left

        # Extract table and column from right side
        if '.' in right:
            to_alias, to_col = right.split('.')
        else:
            to_alias = table2
            to_col = right

        # Add the join
        builder.add_join_manual(
            from_table=from_alias,
            to_table=to_alias,
            from_column=from_col,
            to_column=to_col,
            join_type=join_type
        )
    else:
        # Try to auto-detect relationship
        try:
            builder.auto_join([
                {'table': table1, 'alias': table1},
                {'table': table2, 'alias': table2}
            ])
        except:
            # Fallback to a simple cross join
            builder.add_table(table1)
            builder.add_table(table2)

    return builder


def build_join_chain(join_definitions):
    """
    Wrapper function to build a chain of joins

    Args:
        join_definitions: List of join definitions

    Returns:
        The JoinBuilder instance
    """
    builder = _get_join_builder_instance()

    for join_def in join_definitions:
        if isinstance(join_def, dict):
            table1 = join_def.get('table1')
            table2 = join_def.get('table2')
            join_type = join_def.get('type', 'INNER JOIN')
            condition = join_def.get('condition')

            build_join(table1, table2, join_type, condition)

    return builder


def get_join_info():
    """Wrapper function to get join information"""
    builder = _get_join_builder_instance()
    return builder.preview()


def reset_join_builder():
    """Reset the join builder instance"""
    global _join_builder_instance
    _join_builder_instance = None
    return True


def set_db_info(db_info):
    """Set the DBInfo instance for the join builder"""
    global _join_builder_instance, _default_db_info
    _default_db_info = db_info
    if _join_builder_instance:
        _join_builder_instance.db_info = db_info
    return True