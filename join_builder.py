from pypika import Query, Table, Field, functions as fn
from typing import List, Dict, Optional, Union, Any
import pandas as pd
from db_information import DBInfo
from pypika_query_engine import QueryGenerator
from temporary_table import TemporaryTable


class JoinBuilder:
    def __init__(self, db_info: DBInfo):
        """
        Initialize JoinBuilder with database schema information
        """
        self.db_info = db_info
        self.join_path = []
        self.selected_columns = []
        self.conditions = []
        self.temp_tables = []
        self.cte_queries = []

    # -------------------------
    # JOIN PATH MANAGEMENT
    # -------------------------

    def add_table(self, table_name: str, schema_name: Optional[str] = None,
                  alias: Optional[str] = None) -> 'JoinBuilder':
        """Add a table to the join path"""
        self.join_path.append({
            'table': table_name,
            'schema': schema_name,
            'alias': alias
        })
        return self

    def add_join(self, from_table: str, to_table: str,
                 from_column: str, to_column: str,
                 from_schema: Optional[str] = None,
                 to_schema: Optional[str] = None,
                 join_type: str = 'INNER JOIN') -> 'JoinBuilder':
        """Manually add a join condition"""
        self.join_path.append({
            'from_table': from_table,
            'from_schema': from_schema,
            'from_column': from_column,
            'to_table': to_table,
            'to_schema': to_schema,
            'to_column': to_column,
            'join_type': join_type
        })
        return self

    def auto_join(self, tables: List[Dict]) -> 'JoinBuilder':
        """
        Automatically build join path for multiple tables
        tables: List of {'table': str, 'schema': Optional[str], 'alias': Optional[str]}
        """
        if len(tables) < 2:
            raise ValueError("Need at least 2 tables to build joins")

        # Convert to list of (table, schema) tuples for path finding
        table_tuples = [(t['table'], t.get('schema')) for t in tables]

        # Find join path
        path = self.db_info.find_join_path(table_tuples)

        if not path:
            raise ValueError(f"Could not find join path between tables: {[t['table'] for t in tables]}")

        # Build join path
        for i, table_info in enumerate(tables):
            if i == 0:
                # First table
                self.join_path.append(table_info)
            else:
                # Add join
                join_info = path[i - 1]  # First join connects table 0 and 1, etc.
                self.join_path.append({
                    'from_table': join_info['from_table'],
                    'from_schema': join_info['from_schema'],
                    'from_column': join_info['from_column'],
                    'to_table': join_info['to_table'],
                    'to_schema': join_info['to_schema'],
                    'to_column': join_info['to_column'],
                    'join_type': 'INNER JOIN'
                })

        return self

    # -------------------------
    # COLUMN SELECTION
    # -------------------------

    def select(self, columns: List[Dict]) -> 'JoinBuilder':
        """
        Select columns from joined tables
        columns: List of {'table': str, 'column': str, 'alias': Optional[str]}
        """
        self.selected_columns = columns
        return self

    def select_all_from(self, table_name: str, schema_name: Optional[str] = None):
        """Select all columns from a specific table"""
        self.selected_columns.append({
            'table': table_name,
            'schema': schema_name,
            'column': '*',
            'alias': None
        })
        return self

    # -------------------------
    # CONDITIONS
    # -------------------------

    def where(self, table: str, column: str, operator: str,
              value: Any, schema: Optional[str] = None):
        """Add WHERE condition"""
        self.conditions.append({
            'table': table,
            'schema': schema,
            'column': column,
            'operator': operator,
            'value': value
        })
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
        self.cte_queries.append({
            'name': name,
            'query': query_generator
        })
        return self

    def build_cte_query(self) -> str:
        """Build query with CTEs"""
        if not self.cte_queries:
            return self.build()

        cte_parts = []
        for cte in self.cte_queries:
            cte_parts.append(f"{cte['name']} AS (\n{cte['query'].build()}\n)")

        main_query = self.build()

        return f"WITH\n{',\n'.join(cte_parts)}\n\n{main_query}"

    # -------------------------
    # QUERY BUILDING
    # -------------------------

    def build(self) -> str:
        """Build the complete SQL query"""
        if not self.join_path:
            raise ValueError("No tables added to join")

        # Start with first table
        first_table = self.join_path[0]
        if 'alias' in first_table and first_table['alias']:
            table_name = f"{first_table['schema'] + '.' if first_table.get('schema') else ''}{first_table['table']} AS {first_table['alias']}"
        else:
            table_name = f"{first_table['schema'] + '.' if first_table.get('schema') else ''}{first_table['table']}"

        query = Query.from_(Table(table_name))

        # Add joins
        current_alias = first_table.get('alias', first_table['table'])

        for join_item in self.join_path[1:]:
            if 'from_table' in join_item:
                # This is a join
                from_alias = join_item.get('from_alias', join_item['from_table'])
                to_alias = join_item.get('to_alias', join_item['to_table'])

                to_table_name = f"{join_item['to_schema'] + '.' if join_item.get('to_schema') else ''}{join_item['to_table']}"
                if 'to_alias' in join_item:
                    to_table = Table(to_table_name).as_(join_item['to_alias'])
                else:
                    to_table = Table(to_table_name)

                # Get the actual table references based on aliases
                from_field = Field(f"{from_alias}.{join_item['from_column']}")
                to_field = Field(f"{to_alias}.{join_item['to_column']}")

                join_type = join_item.get('join_type', 'INNER JOIN')

                if join_type.upper() == 'LEFT JOIN':
                    query = query.left_join(to_table).on(from_field == to_field)
                elif join_type.upper() == 'RIGHT JOIN':
                    query = query.right_join(to_table).on(from_field == to_field)
                else:
                    query = query.join(to_table).on(from_field == to_field)

                current_alias = join_item.get('to_alias', join_item['to_table'])

        # Add selected columns
        if self.selected_columns:
            fields = []
            for col_info in self.selected_columns:
                table_ref = col_info.get('alias', col_info['table'])
                field = Field(f"{table_ref}.{col_info['column']}")

                if col_info.get('output_alias'):
                    field = field.as_(col_info['output_alias'])

                fields.append(field)

            query = query.select(*fields)
        else:
            query = query.select('*')

        # Add WHERE conditions
        for condition in self.conditions:
            table_ref = condition.get('alias', condition['table'])
            field = Field(f"{table_ref}.{condition['column']}")

            if condition['operator'].upper() == '=':
                query = query.where(field == condition['value'])
            elif condition['operator'].upper() == '>':
                query = query.where(field > condition['value'])
            elif condition['operator'].upper() == '<':
                query = query.where(field < condition['value'])
            elif condition['operator'].upper() == '>=':
                query = query.where(field >= condition['value'])
            elif condition['operator'].upper() == '<=':
                query = query.where(field <= condition['value'])
            elif condition['operator'].upper() == 'LIKE':
                query = query.where(field.like(condition['value']))
            elif condition['operator'].upper() == 'IN':
                query = query.where(field.isin(condition['value']))

        return str(query)

    def build_with_ctes(self) -> str:
        """Build query with CTEs"""
        return self.build_cte_query()

    def preview(self) -> Dict:
        """Preview the join configuration"""
        return {
            'join_path': self.join_path,
            'selected_columns': self.selected_columns,
            'conditions': self.conditions,
            'temp_tables': [t.name for t in self.temp_tables],
            'ctes': [c['name'] for c in self.cte_queries]
        }