import pandas as pd
from typing import List, Dict, Optional, Tuple, Any


class DBInfo:
    def __init__(self, schema_file, schema_name: str = "default"):
        """
        Load schema CSV file with optional schema name
        """
        self.schema_name = schema_name

        if isinstance(schema_file, str):
            self.schema_df = pd.read_csv(schema_file)
        elif isinstance(schema_file, pd.DataFrame):
            self.schema_df = schema_file
        else:
            raise ValueError("schema_file must be string path or pandas DataFrame")

        # Validate required columns
        required_cols = ['table_name', 'column_name', 'data_type']
        missing_cols = [col for col in required_cols if col not in self.schema_df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Add schema column if not present
        if 'schema_name' not in self.schema_df.columns:
            self.schema_df['schema_name'] = schema_name

        # Normalize boolean columns
        for bool_col in ['is_primary_key', 'is_foreign_key']:
            if bool_col in self.schema_df.columns:
                self.schema_df[bool_col] = self.schema_df[bool_col].astype(bool)
            else:
                self.schema_df[bool_col] = False

    # -------------------------
    # SCHEMA INFORMATION
    # -------------------------

    def get_schemas(self) -> List[str]:
        """Get all schema names"""
        return self.schema_df['schema_name'].unique().tolist()

    def get_tables(self, schema_name: Optional[str] = None) -> List[str]:
        """Get all tables, optionally filtered by schema"""
        if schema_name:
            mask = self.schema_df['schema_name'] == schema_name
            return self.schema_df[mask]['table_name'].unique().tolist()
        return self.schema_df['table_name'].unique().tolist()

    def get_full_table_name(self, table_name: str, schema_name: Optional[str] = None) -> str:
        """Get fully qualified table name with schema"""
        if schema_name:
            return f"{schema_name}.{table_name}"
        return table_name

    # -------------------------
    # COLUMN INFORMATION
    # -------------------------

    def get_columns(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict]:
        """Get columns with their metadata"""
        mask = self.schema_df['table_name'] == table_name
        if schema_name:
            mask &= self.schema_df['schema_name'] == schema_name

        rows = self.schema_df[mask]

        columns = []
        for _, row in rows.iterrows():
            columns.append({
                'column_name': row['column_name'],
                'data_type': row['data_type'],
                'is_primary_key': row.get('is_primary_key', False),
                'is_foreign_key': row.get('is_foreign_key', False),
                'is_nullable': row.get('is_nullable', True)
            })

        return columns

    def get_column_names(self, table_name: str, schema_name: Optional[str] = None) -> List[str]:
        """Get just column names for a table"""
        columns = self.get_columns(table_name, schema_name)
        return [col['column_name'] for col in columns]

    def get_primary_keys(self, table_name: str, schema_name: Optional[str] = None) -> List[str]:
        """Get primary key columns for a table"""
        columns = self.get_columns(table_name, schema_name)
        return [col['column_name'] for col in columns if col['is_primary_key']]

    def get_data_type(self, table_name: str, column_name: str,
                      schema_name: Optional[str] = None) -> Optional[str]:
        """Get data type for a specific column"""
        mask = (self.schema_df['table_name'] == table_name) & \
               (self.schema_df['column_name'] == column_name)
        if schema_name:
            mask &= self.schema_df['schema_name'] == schema_name

        rows = self.schema_df[mask]
        if not rows.empty:
            return rows.iloc[0]['data_type']
        return None

    # -------------------------
    # FOREIGN KEYS
    # -------------------------

    def get_foreign_keys(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict]:
        """Get foreign key relationships for a table"""
        mask = (self.schema_df['table_name'] == table_name) & \
               (self.schema_df['is_foreign_key'] == True)
        if schema_name:
            mask &= self.schema_df['schema_name'] == schema_name

        rows = self.schema_df[mask]

        fks = []
        for _, row in rows.iterrows():
            fks.append({
                "column": row['column_name'],
                "references_table": row.get('references_table'),
                "references_column": row.get('references_column'),
                "references_schema": row.get('references_schema', schema_name)
            })

        return fks

    # -------------------------
    # RELATIONSHIPS
    # -------------------------

    def get_all_relationships(self) -> List[Dict]:
        """Get all foreign key relationships in the database"""
        mask = self.schema_df['is_foreign_key'] == True
        rows = self.schema_df[mask]

        relationships = []
        for _, row in rows.iterrows():
            relationships.append({
                "from_schema": row['schema_name'],
                "from_table": row['table_name'],
                "from_column": row['column_name'],
                "to_schema": row.get('references_schema', row['schema_name']),
                "to_table": row.get('references_table'),
                "to_column": row.get('references_column')
            })

        return relationships

    def find_relationship(self, table1: str, table2: str,
                          schema1: Optional[str] = None,
                          schema2: Optional[str] = None) -> Optional[Dict]:
        """
        Find direct relationship between two tables, potentially across schemas
        """
        relationships = self.get_all_relationships()

        for rel in relationships:
            # Check table1 -> table2 relationship
            if rel['from_table'] == table1 and rel['to_table'] == table2:
                if (schema1 is None or rel['from_schema'] == schema1) and \
                        (schema2 is None or rel['to_schema'] == schema2):
                    return rel

            # Check reverse relationship
            if rel['from_table'] == table2 and rel['to_table'] == table1:
                if (schema2 is None or rel['from_schema'] == schema2) and \
                        (schema1 is None or rel['to_schema'] == schema1):
                    return {
                        "from_schema": schema1 or rel['to_schema'],
                        "from_table": table1,
                        "from_column": rel['to_column'],
                        "to_schema": schema2 or rel['from_schema'],
                        "to_table": table2,
                        "to_column": rel['from_column']
                    }

        return None

    def find_join_path(self, tables: List[Tuple[str, Optional[str]]]) -> List[Dict]:
        """
        Find a join path connecting multiple tables
        tables: List of (table_name, schema_name) tuples
        """
        if len(tables) < 2:
            return []

        path = []
        remaining = tables.copy()
        current = remaining.pop(0)

        while remaining:
            found = False
            for i, next_table in enumerate(remaining):
                rel = self.find_relationship(
                    current[0], next_table[0],
                    current[1], next_table[1]
                )

                if rel:
                    path.append({
                        "from_table": current[0],
                        "from_schema": current[1],
                        "from_column": rel['from_column'],
                        "to_table": next_table[0],
                        "to_schema": next_table[1],
                        "to_column": rel['to_column'],
                        "join_type": "INNER JOIN"
                    })
                    current = remaining.pop(i)
                    found = True
                    break

            if not found:
                # Try to find indirect path through another table
                if remaining:
                    # Simple implementation - just return None if no direct join
                    return None

        return path