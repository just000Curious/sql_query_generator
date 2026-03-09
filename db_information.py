import re
from typing import List, Dict, Optional, Tuple, Any, Set
import pandas as pd


class DBInfo:
    """
    Database Information Module - Parses SQL schema file and extracts metadata.
    Only processes tables with prefixes: gm, hm, pm, si, sa, ta, in
    """

    # Allowed table prefixes
    ALLOWED_PREFIXES = {'gm', 'hm', 'pm', 'si', 'sa', 'ta', 'in'}

    def __init__(self, schema_file_path: str = None):
        """
        Parse SQL schema file and extract table and relationship information

        Args:
            schema_file_path: Path to the SQL schema dump file (optional for testing)
        """
        self.schema_file_path = schema_file_path
        self.schema_name = "public"  # Default schema

        # Data structures for schema information
        self.tables: Dict[str, Dict] = {}  # table_name -> table_info
        self.columns: List[Dict] = []  # List of column info dictionaries
        self.relationships: List[Dict] = []  # List of foreign key relationships

        # Only parse if a file path is provided
        if schema_file_path:
            try:
                self._parse_sql_file()
            except FileNotFoundError:
                print(f"Warning: Schema file '{schema_file_path}' not found. Using empty schema.")
                # Initialize with empty test data if file not found
                self._init_test_data()

            # Create DataFrame for compatibility
            self._create_dataframe()

            print(f"Loaded {len(self.tables)} tables with {len(self.columns)} columns")
            print(f"Found {len(self.relationships)} relationships")
        else:
            # For testing - initialize with test data
            self._init_test_data()
            self._create_dataframe()
            print("DBInfo initialized in test mode with sample data")

    # -------------------------
    # TEST DATA INITIALIZATION
    # -------------------------

    def _init_test_data(self):
        """Initialize with test data for testing purposes"""
        # Sample tables
        test_tables = ['gm_users', 'hm_orders', 'pm_products', 'si_inventory', 'sa_sales']
        for table in test_tables:
            self.tables[table] = {
                'name': table,
                'schema': self.schema_name,
                'full_name': f"{self.schema_name}.{table}",
                'columns': []
            }

        # Sample columns
        self.columns = [
            # gm_users table
            {'schema_name': self.schema_name, 'table_name': 'gm_users', 'column_name': 'user_id',
             'data_type': 'INTEGER', 'is_primary_key': True, 'is_foreign_key': False, 'is_nullable': False},
            {'schema_name': self.schema_name, 'table_name': 'gm_users', 'column_name': 'username',
             'data_type': 'VARCHAR', 'is_primary_key': False, 'is_foreign_key': False, 'is_nullable': False},
            {'schema_name': self.schema_name, 'table_name': 'gm_users', 'column_name': 'email',
             'data_type': 'VARCHAR', 'is_primary_key': False, 'is_foreign_key': False, 'is_nullable': True},

            # hm_orders table
            {'schema_name': self.schema_name, 'table_name': 'hm_orders', 'column_name': 'order_id',
             'data_type': 'INTEGER', 'is_primary_key': True, 'is_foreign_key': False, 'is_nullable': False},
            {'schema_name': self.schema_name, 'table_name': 'hm_orders', 'column_name': 'user_id',
             'data_type': 'INTEGER', 'is_primary_key': False, 'is_foreign_key': True, 'is_nullable': False,
             'references_table': 'gm_users', 'references_column': 'user_id', 'references_schema': self.schema_name},
            {'schema_name': self.schema_name, 'table_name': 'hm_orders', 'column_name': 'order_date',
             'data_type': 'DATE', 'is_primary_key': False, 'is_foreign_key': False, 'is_nullable': True},

            # pm_products table
            {'schema_name': self.schema_name, 'table_name': 'pm_products', 'column_name': 'product_id',
             'data_type': 'INTEGER', 'is_primary_key': True, 'is_foreign_key': False, 'is_nullable': False},
            {'schema_name': self.schema_name, 'table_name': 'pm_products', 'column_name': 'product_name',
             'data_type': 'VARCHAR', 'is_primary_key': False, 'is_foreign_key': False, 'is_nullable': False},
            {'schema_name': self.schema_name, 'table_name': 'pm_products', 'column_name': 'price',
             'data_type': 'DECIMAL', 'is_primary_key': False, 'is_foreign_key': False, 'is_nullable': True},

            # si_inventory table
            {'schema_name': self.schema_name, 'table_name': 'si_inventory', 'column_name': 'inventory_id',
             'data_type': 'INTEGER', 'is_primary_key': True, 'is_foreign_key': False, 'is_nullable': False},
            {'schema_name': self.schema_name, 'table_name': 'si_inventory', 'column_name': 'product_id',
             'data_type': 'INTEGER', 'is_primary_key': False, 'is_foreign_key': True, 'is_nullable': False,
             'references_table': 'pm_products', 'references_column': 'product_id', 'references_schema': self.schema_name},
            {'schema_name': self.schema_name, 'table_name': 'si_inventory', 'column_name': 'quantity',
             'data_type': 'INTEGER', 'is_primary_key': False, 'is_foreign_key': False, 'is_nullable': True},

            # sa_sales table
            {'schema_name': self.schema_name, 'table_name': 'sa_sales', 'column_name': 'sale_id',
             'data_type': 'INTEGER', 'is_primary_key': True, 'is_foreign_key': False, 'is_nullable': False},
            {'schema_name': self.schema_name, 'table_name': 'sa_sales', 'column_name': 'order_id',
             'data_type': 'INTEGER', 'is_primary_key': False, 'is_foreign_key': True, 'is_nullable': False,
             'references_table': 'hm_orders', 'references_column': 'order_id', 'references_schema': self.schema_name},
            {'schema_name': self.schema_name, 'table_name': 'sa_sales', 'column_name': 'product_id',
             'data_type': 'INTEGER', 'is_primary_key': False, 'is_foreign_key': True, 'is_nullable': False,
             'references_table': 'pm_products', 'references_column': 'product_id', 'references_schema': self.schema_name},
            {'schema_name': self.schema_name, 'table_name': 'sa_sales', 'column_name': 'quantity',
             'data_type': 'INTEGER', 'is_primary_key': False, 'is_foreign_key': False, 'is_nullable': True},
        ]

        # Sample relationships
        self.relationships = [
            {'from_schema': self.schema_name, 'from_table': 'hm_orders', 'from_column': 'user_id',
             'to_schema': self.schema_name, 'to_table': 'gm_users', 'to_column': 'user_id'},
            {'from_schema': self.schema_name, 'from_table': 'si_inventory', 'from_column': 'product_id',
             'to_schema': self.schema_name, 'to_table': 'pm_products', 'to_column': 'product_id'},
            {'from_schema': self.schema_name, 'from_table': 'sa_sales', 'from_column': 'order_id',
             'to_schema': self.schema_name, 'to_table': 'hm_orders', 'to_column': 'order_id'},
            {'from_schema': self.schema_name, 'from_table': 'sa_sales', 'from_column': 'product_id',
             'to_schema': self.schema_name, 'to_table': 'pm_products', 'to_column': 'product_id'},
        ]

    # -------------------------
    # PARSING METHODS
    # -------------------------

    def _parse_sql_file(self):
        """
        Parse the SQL schema file to extract tables, columns, and relationships
        """
        if not self.schema_file_path:
            return

        try:
            with open(self.schema_file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Remove comments and clean up
            content = self._remove_sql_comments(content)

            # Split into statements
            statements = self._split_statements(content)

            # Process CREATE TABLE statements
            for statement in statements:
                self._process_create_table(statement)

            # Process ALTER TABLE statements for foreign keys
            for statement in statements:
                self._process_alter_table(statement)
        except FileNotFoundError:
            # Fall back to test data
            self._init_test_data()

    def _remove_sql_comments(self, content: str) -> str:
        """Remove SQL comments (-- and /* */)"""
        # Remove -- style comments
        lines = []
        for line in content.split('\n'):
            if '--' in line:
                line = line[:line.index('--')]
            lines.append(line)
        content = '\n'.join(lines)

        # Remove /* */ style comments
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

        return content

    def _split_statements(self, content: str) -> List[str]:
        """Split SQL content into individual statements"""
        # Simple splitting by semicolon, ignoring semicolons in strings
        statements = []
        current = []
        in_string = False
        string_char = None

        for char in content:
            if char in ['"', "'"] and not in_string:
                in_string = True
                string_char = char
            elif char == string_char and in_string:
                in_string = False
                string_char = None

            if char == ';' and not in_string:
                statements.append(''.join(current).strip())
                current = []
            else:
                current.append(char)

        # Add last statement
        if current:
            statements.append(''.join(current).strip())

        return [s for s in statements if s and s.upper().startswith(('CREATE', 'ALTER'))]

    def _should_include_table(self, table_name: str) -> bool:
        """Check if table should be included based on prefix"""
        if '.' in table_name:
            table_name = table_name.split('.')[-1]

        # Extract prefix (first 2 characters)
        if len(table_name) >= 2:
            prefix = table_name[:2].lower()
            return prefix in self.ALLOWED_PREFIXES

        # Include public.pmm_employee etc. (pmm_ prefix)
        if table_name.lower().startswith('pmm_'):
            return True

        return False

    def _process_create_table(self, statement: str):
        """Process CREATE TABLE statement"""
        if not statement.upper().startswith('CREATE TABLE'):
            return

        # Extract table name
        match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)', statement, re.IGNORECASE)
        if not match:
            return

        full_table_name = match.group(1).strip()

        # Remove schema if present
        if '.' in full_table_name:
            schema, table = full_table_name.split('.', 1)
            table_name = table.strip('"`')
        else:
            table_name = full_table_name.strip('"`')
            schema = self.schema_name

        # Check if table should be included
        if not self._should_include_table(table_name):
            return

        # Extract column definitions (content between first '(' and last ')')
        start_idx = statement.find('(')
        if start_idx == -1:
            return

        # Find matching closing parenthesis
        depth = 1
        end_idx = start_idx + 1
        in_string = False
        string_char = None

        while end_idx < len(statement) and depth > 0:
            char = statement[end_idx]

            if char in ['"', "'"] and not in_string:
                in_string = True
                string_char = char
            elif char == string_char and in_string:
                in_string = False
                string_char = None

            if not in_string:
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1

            end_idx += 1

        columns_def = statement[start_idx + 1:end_idx - 1]

        # Parse columns
        self._parse_columns(table_name, schema, columns_def)

        # Store table info
        self.tables[table_name] = {
            'name': table_name,
            'schema': schema,
            'full_name': f"{schema}.{table_name}",
            'columns': []
        }

    def _parse_columns(self, table_name: str, schema: str, columns_def: str):
        """Parse column definitions"""
        # Split by commas, but not within parentheses
        parts = []
        current = []
        depth = 0
        in_string = False
        string_char = None

        for char in columns_def:
            if char in ['"', "'"] and not in_string:
                in_string = True
                string_char = char
            elif char == string_char and in_string:
                in_string = False
                string_char = None

            if char == '(' and not in_string:
                depth += 1
            elif char == ')' and not in_string:
                depth -= 1

            if char == ',' and depth == 0 and not in_string:
                parts.append(''.join(current).strip())
                current = []
            else:
                current.append(char)

        if current:
            parts.append(''.join(current).strip())

        # Process each column definition
        primary_keys = []

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # Check if it's a PRIMARY KEY constraint
            if part.upper().startswith('PRIMARY KEY'):
                pk_match = re.search(r'PRIMARY\s+KEY\s*\(([^)]+)\)', part, re.IGNORECASE)
                if pk_match:
                    pk_columns = [c.strip().strip('"`') for c in pk_match.group(1).split(',')]
                    primary_keys.extend(pk_columns)
                continue

            # Check if it's a FOREIGN KEY constraint
            if part.upper().startswith('FOREIGN KEY'):
                fk_match = re.search(r'FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+([^\s(]+)\s*\(([^)]+)\)', part,
                                     re.IGNORECASE)
                if fk_match:
                    fk_columns = [c.strip().strip('"`') for c in fk_match.group(1).split(',')]
                    ref_table = fk_match.group(2).strip().strip('"`')
                    ref_columns = [c.strip().strip('"`') for c in fk_match.group(3).split(',')]

                    # Store relationship
                    for fk_col, ref_col in zip(fk_columns, ref_columns):
                        self.relationships.append({
                            'from_schema': schema,
                            'from_table': table_name,
                            'from_column': fk_col,
                            'to_schema': schema,
                            'to_table': ref_table,
                            'to_column': ref_col
                        })
                continue

            # Regular column definition
            col_parts = part.split()
            if len(col_parts) < 2:
                continue

            col_name = col_parts[0].strip('"`')
            data_type = col_parts[1].upper()

            # Clean data type (remove size information)
            data_type = re.sub(r'\([^)]*\)', '', data_type)

            # Check for NOT NULL
            is_nullable = 'NOT NULL' not in part.upper()

            self.columns.append({
                'schema_name': schema,
                'table_name': table_name,
                'column_name': col_name,
                'data_type': data_type,
                'is_primary_key': False,  # Will update later
                'is_foreign_key': False,  # Will update later
                'is_nullable': is_nullable
            })

        # Update primary keys
        for col in self.columns:
            if col['table_name'] == table_name and col['column_name'] in primary_keys:
                col['is_primary_key'] = True

        # Update foreign keys from relationships
        for rel in self.relationships:
            if rel['from_table'] == table_name:
                for col in self.columns:
                    if col['table_name'] == table_name and col['column_name'] == rel['from_column']:
                        col['is_foreign_key'] = True
                        col['references_table'] = rel['to_table']
                        col['references_column'] = rel['to_column']
                        col['references_schema'] = rel['to_schema']

    def _process_alter_table(self, statement: str):
        """Process ALTER TABLE statement for foreign keys"""
        if not statement.upper().startswith('ALTER TABLE'):
            return

        # Extract table name
        match = re.search(r'ALTER\s+TABLE\s+([^\s]+)', statement, re.IGNORECASE)
        if not match:
            return

        full_table_name = match.group(1).strip()

        # Parse schema and table
        if '.' in full_table_name:
            schema, table = full_table_name.split('.', 1)
            table_name = table.strip('"`')
        else:
            table_name = full_table_name.strip('"`')
            schema = self.schema_name

        # Check if table should be included
        if not self._should_include_table(table_name):
            return

        # Look for ADD CONSTRAINT ... FOREIGN KEY
        fk_match = re.search(
            r'ADD\s+CONSTRAINT\s+\S+\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+([^\s(]+)\s*\(([^)]+)\)',
            statement, re.IGNORECASE)
        if fk_match:
            fk_columns = [c.strip().strip('"`') for c in fk_match.group(1).split(',')]
            ref_table = fk_match.group(2).strip().strip('"`')
            ref_columns = [c.strip().strip('"`') for c in fk_match.group(3).split(',')]

            # Store relationship
            for fk_col, ref_col in zip(fk_columns, ref_columns):
                relationship = {
                    'from_schema': schema,
                    'from_table': table_name,
                    'from_column': fk_col,
                    'to_schema': schema,
                    'to_table': ref_table,
                    'to_column': ref_col
                }

                if relationship not in self.relationships:
                    self.relationships.append(relationship)

                    # Update column info
                    for col in self.columns:
                        if (col['table_name'] == table_name and
                                col['column_name'] == fk_col):
                            col['is_foreign_key'] = True
                            col['references_table'] = ref_table
                            col['references_column'] = ref_col
                            col['references_schema'] = schema

    def _create_dataframe(self):
        """Create a pandas DataFrame from the columns data for compatibility"""
        if not self.columns:
            # Create empty DataFrame with required columns
            self.schema_df = pd.DataFrame(columns=[
                'schema_name', 'table_name', 'column_name', 'data_type',
                'is_primary_key', 'is_foreign_key', 'is_nullable',
                'references_table', 'references_column', 'references_schema'
            ])
        else:
            self.schema_df = pd.DataFrame(self.columns)

    # -------------------------
    # SCHEMA INFORMATION
    # -------------------------

    def get_schemas(self) -> List[str]:
        """Get all schema names"""
        return [self.schema_name]

    def get_tables(self, schema_name: Optional[str] = None) -> List[str]:
        """Get all table names, optionally filtered by schema"""
        if schema_name and schema_name != self.schema_name:
            return []
        return list(self.tables.keys())

    def get_all_tables_with_info(self) -> Dict[str, Dict]:
        """Get all tables with their information"""
        return self.tables

    def get_full_table_name(self, table_name: str, schema_name: Optional[str] = None) -> str:
        """Get fully qualified table name"""
        if schema_name:
            return f"{schema_name}.{table_name}"

        # Find schema for this table
        for col in self.columns:
            if col['table_name'] == table_name:
                return f"{col['schema_name']}.{table_name}"

        return table_name

    def table_exists(self, table_name: str, schema_name: Optional[str] = None) -> bool:
        """Check if table exists"""
        if schema_name:
            return any(
                col['table_name'] == table_name and col['schema_name'] == schema_name
                for col in self.columns
            )
        return table_name in self.tables

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
            col_dict = {
                'column_name': row['column_name'],
                'data_type': row['data_type'],
                'is_primary_key': bool(row.get('is_primary_key', False)),
                'is_foreign_key': bool(row.get('is_foreign_key', False)),
                'is_nullable': bool(row.get('is_nullable', True))
            }

            # Add reference info if available
            if 'references_table' in row and pd.notna(row['references_table']):
                col_dict['references_table'] = row['references_table']
                col_dict['references_column'] = row['references_column']
                col_dict['references_schema'] = row.get('references_schema', schema_name)

            columns.append(col_dict)

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

    def column_exists(self, table_name: str, column_name: str,
                      schema_name: Optional[str] = None) -> bool:
        """Check if column exists in table"""
        mask = (self.schema_df['table_name'] == table_name) & \
               (self.schema_df['column_name'] == column_name)
        if schema_name:
            mask &= self.schema_df['schema_name'] == schema_name

        return not self.schema_df[mask].empty

    # -------------------------
    # FOREIGN KEYS
    # -------------------------

    def get_foreign_keys(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict]:
        """Get foreign key relationships for a table"""
        fks = []

        for rel in self.relationships:
            if rel['from_table'] == table_name:
                if schema_name is None or rel['from_schema'] == schema_name:
                    fks.append({
                        "column": rel['from_column'],
                        "references_table": rel['to_table'],
                        "references_column": rel['to_column'],
                        "references_schema": rel['to_schema']
                    })

        return fks

    def get_referenced_by(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict]:
        """Get tables that reference this table"""
        refs = []

        for rel in self.relationships:
            if rel['to_table'] == table_name:
                if schema_name is None or rel['to_schema'] == schema_name:
                    refs.append({
                        "from_table": rel['from_table'],
                        "from_schema": rel['from_schema'],
                        "from_column": rel['from_column'],
                        "references_column": rel['to_column']
                    })

        return refs

    # -------------------------
    # RELATIONSHIPS
    # -------------------------

    def get_all_relationships(self) -> List[Dict]:
        """Get all foreign key relationships"""
        return self.relationships.copy()

    def find_relationship(self, table1: str, table2: str,
                          schema1: Optional[str] = None,
                          schema2: Optional[str] = None) -> Optional[Dict]:
        """
        Find direct relationship between two tables
        Returns the relationship dict if found, None otherwise
        """
        # Check table1 -> table2 relationships
        for rel in self.relationships:
            if (rel['from_table'] == table1 and rel['to_table'] == table2):
                if (schema1 is None or rel['from_schema'] == schema1) and \
                        (schema2 is None or rel['to_schema'] == schema2):
                    return {
                        "type": "forward",
                        "from_table": table1,
                        "from_schema": rel['from_schema'],
                        "from_column": rel['from_column'],
                        "to_table": table2,
                        "to_schema": rel['to_schema'],
                        "to_column": rel['to_column']
                    }

            # Check table2 -> table1 (reverse)
            if (rel['from_table'] == table2 and rel['to_table'] == table1):
                if (schema2 is None or rel['from_schema'] == schema2) and \
                        (schema1 is None or rel['to_schema'] == schema1):
                    return {
                        "type": "reverse",
                        "from_table": table1,
                        "from_schema": schema1 or rel['to_schema'],
                        "from_column": rel['to_column'],
                        "to_table": table2,
                        "to_schema": schema2 or rel['from_schema'],
                        "to_column": rel['from_column']
                    }

        return None

    def get_direct_relationships(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict]:
        """Get all tables directly related to this table"""
        related = []

        # Outgoing relationships
        for rel in self.relationships:
            if rel['from_table'] == table_name:
                if schema_name is None or rel['from_schema'] == schema_name:
                    related.append({
                        "table": rel['to_table'],
                        "schema": rel['to_schema'],
                        "type": "parent",
                        "via": {
                            "from_column": rel['from_column'],
                            "to_column": rel['to_column']
                        }
                    })

        # Incoming relationships
        for rel in self.relationships:
            if rel['to_table'] == table_name:
                if schema_name is None or rel['to_schema'] == schema_name:
                    related.append({
                        "table": rel['from_table'],
                        "schema": rel['from_schema'],
                        "type": "child",
                        "via": {
                            "from_column": rel['from_column'],
                            "to_column": rel['to_column']
                        }
                    })

        return related

    def find_join_path(self, tables: List[Tuple[str, Optional[str]]]) -> List[Dict]:
        """
        Find a join path connecting multiple tables
        tables: List of (table_name, schema_name) tuples

        Returns a list of join steps, or None if no path found
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
                        "from_schema": current[1] or rel.get('from_schema'),
                        "from_column": rel['from_column'],
                        "to_table": next_table[0],
                        "to_schema": next_table[1] or rel.get('to_schema'),
                        "to_column": rel['to_column'],
                        "join_type": "INNER JOIN",
                        "relationship_type": rel.get('type', 'forward')
                    })

                    current = remaining.pop(i)
                    found = True
                    break

            if not found:
                # Try to find indirect path through another table
                # This would require graph traversal - simplified for now
                return None

        return path

    # -------------------------
    # UTILITY METHODS
    # -------------------------

    def get_table_summary(self) -> pd.DataFrame:
        """Get summary of all tables"""
        summary = []

        for table_name in self.tables:
            cols = self.get_columns(table_name)
            pk_count = sum(1 for c in cols if c['is_primary_key'])
            fk_count = sum(1 for c in cols if c['is_foreign_key'])

            summary.append({
                'table_name': table_name,
                'columns': len(cols),
                'primary_keys': pk_count,
                'foreign_keys': fk_count,
                'relationships': len(self.get_direct_relationships(table_name))
            })

        return pd.DataFrame(summary)

    def search_tables(self, pattern: str) -> List[str]:
        """Search for tables matching pattern"""
        pattern = pattern.lower()
        return [t for t in self.tables if pattern in t.lower()]

    def search_columns(self, pattern: str) -> List[Dict]:
        """Search for columns matching pattern"""
        pattern = pattern.lower()
        results = []

        for col in self.columns:
            if pattern in col['column_name'].lower():
                results.append({
                    'table': col['table_name'],
                    'schema': col['schema_name'],
                    'column': col['column_name'],
                    'data_type': col['data_type']
                })

        return results


# ========== WRAPPER FUNCTIONS FOR COMPATIBILITY ==========
# These functions make the module compatible with testing

def create_db_info(schema_file_path: str = None):
    """
    Create a DBInfo instance (factory function)
    """
    return DBInfo(schema_file_path)


def get_test_db_info():
    """
    Get a DBInfo instance with test data
    """
    return DBInfo()  # No file path = test mode


def reset_db_info():
    """
    Reset any cached DBInfo instances
    """
    # This is a no-op for this module, but included for consistency
    return True