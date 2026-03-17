import re
import pandas as pd
from typing import List, Dict, Optional, Union, Any, Set, Tuple
from dataclasses import dataclass, field


@dataclass
class CSVSchema:
    """Schema information loaded from CSV file"""
    tables: Dict[str, Dict] = field(default_factory=dict)
    columns: List[Dict] = field(default_factory=list)
    relationships: List[Dict] = field(default_factory=list)
    table_columns: Dict[str, List[Dict]] = field(default_factory=dict)
    primary_keys: Dict[str, List[str]] = field(default_factory=dict)
    foreign_keys: Dict[str, List[Dict]] = field(default_factory=dict)
    tables_by_schema: Dict[str, List[Dict]] = field(default_factory=dict)
    columns_by_table: Dict[str, List[Dict]] = field(default_factory=dict)


class CSVDBInfo:
    """
    Database Information Module - Loads schema from CSV file and provides metadata
    """

    def __init__(self, csv_file_path: str = None):
        """
        Load schema from CSV file

        Args:
            csv_file_path: Path to the CSV schema file
        """
        self.csv_file_path = csv_file_path
        self.schema = CSVSchema()
        self.schemas: Set[str] = set()

        # Category mapping for business grouping
        self.category_map = {
            "gm": "General Management",
            "hm": "Healthcare Management",
            "pm": "Personnel Management",
            "pmm": "Personnel Management",
            "sa": "Sales & Admin",
            "si": "System Integration",
            "ta": "Taxation & Finance",
            "tatk": "Taxation & Finance",
            "fam": "Financial Management",
            "rms": "Resource Management",
            "qrs": "Quality & Reporting"
        }

        if csv_file_path:
            try:
                self._load_from_csv(csv_file_path)
                print(f"✅ Loaded schema from {csv_file_path}")
            except Exception as e:
                print(f"⚠️ Error loading CSV file: {e}. Using test data.")
                self._init_test_data()
        else:
            self._init_test_data()

        # Pre-cache structures after loading
        self._precache_structures()

        print(f"📊 Loaded {len(self.schema.tables)} tables with {len(self.schema.columns)} columns")
        print(f"🔗 Found {len(self.schema.relationships)} relationships")
        print(f"📁 Found {len(self.schemas)} schemas")

    # -------------------------
    # CSV LOADING
    # -------------------------

    def _load_from_csv(self, csv_file_path: str):
        """Load schema from CSV file"""
        self.df = pd.read_csv(csv_file_path)

        # Clean column names
        self.df.columns = [col.strip().lower() for col in self.df.columns]

        # Extract schema from table name prefix
        self.df['schema'] = self.df['table_name'].apply(
            lambda x: self._extract_prefix(str(x)) if '_' in str(x) else 'public'
        )

        # Build schema structures
        self._build_from_dataframe(self.df)

    def _extract_prefix(self, table_name: str) -> str:
        """Extract prefix from table name to use as schema"""
        if '_' in table_name:
            return table_name.split('_')[0]
        return 'public'

    def _build_from_dataframe(self, df: pd.DataFrame):
        """Build schema structures from dataframe"""
        # Group by table
        for table_name, group in df.groupby('table_name'):
            table_name = str(table_name).strip()

            # Get schema from extracted column
            schema = group.iloc[0]['schema'] if 'schema' in group.columns else 'public'
            clean_table_name = table_name

            if '.' in table_name:
                parts = table_name.split('.', 1)
                schema = parts[0].strip()
                clean_table_name = parts[1].strip()
                full_name = f"{schema}.{clean_table_name}"
            else:
                full_name = clean_table_name

            self.schemas.add(schema)

            # Store table info
            self.schema.tables[full_name] = {
                'name': clean_table_name,
                'schema': schema,
                'full_name': full_name,
                'columns': [],
                'category': self._get_category(schema)
            }

            # Process columns
            table_columns = []
            primary_keys = []
            foreign_keys = []

            for _, row in group.iterrows():
                column_name = str(row['column_name']).strip() if pd.notna(row['column_name']) else ''
                data_type = str(row['data_type']).strip() if pd.notna(row['data_type']) else 'UNKNOWN'
                is_nullable = str(row['is_nullable']).strip().upper() == 'YES' if pd.notna(row['is_nullable']) else True
                is_primary_key = str(row['is_primary_key']).strip().upper() == 'TRUE' if pd.notna(
                    row['is_primary_key']) else False
                is_foreign_key = str(row['is_foreign_key']).strip().upper() == 'TRUE' if pd.notna(
                    row['is_foreign_key']) else False

                column_info = {
                    'schema_name': schema,
                    'table_name': clean_table_name,
                    'column_name': column_name,
                    'data_type': data_type,
                    'is_primary_key': is_primary_key,
                    'is_foreign_key': is_foreign_key,
                    'is_nullable': is_nullable
                }

                # Add reference info if available
                parent_table = row['parent_table'] if pd.notna(row.get('parent_table')) else None
                parent_column = row['parent_column'] if pd.notna(row.get('parent_column')) else None

                if parent_table and parent_column:
                    column_info['references_table'] = str(parent_table).strip()
                    column_info['references_column'] = str(parent_column).strip()

                    # Store relationship
                    rel = {
                        'from_schema': schema,
                        'from_table': clean_table_name,
                        'from_column': column_name,
                        'to_schema': schema,
                        'to_table': str(parent_table).strip(),
                        'to_column': str(parent_column).strip()
                    }

                    # Check if relationship already exists
                    if rel not in self.schema.relationships:
                        self.schema.relationships.append(rel)

                    # Add to foreign keys list
                    foreign_keys.append({
                        'column': column_name,
                        'references_table': str(parent_table).strip(),
                        'references_column': str(parent_column).strip(),
                        'references_schema': schema
                    })

                self.schema.columns.append(column_info)
                table_columns.append(column_info)

                if is_primary_key:
                    primary_keys.append(column_name)

            self.schema.table_columns[full_name] = table_columns
            if primary_keys:
                self.schema.primary_keys[full_name] = primary_keys
            if foreign_keys:
                self.schema.foreign_keys[full_name] = foreign_keys

    def _get_category(self, schema: str) -> str:
        """Get business category for a schema"""
        # Check for exact match
        if schema in self.category_map:
            return self.category_map[schema]

        # Check for prefix match
        for prefix, category in self.category_map.items():
            if schema.startswith(prefix):
                return category

        return "Other"

    def _precache_structures(self):
        """Pre-calculate dictionaries for faster access"""
        # Build tables by schema
        self.schema.tables_by_schema = {}
        for full_name, table_info in self.schema.tables.items():
            schema = table_info['schema']
            if schema not in self.schema.tables_by_schema:
                self.schema.tables_by_schema[schema] = []
            self.schema.tables_by_schema[schema].append(table_info.copy())

        # Build columns by table
        self.schema.columns_by_table = {}
        for col_info in self.schema.columns:
            table_key = f"{col_info['schema_name']}.{col_info['table_name']}"
            if table_key not in self.schema.columns_by_table:
                self.schema.columns_by_table[table_key] = []
            self.schema.columns_by_table[table_key].append(col_info.copy())

    # -------------------------
    # TEST DATA INITIALIZATION
    # -------------------------

    def _init_test_data(self):
        """Initialize with test data"""
        # Sample tables from CSV structure
        test_tables = [
            ('public', 'gmhk_appointment'),
            ('public', 'gmhk_complaint_hdr'),
            ('public', 'gmhk_email_details'),
            ('pmm', 'employees'),
            ('pmm', 'departments'),
            ('fam', 'accounts'),
            ('fam', 'transactions'),
            ('rms', 'resources'),
            ('qrs', 'tickets'),
        ]

        for schema, table in test_tables:
            full_name = f"{schema}.{table}" if schema != 'public' else table
            self.schemas.add(schema)
            self.schema.tables[full_name] = {
                'name': table,
                'schema': schema,
                'full_name': full_name,
                'columns': [],
                'category': self._get_category(schema)
            }

        # Sample columns
        self.schema.columns = [
            # gmhk_appointment table
            {'schema_name': 'public', 'table_name': 'gmhk_appointment', 'column_name': 'emp_no',
             'data_type': 'INTEGER', 'is_primary_key': True, 'is_foreign_key': False, 'is_nullable': False},
            {'schema_name': 'public', 'table_name': 'gmhk_appointment', 'column_name': 'appointment_date',
             'data_type': 'DATE', 'is_primary_key': True, 'is_foreign_key': False, 'is_nullable': False},

            # pmm.employees table
            {'schema_name': 'pmm', 'table_name': 'employees', 'column_name': 'emp_no',
             'data_type': 'INTEGER', 'is_primary_key': True, 'is_foreign_key': False, 'is_nullable': False},
            {'schema_name': 'pmm', 'table_name': 'employees', 'column_name': 'department_id',
             'data_type': 'INTEGER', 'is_primary_key': False, 'is_foreign_key': True, 'is_nullable': False,
             'references_table': 'departments', 'references_column': 'dept_id'},

            # pmm.departments table
            {'schema_name': 'pmm', 'table_name': 'departments', 'column_name': 'dept_id',
             'data_type': 'INTEGER', 'is_primary_key': True, 'is_foreign_key': False, 'is_nullable': False},
        ]

        # Sample relationships
        self.schema.relationships = [
            {'from_schema': 'pmm', 'from_table': 'employees', 'from_column': 'department_id',
             'to_schema': 'pmm', 'to_table': 'departments', 'to_column': 'dept_id'},
        ]

        # Pre-cache test data
        self._precache_structures()

    # -------------------------
    # SCHEMA INFORMATION METHODS
    # -------------------------

    def get_schemas(self) -> List[str]:
        """Get all schema names"""
        return sorted(list(self.schemas))

    def get_tables(self, schema_name: Optional[str] = None) -> List[str]:
        """Get all table names, optionally filtered by schema"""
        if schema_name and schema_name in self.schema.tables_by_schema:
            return [table['name'] for table in self.schema.tables_by_schema[schema_name]]

        tables = []
        for info in self.schema.tables.values():
            if schema_name is None or info['schema'] == schema_name:
                tables.append(info['name'])
        return sorted(tables)

    def get_tables_with_schema(self, schema_name: Optional[str] = None) -> List[Dict]:
        """Get tables with full schema info - uses pre-cached data"""
        if schema_name and schema_name in self.schema.tables_by_schema:
            return self.schema.tables_by_schema[schema_name].copy()

        tables = []
        for info in self.schema.tables.values():
            if schema_name is None or info['schema'] == schema_name:
                tables.append(info.copy())
        return tables

    def table_exists(self, table_name: str, schema_name: Optional[str] = None) -> bool:
        """Check if table exists"""
        if schema_name:
            full_name = f"{schema_name}.{table_name}" if schema_name != 'public' else table_name
            return full_name in self.schema.tables

        # Check without schema
        for info in self.schema.tables.values():
            if info['name'] == table_name:
                return True
        return False

    def get_full_table_name(self, table_name: str, schema_name: Optional[str] = None) -> str:
        """Get fully qualified table name"""
        if schema_name:
            return f"{schema_name}.{table_name}" if schema_name != 'public' else table_name

        # Find schema for this table
        for info in self.schema.tables.values():
            if info['name'] == table_name:
                schema = info['schema']
                return f"{schema}.{table_name}" if schema != 'public' else table_name

        return table_name

    # -------------------------
    # COLUMN INFORMATION METHODS
    # -------------------------

    def get_columns(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict]:
        """Get columns for a table - uses pre-cached data"""
        if schema_name:
            table_key = f"{schema_name}.{table_name}"
            if table_key in self.schema.columns_by_table:
                return [col.copy() for col in self.schema.columns_by_table[table_key]]

        # Fallback to linear search
        columns = []
        for col in self.schema.columns:
            if col['table_name'] == table_name:
                if schema_name is None or col['schema_name'] == schema_name:
                    columns.append(col.copy())
        return columns

    def get_column_names(self, table_name: str, schema_name: Optional[str] = None) -> List[str]:
        """Get column names for a table"""
        columns = self.get_columns(table_name, schema_name)
        return [col['column_name'] for col in columns]

    def column_exists(self, table_name: str, column_name: str,
                      schema_name: Optional[str] = None) -> bool:
        """Check if column exists in table"""
        for col in self.get_columns(table_name, schema_name):
            if col['column_name'] == column_name:
                return True
        return False

    def get_primary_keys(self, table_name: str, schema_name: Optional[str] = None) -> List[str]:
        """Get primary key columns for a table"""
        pk_columns = []

        for col in self.get_columns(table_name, schema_name):
            if col.get('is_primary_key', False):
                pk_columns.append(col['column_name'])

        return pk_columns

    def get_data_type(self, table_name: str, column_name: str,
                      schema_name: Optional[str] = None) -> Optional[str]:
        """Get data type for a column"""
        for col in self.get_columns(table_name, schema_name):
            if col['column_name'] == column_name:
                return col.get('data_type', 'UNKNOWN')
        return None

    # -------------------------
    # FOREIGN KEY METHODS
    # -------------------------

    def get_foreign_keys(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict]:
        """Get foreign keys for a table"""
        fks = []

        for col in self.get_columns(table_name, schema_name):
            if col.get('is_foreign_key', False) and 'references_table' in col:
                fks.append({
                    'column': col['column_name'],
                    'references_table': col['references_table'],
                    'references_column': col['references_column'],
                    'references_schema': col.get('references_schema', col['schema_name'])
                })

        return fks

    def get_referenced_by(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict]:
        """Get tables that reference this table"""
        refs = []

        for col in self.schema.columns:
            if (col.get('references_table') == table_name and
                    col.get('is_foreign_key', False)):
                if schema_name is None or col.get('references_schema') == schema_name:
                    refs.append({
                        'from_table': col['table_name'],
                        'from_schema': col['schema_name'],
                        'from_column': col['column_name'],
                        'references_column': col['references_column']
                    })

        return refs

    def find_relationship(self, table1: str, table2: str,
                          schema1: Optional[str] = None,
                          schema2: Optional[str] = None) -> Optional[Dict]:
        """
        Find relationship between two tables in either direction
        Uses pre-cached relationships for faster lookup
        """
        # Check direct relationships (A -> B)
        for rel in self.schema.relationships:
            if (rel['from_table'] == table1 and rel['to_table'] == table2):
                if (schema1 is None or rel['from_schema'] == schema1) and \
                        (schema2 is None or rel['to_schema'] == schema2):
                    return {
                        'type': 'forward',
                        'from_table': table1,
                        'from_schema': rel['from_schema'],
                        'from_column': rel['from_column'],
                        'to_table': table2,
                        'to_schema': rel['to_schema'],
                        'to_column': rel['to_column']
                    }

            # Check reverse (B -> A)
            if (rel['from_table'] == table2 and rel['to_table'] == table1):
                if (schema2 is None or rel['from_schema'] == schema2) and \
                        (schema1 is None or rel['to_schema'] == schema1):
                    return {
                        'type': 'reverse',
                        'from_table': table1,
                        'from_schema': schema1 or rel['to_schema'],
                        'from_column': rel['to_column'],
                        'to_table': table2,
                        'to_schema': schema2 or rel['from_schema'],
                        'to_column': rel['from_column']
                    }

        return None

    def get_direct_relationships(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict]:
        """Get all tables directly related to this table"""
        related = []

        # Outgoing relationships
        for rel in self.schema.relationships:
            if rel['from_table'] == table_name:
                if schema_name is None or rel['from_schema'] == schema_name:
                    related.append({
                        'table': rel['to_table'],
                        'schema': rel['to_schema'],
                        'type': 'parent',
                        'via': {
                            'from_column': rel['from_column'],
                            'to_column': rel['to_column']
                        }
                    })

        # Incoming relationships
        for rel in self.schema.relationships:
            if rel['to_table'] == table_name:
                if schema_name is None or rel['to_schema'] == schema_name:
                    related.append({
                        'table': rel['from_table'],
                        'schema': rel['from_schema'],
                        'type': 'child',
                        'via': {
                            'from_column': rel['from_column'],
                            'to_column': rel['to_column']
                        }
                    })

        return related

    def get_all_relationships(self) -> List[Dict]:
        """Get all relationships"""
        return self.schema.relationships.copy()

    # -------------------------
    # CATEGORY METHODS
    # -------------------------

    def get_categories(self) -> List[str]:
        """Get all business categories"""
        return sorted(set(self.category_map.values()))

    def get_tables_by_category(self, category: str) -> List[Dict]:
        """Get all tables in a specific business category"""
        tables = []
        for table_info in self.schema.tables.values():
            if table_info.get('category') == category:
                tables.append(table_info.copy())
        return tables

    def get_schemas_by_category(self, category: str) -> List[str]:
        """Get all schemas in a specific business category"""
        schemas = set()
        for table_info in self.schema.tables.values():
            if table_info.get('category') == category:
                schemas.add(table_info['schema'])
        return sorted(schemas)

    # -------------------------
    # SEARCH METHODS
    # -------------------------

    def search_tables(self, pattern: str) -> List[Dict]:
        """Fast fuzzy search for tables matching pattern"""
        pattern = pattern.lower()
        results = []

        for info in self.schema.tables.values():
            if pattern in info['name'].lower() or pattern in info['schema'].lower():
                results.append(info.copy())

        return results

    def search_columns(self, pattern: str) -> List[Dict]:
        """Search for columns matching pattern"""
        pattern = pattern.lower()
        results = []

        for col in self.schema.columns:
            if pattern in col['column_name'].lower():
                results.append({
                    'table': col['table_name'],
                    'schema': col['schema_name'],
                    'column': col['column_name'],
                    'data_type': col['data_type']
                })

        return results

    # -------------------------
    # UTILITY METHODS
    # -------------------------

    def get_table_summary(self) -> pd.DataFrame:
        """Get summary of all tables"""
        summary = []

        for full_name, info in self.schema.tables.items():
            cols = self.get_columns(info['name'], info['schema'])
            pk_count = sum(1 for c in cols if c.get('is_primary_key', False))
            fk_count = sum(1 for c in cols if c.get('is_foreign_key', False))
            rel_count = len(self.get_direct_relationships(info['name'], info['schema']))

            summary.append({
                'table_name': info['name'],
                'schema': info['schema'],
                'category': info.get('category', 'Other'),
                'full_name': full_name,
                'columns': len(cols),
                'primary_keys': pk_count,
                'foreign_keys': fk_count,
                'relationships': rel_count
            })

        return pd.DataFrame(summary)

    def get_stats(self) -> Dict:
        """Get statistics about the database"""
        return {
            'total_tables': len(self.schema.tables),
            'total_columns': len(self.schema.columns),
            'total_relationships': len(self.schema.relationships),
            'schemas': len(self.schemas),
            'categories': len(self.get_categories()),
            'tables_by_schema': {s: len(t) for s, t in self.schema.tables_by_schema.items()},
            'tables_by_category': self._get_tables_by_category_count()
        }

    def get_schema_index(self) -> Dict[str, List[str]]:
        """
        Returns a lookup for the frontend to know which tables belong where
        Creates a map: {"gmhk": ["table1", "table2"], "pmm": ["table3"]}

        Returns:
            Dictionary with schema names as keys and lists of table names as values
        """
        schema_index = {}

        # Use the pre-cached tables_by_schema
        for schema, tables in self.schema.tables_by_schema.items():
            schema_index[schema] = [table['name'] for table in tables]

        return schema_index

    def get_schema_index_with_details(self) -> Dict[str, List[Dict]]:
        """
        Returns a detailed lookup with full table information
        Creates a map: {"gmhk": [{"name": "table1", "category": "..."}, ...]}

        Returns:
            Dictionary with schema names as keys and lists of table info dicts as values
        """
        schema_index = {}

        # Use the pre-cached tables_by_schema with full details
        for schema, tables in self.schema.tables_by_schema.items():
            schema_index[schema] = [table.copy() for table in tables]

        return schema_index

    def get_category_index(self) -> Dict[str, List[str]]:
        """
        Returns a lookup by category
        Creates a map: {"Personnel Management": ["pmm.employees", "pmm.departments"], ...}

        Returns:
            Dictionary with category names as keys and lists of qualified table names as values
        """
        category_index = {}

        for full_name, table_info in self.schema.tables.items():
            category = table_info.get('category', 'Other')
            if category not in category_index:
                category_index[category] = []
            category_index[category].append(table_info['full_name'])

        return category_index

    def get_table_count_by_schema(self) -> Dict[str, int]:
        """
        Returns count of tables per schema for quick stats

        Returns:
            Dictionary with schema names as keys and table counts as values
        """
        return {schema: len(tables) for schema, tables in self.schema.tables_by_schema.items()}

    def get_table_count_by_category(self) -> Dict[str, int]:
        """
        Returns count of tables per category for quick stats

        Returns:
            Dictionary with category names as keys and table counts as values
        """
        return self._get_tables_by_category_count()

    def _get_tables_by_category_count(self) -> Dict[str, int]:
        """Get count of tables per category"""
        counts = {}
        for table_info in self.schema.tables.values():
            category = table_info.get('category', 'Other')
            counts[category] = counts.get(category, 0) + 1
        return counts


# ========== QUERY VALIDATOR ==========

class QueryValidator:
    """
    Query Validator Module - Validates SQL queries against database schema
    Identifies errors and provides feedback for correction
    """

    def __init__(self, db_info: CSVDBInfo):
        """
        Initialize validator with database schema

        Args:
            db_info: CSVDBInfo instance with parsed schema
        """
        self.db_info = db_info
        self.errors = []
        self.warnings = []

    def validate_query_generator(self, query_gen) -> bool:
        """Validate a QueryGenerator instance"""
        self.errors = []
        self.warnings = []

        # Get metadata - assuming QueryGenerator has get_metadata method
        try:
            metadata = query_gen.get_metadata()
        except AttributeError:
            # Try to get attributes directly
            metadata = {
                'table': getattr(query_gen, 'table', ''),
                'schema': getattr(query_gen, 'schema', None),
                'selected_columns': getattr(query_gen, 'selected_columns', []),
                'conditions': getattr(query_gen, 'conditions', [])
            }

        # Check table existence
        if not self.db_info.table_exists(metadata.get('table', ''), metadata.get('schema')):
            self.errors.append(f"Table '{metadata.get('table', '')}' does not exist")
            return False

        # Check columns
        selected_columns = metadata.get('selected_columns', [])
        for col in selected_columns:
            if col == '*':
                continue

            # Handle aliases and aggregates
            col_name = self._extract_column_name(col)
            if col_name and not self.db_info.column_exists(
                    metadata.get('table', ''),
                    col_name,
                    metadata.get('schema')
            ):
                self.warnings.append(
                    f"Column '{col_name}' may not exist in table '{metadata.get('table', '')}'"
                )

        # Check WHERE conditions
        conditions = metadata.get('conditions', [])
        for cond in conditions:
            if isinstance(cond, (list, tuple)) and len(cond) > 0:
                col_name = cond[0]
                if not self.db_info.column_exists(
                        metadata.get('table', ''),
                        col_name,
                        metadata.get('schema')
                ):
                    self.errors.append(f"WHERE column '{col_name}' does not exist")

        return len(self.errors) == 0

    def validate_join_builder(self, join_builder) -> bool:
        """Validate a JoinBuilder instance"""
        self.errors = []
        self.warnings = []

        # Get preview data
        try:
            preview = join_builder.preview()
        except AttributeError:
            # If preview not available, try to get join info directly
            preview = self._extract_join_info(join_builder)

        # Check if all tables exist
        join_path = preview.get('join_path', [])
        for item in join_path:
            if item.get('type') == 'table':
                table = item.get('table', '')
                schema = item.get('schema')

                if not self.db_info.table_exists(table, schema):
                    self.errors.append(f"Table '{table}' does not exist")

            elif item.get('type') == 'join':
                # Check source table
                from_table = item.get('from_table', '')
                from_schema = item.get('from_schema')

                if not self.db_info.table_exists(from_table, from_schema):
                    self.errors.append(f"Join source table '{from_table}' does not exist")

                # Check target table
                to_table = item.get('to_table', '')
                to_schema = item.get('to_schema')

                if not self.db_info.table_exists(to_table, to_schema):
                    self.errors.append(f"Join target table '{to_table}' does not exist")

                # Check relationship
                if not self._validate_relationship(item):
                    self.warnings.append(
                        f"Relationship between {from_table}.{item.get('from_column', '')} "
                        f"and {to_table}.{item.get('to_column', '')} may not exist"
                    )

        # Check selected columns
        selected_columns = preview.get('selected_columns', [])
        table_aliases = preview.get('table_aliases', {})

        for col_info in selected_columns:
            if col_info.get('type') == 'aggregate':
                # Skip aggregate validation for now
                continue

            table = col_info.get('table')
            column = col_info.get('column')

            if column == '*':
                continue

            # Find table info
            table_info = self._find_table_info(table, table_aliases)
            if not table_info:
                self.errors.append(f"Table alias '{table}' not found")
                continue

            if not self.db_info.column_exists(
                    table_info.get('table', ''),
                    column,
                    table_info.get('schema')
            ):
                self.errors.append(
                    f"Column '{column}' does not exist in table '{table_info.get('table', '')}'"
                )

        return len(self.errors) == 0

    def validate_cte_builder(self, cte_builder) -> bool:
        """Validate a CTEBuilder instance"""
        self.errors = []
        self.warnings = []

        try:
            metadata = cte_builder.get_metadata()
        except AttributeError:
            # If get_metadata not available, try to extract info
            metadata = {
                'stage_names': getattr(cte_builder, 'stage_names', []),
                'has_final_query': hasattr(cte_builder, 'final_query')
            }

        # Check for circular references (simplified)
        stage_names = metadata.get('stage_names', [])

        # Could add more validation here

        return len(self.errors) == 0

    def validate_sql(self, sql: str) -> bool:
        """
        Validate raw SQL string

        This is a simplified validation - in production would use a SQL parser
        """
        self.errors = []
        self.warnings = []

        # Check for basic SQL structure
        sql_upper = sql.upper().strip()

        if not sql_upper:
            self.errors.append("Empty SQL query")
            return False

        if 'SELECT' not in sql_upper:
            self.errors.append("Query must contain SELECT")
            return False

        if 'FROM' not in sql_upper:
            self.errors.append("Query must contain FROM")
            return False

        # Extract table names (simplified)
        tables = self._extract_table_names(sql)

        for table in tables:
            if not self.db_info.table_exists(table):
                self.warnings.append(f"Table '{table}' may not exist")

        return len(self.errors) == 0

    def _extract_column_name(self, col_str: str) -> str:
        """Extract column name from SELECT expression"""
        col_str = str(col_str)

        # Remove alias
        if ' AS ' in col_str.upper():
            col_str = col_str.split(' AS ')[0]

        # Remove aggregate functions
        if '(' in col_str and ')' in col_str:
            match = re.search(r'\(([^)]+)\)', col_str)
            if match:
                col_str = match.group(1)

        # Remove table alias
        if '.' in col_str:
            col_str = col_str.split('.')[-1]

        return col_str.strip()

    def _validate_relationship(self, join_item: Dict) -> bool:
        """Validate that a relationship exists"""
        rel = self.db_info.find_relationship(
            join_item.get('from_table', ''),
            join_item.get('to_table', ''),
            join_item.get('from_schema'),
            join_item.get('to_schema')
        )

        if not rel:
            return False

        # Check if the columns match
        return (rel.get('from_column') == join_item.get('from_column') and
                rel.get('to_column') == join_item.get('to_column'))

    def _find_table_info(self, alias: str, table_aliases: Dict) -> Optional[Dict]:
        """Find table info by alias"""
        return table_aliases.get(alias)

    def _extract_table_names(self, sql: str) -> List[str]:
        """Extract table names from SQL (simplified)"""
        tables = []

        # Find FROM clause
        from_match = re.search(r'FROM\s+([^\s;]+)', sql, re.IGNORECASE)
        if from_match:
            table_name = from_match.group(1).strip()
            tables.append(table_name)

        # Find JOIN clauses
        join_matches = re.findall(r'JOIN\s+([^\s]+)', sql, re.IGNORECASE)
        tables.extend([j.strip() for j in join_matches])

        # Remove schema prefixes and clean
        cleaned_tables = []
        for t in tables:
            # Remove any trailing punctuation
            t = re.sub(r'[;,]', '', t)

            # Extract just the table name if schema qualified
            if '.' in t:
                t = t.split('.')[-1]

            # Remove quotes if present
            t = t.strip('"`')

            cleaned_tables.append(t)

        return cleaned_tables

    def _extract_join_info(self, join_builder) -> Dict:
        """Extract join information from a JoinBuilder instance"""
        # Default structure
        info = {
            'join_path': [],
            'selected_columns': [],
            'table_aliases': {}
        }

        # Try to get joins if available
        if hasattr(join_builder, 'joins'):
            for join in join_builder.joins:
                info['join_path'].append({
                    'type': 'join',
                    'from_table': getattr(join, 'from_table', ''),
                    'from_schema': getattr(join, 'from_schema', None),
                    'from_column': getattr(join, 'from_column', ''),
                    'to_table': getattr(join, 'to_table', ''),
                    'to_schema': getattr(join, 'to_schema', None),
                    'to_column': getattr(join, 'to_column', '')
                })

        # Try to get tables
        if hasattr(join_builder, 'tables'):
            for table in join_builder.tables:
                info['join_path'].append({
                    'type': 'table',
                    'table': getattr(table, 'name', str(table)),
                    'schema': getattr(table, 'schema', None)
                })

        return info

    def get_errors(self) -> List[str]:
        """Get validation errors"""
        return self.errors

    def get_warnings(self) -> List[str]:
        """Get validation warnings"""
        return self.warnings

    def clear(self):
        """Clear errors and warnings"""
        self.errors = []
        self.warnings = []


# ========== WRAPPER FUNCTIONS FOR COMPATIBILITY ==========

_validator_instance = None
_default_db_info = None


def _get_default_db_info(csv_file_path: str = None):
    """Get or create a default CSVDBInfo instance"""
    global _default_db_info
    if _default_db_info is None:
        if csv_file_path:
            _default_db_info = CSVDBInfo(csv_file_path)
        else:
            # Try to find CSV file
            import os
            if os.path.exists("master_db_schema.csv"):
                _default_db_info = CSVDBInfo("master_db_schema.csv")
            elif os.path.exists("master_db_schema.csv"):
                _default_db_info = CSVDBInfo("master_db_schema.csv")
            else:
                _default_db_info = CSVDBInfo()  # Use test data
    return _default_db_info


def _get_validator_instance(csv_file_path: str = None):
    """Get or create the validator instance"""
    global _validator_instance
    if _validator_instance is None:
        _validator_instance = QueryValidator(_get_default_db_info(csv_file_path))
    return _validator_instance


def validate(query_or_obj, csv_file_path: str = None):
    """
    Wrapper function to validate a query
    Compatible with test.py

    Args:
        query_or_obj: SQL string, QueryGenerator, JoinBuilder, or CTEBuilder
        csv_file_path: Optional path to CSV schema file

    Returns:
        bool: True if valid, False otherwise
    """
    validator = _get_validator_instance(csv_file_path)
    validator.clear()

    # Check the type and validate accordingly
    if isinstance(query_or_obj, str):
        return validator.validate_sql(query_or_obj)
    elif hasattr(query_or_obj, '__class__'):
        class_name = query_or_obj.__class__.__name__
        if class_name in ['QueryGenerator', 'QueryGenerator']:
            return validator.validate_query_generator(query_or_obj)
        elif class_name in ['JoinBuilder', 'JoinBuilder']:
            return validator.validate_join_builder(query_or_obj)
        elif class_name in ['CTEBuilder', 'CTEBuilder']:
            return validator.validate_cte_builder(query_or_obj)

    # Default fallback
    return False


def validate_sql(sql: str, csv_file_path: str = None) -> bool:
    """
    Wrapper function to validate SQL string
    """
    return validate(sql, csv_file_path)


def get_validation_errors():
    """
    Get validation errors from last validation
    """
    validator = _get_validator_instance()
    return validator.get_errors()


def get_validation_warnings():
    """
    Get validation warnings from last validation
    """
    validator = _get_validator_instance()
    return validator.get_warnings()


def reset_validator():
    """
    Reset the validator instance
    """
    global _validator_instance
    _validator_instance = None
    return True


def set_csv_schema(csv_file_path: str):
    """
    Set the CSV schema file to use for validation

    Args:
        csv_file_path: Path to the CSV schema file
    """
    global _default_db_info, _validator_instance
    _default_db_info = CSVDBInfo(csv_file_path)
    _validator_instance = QueryValidator(_default_db_info)
    return True

# ========== COMPATIBILITY LAYER ==========
# This provides backward compatibility for code that still imports DBInfo

class DBInfo(CSVDBInfo):
    """
    Compatibility class for code that expects DBInfo
    Inherits all functionality from CSVDBInfo
    """
    pass


# Also export CSVDBInfo as DBInfo for direct imports
# This allows both "from db_information import CSVDBInfo" and "from db_information import DBInfo" to work
__all__ = ['CSVDBInfo', 'DBInfo', 'CSVSchema', 'QueryValidator']
# ========== MAIN DEMO ==========

if __name__ == "__main__":
    # Demo usage
    print("=" * 60)
    print("CSV-BASED QUERY VALIDATOR")
    print("=" * 60)

    # Initialize with CSV file
    validator = QueryValidator(_get_default_db_info())

    print(f"\n✅ Validator initialized")
    print(f"📊 Tables loaded: {len(validator.db_info.get_tables())}")
    print(f"📁 Schemas found: {validator.db_info.get_schemas()}")
    print(f"📂 Categories: {validator.db_info.get_categories()}")

    # Test table existence
    test_table = "gmhk_appointment"
    exists = validator.db_info.table_exists(test_table)
    print(f"\n📋 Table '{test_table}' exists: {exists}")

    if exists:
        columns = validator.db_info.get_columns(test_table)
        print(f"   Columns: {[c['column_name'] for c in columns[:5]]}...")

    # Test categories
    print(f"\n📂 Categories: {validator.db_info.get_categories()}")
    for category in validator.db_info.get_categories()[:2]:  # Show first 2 categories
        tables = validator.db_info.get_tables_by_category(category)
        print(f"   {category}: {len(tables)} tables")

    # Test SQL validation
    print("\n" + "=" * 60)
    print("SQL VALIDATION TEST")
    print("=" * 60)

    valid_sql = "SELECT * FROM gmhk_appointment WHERE emp_no = 100"
    invalid_sql = "SELECT * FROM non_existent_table"

    print(f"\nValid SQL: {valid_sql}")
    is_valid = validator.validate_sql(valid_sql)
    print(f"Result: {'✅ VALID' if is_valid else '❌ INVALID'}")
    if validator.get_warnings():
        print(f"Warnings: {validator.get_warnings()}")

    validator.clear()

    print(f"\nInvalid SQL: {invalid_sql}")
    is_valid = validator.validate_sql(invalid_sql)
    print(f"Result: {'✅ VALID' if is_valid else '❌ INVALID'}")
    if validator.get_errors():
        print(f"Errors: {validator.get_errors()}")