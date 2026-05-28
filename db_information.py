"""
db_information.py
Database Information Module - Loads schema from JSON file
"""

import json
import os
from typing import Dict, List, Optional, Any, Set, Tuple
from collections import deque
import pathlib


# Resolve default JSON path relative to this file
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_JSON_PATH = os.path.join(_THIS_DIR, "db_files", "metadata.json")


class CSVDBInfo:
    """
    Database Information Module - Loads schema from JSON file
    Provides metadata for the SQL Query Generator API
    """

    def __init__(self, json_file_path: str = _DEFAULT_JSON_PATH):
        """
        Load schema from JSON file

        Args:
            json_file_path: Path to the JSON schema file (metadata.json)
        """
        self.json_file_path = json_file_path

        # Simple storage structures
        self.schemas: Set[str] = set()
        self.tables: Dict[str, List[str]] = {}
        self.columns: Dict[str, List[Dict]] = {}
        self.table_full_names: Dict[str, str] = {}
        self.table_schema: Dict[str, str] = {}

        # Category mapping
        self.category_map = {
            "GM": "General Management",
            "HM": "Healthcare Management",
            "PM": "Personnel Management",
            "SI": "Stores & Inventory",
            "SA": "Security & Administration",
            "TA": "Traffic & Accounts",
        }

        # Target schemas
        self.target_schemas = {'GM', 'HM', 'PM', 'SI', 'SA', 'TA'}

        # Store raw data
        self.raw_tables: Dict[str, Dict] = {}
        self.relationships: List[Dict] = []
        self.primary_keys: Dict[str, List[str]] = {}
        self.foreign_keys: Dict[str, List[Dict]] = {}
        self.composite_keys: Dict[str, List[str]] = {}

        # Track counts
        self.loaded_tables_count = 0
        self.all_tables_in_json = {}

        # Check if JSON file exists
        if json_file_path and os.path.exists(json_file_path):
            try:
                print(f"📂 Loading schema from: {json_file_path}")
                self._load_all_tables_from_json(json_file_path)
                print(f"✅ Successfully loaded {self.loaded_tables_count} tables")
                self._print_load_summary()
            except Exception as e:
                print(f"⚠️ Error loading JSON file: {e}")
                import traceback
                traceback.print_exc()
                print("⚠️ Falling back to test data")
                self._init_test_data()
        else:
            print(f"⚠️ JSON file not found: {json_file_path}")
            print("⚠️ Using test data")
            self._init_test_data()

        print(f"\n📊 Final Stats:")
        print(f"   - {len(self.schemas)} schemas")
        print(f"   - {self.loaded_tables_count} total tables")
        print(f"   - {len(self.relationships)} relationships")

    def _load_all_tables_from_json(self, json_file_path: str):
        """Load ALL tables from JSON file"""
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # First, count all tables in JSON
        total_json_tables = 0
        for schema_name, schema_data in data.items():
            if schema_name in self.target_schemas:
                total_json_tables += len(schema_data)
                self.all_tables_in_json[schema_name] = len(schema_data)

        print(f"📊 Found {total_json_tables} tables in JSON (target schemas)")

        # Process each schema
        for schema_name, schema_data in data.items():
            if schema_name not in self.target_schemas:
                continue

            print(f"  Loading schema: {schema_name} ({len(schema_data)} tables)")
            self.schemas.add(schema_name)
            self.tables[schema_name] = []

            # Process each table in this schema
            for table_name, table_info in schema_data.items():
                full_name = f"{schema_name}.{table_name}"

                # Store raw table data
                self.raw_tables[full_name] = table_info

                # Add to schema tables list
                self.tables[schema_name].append(table_name)

                # Store mapping
                self.table_full_names[table_name] = full_name
                self.table_schema[table_name] = schema_name

                # Process columns
                columns_list = table_info.get('columns', [])
                keys_info = table_info.get('keys', {})

                table_columns = []
                table_pks = []
                table_fks = []

                # Process each column
                for column_name in columns_list:
                    column_info = {
                        'column_name': column_name,
                        'data_type': self._infer_data_type(column_name, table_name),
                        'is_primary_key': False,
                        'is_foreign_key': False,
                        'is_nullable': True
                    }

                    # Check if column is in keys
                    if column_name in keys_info:
                        key_details = keys_info[column_name]
                        key_type = key_details.get('type', '')

                        if key_type == 'PRIMARY KEY':
                            column_info['is_primary_key'] = True
                            table_pks.append(column_name)

                        # Check foreign key
                        foreign_table = key_details.get('foreign_table')
                        foreign_column = key_details.get('foreign_column')

                        if foreign_table and foreign_table != '-':
                            column_info['is_foreign_key'] = True
                            column_info['references_table'] = foreign_table
                            column_info['references_column'] = foreign_column

                            # Find schema for referenced table
                            ref_schema = self._find_schema_for_table(foreign_table, data)

                            # Store relationship
                            rel = {
                                'from_schema': schema_name,
                                'from_table': table_name,
                                'from_column': column_name,
                                'to_schema': ref_schema or schema_name,
                                'to_table': foreign_table,
                                'to_column': foreign_column or column_name
                            }

                            if rel not in self.relationships:
                                self.relationships.append(rel)

                            table_fks.append({
                                'column': column_name,
                                'references_table': foreign_table,
                                'references_column': foreign_column or column_name,
                                'references_schema': ref_schema or schema_name
                            })

                    table_columns.append(column_info)

                # Store columns
                self.columns[full_name] = table_columns

                # Store primary keys
                if table_pks:
                    self.primary_keys[full_name] = table_pks
                    if len(table_pks) > 1:
                        self.composite_keys[full_name] = table_pks

                # Store foreign keys
                if table_fks:
                    self.foreign_keys[full_name] = table_fks

                self.loaded_tables_count += 1

        # Add additional relationships based on common column names
        self._add_common_column_relationships()

    def _find_schema_for_table(self, table_name: str, data: Dict) -> Optional[str]:
        """Find which schema contains a given table name"""
        for schema_name, schema_data in data.items():
            if schema_name in self.target_schemas and table_name in schema_data:
                return schema_name
        return None

    def _add_common_column_relationships(self):
        """Add relationships based on common column names (optimized).
        
        Instead of creating O(n²) pairs, we only link each table to the 
        first table found containing that column (star topology),
        keeping the relationship count manageable.
        """
        common_columns = ['emp_no', 'complaint_no', 'case_reg_no', 'dept_cd', 'desig_cd']

        existing = set()
        for rel in self.relationships:
            existing.add((rel['from_table'], rel['to_table'], rel['from_column']))

        for col_name in common_columns:
            tables_with_col = []
            for full_name, columns in self.columns.items():
                for col in columns:
                    if col['column_name'] == col_name:
                        schema, table = full_name.split('.')
                        tables_with_col.append((schema, table))
                        break  # column found in this table, move on

            if len(tables_with_col) < 2:
                continue

            # Star topology: link every table to the first one (hub)
            hub_schema, hub_table = tables_with_col[0]
            for schema, table in tables_with_col[1:]:
                if table == hub_table:
                    continue
                key = (table, hub_table, col_name)
                rev_key = (hub_table, table, col_name)
                if key in existing or rev_key in existing:
                    continue

                self.relationships.append({
                    'from_schema': schema,
                    'from_table': table,
                    'from_column': col_name,
                    'to_schema': hub_schema,
                    'to_table': hub_table,
                    'to_column': col_name
                })
                existing.add(key)

    def _infer_data_type(self, column_name: str, table_name: str) -> str:
        """Infer data type from column name patterns"""
        column_lower = column_name.lower()

        if any(x in column_lower for x in ['date', 'dt', 'timestamp', 'time_stamp']):
            return 'TIMESTAMP'
        elif any(x in column_lower for x in ['no', 'num', 'count', 'qty', 'amount', 'amt']):
            return 'NUMBER'
        elif any(x in column_lower for x in ['flag', 'status', 'ind', 'type']):
            return 'VARCHAR(1)'
        elif 'id' in column_lower or 'code' in column_lower or 'cd' in column_lower:
            return 'VARCHAR(20)'
        elif 'file' in column_lower or 'blob' in column_lower:
            return 'BLOB'
        elif 'remarks' in column_lower or 'desc' in column_lower:
            return 'VARCHAR(500)'
        elif 'name' in column_lower:
            return 'VARCHAR(100)'
        else:
            return 'VARCHAR(100)'

    def _print_load_summary(self):
        """Print summary of what was loaded"""
        print("\n" + "=" * 60)
        print("LOAD SUMMARY")
        print("=" * 60)

        for schema in sorted(self.schemas):
            tables = self.tables.get(schema, [])
            json_count = self.all_tables_in_json.get(schema, 0)
            print(f"  {schema}: {len(tables)}/{json_count} tables loaded")

        print(f"\n  Total tables loaded: {self.loaded_tables_count}")
        print(f"  Total relationships: {len(self.relationships)}")

    def _init_test_data(self):
        """Fallback test data"""
        test_schemas = ['GM', 'HM', 'PM', 'SI', 'SA', 'TA']

        for schema in test_schemas:
            self.schemas.add(schema)
            self.tables[schema] = []

        # Sample tables for GM
        gm_tables = ['gmtk_coms_hdr', 'gmtk_fwd_dtl', 'gmhk_appointment', 'gmtk_dms_blob']
        for table in gm_tables:
            self.tables['GM'].append(table)
            full_name = f"GM.{table}"
            self.table_full_names[table] = full_name
            self.table_schema[table] = 'GM'
            self.columns[full_name] = [
                {'column_name': 'complaint_no', 'data_type': 'VARCHAR', 'is_primary_key': True, 'is_foreign_key': False},
                {'column_name': 'emp_no', 'data_type': 'VARCHAR', 'is_primary_key': False, 'is_foreign_key': True},
                {'column_name': 'status', 'data_type': 'VARCHAR', 'is_primary_key': False, 'is_foreign_key': False},
                {'column_name': 'reg_date', 'data_type': 'DATE', 'is_primary_key': False, 'is_foreign_key': False}
            ]
            self.primary_keys[full_name] = ['complaint_no']
            self.loaded_tables_count += 1

        # Sample tables for HM
        hm_tables = ['hmt_case_reg', 'hmt_cln_exam', 'hmt_lab_reg', 'hmt_cert_hdr']
        for table in hm_tables:
            self.tables['HM'].append(table)
            full_name = f"HM.{table}"
            self.table_full_names[table] = full_name
            self.table_schema[table] = 'HM'
            self.columns[full_name] = [
                {'column_name': 'case_reg_no', 'data_type': 'VARCHAR', 'is_primary_key': True, 'is_foreign_key': False},
                {'column_name': 'emp_no', 'data_type': 'VARCHAR', 'is_primary_key': False, 'is_foreign_key': True},
                {'column_name': 'patient_name', 'data_type': 'VARCHAR', 'is_primary_key': False, 'is_foreign_key': False},
                {'column_name': 'visit_no', 'data_type': 'INTEGER', 'is_primary_key': False, 'is_foreign_key': False}
            ]
            self.primary_keys[full_name] = ['case_reg_no']
            self.loaded_tables_count += 1

        # Add relationships
        self._add_test_relationships()

    def _add_test_relationships(self):
        """Add test relationships"""
        for table in ['hmt_case_reg', 'hmt_cln_exam', 'hmt_lab_reg']:
            self.relationships.append({
                'from_schema': 'HM',
                'from_table': table,
                'from_column': 'emp_no',
                'to_schema': 'PM',
                'to_table': 'pmm_employee',
                'to_column': 'emp_no'
            })

    # ========== SCHEMA INFORMATION METHODS ==========

    def get_schemas(self) -> List[str]:
        """Get all schema names"""
        return sorted(list(self.schemas))

    def get_tables(self, schema_name: Optional[str] = None) -> List[str]:
        """Get all table names, optionally filtered by schema"""
        if schema_name:
            return self.tables.get(schema_name, [])
        all_tables = []
        for tables in self.tables.values():
            all_tables.extend(tables)
        return sorted(all_tables)

    def get_tables_with_schema(self, schema_name: Optional[str] = None) -> List[Dict]:
        """Get tables with schema info"""
        if schema_name:
            tables = self.tables.get(schema_name, [])
            return [{'name': t, 'schema': schema_name, 'full_name': f"{schema_name}.{t}",
                     'category': self._get_category(schema_name)} for t in tables]
        result = []
        for schema, tables in self.tables.items():
            for table in tables:
                result.append({'name': table, 'schema': schema, 'full_name': f"{schema}.{table}",
                               'category': self._get_category(schema)})
        return result

    def table_exists(self, table_name: str, schema_name: Optional[str] = None) -> bool:
        """Check if table exists"""
        if schema_name:
            return table_name in self.tables.get(schema_name, [])
        return table_name in self.table_full_names

    def get_full_table_name(self, table_name: str, schema_name: Optional[str] = None) -> str:
        """Get fully qualified table name"""
        if schema_name:
            return f"{schema_name}.{table_name}"
        return self.table_full_names.get(table_name, table_name)

    def get_table_info(self, table_name: str, schema_name: Optional[str] = None) -> Dict:
        """Get full table information"""
        full_name = self.get_full_table_name(table_name, schema_name)
        schema = schema_name or self.table_schema.get(table_name, 'unknown')
        return {
            'name': table_name,
            'schema': schema,
            'full_name': full_name,
            'category': self._get_category(schema),
            'columns': self.get_columns(table_name, schema_name),
            'primary_keys': self.get_primary_keys(table_name, schema_name),
            'foreign_keys': self.get_foreign_keys(table_name, schema_name),
            'has_composite_key': len(self.get_primary_keys(table_name, schema_name)) > 1
        }

    def get_columns(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict]:
        """Get column details for a table"""
        if schema_name:
            full_name = f"{schema_name}.{table_name}"
            if full_name in self.columns:
                return self.columns[full_name].copy()
        full_name = self.table_full_names.get(table_name)
        if full_name and full_name in self.columns:
            return self.columns[full_name].copy()
        return []

    def get_column_names(self, table_name: str, schema_name: Optional[str] = None) -> List[str]:
        """Get column names for a table"""
        columns = self.get_columns(table_name, schema_name)
        return [col['column_name'] for col in columns]

    def column_exists(self, table_name: str, column_name: str, schema_name: Optional[str] = None) -> bool:
        """Check if column exists in table"""
        for col in self.get_columns(table_name, schema_name):
            if col['column_name'] == column_name:
                return True
        return False

    def get_primary_keys(self, table_name: str, schema_name: Optional[str] = None) -> List[str]:
        """Get primary key columns for a table"""
        full_name = self.get_full_table_name(table_name, schema_name)
        return self.primary_keys.get(full_name, [])

    def get_foreign_keys(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict]:
        """Get foreign keys for a table"""
        full_name = self.get_full_table_name(table_name, schema_name)
        return self.foreign_keys.get(full_name, [])

    def get_referenced_by(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict]:
        """Get tables that reference this table"""
        refs = []
        for rel in self.relationships:
            if rel['to_table'] == table_name:
                if schema_name is None or rel['to_schema'] == schema_name:
                    refs.append({
                        'from_table': rel['from_table'],
                        'from_schema': rel['from_schema'],
                        'from_column': rel['from_column'],
                        'references_column': rel['to_column']
                    })
        return refs

    def find_relationship(self, table1: str, table2: str, schema1: Optional[str] = None, schema2: Optional[str] = None) -> Optional[Dict]:
        """Find relationship between two tables"""
        for rel in self.relationships:
            if (rel['from_table'] == table1 and rel['to_table'] == table2):
                if (schema1 is None or rel['from_schema'] == schema1) and (schema2 is None or rel['to_schema'] == schema2):
                    return {
                        'type': 'forward',
                        'from_table': table1,
                        'from_schema': rel['from_schema'],
                        'from_column': rel['from_column'],
                        'to_table': table2,
                        'to_schema': rel['to_schema'],
                        'to_column': rel['to_column']
                    }
            if (rel['from_table'] == table2 and rel['to_table'] == table1):
                if (schema2 is None or rel['from_schema'] == schema2) and (schema1 is None or rel['to_schema'] == schema1):
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

    def find_join_path(self, table_tuples: List[Tuple[str, Optional[str]]]) -> List[Dict]:
        """
        Find a join path between multiple tables using BFS over the relationship graph.

        Args:
            table_tuples: List of (table_name, schema_name) tuples

        Returns:
            List of join step dicts with from/to table, column, schema info
        """
        if len(table_tuples) < 2:
            return []

        # Build adjacency list from relationships
        adjacency: Dict[str, List[Dict]] = {}
        for rel in self.relationships:
            key_from = rel['from_table']
            key_to = rel['to_table']

            if key_from not in adjacency:
                adjacency[key_from] = []
            adjacency[key_from].append({
                'to_table': key_to,
                'to_schema': rel['to_schema'],
                'from_column': rel['from_column'],
                'to_column': rel['to_column'],
                'from_schema': rel['from_schema'],
            })

            if key_to not in adjacency:
                adjacency[key_to] = []
            adjacency[key_to].append({
                'to_table': key_from,
                'to_schema': rel['from_schema'],
                'from_column': rel['to_column'],
                'to_column': rel['from_column'],
                'from_schema': rel['to_schema'],
            })

        # BFS from the first table to each subsequent table
        join_path = []
        visited_tables = {table_tuples[0][0]}

        for i in range(1, len(table_tuples)):
            target_table = table_tuples[i][0]
            target_schema = table_tuples[i][1]

            if target_table in visited_tables:
                continue

            # BFS
            queue = deque()
            # seed: all currently visited tables
            for vt in visited_tables:
                queue.append((vt, []))

            found = False
            bfs_visited = set(visited_tables)

            while queue and not found:
                current, path = queue.popleft()

                for edge in adjacency.get(current, []):
                    next_table = edge['to_table']
                    if next_table in bfs_visited:
                        continue

                    new_path = path + [{
                        'from_table': current,
                        'from_schema': edge.get('from_schema', self.table_schema.get(current)),
                        'from_alias': current,
                        'from_column': edge['from_column'],
                        'to_table': next_table,
                        'to_schema': edge['to_schema'],
                        'to_alias': next_table,
                        'to_column': edge['to_column'],
                        'join_type': 'INNER JOIN',
                        'relationship_type': 'forward'
                    }]

                    if next_table == target_table:
                        join_path.extend(new_path)
                        for step in new_path:
                            visited_tables.add(step['to_table'])
                        found = True
                        break

                    bfs_visited.add(next_table)
                    queue.append((next_table, new_path))

            if not found:
                # No path found — return empty so caller can handle
                return []

        return join_path

    def get_direct_relationships(self, table_name: str, schema_name: Optional[str] = None) -> List[Dict]:
        """Get all tables directly related to this table"""
        related = []
        for rel in self.relationships:
            if rel['from_table'] == table_name:
                if schema_name is None or rel['from_schema'] == schema_name:
                    related.append({
                        'table': rel['to_table'],
                        'schema': rel['to_schema'],
                        'type': 'parent',
                        'via': {'from_column': rel['from_column'], 'to_column': rel['to_column']}
                    })
            if rel['to_table'] == table_name:
                if schema_name is None or rel['to_schema'] == schema_name:
                    related.append({
                        'table': rel['from_table'],
                        'schema': rel['from_schema'],
                        'type': 'child',
                        'via': {'from_column': rel['from_column'], 'to_column': rel['to_column']}
                    })
        return related

    def get_all_relationships(self) -> List[Dict]:
        """Get all relationships"""
        return self.relationships.copy()

    def _get_category(self, schema: str) -> str:
        """Get business category for a schema"""
        return self.category_map.get(schema, "Other")

    def get_categories(self) -> List[str]:
        """Get all business categories"""
        return sorted(set(self.category_map.values()))

    def search_tables(self, pattern: str) -> List[Dict]:
        """Search for tables matching pattern"""
        pattern = pattern.lower()
        results = []
        for schema, tables in self.tables.items():
            for table in tables:
                if pattern in table.lower() or pattern in schema.lower():
                    results.append({
                        'name': table,
                        'schema': schema,
                        'full_name': f"{schema}.{table}",
                        'category': self._get_category(schema)
                    })
        return results

    def search_columns(self, pattern: str) -> List[Dict]:
        """Search for columns matching pattern"""
        pattern = pattern.lower()
        results = []
        for full_name, columns in self.columns.items():
            if '.' in full_name:
                schema, table = full_name.split('.', 1)
            else:
                schema, table = 'unknown', full_name
            for col in columns:
                if pattern in col['column_name'].lower():
                    results.append({
                        'table': table,
                        'schema': schema,
                        'column': col['column_name'],
                        'data_type': col.get('data_type', 'UNKNOWN')
                    })
        return results

    def get_stats(self) -> Dict:
        """Get statistics about the database"""
        total_columns = sum(len(cols) for cols in self.columns.values())
        return {
            'total_tables': len(self.table_full_names),
            'total_columns': total_columns,
            'total_relationships': len(self.relationships),
            'schemas': len(self.schemas),
            'target_schemas': list(self.target_schemas),
            'tables_by_schema': {s: len(t) for s, t in self.tables.items()},
            'composite_key_tables': len(self.composite_keys)
        }

    def get_schema_index(self) -> Dict[str, List[str]]:
        """Returns a lookup for frontend dropdown"""
        return {schema: tables.copy() for schema, tables in self.tables.items()}


# ========== HELPER FUNCTIONS ==========

def get_test_db_info() -> CSVDBInfo:
    """Get a test database info instance"""
    return CSVDBInfo()


def get_db_info_from_json(json_path: str) -> CSVDBInfo:
    """Get database info from JSON file"""
    return CSVDBInfo(json_path)


class DBInfo(CSVDBInfo):
    """Compatibility class for code that expects DBInfo"""
    pass


class QueryValidator:
    """Query Validator for API endpoints"""
    def __init__(self, db_info: CSVDBInfo):
        self.db_info = db_info
        self.errors = []
        self.warnings = []

    def validate_sql(self, sql: str) -> bool:
        self.errors = []
        self.warnings = []
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
        return len(self.errors) == 0

    def get_errors(self) -> List[str]:
        return self.errors

    def get_warnings(self) -> List[str]:
        return self.warnings

    def clear(self):
        self.errors = []
        self.warnings = []


if __name__ == "__main__":
    print("=" * 80)
    print("DATABASE INFORMATION MODULE")
    print("=" * 80)
    import sys
    json_path = None
    if len(sys.argv) > 1:
        json_path = sys.argv[1]
    db_info = CSVDBInfo(json_path) if json_path else CSVDBInfo()

    print(f"\nSchemas: {db_info.get_schemas()}")
    print(f"Total tables: {len(db_info.get_tables())}")
    print("\nTables per schema:")
    for schema, tables in db_info.get_schema_index().items():
        print(f"  {schema}: {len(tables)} tables")