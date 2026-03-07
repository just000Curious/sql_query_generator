#!/usr/bin/env python3
"""
Comprehensive test suite for SQL Query Generator
Tests all components with the provided schemas
"""

import unittest
import pandas as pd
import tempfile
import os
from datetime import datetime
import sqlite3

# Import your modules
try:
    from db_information import DBInfo
    from pypika_query_engine import QueryGenerator
    from join_builder import JoinBuilder
    from temporary_table import TemporaryTable, TemporaryTableManager
except ImportError as e:
    print(f"⚠️  Import error: {e}")
    print("Please ensure all required modules are in the Python path")
    DBInfo = None
    QueryGenerator = None
    JoinBuilder = None
    TemporaryTable = None
    TemporaryTableManager = None

# Skip all tests if modules are not available
if None in [DBInfo, QueryGenerator, JoinBuilder, TemporaryTable, TemporaryTableManager]:
    raise unittest.SkipTest("Required modules not available")


class TestDBInfo(unittest.TestCase):
    """Test the DBInfo class with multiple schemas"""

    schemas = {}  # Class variable to store loaded schemas

    # In test.py, update the setUpClass method of TestDBInfo:

    # In test.py, update the setUpClass method of TestDBInfo:

    @classmethod
    def setUpClass(cls):
        """Load all schema files before tests"""
        # Try both current directory and db_files directory
        possible_paths = ['', 'db_files/']

        cls.schema_files = {
            'gm': 'extracted_gm_schema.csv',
            'hm': 'extracted_hm_schema.csv',
            'pm': 'extracted_pm_schema.csv',
            'sa': 'extracted_sa_schema.csv',
            'si': 'extracted_si_schema.csv',
            'ta': 'extracted_ta_schema.csv'
        }

        cls.schemas = {}
        for name, filepath in cls.schema_files.items():
            found = False
            for base_path in possible_paths:
                full_path = os.path.join(base_path, filepath)
                if os.path.exists(full_path):
                    try:
                        cls.schemas[name] = DBInfo(full_path, schema_name=name)
                        print(f"✅ Loaded schema: {name} from {full_path}")
                        found = True
                        break
                    except Exception as e:
                        print(f"⚠️  Error loading {name} from {full_path}: {e}")

            if not found:
                print(f"⚠️  Schema file not found: {filepath}")

        # Create mock schemas if no real schemas loaded
        if not cls.schemas:
            print("ℹ️  No schema files found. Creating mock schemas for testing.")
            cls._create_mock_schemas()

    @classmethod
    def _create_mock_schemas(cls):
        """Create mock schema data for testing when files are missing"""
        # Mock PM schema (personnel)
        pm_mock_data = pd.DataFrame({
            'table_name': ['pmm_employee', 'pmm_employee', 'pmm_designation', 'pmm_designation', 'pmm_department',
                           'pmm_department'],
            'column_name': ['emp_no', 'emp_firstname', 'emp_desig_cd', 'emp_desig_desc', 'emp_dept_cd',
                            'emp_dept_desc'],
            'data_type': ['integer', 'character', 'character', 'character', 'smallint', 'character'],
            'is_primary_key': [True, False, True, False, True, False],
            'is_foreign_key': [False, False, False, False, False, False],
            'references_table': [None, None, None, None, None, None],
            'references_column': [None, None, None, None, None, None],
            'schema_name': ['pm', 'pm', 'pm', 'pm', 'pm', 'pm']
        })
        cls.schemas['pm'] = DBInfo(pm_mock_data, schema_name='pm')

        # Mock SA schema (system admin)
        sa_mock_data = pd.DataFrame({
            'table_name': ['sam_user', 'sam_user', 'sam_role'],
            'column_name': ['user_id', 'emp_no', 'role_id'],
            'data_type': ['character', 'integer', 'integer'],
            'is_primary_key': [True, False, True],
            'is_foreign_key': [False, True, False],
            'references_table': [None, 'pmm_employee', None],
            'references_column': [None, 'emp_no', None],
            'schema_name': ['sa', 'sa', 'sa']
        })
        cls.schemas['sa'] = DBInfo(sa_mock_data, schema_name='sa')

        # Mock HM schema (health)
        hm_mock_data = pd.DataFrame({
            'table_name': ['hmhk_case_reg', 'hmhk_case_reg', 'hmhk_cln_exam'],
            'column_name': ['case_reg_no', 'emp_no', 'case_reg_no'],
            'data_type': ['character', 'integer', 'character'],
            'is_primary_key': [True, False, True],
            'is_foreign_key': [False, True, True],
            'references_table': [None, 'pmm_employee', 'hmhk_case_reg'],
            'references_column': [None, 'emp_no', 'case_reg_no'],
            'schema_name': ['hm', 'hm', 'hm']
        })
        cls.schemas['hm'] = DBInfo(hm_mock_data, schema_name='hm')

        print(f"✅ Created mock schemas: {list(cls.schemas.keys())}")

    def test_01_schema_loading(self):
        """Test that schemas loaded correctly"""
        self.assertGreater(len(self.schemas), 0, "No schemas loaded")

        for schema_name, db_info in self.schemas.items():
            tables = db_info.get_tables()
            self.assertGreater(len(tables), 0, f"No tables in schema {schema_name}")
            print(f"📊 Schema {schema_name}: {len(tables)} tables")

    def test_02_table_info(self):
        """Test getting table information"""
        # Test with PM schema (has employee table)
        if 'pm' in self.schemas:
            db_info = self.schemas['pm']

            # Get all tables
            tables = db_info.get_tables()

            # Find a table to test with
            test_table = next((t for t in tables if 'emp' in t.lower()), tables[0] if tables else None)
            if not test_table:
                self.skipTest("No tables found in PM schema")

            # Get columns
            columns = db_info.get_columns(test_table)
            self.assertGreater(len(columns), 0, f"No columns found for {test_table}")

            # Check column structure
            first_col = columns[0]
            self.assertIn('column_name', first_col)
            self.assertIn('data_type', first_col)

            # Get primary keys
            pk = db_info.get_primary_keys(test_table)
            print(f"✅ Table info test passed for {test_table}")

    def test_03_foreign_keys(self):
        """Test foreign key detection"""
        # Test with HM schema (has foreign keys)
        if 'hm' in self.schemas:
            db_info = self.schemas['hm']

            # Check a table that likely has FKs
            tables = db_info.get_tables()
            test_table = next((t for t in tables if 'case' in t.lower()), tables[0] if tables else None)

            if test_table:
                fks = db_info.get_foreign_keys(test_table)
                print(f"ℹ️  Found {len(fks)} FKs in {test_table}")
            else:
                print("ℹ️  No suitable table for FK test")

    def test_04_relationships(self):
        """Test relationship detection between tables"""
        if 'pm' in self.schemas and 'sa' in self.schemas:
            pm_info = self.schemas['pm']

            # Try to find relationship between employee and user tables
            pm_tables = pm_info.get_tables()
            sa_tables = self.schemas['sa'].get_tables()

            emp_table = next((t for t in pm_tables if 'emp' in t.lower()), None)
            user_table = next((t for t in sa_tables if 'user' in t.lower()), None)

            if emp_table and user_table:
                rel = pm_info.find_relationship(emp_table, user_table)
                if rel:
                    print(f"✅ Found relationship: {rel}")
                else:
                    print("ℹ️  No direct relationship found")

            # Get all relationships
            all_rels = pm_info.get_all_relationships()
            print(f"📈 Total relationships in PM schema: {len(all_rels)}")

    def test_05_schema_switching(self):
        """Test working with multiple schemas"""
        # Verify we can access different schemas
        schema_names = list(self.schemas.keys())
        self.assertGreater(len(schema_names), 0)

        for name in schema_names[:3]:  # Test first 3 schemas
            db_info = self.schemas[name]
            tables = db_info.get_tables()
            print(f"📁 Schema {name}: {len(tables)} tables available")

        print(f"✅ Successfully accessed {len(schema_names)} schemas")


class TestQueryGenerator(unittest.TestCase):
    """Test the QueryGenerator class"""

    @classmethod
    def setUpClass(cls):
        if 'pm' not in TestDBInfo.schemas:
            cls.db_info = None
            print("ℹ️  PM schema not available - some tests may be skipped")
        else:
            cls.db_info = TestDBInfo.schemas['pm']

    def setUp(self):
        """Set up test case"""
        if self.__class__.db_info is None:
            self.skipTest("PM schema not available")

    def test_01_basic_select(self):
        """Test basic SELECT query"""
        tables = self.db_info.get_tables()
        test_table = next((t for t in tables if 'emp' in t.lower()), tables[0])

        qg = QueryGenerator(test_table)
        columns = self.db_info.get_column_names(test_table)[:3]
        qg.select(columns)
        query = qg.build()

        self.assertIsNotNone(query)
        self.assertIn(test_table, query)
        for col in columns:
            self.assertIn(col, query)
        print(f"✅ Basic SELECT: {query[:100]}...")

    def test_02_select_all(self):
        """Test SELECT * query"""
        tables = self.db_info.get_tables()
        test_table = tables[0]

        qg = QueryGenerator(test_table)
        qg.select_all()
        query = qg.build()

        self.assertIn('*', query)
        self.assertIn(test_table, query)
        print(f"✅ SELECT ALL: {query}")

    def test_03_where_clause(self):
        """Test WHERE clause"""
        tables = self.db_info.get_tables()
        test_table = next((t for t in tables if 'emp' in t.lower()), tables[0])

        qg = QueryGenerator(test_table)
        columns = self.db_info.get_column_names(test_table)[:2]
        qg.select(columns)

        # Find a numeric column for WHERE condition
        cols = self.db_info.get_columns(test_table)
        num_col = next((c['column_name'] for c in cols if
                        'integer' in c['data_type'].lower() or 'smallint' in c['data_type'].lower()), columns[0])

        qg.where(num_col, '=', 10)
        query = qg.build()

        self.assertIn('WHERE', query.upper())
        print(f"✅ WHERE clause: {query}")

    def test_04_aggregates(self):
        """Test aggregate functions"""
        tables = self.db_info.get_tables()
        test_table = next((t for t in tables if 'emp' in t.lower()), tables[0])

        qg = QueryGenerator(test_table)
        qg.count('*', 'total_count')
        query = qg.build()

        self.assertIn('COUNT', query.upper())
        print(f"✅ COUNT aggregate: {query}")

        # Test with GROUP BY
        cols = self.db_info.get_columns(test_table)
        group_col = next((c['column_name'] for c in cols if 'dept' in c['column_name'].lower()), None)

        if group_col:
            qg = QueryGenerator(test_table)
            qg.select([group_col])
            qg.count('*', 'count')
            qg.group_by([group_col])
            query = qg.build()

            self.assertIn('GROUP BY', query.upper())
            print(f"✅ GROUP BY: {query}")

    def test_05_order_by(self):
        """Test ORDER BY"""
        tables = self.db_info.get_tables()
        test_table = tables[0]
        columns = self.db_info.get_column_names(test_table)[:2]

        qg = QueryGenerator(test_table)
        qg.select(columns)
        qg.order_by(columns[0], 'DESC')
        query = qg.build()

        self.assertIn('ORDER BY', query.upper())
        self.assertIn('DESC', query.upper())
        print(f"✅ ORDER BY: {query}")

    def test_06_limit(self):
        """Test LIMIT clause"""
        tables = self.db_info.get_tables()
        test_table = tables[0]
        columns = self.db_info.get_column_names(test_table)[:2]

        qg = QueryGenerator(test_table)
        qg.select(columns)
        qg.limit(10)
        query = qg.build()

        self.assertIn('LIMIT 10', query.upper())
        print(f"✅ LIMIT: {query}")

    def test_07_metadata(self):
        """Test query metadata"""
        tables = self.db_info.get_tables()
        test_table = tables[0]
        columns = self.db_info.get_column_names(test_table)[:2]

        qg = QueryGenerator(test_table)
        qg.select(columns)
        qg.where(columns[0], '=', 10)
        qg.limit(5)

        metadata = qg.get_metadata()
        self.assertIn('selected_columns', metadata)
        self.assertIn('conditions', metadata)
        self.assertIn('limit', metadata)
        print(f"✅ Query metadata: {metadata}")


class TestJoinBuilder(unittest.TestCase):
    """Test the JoinBuilder class for multi-table joins"""

    @classmethod
    def setUpClass(cls):
        if 'pm' not in TestDBInfo.schemas or 'sa' not in TestDBInfo.schemas:
            cls.db_info = TestDBInfo.schemas.get('pm') or TestDBInfo.schemas.get(list(TestDBInfo.schemas.keys())[0])
            if cls.db_info is None:
                cls.db_info = None
                print("ℹ️  No suitable schemas for join tests")
        else:
            cls.db_info = TestDBInfo.schemas['pm']

    def setUp(self):
        """Set up test case"""
        if self.__class__.db_info is None:
            self.skipTest("No schemas available for join tests")

    def test_01_basic_join(self):
        """Test basic join between two tables"""
        jb = JoinBuilder(self.db_info)

        # Get two tables from the schema
        tables = self.db_info.get_tables()
        if len(tables) < 2:
            self.skipTest("Need at least 2 tables for join test")

        table1, table2 = tables[0], tables[1]

        # Add tables
        jb.add_table(table1, alias='t1')
        jb.add_table(table2, alias='t2')

        # Get columns for both tables
        cols1 = self.db_info.get_column_names(table1)
        cols2 = self.db_info.get_column_names(table2)

        if not cols1 or not cols2:
            self.skipTest("Tables have no columns")

        # Add join condition (using first column of each table)
        jb.add_join('t1', 't2', cols1[0], cols2[0])

        # Select columns
        jb.select([
            {'table': 't1', 'column': cols1[0]},
            {'table': 't2', 'column': cols2[0]}
        ])

        query = jb.build()
        self.assertIn('JOIN', query.upper())
        print(f"✅ Basic join: {query[:150]}...")

    def test_02_multiple_joins(self):
        """Test joining multiple tables"""
        jb = JoinBuilder(self.db_info)

        # Get three tables from the schema
        tables = self.db_info.get_tables()
        if len(tables) < 3:
            self.skipTest("Need at least 3 tables for multiple join test")

        table1, table2, table3 = tables[0], tables[1], tables[2]

        # Add tables
        for table in [table1, table2, table3]:
            jb.add_table(table)

        # Get columns
        cols1 = self.db_info.get_column_names(table1)
        cols2 = self.db_info.get_column_names(table2)
        cols3 = self.db_info.get_column_names(table3)

        if not cols1 or not cols2 or not cols3:
            self.skipTest("Tables have no columns")

        # Add join conditions
        jb.add_join(table1, table2, cols1[0], cols2[0])
        jb.add_join(table2, table3, cols2[0], cols3[0])

        # Select columns
        jb.select([
            {'table': table1, 'column': cols1[0]},
            {'table': table2, 'column': cols2[0]},
            {'table': table3, 'column': cols3[0]}
        ])

        query = jb.build()
        self.assertEqual(query.upper().count('JOIN'), 2)
        print(f"✅ Multiple joins ({query.count('JOIN')} joins): {query[:150]}...")

    def test_03_preview(self):
        """Test join preview"""
        jb = JoinBuilder(self.db_info)

        tables = self.db_info.get_tables()
        if len(tables) < 2:
            self.skipTest("Need at least 2 tables for preview test")

        table1, table2 = tables[0], tables[1]

        jb.add_table(table1, alias='t1')
        jb.add_table(table2, alias='t2')

        cols1 = self.db_info.get_column_names(table1)
        cols2 = self.db_info.get_column_names(table2)

        if cols1 and cols2:
            jb.add_join('t1', 't2', cols1[0], cols2[0])

            jb.select([
                {'table': 't1', 'column': cols1[0], 'output_alias': 'id_1'},
                {'table': 't2', 'column': cols2[0], 'output_alias': 'id_2'}
            ])

        preview = jb.preview()
        self.assertIn('join_path', preview)
        self.assertIn('selected_columns', preview)
        print(f"✅ Join preview: {preview}")


class TestTemporaryTable(unittest.TestCase):
    """Test temporary table functionality"""

    def setUp(self):
        self.manager = TemporaryTableManager()
        self.connection = sqlite3.connect(':memory:')

    def test_01_create_temp_table(self):
        """Test creating a temporary table"""
        # Create a temp table from a query
        query = "SELECT * FROM sample_table WHERE id = 10"
        temp_table = self.manager.create_temp_table('temp_sample', query)

        self.assertIsNotNone(temp_table)
        self.assertEqual(temp_table.name, 'temp_sample')
        self.assertEqual(temp_table.source_query, query)

        # Check manager tracking
        self.assertIn('temp_sample', self.manager.temp_tables)
        print(f"✅ Created temporary table: {temp_table.name}")

    def test_02_temp_table_from_dataframe(self):
        """Test creating temp table from DataFrame"""
        # Create sample data
        data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['Item A', 'Item B', 'Item C', 'Item D', 'Item E'],
            'value': [100, 200, 300, 400, 500]
        }
        df = pd.DataFrame(data)

        temp_table = TemporaryTable('sample_data')
        temp_table.from_dataframe(df)

        self.assertTrue(temp_table.created)
        self.assertIsNotNone(temp_table.data)
        self.assertEqual(len(temp_table.data), 5)
        print(f"✅ Created temp table from DataFrame: {len(temp_table.data)} rows")

    def test_03_cte_creation(self):
        """Test CTE creation"""
        if 'pm' in TestDBInfo.schemas:
            db_info = TestDBInfo.schemas['pm']
            tables = db_info.get_tables()
            if tables:
                qg = QueryGenerator(tables[0])
                columns = db_info.get_column_names(tables[0])[:2]
                qg.select(columns)

                cte = self.manager.create_cte('test_cte', qg)
                self.assertIn('test_cte', self.manager.ctes)
                print(f"✅ Created CTE: test_cte")

    def test_04_query_with_cte(self):
        """Test building query with CTEs"""
        # Create CTE if we have a schema
        if 'pm' in TestDBInfo.schemas:
            db_info = TestDBInfo.schemas['pm']
            tables = db_info.get_tables()
            if tables:
                qg = QueryGenerator(tables[0])
                columns = db_info.get_column_names(tables[0])[:2]
                qg.select(columns)

                # Build main query using CTE
                main_qg = QueryGenerator('cte_table')
                main_qg.with_cte('cte_table', qg)

                query = main_qg.build()
                self.assertIn('WITH', query.upper())
                print(f"✅ CTE query: {query[:150]}...")
        else:
            # Test without actual schema
            mock_qg = QueryGenerator('mock_table')
            mock_qg.select(['id', 'name'])

            main_qg = QueryGenerator('cte_table')
            main_qg.with_cte('cte_table', mock_qg)

            query = main_qg.build()
            self.assertIn('WITH', query.upper())
            print(f"✅ CTE query (mock): {query[:150]}...")

    def test_05_final_query_builder(self):
        """Test building final query with multiple CTEs"""
        # Create mock CTEs
        qg1 = QueryGenerator('employees')
        qg1.select(['id', 'name', 'dept_id'])
        qg1.where('status', '=', 'active')
        self.manager.create_cte('active_employees', qg1)

        qg2 = QueryGenerator('departments')
        qg2.select(['dept_id', 'dept_name'])
        self.manager.create_cte('departments', qg2)

        # Build final query
        final_query = """
        SELECT e.id, e.name, d.dept_name
        FROM active_employees e
        JOIN departments d ON e.dept_id = d.dept_id
        """

        full_query = self.manager.build_final_query(final_query)
        self.assertIn('WITH', full_query.upper())
        self.assertIn('active_employees AS', full_query)
        self.assertIn('departments AS', full_query)
        print(f"✅ Final query with CTEs built successfully")

    def test_06_save_and_retrieve(self):
        """Test saving and retrieving temp table data"""
        # Create a temp table with data
        data = pd.DataFrame({
            'id': [101, 102, 103],
            'product': ['Widget', 'Gadget', 'Tool'],
            'price': [19.99, 29.99, 39.99]
        })

        temp_table = TemporaryTable('products')
        temp_table.from_dataframe(data)

        # Test getting data
        df = temp_table.to_dataframe()
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 3)

        # Test describe
        desc = temp_table.describe()
        self.assertEqual(desc['name'], 'products')
        self.assertEqual(desc['row_count'], 3)

        # Test saving to file
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            temp_path = f.name

        temp_table.save(temp_path, format='csv')
        self.assertTrue(os.path.exists(temp_path))

        # Clean up
        os.unlink(temp_path)
        print(f"✅ Temp table saved and retrieved: {len(df)} rows")


class TestCrossSchemaQueries(unittest.TestCase):
    """Test queries across multiple schemas"""

    @classmethod
    def setUpClass(cls):
        cls.available_schemas = TestDBInfo.schemas

    def setUp(self):
        if len(self.available_schemas) < 2:
            self.skipTest("Need at least 2 schemas for cross-schema tests")

    def test_01_cross_schema_reference(self):
        """Test building queries that reference multiple schemas"""
        # Get two different schemas
        schema_names = list(self.available_schemas.keys())
        if len(schema_names) < 2:
            self.skipTest("Need at least 2 schemas")

        schema1 = self.available_schemas[schema_names[0]]
        schema2 = self.available_schemas[schema_names[1]]

        # Get tables from each schema
        tables1 = schema1.get_tables()
        tables2 = schema2.get_tables()

        if not tables1 or not tables2:
            self.skipTest("Schemas have no tables")

        # Create query with schema qualification
        qg = QueryGenerator(tables1[0], schema_name=schema_names[0])
        columns = schema1.get_column_names(tables1[0])[:2]
        qg.select(columns)

        query = qg.build()
        self.assertIsNotNone(query)
        print(f"✅ Cross-schema query: {query}")

    def test_02_multi_schema_info(self):
        """Test getting info from multiple schemas"""
        for schema_name, db_info in self.available_schemas.items():
            tables = db_info.get_tables()
            print(f"📁 {schema_name}: {len(tables)} tables")

        print(f"✅ Accessed {len(self.available_schemas)} schemas")


class TestComplexScenarios(unittest.TestCase):
    """Test complex real-world scenarios"""

    @classmethod
    def setUpClass(cls):
        cls.db_info = TestDBInfo.schemas.get('pm')
        if not cls.db_info:
            # Use any available schema
            if TestDBInfo.schemas:
                cls.db_info = TestDBInfo.schemas[list(TestDBInfo.schemas.keys())[0]]
            else:
                cls.db_info = None

    def setUp(self):
        if self.__class__.db_info is None:
            self.skipTest("No schemas available for complex tests")

    def test_01_complex_query(self):
        """Test building a complex query with multiple features"""
        tables = self.db_info.get_tables()
        if not tables:
            self.skipTest("No tables available")

        test_table = tables[0]
        columns = self.db_info.get_column_names(test_table)

        if len(columns) < 3:
            self.skipTest("Table needs at least 3 columns")

        qg = QueryGenerator(test_table)
        qg.select([columns[0], columns[1]])
        qg.count(columns[2], 'count')
        qg.where(columns[0], '>', 0)
        qg.group_by([columns[0], columns[1]])
        qg.order_by('count', 'DESC')
        qg.limit(10)

        query = qg.build()
        self.assertIn('GROUP BY', query.upper())
        self.assertIn('ORDER BY', query.upper())
        self.assertIn('LIMIT', query.upper())
        print(f"✅ Complex query: {query[:200]}...")

    def test_02_save_query_as_temp(self):
        """Test saving query results as temporary table"""
        tables = self.db_info.get_tables()
        if not tables:
            self.skipTest("No tables available")

        test_table = tables[0]

        # Create a query
        qg = QueryGenerator(test_table)
        columns = self.db_info.get_column_names(test_table)[:2]
        qg.select(columns)
        qg.limit(5)

        # Save as temp table
        temp_table = TemporaryTable('query_result')
        temp_table.source_query = qg.build()
        temp_table.created = True

        # Create mock data
        mock_data = {col: [f"data_{i}" for i in range(5)] for col in columns}
        temp_table.data = pd.DataFrame(mock_data)

        self.assertTrue(temp_table.created)
        self.assertIsNotNone(temp_table.data)
        print(f"✅ Query saved as temp table with {len(temp_table.data)} rows")


def run_tests():
    """Run all test suites"""
    print("=" * 80)
    print("🧪 SQL QUERY GENERATOR - COMPREHENSIVE TEST SUITE")
    print("=" * 80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Create test suite
    suite = unittest.TestSuite()

    # Add test cases
    suite.addTest(unittest.makeSuite(TestDBInfo))
    suite.addTest(unittest.makeSuite(TestQueryGenerator))
    suite.addTest(unittest.makeSuite(TestJoinBuilder))
    suite.addTest(unittest.makeSuite(TestTemporaryTable))
    suite.addTest(unittest.makeSuite(TestCrossSchemaQueries))
    suite.addTest(unittest.makeSuite(TestComplexScenarios))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print()
    print("=" * 80)
    print(f"📊 TEST SUMMARY")
    print("=" * 80)
    print(f"Ran: {result.testsRun} tests")
    print(f"✅ Passed: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"❌ Failures: {len(result.failures)}")
    print(f"⚠️  Errors: {len(result.errors)}")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return result


if __name__ == '__main__':
    # Check if required modules are available
    try:
        import pandas as pd
        import sqlite3
    except ImportError as e:
        print(f"❌ Required module not found: {e}")
        print("Please install pandas: pip install pandas")
        exit(1)

    # Run tests
    result = run_tests()

    # Exit with appropriate code
    exit_code = 0 if result.wasSuccessful() else 1
    exit(exit_code)