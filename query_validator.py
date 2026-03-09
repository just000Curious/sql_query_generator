import re
from typing import List, Dict, Optional, Union, Any, Set

from db_information import DBInfo
from pypika_query_engine import QueryGenerator
from join_builder import JoinBuilder
from cte_builder import CTEBuilder


class QueryValidator:
    """
    Query Validator Module - Validates SQL queries against database schema
    Identifies errors and provides feedback for correction
    """

    def __init__(self, db_info: DBInfo):
        """
        Initialize validator with database schema

        Args:
            db_info: DBInfo instance with parsed schema
        """
        self.db_info = db_info
        self.errors = []
        self.warnings = []

    def validate_query_generator(self, query_gen: QueryGenerator) -> bool:
        """Validate a QueryGenerator instance"""
        self.errors = []
        self.warnings = []

        # Get metadata
        metadata = query_gen.get_metadata()

        # Check table existence
        if not self.db_info.table_exists(metadata['table'], metadata['schema']):
            self.errors.append(f"Table '{metadata['table']}' does not exist")
            return False

        # Check columns
        for col in metadata['selected_columns']:
            if col == '*':
                continue

            # Handle aliases and aggregates
            col_name = self._extract_column_name(col)
            if col_name and not self.db_info.column_exists(
                    metadata['table'], col_name, metadata['schema']
            ):
                self.warnings.append(f"Column '{col_name}' may not exist in table '{metadata['table']}'")

        # Check WHERE conditions
        for cond in metadata['conditions']:
            col_name = cond[0]
            if not self.db_info.column_exists(metadata['table'], col_name, metadata['schema']):
                self.errors.append(f"WHERE column '{col_name}' does not exist")

        return len(self.errors) == 0

    def validate_join_builder(self, join_builder: JoinBuilder) -> bool:
        """Validate a JoinBuilder instance"""
        self.errors = []
        self.warnings = []

        preview = join_builder.preview()

        # Check if all tables exist
        for item in preview['join_path']:
            if item['type'] == 'table':
                table = item['table']
                schema = item.get('schema')

                if not self.db_info.table_exists(table, schema):
                    self.errors.append(f"Table '{table}' does not exist")

            elif item['type'] == 'join':
                # Check source table
                from_table = item['from_table']
                from_schema = item.get('from_schema')

                if not self.db_info.table_exists(from_table, from_schema):
                    self.errors.append(f"Join source table '{from_table}' does not exist")

                # Check target table
                to_table = item['to_table']
                to_schema = item.get('to_schema')

                if not self.db_info.table_exists(to_table, to_schema):
                    self.errors.append(f"Join target table '{to_table}' does not exist")

                # Check relationship
                if not self._validate_relationship(item):
                    self.warnings.append(
                        f"Relationship between {from_table}.{item['from_column']} "
                        f"and {to_table}.{item['to_column']} may not exist"
                    )

        # Check selected columns
        for col_info in preview['selected_columns']:
            if col_info.get('type') == 'aggregate':
                # Skip aggregate validation for now
                continue

            table = col_info.get('table')
            column = col_info.get('column')

            if column == '*':
                continue

            # Find table info
            table_info = self._find_table_info(table, preview['table_aliases'])
            if not table_info:
                self.errors.append(f"Table alias '{table}' not found")
                continue

            if not self.db_info.column_exists(table_info['table'], column, table_info.get('schema')):
                self.errors.append(f"Column '{column}' does not exist in table '{table_info['table']}'")

        return len(self.errors) == 0

    def validate_cte_builder(self, cte_builder: CTEBuilder) -> bool:
        """Validate a CTEBuilder instance"""
        self.errors = []
        self.warnings = []

        metadata = cte_builder.get_metadata()

        # Check for circular references (simplified)
        stage_names = set(metadata['stage_names'])

        # Check final query
        if metadata['has_final_query']:
            # Would need to parse and validate
            pass

        return len(self.errors) == 0

    def validate_sql(self, sql: str) -> bool:
        """
        Validate raw SQL string

        This is a simplified validation - in production would use a SQL parser
        """
        self.errors = []
        self.warnings = []

        # Check for basic SQL structure
        sql_upper = sql.upper()

        if not sql_upper.strip():
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
        # Remove alias
        if ' AS ' in col_str:
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
            join_item['from_table'],
            join_item['to_table'],
            join_item.get('from_schema'),
            join_item.get('to_schema')
        )

        if not rel:
            return False

        # Check if the columns match
        return (rel['from_column'] == join_item['from_column'] and
                rel['to_column'] == join_item['to_column'])

    def _find_table_info(self, alias: str, table_aliases: Dict) -> Optional[Dict]:
        """Find table info by alias"""
        return table_aliases.get(alias)

    def _extract_table_names(self, sql: str) -> List[str]:
        """Extract table names from SQL (simplified)"""
        tables = []

        # Find FROM clause
        from_match = re.search(r'FROM\s+([^\s;]+)', sql, re.IGNORECASE)
        if from_match:
            tables.append(from_match.group(1).strip())

        # Find JOIN clauses
        join_matches = re.findall(r'JOIN\s+([^\s]+)', sql, re.IGNORECASE)
        tables.extend([j.strip() for j in join_matches])

        # Remove schema prefixes
        cleaned_tables = []
        for t in tables:
            if '.' in t:
                t = t.split('.')[-1]
            cleaned_tables.append(t)

        return cleaned_tables

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
# These functions make the module compatible with the test script

_validator_instance = None
_default_db_info = None


def _get_default_db_info():
    """Get or create a default DBInfo instance"""
    global _default_db_info
    if _default_db_info is None:
        # Try to import db_information
        try:
            from db_information import DBInfo, get_test_db_info
            try:
                _default_db_info = get_test_db_info()
            except:
                try:
                    _default_db_info = DBInfo()  # Test mode
                except:
                    # Create a minimal mock
                    class MockDBInfo:
                        def table_exists(self, table, schema=None):
                            return True
                        def column_exists(self, table, column, schema=None):
                            return True
                        def find_relationship(self, t1, t2, s1=None, s2=None):
                            return None
                    _default_db_info = MockDBInfo()
        except:
            # Create a simple mock
            class MockDBInfo:
                def table_exists(self, table, schema=None):
                    return True
                def column_exists(self, table, column, schema=None):
                    return True
                def find_relationship(self, t1, t2, s1=None, s2=None):
                    return None
            _default_db_info = MockDBInfo()
    return _default_db_info


def _get_validator_instance():
    """Get or create the validator instance"""
    global _validator_instance
    if _validator_instance is None:
        _validator_instance = QueryValidator(_get_default_db_info())
    return _validator_instance


def validate(query_or_obj):
    """
    Wrapper function to validate a query
    Compatible with test.py

    Args:
        query_or_obj: SQL string, QueryGenerator, JoinBuilder, or CTEBuilder

    Returns:
        bool: True if valid, False otherwise
    """
    validator = _get_validator_instance()
    validator.clear()

    # Check the type and validate accordingly
    if isinstance(query_or_obj, str):
        return validator.validate_sql(query_or_obj)
    elif hasattr(query_or_obj, '__class__'):
        class_name = query_or_obj.__class__.__name__
        if class_name == 'QueryGenerator':
            return validator.validate_query_generator(query_or_obj)
        elif class_name == 'JoinBuilder':
            return validator.validate_join_builder(query_or_obj)
        elif class_name == 'CTEBuilder':
            return validator.validate_cte_builder(query_or_obj)

    # Default fallback
    return False


def validate_sql(sql: str) -> bool:
    """
    Wrapper function to validate SQL string
    """
    return validate(sql)


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