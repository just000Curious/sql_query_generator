"""
SQL Query Generator - Main Application
Integrates all query building and validation modules
"""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from query_engine import QueryEngine
from query_validator import QueryValidator
from query_assembler import QueryAssembler
from cte_builder import CTEBuilder
from join_builder import JoinBuilder
from temporary_table import TemporaryTable
from db_information import DBInfo
from pypika_query_engine import QueryGenerator


class SQLQueryGenerator:
    """Main application class for SQL Query Generator"""

    def __init__(self, db_path="db_files"):
        self.db_path = Path(db_path)
        self.db_path.mkdir(exist_ok=True)

        # Initialize components
        self.db_info = DBInfo(self.db_path)
        self.query_engine = QueryEngine()
        self.query_validator = QueryValidator()
        self.query_assembler = QueryAssembler()
        self.cte_builder = CTEBuilder()
        self.join_builder = JoinBuilder()
        self.temp_table = TemporaryTable()
        self.pypika_engine = PyPikaQueryEngine()

        self.current_query = None
        self.query_history = []

    def create_select_query(self, table, columns=None, where=None):
        """Create a SELECT query"""
        query = self.query_assembler.select(table, columns, where)
        self.current_query = query
        self.query_history.append(('SELECT', query))
        return query

    def create_cte_query(self, cte_name, cte_query, main_query):
        """Create a query with CTE"""
        query = self.cte_builder.build_cte(cte_name, cte_query, main_query)
        self.current_query = query
        self.query_history.append(('CTE', query))
        return query

    def create_join_query(self, tables, join_conditions, join_type='INNER'):
        """Create a JOIN query"""
        query = self.join_builder.build_join(tables, join_conditions, join_type)
        self.current_query = query
        self.query_history.append(('JOIN', query))
        return query

    def create_temp_table_query(self, table_name, select_query):
        """Create a temporary table query"""
        query = self.temp_table.create_temp_table(table_name, select_query)
        self.current_query = query
        self.query_history.append(('TEMP TABLE', query))
        return query

    def validate_query(self, query=None):
        """Validate the current or provided query"""
        if query is None:
            query = self.current_query

        if not query:
            return {"valid": False, "error": "No query to validate"}

        return self.query_validator.validate(query)

    def execute_query(self, query=None):
        """Execute the current or provided query"""
        if query is None:
            query = self.current_query

        if not query:
            return {"success": False, "error": "No query to execute"}

        # Validate before executing
        validation = self.validate_query(query)
        if not validation.get('valid', False):
            return {"success": False, "error": validation.get('error', 'Invalid query')}

        return self.query_engine.execute(query)

    def get_table_info(self, table_name=None):
        """Get database table information"""
        if table_name:
            return self.db_info.get_table_schema(table_name)
        return self.db_info.get_all_tables()

    def build_with_pypika(self, table_name):
        """Build query using PyPika"""
        return self.pypika_engine.build_query(table_name)

    def show_history(self):
        """Show query history"""
        if not self.query_history:
            print("No queries in history")
            return

        print("\n=== Query History ===")
        for i, (q_type, query) in enumerate(self.query_history, 1):
            print(f"{i}. [{q_type}] {query[:50]}...")

    def save_query(self, filename, query=None):
        """Save query to file"""
        if query is None:
            query = self.current_query

        if not query:
            print("No query to save")
            return False

        filepath = self.db_path / filename
        with open(filepath, 'w') as f:
            f.write(query)
        print(f"Query saved to {filepath}")
        return True

    def load_query(self, filename):
        """Load query from file"""
        filepath = self.db_path / filename
        if not filepath.exists():
            print(f"File {filename} not found")
            return None

        with open(filepath, 'r') as f:
            query = f.read()

        self.current_query = query
        self.query_history.append(('LOADED', query))
        return query


def main():
    """Main CLI interface"""
    generator = SQLQueryGenerator()

    print("=" * 50)
    print("SQL Query Generator")
    print("=" * 50)

    while True:
        print("\nOptions:")
        print("1. Create SELECT query")
        print("2. Create CTE query")
        print("3. Create JOIN query")
        print("4. Create TEMP TABLE query")
        print("5. Validate current query")
        print("6. Execute current query")
        print("7. Show table info")
        print("8. Show query history")
        print("9. Save query to file")
        print("10. Load query from file")
        print("11. Build with PyPika")
        print("0. Exit")

        choice = input("\nEnter your choice: ").strip()

        if choice == '1':
            table = input("Table name: ")
            columns = input("Columns (comma-separated, or *): ").strip()
            columns = ['*'] if columns == '*' else [c.strip() for c in columns.split(',')]
            where = input("WHERE clause (optional): ").strip() or None

            query = generator.create_select_query(table, columns, where)
            print(f"\nGenerated Query:\n{query}")

        elif choice == '2':
            cte_name = input("CTE name: ")
            print("Enter CTE query (end with blank line):")
            cte_lines = []
            while True:
                line = input()
                if not line:
                    break
                cte_lines.append(line)
            cte_query = '\n'.join(cte_lines)

            print("Enter main query (end with blank line):")
            main_lines = []
            while True:
                line = input()
                if not line:
                    break
                main_lines.append(line)
            main_query = '\n'.join(main_lines)

            query = generator.create_cte_query(cte_name, cte_query, main_query)
            print(f"\nGenerated Query:\n{query}")

        elif choice == '3':
            tables = input("Tables (comma-separated): ").split(',')
            tables = [t.strip() for t in tables]

            print("Enter join conditions (one per line, end with blank line):")
            conditions = []
            while True:
                line = input()
                if not line:
                    break
                conditions.append(line)

            join_type = input("Join type (INNER/LEFT/RIGHT/FULL, default INNER): ").strip().upper() or 'INNER'

            query = generator.create_join_query(tables, conditions, join_type)
            print(f"\nGenerated Query:\n{query}")

        elif choice == '4':
            temp_name = input("Temporary table name: ")
            print("Enter SELECT query for temp table (end with blank line):")
            select_lines = []
            while True:
                line = input()
                if not line:
                    break
                select_lines.append(line)
            select_query = '\n'.join(select_lines)

            query = generator.create_temp_table_query(temp_name, select_query)
            print(f"\nGenerated Query:\n{query}")

        elif choice == '5':
            result = generator.validate_query()
            if result.get('valid'):
                print("✓ Query is valid")
            else:
                print(f"✗ Invalid query: {result.get('error')}")

        elif choice == '6':
            result = generator.execute_query()
            if result.get('success'):
                print("✓ Query executed successfully")
                if 'data' in result:
                    print(f"Results: {result['data']}")
            else:
                print(f"✗ Execution failed: {result.get('error')}")

        elif choice == '7':
            table = input("Table name (optional, press Enter for all): ").strip() or None
            info = generator.get_table_info(table)
            print(f"\nTable Information:\n{info}")

        elif choice == '8':
            generator.show_history()

        elif choice == '9':
            filename = input("Filename to save: ")
            generator.save_query(filename)

        elif choice == '10':
            filename = input("Filename to load: ")
            query = generator.load_query(filename)
            if query:
                print(f"\nLoaded Query:\n{query}")

        elif choice == '11':
            table = input("Table name: ")
            query = generator.build_with_pypika(table)
            print(f"\nPyPika Generated Query:\n{query}")
            generator.current_query = query

        elif choice == '0':
            print("Goodbye!")
            break
        else:
            print("Invalid choice, please try again")


if __name__ == "__main__":
    main()
    main()