import pandas as pd
import sqlite3
from typing import Optional, Any, List, Dict
from pypika_query_engine import QueryGenerator


class TemporaryTable:
    def __init__(self, name: str, source_query=None):
        """
        Initialize a temporary table
        """
        self.name = name
        self.source_query = source_query
        self.data = None
        self.created = False
        self.columns = []
        self.connection = None

    def create(self, query: str, engine=None):
        """
        Create the temporary table using a SQL query
        """
        self.source_query = query
        self.created = True

        # If engine is provided (SQL connection), execute the query
        if engine:
            if isinstance(engine, sqlite3.Connection):
                # For SQLite
                engine.execute(f"DROP TABLE IF EXISTS temp.{self.name}")
                engine.execute(f"CREATE TEMP TABLE {self.name} AS {query}")
            else:
                # For other databases
                engine.execute(f"CREATE TEMPORARY TABLE {self.name} AS {query}")

        return self

    def from_dataframe(self, df: pd.DataFrame):
        """
        Create temporary table from pandas DataFrame
        """
        self.data = df
        self.columns = df.columns.tolist()
        self.created = True
        return self

    def query(self) -> QueryGenerator:
        """
        Get a QueryGenerator for this temporary table
        """
        return QueryGenerator(self.name, schema='temp')

    def to_dataframe(self) -> Optional[pd.DataFrame]:
        """
        Get the data as a pandas DataFrame
        """
        if self.data is not None:
            return self.data

        # If we have a connection and the table was created, we could fetch data
        if self.connection and self.created:
            return pd.read_sql(f"SELECT * FROM temp.{self.name}", self.connection)

        return None

    def save(self, path: str, format: str = 'csv'):
        """
        Save temporary table data to file
        """
        if self.data is None:
            raise ValueError("No data available to save")

        if format.lower() == 'csv':
            self.data.to_csv(path, index=False)
        elif format.lower() == 'parquet':
            self.data.to_parquet(path, index=False)
        elif format.lower() == 'json':
            self.data.to_json(path, orient='records')
        else:
            raise ValueError(f"Unsupported format: {format}")

    def describe(self) -> Dict:
        """
        Get metadata about the temporary table
        """
        return {
            'name': self.name,
            'created': self.created,
            'columns': self.columns,
            'source_query': self.source_query,
            'has_data': self.data is not None,
            'row_count': len(self.data) if self.data is not None else None
        }


class TemporaryTableManager:
    """
    Manages multiple temporary tables and CTEs
    """

    def __init__(self):
        self.temp_tables = {}
        self.ctes = {}

    def create_temp_table(self, name: str, query: str) -> TemporaryTable:
        """
        Create a new temporary table
        """
        if name in self.temp_tables:
            raise ValueError(f"Temporary table '{name}' already exists")

        temp_table = TemporaryTable(name, query)
        self.temp_tables[name] = temp_table
        return temp_table

    def create_cte(self, name: str, query_generator: QueryGenerator):
        """
        Register a CTE
        """
        self.ctes[name] = query_generator
        return query_generator

    def get_temp_table(self, name: str) -> Optional[TemporaryTable]:
        """
        Get a temporary table by name
        """
        return self.temp_tables.get(name)

    def get_cte(self, name: str) -> Optional[QueryGenerator]:
        """
        Get a CTE by name
        """
        return self.ctes.get(name)

    def list_temp_tables(self) -> List[str]:
        """
        List all temporary tables
        """
        return list(self.temp_tables.keys())

    def list_ctes(self) -> List[str]:
        """
        List all CTEs
        """
        return list(self.ctes.keys())

    def drop_temp_table(self, name: str):
        """
        Drop a temporary table
        """
        if name in self.temp_tables:
            del self.temp_tables[name]

    def build_final_query(self, main_query: str) -> str:
        """
        Build a final query combining all CTEs and temporary tables
        """
        if not self.ctes and not self.temp_tables:
            return main_query

        parts = []

        # Add CTEs
        if self.ctes:
            cte_parts = []
            for name, query_gen in self.ctes.items():
                cte_parts.append(f"{name} AS (\n{query_gen.build()}\n)")
            parts.append("WITH " + ",\n".join(cte_parts))

        # Add main query
        parts.append(main_query)

        return "\n\n".join(parts)


# ========== WRAPPER FUNCTIONS FOR COMPATIBILITY ==========
# These functions make the module compatible with the test script

_manager = TemporaryTableManager()


def create_temp_table(name: str, columns: List[str] = None):
    """
    Wrapper function to create a temporary table
    Compatible with test.py
    """
    if columns:
        # Convert column definitions to a SELECT query
        col_defs = ", ".join(columns)
        query = f"SELECT {col_defs} WHERE 1=0"  # Empty table with correct schema
    else:
        query = "SELECT 1 WHERE 1=0"

    return _manager.create_temp_table(name, query)


def drop_temp_table(name: str):
    """
    Wrapper function to drop a temporary table
    """
    _manager.drop_temp_table(name)


def get_temp_table(name: str):
    """
    Wrapper function to get a temporary table
    """
    return _manager.get_temp_table(name)


def list_temp_tables():
    """
    Wrapper function to list all temporary tables
    """
    return _manager.list_temp_tables()


def create_cte(name: str, query_gen):
    """
    Wrapper function to create a CTE
    """
    return _manager.create_cte(name, query_gen)