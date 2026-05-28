"""
api.py
SQL Query Generator API with Schema-Based Navigation
"""

import os
import re
import sys
import socket
import sqlite3
import json
import time
import traceback
import uuid
import warnings
import webbrowser
import threading
from contextlib import asynccontextmanager
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Union
import logging
import shutil

# Suppress Pydantic warning for "schema" field shadowing
warnings.filterwarnings("ignore", message='Field name "schema" .* shadows an attribute .*')

# ============================================================
# PYINSTALLER RESOURCE PATH HELPER
# ============================================================

def get_resource_path(relative_path: str) -> str:
    """
    Return the absolute path to a bundled resource.
    Priority:
      1. sys._MEIPASS  (set by PyInstaller --onefile at runtime)
      2. Directory of this script  (normal development run)
    """
    if hasattr(sys, '_MEIPASS'):
        base = sys._MEIPASS
    else:
        base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, relative_path)

# Resolve the frontend dist directory ONCE at import time so every route
# sees the same path and we can log it for debugging.
DIST_DIR  = get_resource_path(os.path.join("frontend", "dist"))
INDEX_HTML = os.path.join(DIST_DIR, "index.html")

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ============================================================
# LOGGING — write to file so crashes are visible even with --noconsole
# ============================================================

_LOG_DIR = os.path.join(os.path.expanduser("~"), "SQL_Query_Generator_logs")
os.makedirs(_LOG_DIR, exist_ok=True)
_LOG_FILE = os.path.join(_LOG_DIR, "app.log")

# --noconsole (PyInstaller) sets sys.stdout / sys.stderr to None.
# Redirect them to the log file BEFORE anything (uvicorn, logging) touches them.
# Also handles the case where frozen stdout is a dummy object without isatty().
_IS_FROZEN = getattr(sys, 'frozen', False)
_IS_FROZEN_NOCONSOLE = _IS_FROZEN and (sys.stdout is None or not hasattr(sys.stdout, 'isatty'))
if _IS_FROZEN_NOCONSOLE:
    _log_stream = open(_LOG_FILE, 'a', encoding='utf-8', buffering=1)
    sys.stdout = _log_stream
    sys.stderr = _log_stream

# Build the handler list — only add StreamHandler when stdout is a real stream
_log_handlers: list = [logging.FileHandler(_LOG_FILE, encoding="utf-8")]
if sys.stdout is not None:
    _log_handlers.append(logging.StreamHandler(sys.stdout))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=_log_handlers,
)
logger = logging.getLogger(__name__)

# Try to import FastAPI
try:
    from fastapi import FastAPI, HTTPException, Query, UploadFile, File
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel, Field, field_validator
    import uvicorn
    FASTAPI_AVAILABLE = True
except ImportError as e:
    FASTAPI_AVAILABLE = False
    print(f"❌ FastAPI not available: {e}")
    print("Please install: pip install fastapi uvicorn")
    sys.exit(1)


# ============================================================
# PYDANTIC MODELS
# ============================================================

class SchemaInfo(BaseModel):
    schema_name: str
    schema_desc: str
    table_count: int

class TableInfo(BaseModel):
    table_name: str
    schema: str
    columns: List[str]
    primary_keys: List[str]
    foreign_keys: List[Dict]

class WhereCondition(BaseModel):
    column: str
    operator: str = "="
    value: Any

    @field_validator('operator')
    @classmethod
    def validate_operator(cls, v):
        valid_ops = ['=', '!=', '<>', '>', '>=', '<', '<=', 'LIKE', 'NOT LIKE',
                     'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL', 'BETWEEN']
        if v.upper() not in valid_ops:
            raise ValueError(f'Operator must be one of: {valid_ops}')
        return v

class TableInput(BaseModel):
    table: str
    schema: str
    alias: str

class ColumnInput(BaseModel):
    table: str = ""
    column: str
    alias: Optional[str] = None
    cast_as: Optional[str] = None   # e.g. "VARCHAR", "INTEGER", "DATE", "NUMERIC(10,2)"

class ConditionInput(BaseModel):
    table: str = ""
    column: str = ""
    operator: str = "="
    value: Optional[Any] = None
    logic: str = "AND"  # AND or OR — used between conditions
    group_start: bool = False  # opens parenthesis before this condition
    group_end: bool = False    # closes parenthesis after this condition

class OrderByInput(BaseModel):
    column: str
    direction: str = "ASC"

class JoinInput(BaseModel):
    join_type: str = "INNER JOIN"
    from_alias: str
    from_column: str
    to_alias: str
    to_column: str
    operator: str = "="  # Support non-equi joins (=, !=, <, <=, >, >=, BETWEEN)

class GenerateRequest(BaseModel):
    tables: List[TableInput]
    columns: Optional[List[ColumnInput]] = []
    conditions: Optional[List[ConditionInput]] = []
    joins: Optional[List[JoinInput]] = []
    limit: Optional[int] = Field(None, ge=1, le=100000)
    offset: Optional[int] = Field(None, ge=0)
    order_by: Optional[List[OrderByInput]] = []
    group_by: Optional[List[str]] = []
    aggregates: Optional[List[Dict[str, str]]] = []
    having: Optional[List[ConditionInput]] = []
    distinct: Optional[bool] = False
    computed_columns: Optional[List[str]] = []  # Raw SQL expressions: CASE, functions, window funcs

class UnionQueryRequest(BaseModel):
    queries: List[GenerateRequest]
    operation: str = "UNION ALL"
    wrap_in_cte: Optional[str] = None

class SQLQueryRequest(BaseModel):
    sql: str
    limit: Optional[int] = Field(1000, ge=1, le=100000)

class QueryResponse(BaseModel):
    success: bool
    query: Optional[str] = None
    error: Optional[str] = None
    execution_time: float
    row_count: Optional[int] = None

class ExecutionResponse(BaseModel):
    success: bool
    data: List[Dict] = []
    columns: List[str] = []
    row_count: int = 0
    execution_time: float = 0
    message: Optional[str] = None
    sql: Optional[str] = None


# ============================================================
# ENHANCED QUERY GENERATOR
# ============================================================

class SchemaQueryGenerator:
    """Query Generator that understands schemas"""

    def __init__(self, schema: str, table: str, alias: str = None):
        self.schema = schema
        self.table = table
        self.alias = alias or table
        # No schema prefix — user's PostgreSQL search_path resolves the schema
        self.table_ref = table
        if alias:
            self.table_ref += f" AS {alias}"

        self.selected_columns = []
        self.where_conditions = []
        self.group_by_cols = []
        self.having_conditions = []
        self.order_by_cols = []
        self.limit_val = None
        self.offset_val = None
        self.distinct_flag = False

    # Column types that are numeric — value must NOT be quoted
    _NUMERIC_TYPES = frozenset({
        "integer", "int", "int2", "int4", "int8", "bigint", "smallint",
        "numeric", "decimal", "real", "float", "float4", "float8",
        "double precision", "serial", "bigserial", "money",
    })
    # Column types that are date/time — value MUST be quoted as a date literal
    _DATE_TYPES = frozenset({
        "date", "timestamp", "timestamptz", "timetz", "time",
        "timestamp without time zone", "timestamp with time zone",
        "interval",
    })

    def _format_value(self, value, col_type: str = None):
        """
        Properly format a WHERE / HAVING value for SQL.
        When col_type is provided (from schema metadata) it drives quoting:
          - numeric types  → no quotes
          - date/time types → always quoted as 'YYYY-MM-DD' literals
          - everything else → string-escaped and quoted
        When col_type is None the old heuristic (try float()) is used.
        """
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, (datetime, date)):
            return f"'{value.strftime('%Y-%m-%d')}'"

        if isinstance(value, str):
            # Strip surrounding quotes that may have been added in the UI
            cleaned = value.strip().strip("'").strip('"')
            if not cleaned:
                return "''"

            ct = (col_type or "").lower().split("(")[0].strip()

            # --- Numeric column: never quote ---
            if ct in self._NUMERIC_TYPES:
                try:
                    float(cleaned)
                    return cleaned
                except ValueError:
                    # User typed something non-numeric into a numeric column; pass through
                    return cleaned

            # --- Date/time column: always quote and normalise format ---
            if ct in self._DATE_TYPES:
                # Strip any quotes user may have added
                date_val = cleaned.strip("'\"")
                escaped = date_val.replace("'", "''")
                return f"'{escaped}'"

            # --- No type info: use heuristic (backward-compatible) ---
            if ct == "":
                try:
                    float(cleaned)
                    return cleaned
                except ValueError:
                    pass

            # --- String / unknown: quote and escape ---
            escaped = cleaned.replace("'", "''")
            return f"'{escaped}'"

        return f"'{str(value)}'"

    def select(self, columns):
        if isinstance(columns, str):
            self.selected_columns = [columns]
        else:
            self.selected_columns = columns
        return self

    def select_all(self):
        self.selected_columns = ["*"]
        return self

    def select_distinct(self):
        self.distinct_flag = True
        return self

    def select_with_alias(self, column: str, alias: str):
        self.selected_columns.append(f"{column} AS {alias}")
        return self

    def where(self, column: str, operator: str, value):
        formatted_value = self._format_value(value)
        self.where_conditions.append({
            'column': column,
            'operator': operator,
            'value': formatted_value
        })
        return self

    def where_between(self, column: str, start, end):
        formatted_start = self._format_value(start)
        formatted_end = self._format_value(end)
        self.where_conditions.append({
            'column': column,
            'operator': 'BETWEEN',
            'value': f"{formatted_start} AND {formatted_end}"
        })
        return self

    def where_in(self, column: str, values: List):
        formatted_values = [self._format_value(v) for v in values]
        self.where_conditions.append({
            'column': column,
            'operator': 'IN',
            'value': f"({', '.join(formatted_values)})"
        })
        return self

    def group_by(self, columns):
        if isinstance(columns, str):
            self.group_by_cols = [columns]
        else:
            self.group_by_cols = columns
        return self

    def having(self, column: str, operator: str, value):
        formatted_value = self._format_value(value)
        self.having_conditions.append({
            'column': column,
            'operator': operator,
            'value': formatted_value
        })
        return self

    def order_by(self, column: str, direction: str = 'ASC'):
        self.order_by_cols.append({'column': column, 'direction': direction})
        return self

    def limit(self, number: int, offset: int = 0):
        self.limit_val = number
        self.offset_val = offset
        return self

    def build(self) -> str:
        parts = []

        # SELECT clause
        select_clause = "SELECT "
        if self.distinct_flag:
            select_clause += "DISTINCT "
        if self.selected_columns:
            select_clause += ", ".join(self.selected_columns)
        else:
            select_clause += "*"
        parts.append(select_clause)

        # FROM clause
        from_clause = f"FROM {self.table_ref}"
        parts.append(from_clause)

        # WHERE clause
        if self.where_conditions:
            where_parts = []
            for cond in self.where_conditions:
                if cond['operator'] == 'BETWEEN':
                    where_parts.append(f"{cond['column']} BETWEEN {cond['value']}")
                elif cond['operator'] in ('IN', 'NOT IN'):
                    where_parts.append(f"{cond['column']} {cond['operator']} {cond['value']}")
                elif cond['operator'] in ('IS NULL', 'IS NOT NULL'):
                    where_parts.append(f"{cond['column']} {cond['operator']}")
                else:
                    where_parts.append(f"{cond['column']} {cond['operator']} {cond['value']}")
            parts.append("WHERE " + " AND ".join(where_parts))

        # GROUP BY clause
        if self.group_by_cols:
            parts.append("GROUP BY " + ", ".join(self.group_by_cols))

        # HAVING clause
        if self.having_conditions:
            having_parts = []
            for cond in self.having_conditions:
                having_parts.append(f"{cond['column']} {cond['operator']} {cond['value']}")
            parts.append("HAVING " + " AND ".join(having_parts))

        # ORDER BY clause
        if self.order_by_cols:
            order_parts = [f"{o['column']} {o['direction']}" for o in self.order_by_cols]
            parts.append("ORDER BY " + ", ".join(order_parts))

        # LIMIT clause
        if self.limit_val is not None and self.limit_val > 0:
            limit_clause = f"LIMIT {self.limit_val}"
            if self.offset_val is not None and self.offset_val > 0:
                limit_clause += f" OFFSET {self.offset_val}"
            parts.append(limit_clause)

        return "\n".join(parts)

    def get_metadata(self):
        return {
            'schema': self.schema,
            'table': self.table,
            'alias': self.alias,
            'selected_columns': self.selected_columns,
            'conditions': self.where_conditions,
            'group_by': self.group_by_cols,
            'having': self.having_conditions,
            'order_by': self.order_by_cols,
            'limit': self.limit_val,
            'offset': self.offset_val,
            'distinct': self.distinct_flag
        }


# ============================================================
# DATABASE MANAGER WITH SCHEMA SUPPORT
# ============================================================

class SchemaDatabaseManager:
    """Database manager with schema awareness"""

    # Schema descriptions
    SCHEMA_DESCRIPTIONS = {
        'GM': 'General Management — Complaints, Forwarding, DMS',
        'HM': 'Healthcare Management — Medical Records, Lab Tests, Certificates',
        'PM': 'Personnel Management — Employee Data, Payroll, Leave',
        'SI': 'Stores & Inventory — Materials, Purchase, Tenders',
        'SA': 'Security & Administration — User Management, Roles',
        'TA': 'Traffic & Accounts — Ticketing, Freight, Accounting',
    }

    # Category mapping (business-friendly names)
    CATEGORY_MAP = {
        'GM': 'General Management',
        'HM': 'Healthcare Management',
        'PM': 'Personnel Management',
        'SI': 'Stores & Inventory',
        'SA': 'Security & Administration',
        'TA': 'Traffic & Accounts',
    }

    def __init__(self, json_file_path: str = None):
        self.connection = None
        self.schema_data = None
        self.json_file_path = json_file_path
        self.schemas = {}  # schema_name -> {table_name: table_info}
        self.tables_created = 0
        self.total_tables = 0

    def load_schema_from_json(self, json_path: str = None) -> Dict:
        """Load schema from metadata.json"""
        path = json_path or self.json_file_path

        if not path or not os.path.exists(path):
            logger.warning(f"JSON file not found: {path}")
            return {}

        try:
            with open(path, 'r', encoding='utf-8') as f:
                self.schema_data = json.load(f)

            # Organize by schema
            self.schemas = {}
            self.total_tables = 0

            for schema_name, schema_tables in self.schema_data.items():
                self.schemas[schema_name] = {}
                for table_name, table_info in schema_tables.items():
                    self.schemas[schema_name][table_name] = {
                        'columns': table_info.get('columns', []),
                        'keys': table_info.get('keys', {}),
                        # *** FIX: preserve column_types if present (live feed enriches this) ***
                        'column_types': table_info.get('column_types', {}),
                    }
                    self.total_tables += 1

            logger.info(f"Loaded schema with {len(self.schemas)} schemas, {self.total_tables} tables")
            return self.schema_data

        except Exception as e:
            logger.error(f"Error loading JSON: {e}")
            return {}

    def init_database(self):
        """Initialize database connection and create tables"""
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        cursor = self.connection.cursor()

        # Create tables for each schema
        for schema_name, tables in self.schemas.items():
            for table_name, table_info in tables.items():
                columns = table_info.get('columns', [])
                if not columns:
                    continue

                full_table_name = f"{schema_name}_{table_name}"

                col_defs = []
                for col in columns:
                    data_type = self._infer_data_type(col)
                    col_defs.append(f"{col} {data_type}")

                create_sql = f"CREATE TABLE IF NOT EXISTS {full_table_name} ({', '.join(col_defs)})"
                try:
                    cursor.execute(create_sql)
                    self.tables_created += 1
                except Exception as e:
                    logger.debug(f"Could not create table {full_table_name}: {e}")

        self.connection.commit()
        logger.info(f"Created {self.tables_created} tables")
        return self.connection

    def _infer_data_type(self, column_name: str) -> str:
        """Infer data type from column name — used only when column_types is absent.
        Returns lowercase type string to match PostgreSQL conventions."""
        col_lower = column_name.lower()
        # Date/time: must contain 'date', 'dt' standalone, or 'timestamp'
        if any(col_lower == x or col_lower.endswith('_' + x) or col_lower.startswith(x + '_')
               for x in ['date', 'dt', 'timestamp']):
            return 'date'
        if 'timestamp' in col_lower:
            return 'timestamp'
        # Numeric: only whole-word numeric suffixes — avoid matching 'no' in every column
        NUMERIC_SUFFIXES = {'_qty', '_count', '_cnt', '_amt', '_amount', '_num', '_id',
                            '_seq', '_rate', '_price', '_cost', '_total', '_pct', '_percent'}
        if any(col_lower.endswith(sfx) for sfx in NUMERIC_SUFFIXES):
            return 'integer'
        return 'text'

    def get_schemas(self) -> List[Dict]:
        """Get all schemas with counts"""
        result = []
        for schema_name, tables in self.schemas.items():
            result.append({
                'name': schema_name,
                'description': self.SCHEMA_DESCRIPTIONS.get(schema_name, f'{schema_name} Schema'),
                'table_count': len(tables)
            })
        return result

    def get_categories(self) -> Dict[str, str]:
        """Get schema-to-category mapping"""
        return dict(self.CATEGORY_MAP)

    def get_tables(self, schema_name: str) -> List[Dict]:
        """Get all tables in a schema"""
        if schema_name not in self.schemas:
            return []

        tables = []
        for table_name, table_info in self.schemas[schema_name].items():
            tables.append({
                'name': table_name,
                'columns': table_info.get('columns', []),
                'column_count': len(table_info.get('columns', [])),
                'has_keys': bool(table_info.get('keys', {}))
            })
        return tables

    def get_all_tables(self) -> Dict[str, List[str]]:
        """Get all tables grouped by schema"""
        result = {}
        for schema_name, tables in self.schemas.items():
            result[schema_name] = list(tables.keys())
        return result

    def get_table_info(self, schema_name: str, table_name: str) -> Dict:
        """Get detailed information about a specific table"""
        if schema_name not in self.schemas:
            raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found")

        if table_name not in self.schemas[schema_name]:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in schema '{schema_name}'")

        table_info = self.schemas[schema_name][table_name]
        columns = table_info.get('columns', [])
        keys = table_info.get('keys', {})

        # Process primary keys and foreign keys
        primary_keys = []
        foreign_keys = []

        for col_name, key_info in keys.items():
            if key_info.get('type') == 'PRIMARY KEY':
                primary_keys.append(col_name)

            foreign_table = key_info.get('foreign_table')
            if foreign_table and foreign_table != '-':
                foreign_keys.append({
                    'column': col_name,
                    'references_table': foreign_table,
                    'references_column': key_info.get('foreign_column', col_name)
                })

        return {
            'name': table_name,
            'schema': schema_name,
            'columns': columns,
            'column_count': len(columns),
            'primary_keys': primary_keys,
            'foreign_keys': foreign_keys,
            'has_composite_key': len(primary_keys) > 1
        }

    def execute_query(self, sql: str, limit: int = 1000):
        """Execute a SQL query"""
        cursor = self.connection.cursor()

        if limit and "LIMIT" not in sql.upper():
            sql = f"{sql} LIMIT {limit}"

        try:
            cursor.execute(sql)
            columns = [description[0] for description in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            data = [dict(zip(columns, row)) for row in rows]
            return data, columns, len(data), None
        except Exception as e:
            return [], [], 0, str(e)

    def search_tables(self, search_term: str) -> List[Dict]:
        """Search for tables across all schemas"""
        results = []
        search_term_lower = search_term.lower()

        for schema_name, tables in self.schemas.items():
            for table_name in tables.keys():
                if search_term_lower in table_name.lower():
                    results.append({
                        'schema': schema_name,
                        'table': table_name,
                        'full_name': f"{schema_name}.{table_name}"
                    })

        return results

    def search_columns(self, search_term: str) -> List[Dict]:
        """Search for columns across all tables"""
        results = []
        search_term_lower = search_term.lower()

        for schema_name, tables in self.schemas.items():
            for table_name, table_info in tables.items():
                columns = table_info.get('columns', [])
                for col in columns:
                    if search_term_lower in col.lower():
                        results.append({
                            'schema': schema_name,
                            'table': table_name,
                            'column': col,
                            'full_name': f"{schema_name}.{table_name}.{col}"
                        })
                        if len(results) >= 100:
                            return results

        return results

    def get_stats(self) -> Dict:
        """Get database statistics"""
        total_columns = 0
        for schema_name, tables in self.schemas.items():
            for table_info in tables.values():
                total_columns += len(table_info.get('columns', []))

        return {
            'total_schemas': len(self.schemas),
            'total_tables': self.total_tables,
            'total_columns': total_columns,
            'schemas': list(self.schemas.keys()),
            'tables_per_schema': {s: len(t) for s, t in self.schemas.items()}
        }

    def get_column_names(self, schema_name: str, table_name: str) -> set:
        """Return the set of column names for a table (empty if table not found)."""
        schema = self.schemas.get(schema_name, {})
        table = schema.get(table_name, {})
        cols = table.get('columns', [])
        result: set = set()
        for c in cols:
            if isinstance(c, dict):
                name = c.get('name', '')
                if name:
                    result.add(name)
            elif c:
                result.add(str(c))
        return result

    def validate_generate_request(self, request, alias_map: dict) -> List[str]:
        """
        Validate a GenerateRequest against metadata.
        Returns a list of human-readable error strings (empty = valid).
        """
        errors: List[str] = []

        # Pre-build per-alias column sets
        alias_cols: Dict[str, set] = {}
        for tbl in request.tables:
            col_set = self.get_column_names(tbl.schema, tbl.table)
            if col_set:
                alias_cols[tbl.alias] = col_set

        # ── SELECT columns
        for c in (request.columns or []):
            if not c.column:
                continue
            col_set = alias_cols.get(c.table)
            if col_set is not None and c.column not in col_set:
                tbl = alias_map.get(c.table)
                tbl_name = tbl.table if tbl else c.table
                errors.append(
                    f'SELECT: column "{c.column}" does not exist in "{tbl_name}".'
                )

        # ── JOIN conditions
        for j in (request.joins or []):
            if not (j.from_column and j.from_alias and j.to_column and j.to_alias):
                errors.append("JOIN condition is incomplete — all four fields are required.")
                continue

            from_set = alias_cols.get(j.from_alias)
            if from_set is not None and j.from_column not in from_set:
                tbl = alias_map.get(j.from_alias)
                errors.append(
                    f'JOIN ON: column "{j.from_column}" not found in "{tbl.table if tbl else j.from_alias}".'
                )

            to_set = alias_cols.get(j.to_alias)
            if to_set is not None and j.to_column not in to_set:
                tbl = alias_map.get(j.to_alias)
                errors.append(
                    f'JOIN ON: column "{j.to_column}" not found in "{tbl.table if tbl else j.to_alias}".'
                )

        # ── WHERE conditions
        for cond in (request.conditions or []):
            if not cond.column:
                continue
            col_set = alias_cols.get(cond.table)
            if col_set is not None and cond.column not in col_set:
                tbl = alias_map.get(cond.table)
                errors.append(
                    f'WHERE: column "{cond.column}" not found in "{tbl.table if tbl else cond.table}".'
                )

        # ── GROUP BY
        for g in (request.group_by or []):
            if "." in g:
                alias, col = g.split(".", 1)
                col_set = alias_cols.get(alias)
                if col_set is not None and col not in col_set:
                    tbl = alias_map.get(alias)
                    errors.append(
                        f'GROUP BY: column "{col}" not found in "{tbl.table if tbl else alias}".'
                    )

        # ── ORDER BY
        for o in (request.order_by or []):
            if o.column and "." in o.column:
                alias, col = o.column.split(".", 1)
                col_set = alias_cols.get(alias)
                if col_set is not None and col not in col_set:
                    tbl = alias_map.get(alias)
                    errors.append(
                        f'ORDER BY: column "{col}" not found in "{tbl.table if tbl else alias}".'
                    )

        return errors


# ============================================================
# LIVE POSTGRESQL CONNECTION — Pydantic Models
# ============================================================

class DBConnectionConfig(BaseModel):
    host: str = "localhost"
    port: int = 5432
    database: str
    username: str
    password: str

class DBConnectionStatus(BaseModel):
    configured: bool
    last_refresh: Optional[str] = None
    source: str = "none"           # "live" | "cache" | "none"
    host_masked: Optional[str] = None
    database: Optional[str] = None
    schemas_loaded: int = 0
    tables_loaded: int = 0


# ============================================================
# CREDENTIAL MANAGER  — encrypts password on disk
# ============================================================

class CredentialManager:
    """
    Saves connection credentials to a local JSON file in the user's home dir.
    Password is encrypted with Fernet (symmetric, machine-local key).
    """
    CONFIG_DIR  = os.path.join(os.path.expanduser("~"), "SQL_Query_Generator_config")
    CONFIG_FILE = os.path.join(CONFIG_DIR, "conn_config.json")
    KEY_FILE    = os.path.join(CONFIG_DIR, "conn.key")

    def _get_or_create_key(self) -> bytes:
        os.makedirs(self.CONFIG_DIR, exist_ok=True)
        if os.path.exists(self.KEY_FILE):
            with open(self.KEY_FILE, "rb") as f:
                return f.read()
        try:
            from cryptography.fernet import Fernet
            key = Fernet.generate_key()
            with open(self.KEY_FILE, "wb") as f:
                f.write(key)
            return key
        except ImportError:
            # Fallback: no encryption, store base64-encoded password
            return b""

    def save(self, config: DBConnectionConfig) -> None:
        os.makedirs(self.CONFIG_DIR, exist_ok=True)
        key = self._get_or_create_key()
        try:
            from cryptography.fernet import Fernet
            f = Fernet(key)
            encrypted_pw = f.encrypt(config.password.encode()).decode()
        except ImportError:
            import base64
            encrypted_pw = base64.b64encode(config.password.encode()).decode()

        data = {
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "username": config.username,
            "password_enc": encrypted_pw,
        }
        with open(self.CONFIG_FILE, "w", encoding="utf-8") as fp:
            json.dump(data, fp, indent=2)
        logger.info(f"Credentials saved for {config.username}@{config.host}:{config.port}/{config.database}")

    def load(self) -> Optional[DBConnectionConfig]:
        if not os.path.exists(self.CONFIG_FILE):
            return None
        try:
            with open(self.CONFIG_FILE, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            key = self._get_or_create_key()
            try:
                from cryptography.fernet import Fernet
                f = Fernet(key)
                password = f.decrypt(data["password_enc"].encode()).decode()
            except Exception:
                import base64
                password = base64.b64decode(data["password_enc"].encode()).decode()
            return DBConnectionConfig(
                host=data["host"],
                port=data["port"],
                database=data["database"],
                username=data["username"],
                password=password,
            )
        except Exception as e:
            logger.error(f"Failed to load credentials: {e}")
            return None

    def clear(self) -> None:
        if os.path.exists(self.CONFIG_FILE):
            os.remove(self.CONFIG_FILE)
            logger.info("Credentials cleared")

    def is_configured(self) -> bool:
        return os.path.exists(self.CONFIG_FILE)

    def host_masked(self) -> Optional[str]:
        if not os.path.exists(self.CONFIG_FILE):
            return None
        try:
            with open(self.CONFIG_FILE, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            return data.get("host")
        except Exception:
            return None

    def database_name(self) -> Optional[str]:
        if not os.path.exists(self.CONFIG_FILE):
            return None
        try:
            with open(self.CONFIG_FILE, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            return data.get("database")
        except Exception:
            return None


# ============================================================
# POSTGRES SCHEMA FETCHER — live introspection
# ============================================================

class PostgresSchemaFetcher:
    """
    Opens a SHORT-LIVED read-only PostgreSQL connection, introspects
    information_schema, and returns a metadata.json-compatible dict
    that also includes column_types for smarter UI rendering.
    """

    EXCLUDED_SCHEMAS = frozenset({
        "pg_catalog", "information_schema", "pg_toast",
        "pg_temp_1", "pg_toast_temp_1",
    })

    def test_connection(self, config: DBConnectionConfig) -> dict:
        """Quick connectivity check — connect and immediately disconnect."""
        start = time.time()
        try:
            import psycopg2
        except ImportError:
            return {"success": False, "error": "psycopg2 not installed. Run: pip install psycopg2-binary"}
        try:
            conn = psycopg2.connect(
                host=config.host,
                port=config.port,
                dbname=config.database,
                user=config.username,
                password=config.password,
                connect_timeout=10,
                options="-c default_transaction_read_only=on",
            )
            conn.close()
            latency = round((time.time() - start) * 1000)
            return {"success": True, "latency_ms": latency}
        except Exception as e:
            err = str(e)
            if "timeout" in err.lower() or "could not connect" in err.lower() or "connection refused" in err.lower():
                msg = f"Server unreachable ({config.host}:{config.port}). Is FortiClient VPN active?"
            elif "password authentication" in err.lower():
                msg = "Authentication failed. Check username/password."
            elif "database" in err.lower() and "does not exist" in err.lower():
                msg = f'Database "{config.database}" not found on server.'
            else:
                msg = err
            return {"success": False, "error": msg}

    def fetch_schema(self, config: DBConnectionConfig,
                      base_schema_data: dict = None) -> dict:
        """
        Connect → introspect → disconnect immediately.

        If `base_schema_data` is supplied (the existing metadata.json structure):
          - ONLY queries the schemas and tables that already exist in metadata.json
            (e.g. GM, HM, PM, SI, SA, TA — never 'public' or anything else)
          - Enriches each table with real `column_types` from the live DB
          - Tables that exist in metadata.json but are not found in the live DB
            are kept intact (column_types stays empty — inferred types are used)
          - Tables that exist ONLY in the live DB are completely IGNORED

        If `base_schema_data` is None, falls back to the old behaviour
        (fetch all non-system schemas — for diagnostics only).
        """
        try:
            import psycopg2
        except ImportError:
            raise RuntimeError("psycopg2 not installed. Run: pip install psycopg2-binary")

        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.database,
            user=config.username,
            password=config.password,
            connect_timeout=10,
            options="-c default_transaction_read_only=on",
        )
        try:
            if base_schema_data:
                result = self._introspect_targeted(conn, base_schema_data)
            else:
                result = self._introspect(conn)
        finally:
            conn.close()  # always disconnect immediately
        return result

    # ------------------------------------------------------------------
    # Metadata-guided enrichment (primary path)
    # ------------------------------------------------------------------

    def _introspect_targeted(self, conn, base_schema_data: dict) -> dict:
        """
        Only query schemas/tables that already exist in base_schema_data.
        Returns a new dict with the same structure as base_schema_data but
        with `column_types` populated from the live PostgreSQL database.
        """
        cur = conn.cursor()

        # Build the exact (schema, table) pairs we care about.
        # metadata.json uses UPPERCASE schema keys (GM, HM, PM ...)
        # The actual DB may use lowercase (gm, hm, pm ...) — we match both.
        target_pairs: list[tuple[str, str]] = []
        for schema_key, tables in base_schema_data.items():
            for table_name in tables.keys():
                target_pairs.append((schema_key.lower(), table_name))

        if not target_pairs:
            return base_schema_data  # nothing to enrich

        # Build a VALUES list for the IN clause: (lower_schema, table_name)
        placeholders = ",".join(["%s"] * len(target_pairs))
        flat_values = [item for pair in target_pairs for item in pair]

        # Query columns for ONLY those (schema, table) pairs
        cur.execute(f"""
            SELECT
                table_schema,
                table_name,
                column_name,
                CASE
                    WHEN data_type = 'character varying'                  THEN 'varchar'
                    WHEN data_type = 'character'                          THEN 'char'
                    WHEN data_type = 'timestamp without time zone'        THEN 'timestamp'
                    WHEN data_type = 'timestamp with time zone'           THEN 'timestamptz'
                    WHEN data_type = 'double precision'                   THEN 'float'
                    WHEN data_type = 'integer'                            THEN 'integer'
                    WHEN data_type = 'bigint'                             THEN 'bigint'
                    WHEN data_type = 'smallint'                           THEN 'smallint'
                    WHEN data_type = 'numeric'                            THEN 'numeric'
                    WHEN data_type = 'boolean'                            THEN 'boolean'
                    WHEN data_type = 'text'                               THEN 'text'
                    WHEN data_type = 'date'                               THEN 'date'
                    ELSE data_type
                END AS data_type
            FROM information_schema.columns
            WHERE (LOWER(table_schema), table_name) IN ({placeholders})
            ORDER BY table_schema, table_name, ordinal_position
        """, flat_values)
        live_rows = cur.fetchall()

        # Build lookup: (UPPER_schema, table) -> {col -> dtype}
        live_types: dict[tuple[str, str], dict[str, str]] = {}
        for schema, table, col, dtype in live_rows:
            key = (schema.upper(), table)
            if key not in live_types:
                live_types[key] = {}
            live_types[key][col] = dtype

        # Deep-copy the base structure and inject real column_types
        import copy
        result = copy.deepcopy(base_schema_data)
        for schema_key, tables in result.items():
            for table_name, table_info in tables.items():
                live = live_types.get((schema_key.upper(), table_name), {})
                if live:
                    # Merge: existing inferred types are overridden by real DB types
                    merged = dict(table_info.get('column_types', {}))
                    merged.update(live)
                    table_info['column_types'] = merged
                    logger.debug(f"Enriched {schema_key}.{table_name} with {len(live)} live column types")
                else:
                    # Table not found in DB — keep existing structure unchanged
                    logger.debug(f"Table {schema_key}.{table_name} not found in live DB; keeping cached types")

        return result

    # ------------------------------------------------------------------
    # Metadata-guided enrichment  (PRIMARY PATH)
    # ------------------------------------------------------------------

    def _introspect_targeted(self, conn, base_schema_data: dict) -> dict:
        """
        Queries ONLY the table names that already exist in base_schema_data.

        Architecture insight (from client's metadata extraction SQL):
          - ALL application tables live in the PostgreSQL 'public' schema.
          - The 'schema' grouping (GM, HM, PM, SI, SA, TA) is a LOGICAL concept
            derived from UPPER(LEFT(table_name, 2)) — it is NOT a real pg schema.
          - So we filter by table_name IN (...) within 'public', not by schema name.

        Returns a deep-copy of base_schema_data with 'column_types' and 'keys'
        enriched from the live database.  Tables not found in the live DB are
        kept intact so the full metadata structure is always preserved.
        """
        cur = conn.cursor()

        # Collect every table name across all logical modules
        all_table_names: list[str] = []
        for tables in base_schema_data.values():
            all_table_names.extend(tables.keys())

        if not all_table_names:
            return base_schema_data

        placeholders = ",".join(["%s"] * len(all_table_names))

        # ── Single query adapted from the client's reference SQL ─────────
        # Uses the same pg_constraint approach for keys and
        # information_schema.columns for types — filtered to known table names.
        cur.execute(f"""
            WITH key_details AS (
                SELECT
                    c.conrelid::regclass::text                  AS table_name,
                    jsonb_object_agg(
                        a.attname,
                        jsonb_build_object(
                            'type',
                                CASE WHEN c.contype = 'p'
                                     THEN 'PRIMARY KEY'
                                     ELSE 'FOREIGN KEY'
                                END,
                            'foreign_table',
                                c.confrelid::regclass::text,
                            'foreign_column',
                                (SELECT attname
                                   FROM pg_attribute
                                  WHERE attrelid = c.confrelid
                                    AND attnum   = c.confkey[1])
                        )
                    ) AS keys
                FROM pg_constraint c
                JOIN pg_attribute  a
                  ON a.attrelid = c.conrelid
                 AND a.attnum   = ANY(c.conkey)
                WHERE c.connamespace = 'public'::regnamespace
                  AND c.contype IN ('p', 'f')
                GROUP BY c.conrelid
            ),
            col_info AS (
                SELECT
                    table_name,
                    column_name,
                    ordinal_position,
                    CASE
                        WHEN data_type = 'character varying'           THEN 'varchar'
                        WHEN data_type = 'character'                   THEN 'char'
                        WHEN data_type = 'timestamp without time zone' THEN 'timestamp'
                        WHEN data_type = 'timestamp with time zone'    THEN 'timestamptz'
                        WHEN data_type = 'double precision'            THEN 'float'
                        WHEN data_type = 'integer'                     THEN 'integer'
                        WHEN data_type = 'bigint'                      THEN 'bigint'
                        WHEN data_type = 'smallint'                    THEN 'smallint'
                        WHEN data_type = 'numeric'                     THEN 'numeric'
                        WHEN data_type = 'boolean'                     THEN 'boolean'
                        WHEN data_type = 'text'                        THEN 'text'
                        WHEN data_type = 'date'                        THEN 'date'
                        ELSE data_type
                    END AS norm_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name IN ({placeholders})
            )
            SELECT
                ci.table_name,
                jsonb_object_agg(ci.column_name, ci.norm_type
                                 ORDER BY ci.ordinal_position)  AS column_types,
                COALESCE(kd.keys, '{{}}'::jsonb)                AS keys
            FROM col_info ci
            LEFT JOIN key_details kd ON kd.table_name = ci.table_name
            GROUP BY ci.table_name, kd.keys
            ORDER BY ci.table_name
        """, all_table_names)

        rows = cur.fetchall()

        # Build lookup: table_name -> {column_types, keys}
        live: dict[str, dict] = {}
        for table_name, col_types_json, keys_json in rows:
            live[table_name] = {
                "column_types": dict(col_types_json) if col_types_json else {},
                "keys":         dict(keys_json)       if keys_json       else {},
            }

        matched   = len(live)
        unmatched = len(all_table_names) - matched
        logger.info(
            "Live enrichment: %d/%d tables found in DB (%d not in live DB, kept from cache)",
            matched, len(all_table_names), unmatched
        )

        # Deep-copy the base structure and inject live data
        import copy
        result = copy.deepcopy(base_schema_data)
        for schema_key, tables in result.items():
            for table_name, table_info in tables.items():
                if table_name in live:
                    lv = live[table_name]
                    # Merge column_types: live DB overrides cached inferred types
                    merged = dict(table_info.get("column_types", {}))
                    merged.update(lv["column_types"])
                    table_info["column_types"] = merged
                    # Update keys from live DB if available
                    if lv["keys"]:
                        table_info["keys"] = lv["keys"]
                # else: table not in live DB — keep cached data unchanged

        return result

    # ------------------------------------------------------------------
    # Full introspection  (fallback — used when no base_schema_data)
    # ------------------------------------------------------------------

    def _introspect(self, conn) -> dict:
        """
        Fetch ALL application tables from the 'public' schema and group them
        by the logical module prefix  UPPER(LEFT(table_name, 2))  — exactly
        the same logic as the client's metadata-extraction SQL.
        Used as fallback only when no base_schema_data is available.
        """
        cur = conn.cursor()

        # Mirror the client's reference SQL exactly
        cur.execute("""
            WITH key_details AS (
                SELECT
                    c.conrelid::regclass::text AS table_name,
                    jsonb_object_agg(
                        a.attname,
                        jsonb_build_object(
                            'type',
                                CASE WHEN c.contype = 'p'
                                     THEN 'PRIMARY KEY'
                                     ELSE 'FOREIGN KEY'
                                END,
                            'foreign_table',
                                c.confrelid::regclass::text,
                            'foreign_column',
                                (SELECT attname
                                   FROM pg_attribute
                                  WHERE attrelid = c.confrelid
                                    AND attnum   = c.confkey[1])
                        )
                    ) AS keys
                FROM pg_constraint c
                JOIN pg_attribute  a
                  ON a.attrelid = c.conrelid
                 AND a.attnum   = ANY(c.conkey)
                WHERE c.connamespace = 'public'::regnamespace
                  AND c.contype IN ('p', 'f')
                GROUP BY c.conrelid
            ),
            col_info AS (
                SELECT
                    table_name,
                    column_name,
                    ordinal_position,
                    CASE
                        WHEN data_type = 'character varying'           THEN 'varchar'
                        WHEN data_type = 'character'                   THEN 'char'
                        WHEN data_type = 'timestamp without time zone' THEN 'timestamp'
                        WHEN data_type = 'timestamp with time zone'    THEN 'timestamptz'
                        WHEN data_type = 'double precision'            THEN 'float'
                        WHEN data_type = 'integer'                     THEN 'integer'
                        WHEN data_type = 'bigint'                      THEN 'bigint'
                        WHEN data_type = 'smallint'                    THEN 'smallint'
                        WHEN data_type = 'numeric'                     THEN 'numeric'
                        WHEN data_type = 'boolean'                     THEN 'boolean'
                        WHEN data_type = 'text'                        THEN 'text'
                        WHEN data_type = 'date'                        THEN 'date'
                        ELSE data_type
                    END AS norm_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
            )
            SELECT
                UPPER(LEFT(ci.table_name, 2))                   AS module_prefix,
                ci.table_name,
                jsonb_object_agg(ci.column_name, ci.norm_type
                                 ORDER BY ci.ordinal_position)  AS column_types,
                jsonb_agg(ci.column_name ORDER BY ci.ordinal_position) AS columns,
                COALESCE(kd.keys, '{}'::jsonb)                  AS keys
            FROM col_info ci
            LEFT JOIN key_details kd ON kd.table_name = ci.table_name
            GROUP BY ci.table_name, kd.keys
            ORDER BY ci.table_name
        """)
        rows = cur.fetchall()

        result: dict = {}
        for module_prefix, table_name, col_types_json, columns_json, keys_json in rows:
            if module_prefix not in result:
                result[module_prefix] = {}
            result[module_prefix][table_name] = {
                "columns":      list(columns_json)       if columns_json  else [],
                "column_types": dict(col_types_json)     if col_types_json else {},
                "keys":         dict(keys_json)           if keys_json     else {},
            }

        return result


# ============================================================
# Global singletons for live connection
# ============================================================

credential_manager = CredentialManager()
pg_fetcher = PostgresSchemaFetcher()

# Tracks refresh state in memory
_db_connection_status = {
    "last_refresh": None,
    "source": "cache",       # "live" | "cache" | "none"
    "schemas_loaded": 0,
    "tables_loaded": 0,
}


# ============================================================
# Global database manager
# ============================================================

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Use get_resource_path so the bundled .exe can locate metadata.json
JSON_PATH = get_resource_path(os.path.join("db_files", "metadata.json"))
db_manager = SchemaDatabaseManager(json_file_path=JSON_PATH)

# Port is resolved in __main__ (may differ from 8000 if port is in use);
# lifespan reads this global to print the correct URL and poll health.
_SERVER_PORT: int = 8000


# ============================================================
# LIFESPAN (replaces deprecated on_event)
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown logic"""
    # Startup
    print("=" * 60)
    print("Starting SQL Query Generator API v5.0")
    print("=" * 60)

    print("Loading schema from JSON...")
    db_manager.load_schema_from_json()

    if db_manager.schema_data:
        print("Loaded %d schemas with %d tables" % (len(db_manager.schemas), db_manager.total_tables))
        for schema_name, tables in db_manager.schemas.items():
            print("   - %s: %d tables" % (schema_name, len(tables)))

    print("Creating database tables...")
    db_manager.init_database()

    print("Database initialized")
    print("Tables created: %d" % db_manager.tables_created)
    print("Server ready at http://127.0.0.1:%d" % _SERVER_PORT)
    print("Log file: %s" % _LOG_FILE)
    print("Frontend dist dir : %s" % DIST_DIR)
    print("index.html exists : %s" % os.path.isfile(INDEX_HTML))
    print("=" * 60)

    # Open browser ONLY after server is confirmed healthy — poll /health
    def _open_browser_when_ready():
        url = "http://127.0.0.1:%d" % _SERVER_PORT
        import urllib.request
        for _ in range(30):          # wait up to 15 seconds
            try:
                urllib.request.urlopen(url + "/health", timeout=1)
                webbrowser.open(url)
                return
            except Exception:
                time.sleep(0.5)
        # Last resort: open anyway after 15 s
        webbrowser.open(url)

    threading.Thread(target=_open_browser_when_ready, daemon=True).start()

    yield  # App runs here

    # Shutdown
    if db_manager.connection:
        db_manager.connection.close()
    print("Server stopped")


# ============================================================
# FASTAPI APPLICATION
# ============================================================

app = FastAPI(
    title="SQL Query Generator API",
    description="Complete SQL query generation API with schema-based navigation",
    version="5.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# HELPER: build SQL from a GenerateRequest
# ============================================================

def _build_sql_from_request(request: GenerateRequest) -> str:
    """Shared logic to build SQL from a GenerateRequest — used by /query/generate and /query/union."""
    alias_map = {t.alias: t for t in request.tables}
    main = request.tables[0]
    helper = SchemaQueryGenerator(main.schema, main.table)

    # ── Helper: normalise a column reference
    def col_ref(table_part: str, col_part: str) -> str:
        t = table_part.strip() if table_part else ""
        c = col_part.strip()
        if t and t in alias_map:
            return f"{t}.{c}"
        return c

    # ── SELECT clause
    select_parts: List[str] = []

    # DISTINCT
    distinct_prefix = "DISTINCT " if request.distinct else ""

    # Aggregate functions first
    for agg in (request.aggregates or []):
        func = agg.get("func", "COUNT").upper()
        acol = agg.get("column", "*")
        aalias = agg.get("alias", "")
        expr = f"{func}({acol})"
        if aalias:
            expr += f" AS {aalias}"
        select_parts.append(expr)

    # Regular columns (with optional CAST)
    for c in (request.columns or []):
        ref = col_ref(c.table, c.column)
        # Apply CAST if requested
        cast = (c.cast_as or "").strip().upper()
        if cast:
            ref = f"CAST({ref} AS {cast})"
        if c.alias:
            ref += f" AS {c.alias}"
        select_parts.append(ref)

    # Computed columns: CASE expressions, scalar functions, window functions
    for expr in (request.computed_columns or []):
        if expr and expr.strip():
            select_parts.append(expr.strip())

    if not select_parts:
        if len(request.tables) > 1:
            select_parts = [f"{t.alias}.*" for t in request.tables]
        else:
            select_parts = ["*"]

    select_str = ",\n       ".join(select_parts)

    # ── FROM + JOIN clauses
    sql = f"SELECT {distinct_prefix}{select_str}\nFROM {main.table} {main.alias}"

    if request.joins:
        joined_aliases = {main.alias}
        for j in request.joins:
            to_tbl = alias_map.get(j.to_alias)
            if not to_tbl:
                continue
            if j.to_alias not in joined_aliases:
                sql += f"\n{j.join_type} {to_tbl.table} {j.to_alias}"
                joined_aliases.add(j.to_alias)
            op = getattr(j, 'operator', '=') or '='
            sql += f"\n  ON {j.from_alias}.{j.from_column} {op} {j.to_alias}.{j.to_column}"
    else:
        for extra in request.tables[1:]:
            sql += f"\n-- WARNING: no JOIN condition defined for {extra.table}"
            sql += f"\nCROSS JOIN {extra.table} {extra.alias}"

    # ── WHERE clause (supports AND / OR logic, EXISTS, ILIKE, and parenthesis groups)
    where_items: List[dict] = []
    for i, cond in enumerate(request.conditions or []):
        op = (cond.operator or "=").upper().strip()
        # EXISTS/NOT EXISTS don't need a column reference
        is_exists = op in ("EXISTS", "NOT EXISTS")
        if not cond.column and not is_exists:
            continue
        ref = col_ref(cond.table, cond.column) if not is_exists else ""

        # Look up the column's data type from schema metadata for smart quoting
        def _col_type_from_meta(cond) -> str:
            """Return the data type string from db_manager.schemas, or empty string."""
            try:
                t_alias = (cond.table or "").strip()
                col_name = (cond.column or "").strip()
                tbl_obj = alias_map.get(t_alias)
                if not tbl_obj:
                    return ""
                schema_upper = tbl_obj.schema.upper()
                tables_in_schema = db_manager.schemas.get(schema_upper, {})
                tbl_meta = tables_in_schema.get(tbl_obj.table, {})
                # column_types key is populated by live feed and JSON if present
                return tbl_meta.get("column_types", {}).get(col_name, "")
            except Exception:
                return ""

        ct = _col_type_from_meta(cond) if not is_exists else ""

        if op in ("IS NULL", "IS NOT NULL"):
            clause = f"{ref} {op}"
        elif op == "EXISTS":
            clause = f"EXISTS ({cond.value})"
        elif op == "NOT EXISTS":
            clause = f"NOT EXISTS ({cond.value})"
        elif op in ("IN", "NOT IN"):
            # For IN lists, format each item individually
            raw_items = str(cond.value).split(",")
            formatted_items = [helper._format_value(v.strip(), ct) for v in raw_items]
            clause = f"{ref} {op} ({', '.join(formatted_items)})"
        elif op == "BETWEEN":
            # Expect "start AND end" (any case) or "start,end"
            raw = str(cond.value).strip()
            and_match = re.split(r'\s+and\s+', raw, maxsplit=1, flags=re.IGNORECASE)
            if len(and_match) == 2:
                start, end = and_match[0].strip(), and_match[1].strip()
            elif "," in raw:
                parts2 = raw.split(",", 1)
                start, end = parts2[0].strip(), parts2[1].strip()
            else:
                start = end = raw
            clause = f"{ref} BETWEEN {helper._format_value(start, ct)} AND {helper._format_value(end, ct)}"
        elif op in ("LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE"):
            val = helper._format_value(cond.value, "text")  # LIKE values are always strings
            clause = f"{ref} {op} {val}"
        else:
            val = helper._format_value(cond.value, ct)
            clause = f"{ref} {op} {val}"

        logic = (getattr(cond, 'logic', 'AND') or 'AND').upper()
        where_items.append({
            "sql": clause,
            "logic": logic,
            "group_start": getattr(cond, 'group_start', False),
            "group_end": getattr(cond, 'group_end', False),
        })

    if where_items:
        where_str = ""
        for idx, item in enumerate(where_items):
            prefix = "" if idx == 0 else f"\n  {item['logic']} "
            open_p = "(" if item.get("group_start") else ""
            close_p = ")" if item.get("group_end") else ""
            where_str += f"{prefix}{open_p}{item['sql']}{close_p}"
        sql += "\nWHERE " + where_str

    # ── GROUP BY
    if request.group_by:
        grp_parts = []
        for g in request.group_by:
            if "." in g:
                parts = g.split(".", 1)
                grp_parts.append(col_ref(parts[0], parts[1]))
            else:
                grp_parts.append(g)
        sql += "\nGROUP BY " + ", ".join(grp_parts)

    # ── HAVING
    if request.having:
        having_parts: List[str] = []
        for h in request.having:
            if not h.column:
                continue
            ref = col_ref(h.table, h.column)
            op = (h.operator or "=").upper().strip()
            val = helper._format_value(h.value)
            having_parts.append(f"{ref} {op} {val}")
        if having_parts:
            sql += "\nHAVING " + " AND ".join(having_parts)

    # ── ORDER BY
    if request.order_by:
        ord_parts = []
        for o in request.order_by:
            if not o.column:
                continue
            if "." in o.column:
                parts = o.column.split(".", 1)
                c_ref = col_ref(parts[0], parts[1])
            else:
                c_ref = o.column
            ord_parts.append(f"{c_ref} {o.direction.upper()}")
        if ord_parts:
            sql += "\nORDER BY " + ", ".join(ord_parts)

    # ── LIMIT / OFFSET
    if request.limit is not None and request.limit > 0:
        sql += f"\nLIMIT {request.limit}"
    if request.offset is not None and request.offset > 0:
        sql += f"\nOFFSET {request.offset}"

    return sql


# ============================================================
# SCHEMA ENDPOINTS
# ============================================================

@app.get("/")
async def root():
    """Serve the React frontend at the root, or return API info when no frontend is bundled."""
    if os.path.isfile(INDEX_HTML):
        from fastapi.responses import FileResponse
        return FileResponse(INDEX_HTML)
    # Fallback: API info (development / API-only mode)
    logger.warning("index.html not found at: %s", INDEX_HTML)
    return {
        "name": "SQL Query Generator API",
        "version": "5.0.0",
        "status": "running",
        "frontend": "not bundled",
        "index_html_checked": INDEX_HTML,
        "schemas": [s['name'] for s in db_manager.get_schemas()],
        "total_schemas": len(db_manager.get_schemas()),
        "total_tables": db_manager.total_tables,
    }



# ============================================================
# LIVE POSTGRESQL CONNECTION — API Endpoints
# ============================================================

@app.post("/api/db-connection/test")
async def test_db_connection(config: DBConnectionConfig):
    """Test PostgreSQL connection without saving credentials."""
    result = pg_fetcher.test_connection(config)
    return result


@app.post("/api/db-connection/save")
async def save_db_connection(config: DBConnectionConfig):
    """Encrypt and save PostgreSQL credentials locally."""
    try:
        credential_manager.save(config)
        return {
            "success": True,
            "message": f"Credentials saved for {config.username}@{config.host}:{config.port}/{config.database}",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save credentials: {e}")


@app.get("/api/db-connection/status")
async def get_db_connection_status():
    """Return current connection configuration and last refresh info."""
    return DBConnectionStatus(
        configured=credential_manager.is_configured(),
        last_refresh=_db_connection_status.get("last_refresh"),
        source=_db_connection_status.get("source", "cache"),
        host_masked=credential_manager.host_masked(),
        database=credential_manager.database_name(),
        schemas_loaded=_db_connection_status.get("schemas_loaded", 0),
        tables_loaded=_db_connection_status.get("tables_loaded", 0),
    )


@app.delete("/api/db-connection/clear")
async def clear_db_connection():
    """Delete saved credentials from disk."""
    credential_manager.clear()
    return {"success": True, "message": "Credentials cleared"}


@app.post("/api/refresh-schema")
async def refresh_schema_from_live_db():
    """
    Connect to PostgreSQL, fetch live schema, hot-reload in memory,
    and overwrite metadata.json cache. Connection is closed immediately after.
    """
    global _db_connection_status

    config = credential_manager.load()
    if not config:
        raise HTTPException(
            status_code=400,
            detail="No database credentials configured. Please set up your connection in DB Settings first."
        )

    logger.info(f"Starting live schema refresh from {config.host}:{config.port}/{config.database}")

    try:
        # Pass the CURRENT metadata.json structure so fetch_schema only
        # queries the known schemas (GM, HM, PM, SI, SA, TA) and enriches
        # them with real column_types — never replacing or discarding tables.
        base_data = db_manager.schema_data  # may be None if not yet loaded
        schema_data = pg_fetcher.fetch_schema(config, base_data)
    except Exception as e:
        err = str(e)
        if "timeout" in err.lower() or "could not connect" in err.lower():
            raise HTTPException(
                status_code=503,
                detail=f"Cannot reach server ({config.host}:{config.port}). Is FortiClient VPN active? Original error: {err}"
            )
        raise HTTPException(status_code=500, detail=f"Schema fetch failed: {err}")

    if not schema_data:
        raise HTTPException(status_code=500, detail="No schemas found. Check that the DB user has access to the schemas.")

    # Count totals
    total_tables = sum(len(tables) for tables in schema_data.values())
    total_schemas = len(schema_data)

    # Save to metadata.json as cache
    target_path = db_manager.json_file_path or get_resource_path(os.path.join("db_files", "metadata.json"))
    try:
        # Backup existing file
        if os.path.exists(target_path):
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup = target_path.replace("metadata.json", f"metadata_backup_{ts}.json")
            shutil.copy2(target_path, backup)
            logger.info(f"Backed up existing schema to {backup}")
        with open(target_path, "w", encoding="utf-8") as f:
            json.dump(schema_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Schema saved to {target_path}")
    except Exception as e:
        logger.warning(f"Could not write metadata.json: {e}")

    # Hot-reload in memory
    db_manager.load_schema_from_json(target_path)
    db_manager.init_database()

    # Update status
    _db_connection_status = {
        "last_refresh": datetime.now().isoformat(),
        "source": "live",
        "schemas_loaded": total_schemas,
        "tables_loaded": total_tables,
    }

    logger.info(f"Live schema refresh complete: {total_schemas} schemas, {total_tables} tables")

    return {
        "success": True,
        "schemas_loaded": total_schemas,
        "tables_loaded": total_tables,
        "timestamp": _db_connection_status["last_refresh"],
        "message": f"Schema refreshed from live DB: {total_schemas} schemas, {total_tables} tables loaded",
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "database": "connected" if db_manager.connection else "disconnected",
        "schemas_loaded": len(db_manager.schemas),
        "tables_loaded": db_manager.total_tables
    }


@app.get("/schemas")
async def list_schemas():
    """List all available schemas — returns plain string names"""
    schemas = db_manager.get_schemas()
    schema_names = []
    for s in schemas:
        if isinstance(s, dict):
            schema_names.append(str(s.get('name', s)))
        else:
            schema_names.append(str(s))
    return {
        "schemas": schema_names,
        "count": len(schema_names)
    }


@app.get("/categories")
async def list_categories():
    """List schema categories (business-friendly names)"""
    return db_manager.get_categories()


@app.get("/schemas/{schema_name}")
async def get_schema_info(schema_name: str):
    """Get information about a specific schema"""
    schemas = db_manager.get_schemas()
    schema_info = None

    for s in schemas:
        if s['name'] == schema_name:
            schema_info = s
            break

    if not schema_info:
        raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found")

    tables = db_manager.get_tables(schema_name)

    return {
        "schema": schema_info,
        "tables": tables,
        "table_count": len(tables)
    }


@app.get("/schemas/{schema_name}/tables")
async def list_tables(schema_name: str):
    """List all tables in a schema"""
    tables = db_manager.get_tables(schema_name)

    if not tables:
        raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found or has no tables")

    return {
        "schema": schema_name,
        "tables": tables,
        "count": len(tables)
    }


@app.get("/schemas/{schema_name}/tables/{table_name}")
async def get_table_info(schema_name: str, table_name: str):
    """Get detailed information about a specific table"""
    try:
        table_info = db_manager.get_table_info(schema_name, table_name)
        return table_info
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/schema/upload")
async def upload_schema(file: UploadFile = File(...)):
    """Upload a new metadata.json file, create backups, and hot-reload the database"""
    try:
        content = await file.read()
        
        # Validate it is proper JSON
        try:
            schema_data = json.loads(content)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON file format")

        # Basic validation to ensure it looks like a schema structure
        if not isinstance(schema_data, dict):
            raise HTTPException(status_code=400, detail="Schema JSON must be a dictionary at its root")

        # Define file paths
        target_path = db_manager.json_file_path or get_resource_path("metadata.json")
        
        # Backup rotation logic (keep up to 3 backups)
        if os.path.exists(target_path):
            dir_name = os.path.dirname(target_path)
            base_name = os.path.basename(target_path)
            name_part, ext_part = os.path.splitext(base_name)
            
            # Rotate backups (3 -> delete, 2 -> 3, 1 -> 2, current -> 1)
            backup_3 = os.path.join(dir_name, f"{name_part}_backup_3{ext_part}")
            backup_2 = os.path.join(dir_name, f"{name_part}_backup_2{ext_part}")
            backup_1 = os.path.join(dir_name, f"{name_part}_backup_1{ext_part}")
            
            if os.path.exists(backup_3):
                os.remove(backup_3)
            if os.path.exists(backup_2):
                os.rename(backup_2, backup_3)
            if os.path.exists(backup_1):
                os.rename(backup_1, backup_2)
                
            # Create backup 1
            shutil.copy2(target_path, backup_1)
            logger.info(f"Created schema backup at {backup_1}")

        # Write the new file
        with open(target_path, "wb") as f:
            f.write(content)
            
        logger.info(f"Successfully saved new schema to {target_path}")

        # Hot-reload the database
        db_manager.load_schema_from_json(target_path)
        db_manager.init_database()
        
        return {
            "success": True,
            "message": "Schema updated and loaded successfully",
            "schemas_loaded": len(db_manager.schemas),
            "tables_loaded": db_manager.total_tables
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading schema: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to process schema upload: {str(e)}")


@app.get("/api/schema/backups")
async def list_schema_backups():
    """List all available schema backup files"""
    try:
        target_path = db_manager.json_file_path or get_resource_path("metadata.json")
        dir_name = os.path.dirname(target_path)
        base_name = os.path.basename(target_path)
        name_part, ext_part = os.path.splitext(base_name)
        
        backups = []
        for i in range(1, 4):
            backup_file = f"{name_part}_backup_{i}{ext_part}"
            backup_path = os.path.join(dir_name, backup_file)
            if os.path.exists(backup_path):
                stat = os.stat(backup_path)
                backups.append({
                    "filename": backup_file,
                    "size": stat.st_size,
                    "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                })
                
        return {"success": True, "backups": backups}
    except Exception as e:
        logger.error(f"Error listing backups: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class RestoreRequest(BaseModel):
    filename: str

@app.post("/api/schema/restore")
async def restore_schema_backup(request: RestoreRequest):
    """Restore a specific schema backup"""
    try:
        target_path = db_manager.json_file_path or get_resource_path("metadata.json")
        dir_name = os.path.dirname(target_path)
        backup_path = os.path.join(dir_name, request.filename)
        
        if not os.path.exists(backup_path):
            raise HTTPException(status_code=404, detail="Backup file not found")
            
        # Optional: Save current to backup before restoring
        # We'll skip complex rotation here and just overwrite, or we could rotate.
        # Let's just do a simple copy for the restore.
        shutil.copy2(backup_path, target_path)
        logger.info(f"Restored schema from {backup_path}")
        
        # Hot-reload
        db_manager.load_schema_from_json(target_path)
        db_manager.init_database()
        
        return {
            "success": True,
            "message": f"Successfully restored {request.filename}",
            "schemas_loaded": len(db_manager.schemas),
            "tables_loaded": db_manager.total_tables
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error restoring backup: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/schema/backups/{filename}")
async def delete_schema_backup(filename: str):
    """Delete a specific schema backup"""
    try:
        target_path = db_manager.json_file_path or get_resource_path("metadata.json")
        dir_name = os.path.dirname(target_path)
        backup_path = os.path.join(dir_name, filename)
        
        if not os.path.exists(backup_path):
            raise HTTPException(status_code=404, detail="Backup file not found")
            
        # Ensure it's actually a backup file being deleted, not something else
        if not filename.startswith("metadata_backup_"):
            raise HTTPException(status_code=403, detail="Can only delete backup files")
            
        os.remove(backup_path)
        logger.info(f"Deleted backup {backup_path}")
        
        return {"success": True, "message": f"Deleted {filename}"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting backup: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# QUERY GENERATION ENDPOINTS
# ============================================================

@app.post("/query/generate", response_model=QueryResponse)
async def generate_query(request: GenerateRequest):
    """Generate a valid PostgreSQL SELECT query (supports JOINs, aggregates, multi-table)"""
    start_time = time.time()

    try:
        if not request.tables:
            return QueryResponse(
                success=False,
                error="At least one table is required",
                execution_time=time.time() - start_time
            )

        # Build alias → TableInput map for quick lookups
        alias_map = {t.alias: t for t in request.tables}

        # ── Validate columns/joins against metadata
        validation_errors = db_manager.validate_generate_request(request, alias_map)
        if validation_errors:
            return QueryResponse(
                success=False,
                error="Validation failed:\n• " + "\n• ".join(validation_errors),
                execution_time=time.time() - start_time
            )

        sql = _build_sql_from_request(request)

        return QueryResponse(
            success=True,
            query=sql,
            execution_time=time.time() - start_time,
            row_count=0
        )

    except Exception as e:
        logger.error(f"Query generation error: {e}")
        return QueryResponse(
            success=False,
            error=str(e),
            execution_time=time.time() - start_time
        )


@app.post("/query/union", response_model=QueryResponse)
async def union_query(request: UnionQueryRequest):
    """Generate a UNION / UNION ALL / INTERSECT / EXCEPT query from multiple sub-queries"""
    start_time = time.time()

    try:
        if len(request.queries) < 2:
            return QueryResponse(
                success=False,
                error="At least 2 sub-queries are required for a UNION",
                execution_time=time.time() - start_time
            )

        operation = request.operation.upper().strip()
        valid_ops = {"UNION", "UNION ALL", "INTERSECT", "EXCEPT"}
        if operation not in valid_ops:
            return QueryResponse(
                success=False,
                error=f"Operation must be one of: {', '.join(valid_ops)}",
                execution_time=time.time() - start_time
            )

        # Build each sub-query
        sub_sqls = []
        for i, sub_req in enumerate(request.queries):
            if not sub_req.tables:
                return QueryResponse(
                    success=False,
                    error=f"Sub-query {i + 1} has no tables",
                    execution_time=time.time() - start_time
                )
            sub_sqls.append(_build_sql_from_request(sub_req))

        # Combine
        combined = f"\n\n{operation}\n\n".join(sub_sqls)

        # Optional CTE wrapping
        if request.wrap_in_cte:
            cte_name = request.wrap_in_cte.strip()
            combined = f"WITH {cte_name} AS (\n{combined}\n)\nSELECT * FROM {cte_name}"

        return QueryResponse(
            success=True,
            query=combined,
            execution_time=time.time() - start_time,
            row_count=0
        )

    except Exception as e:
        logger.error(f"Union query error: {e}")
        return QueryResponse(
            success=False,
            error=str(e),
            execution_time=time.time() - start_time
        )


@app.post("/query/execute", response_model=ExecutionResponse)
async def execute_query(request: SQLQueryRequest):
    """Returns the SQL ready for manual execution on PostgreSQL"""
    start_time = time.time()
    return ExecutionResponse(
        success=True,
        data=[],
        columns=[],
        row_count=0,
        execution_time=round(time.time() - start_time, 4),
        message="SQL is ready. Copy the query and run it on your PostgreSQL server.",
        sql=request.sql
    )


# ============================================================
# SEARCH ENDPOINTS
# ============================================================

@app.get("/search/tables")
async def search_tables(q: str = Query(..., min_length=1)):
    """Search for tables across all schemas"""
    results = db_manager.search_tables(q)
    return {
        "query": q,
        "results": results,
        "count": len(results)
    }


@app.get("/search/columns")
async def search_columns(q: str = Query(..., min_length=1)):
    """Search for columns across all tables"""
    results = db_manager.search_columns(q)
    return {
        "query": q,
        "results": results,
        "count": len(results)
    }


@app.get("/stats")
async def get_stats():
    """Get database statistics"""
    stats = db_manager.get_stats()
    return stats


# ============================================================
# COMPATIBILITY ENDPOINTS (matches frontend api.ts calls)
# ============================================================

@app.post("/sessions/create")
async def create_session():
    """Create a session token (frontend compatibility)"""
    return {"session_id": str(uuid.uuid4()), "message": "Session created"}


@app.get("/tables")
async def get_tables_flat(schema: str = Query(None, description="Schema name e.g. GM, PM")):
    """List tables — if schema is given, returns flat string names; otherwise returns all grouped by schema"""
    if schema:
        tables = db_manager.get_tables(schema)
        if not tables:
            return {"tables": [], "schema": schema}
        table_names = []
        for t in tables:
            if isinstance(t, dict):
                table_names.append(str(t.get('name', t)))
            else:
                table_names.append(str(t))
        return {"tables": table_names, "schema": schema, "count": len(table_names)}
    else:
        # No schema specified — return all grouped
        all_tables = db_manager.get_all_tables()
        return {"tables_by_schema": all_tables, "total": db_manager.total_tables}


@app.get("/tables/{table_name}/columns")
async def get_table_columns(table_name: str, schema: str = Query(..., description="Schema name")):
    """Get columns, PKs and FKs for a table — frontend-compatible format.
    Returns real column types from column_types (live feed) or inferred types as fallback.
    """
    try:
        table_info = db_manager.get_table_info(schema, table_name)
        # Prefer real column_types from schema metadata; fall back to name heuristic
        real_types: dict = db_manager.schemas.get(schema, {}).get(table_name, {}).get('column_types', {})
        columns = [
            {
                "name": c,
                "type": real_types.get(c) or db_manager._infer_data_type(c),
                "is_primary_key": c in table_info['primary_keys']
            }
            for c in table_info['columns']
        ]
        return {
            "columns": columns,
            "primary_keys": table_info['primary_keys'],
            "foreign_keys": [
                {
                    "column": fk['column'],
                    "references": f"{fk['references_table']}.{fk['references_column']}"
                }
                for fk in table_info['foreign_keys']
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# SAMPLE QUERIES ENDPOINT
# ============================================================

@app.get("/samples")
async def get_sample_queries():
    """Get sample SQL queries for testing"""
    return {
        "samples": [
            {
                "name": "Basic SELECT",
                "description": "Select all records from a table",
                "sql": "SELECT * FROM gmtk_coms_hdr LIMIT 10"
            },
            {
                "name": "SELECT with WHERE",
                "description": "Filter records by condition",
                "sql": "SELECT complaint_no, emp_no, status FROM gmtk_coms_hdr WHERE status = 'OPEN' LIMIT 10"
            },
            {
                "name": "Aggregate Query",
                "description": "Count records by status",
                "sql": "SELECT status, COUNT(*) AS count FROM gmtk_coms_hdr GROUP BY status"
            },
            {
                "name": "Date Range Filter",
                "description": "Filter by date range",
                "sql": "SELECT * FROM gmtk_coms_hdr WHERE reg_date BETWEEN '2024-01-01' AND '2024-12-31' LIMIT 10"
            },
            {
                "name": "ORDER BY",
                "description": "Sort results",
                "sql": "SELECT complaint_no, reg_date FROM gmtk_coms_hdr ORDER BY reg_date DESC LIMIT 10"
            },
            {
                "name": "JOIN Example",
                "description": "Join two tables",
                "sql": "SELECT e.emp_no, e.emp_firstname, c.complaint_no\nFROM pmm_employee e\nINNER JOIN gmtk_coms_hdr c\n  ON e.emp_no = c.emp_no\nLIMIT 10"
            }
        ]
    }


# ============================================================
# STATIC FILE SERVING - React frontend (production / .exe mode)
# ============================================================

try:
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import FileResponse

    if os.path.isdir(DIST_DIR):
        _assets_dir = os.path.join(DIST_DIR, "assets")
        if os.path.isdir(_assets_dir):
            app.mount("/assets", StaticFiles(directory=_assets_dir), name="assets")

        # SPA catch-all: every non-API path returns index.html so
        # client-side routing (React Router) works on page refresh.
        @app.get("/{full_path:path}", include_in_schema=False)
        async def spa_catch_all(full_path: str):
            # Check if it's a real file in dist/ (e.g., /krc-logo.png, /favicon.ico)
            file_path = os.path.join(DIST_DIR, full_path)
            if os.path.isfile(file_path):
                return FileResponse(file_path)
                
            if os.path.isfile(INDEX_HTML):
                return FileResponse(INDEX_HTML)
            return JSONResponse(status_code=404, content={"error": "Frontend build not found"})

        logger.info("Serving React frontend from: %s", DIST_DIR)
    else:
        logger.warning("frontend/dist not found at %s - UI will not be served", DIST_DIR)
except Exception as _static_err:
    logger.error("Could not mount static files: %s", _static_err)


# ============================================================
# ERROR HANDLERS
# ============================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail, "success": False}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}")
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={
            "error": str(exc),
            "success": False,
            "traceback": traceback.format_exc().split("\n")[-5:]
        }
    )


# ============================================================
# FREE PORT FINDER  — picks 8000 if free, else next available
# ============================================================

def _find_free_port(preferred: int = 8000) -> int:
    """Return `preferred` if it's free, otherwise bind to any free port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", preferred))
            return preferred
        except OSError:
            pass
    # preferred is taken — let the OS choose a free port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


if __name__ == "__main__":
    # When sys.stdout was None we already redirected it above, so print() is safe.
    print("=" * 60)
    print("SQL Query Generator API v5.0 (Schema-Based)")
    print("=" * 60)
    print()

    # Pick a free port — prefers 8000 but auto-falls back if it's occupied
    _SERVER_PORT = _find_free_port(8000)
    if _SERVER_PORT != 8000:
        logger.warning("Port 8000 is in use — starting on port %d instead", _SERVER_PORT)

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=_SERVER_PORT,
        reload=False,     # Must be False inside a PyInstaller bundle
        log_level="info",
    )
