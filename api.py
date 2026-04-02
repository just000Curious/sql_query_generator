"""
api.py
SQL Query Generator API with Schema-Based Navigation
"""

import os
import sys
import sqlite3
import json
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Union
import logging

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import FastAPI
try:
    from fastapi import FastAPI, HTTPException, Query
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

class ConditionInput(BaseModel):
    table: str = ""
    column: str
    operator: str = "="
    value: Optional[Any] = None

class OrderByInput(BaseModel):
    column: str
    direction: str = "ASC"

class JoinInput(BaseModel):
    join_type: str = "INNER JOIN"
    from_alias: str
    from_column: str
    to_alias: str
    to_column: str

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

    def _format_value(self, value):
        """Properly format values for SQL"""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, str):
            cleaned = value.strip("'").strip('"')
            if not cleaned:
                return "''"
            try:
                float(cleaned)
                return cleaned
            except ValueError:
                escaped = cleaned.replace("'", "''")
                return f"'{escaped}'"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, (datetime, date)):
            return f"'{value.strftime('%Y-%m-%d')}'"
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
                        'keys': table_info.get('keys', {})
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
        """Infer SQLite data type from column name"""
        col_lower = column_name.lower()

        if any(x in col_lower for x in ['date', 'dt', 'timestamp']):
            return 'DATE'
        if any(x in col_lower for x in ['no', 'num', 'count', 'qty', 'amount', 'amt']):
            return 'NUMERIC'
        return 'TEXT'

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
# Global database manager
# ============================================================

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_PATH = os.path.join(_BASE_DIR, "db_files", "metadata.json")
db_manager = SchemaDatabaseManager(json_file_path=JSON_PATH)


# ============================================================
# LIFESPAN (replaces deprecated on_event)
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown logic"""
    # ── Startup ──
    print("=" * 60)
    print("🚀 Starting SQL Query Generator API v5.0")
    print("=" * 60)

    print("📂 Loading schema from JSON...")
    db_manager.load_schema_from_json()

    if db_manager.schema_data:
        print(f"✅ Loaded {len(db_manager.schemas)} schemas with {db_manager.total_tables} tables")
        for schema_name, tables in db_manager.schemas.items():
            print(f"   - {schema_name}: {len(tables)} tables")

    print("🗄️ Creating database tables...")
    db_manager.init_database()

    print(f"\n✅ Database initialized")
    print(f"✅ Tables created: {db_manager.tables_created}")
    print(f"✅ Server ready at http://127.0.0.1:8000")
    print(f"✅ API docs at http://127.0.0.1:8000/docs")
    print("=" * 60)

    yield  # App runs here

    # ── Shutdown ──
    if db_manager.connection:
        db_manager.connection.close()
    print("🛑 Server stopped")


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

    # Regular columns
    for c in (request.columns or []):
        ref = col_ref(c.table, c.column)
        if c.alias:
            ref += f" AS {c.alias}"
        select_parts.append(ref)

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
            sql += f"\n  ON {j.from_alias}.{j.from_column} = {j.to_alias}.{j.to_column}"
    else:
        for extra in request.tables[1:]:
            sql += f"\n-- WARNING: no JOIN condition defined for {extra.table}"
            sql += f"\nCROSS JOIN {extra.table} {extra.alias}"

    # ── WHERE clause
    where_parts: List[str] = []
    for cond in (request.conditions or []):
        if not cond.column:
            continue
        ref = col_ref(cond.table, cond.column)
        op = (cond.operator or "=").upper().strip()

        if op in ("IS NULL", "IS NOT NULL"):
            where_parts.append(f"{ref} {op}")
        elif op in ("IN", "NOT IN"):
            val = helper._format_value(cond.value)
            where_parts.append(f"{ref} {op} ({val})")
        elif op == "BETWEEN":
            val = helper._format_value(cond.value)
            where_parts.append(f"{ref} BETWEEN {val}")
        elif op == "LIKE":
            val = helper._format_value(cond.value)
            where_parts.append(f"{ref} LIKE {val}")
        else:
            val = helper._format_value(cond.value)
            where_parts.append(f"{ref} {op} {val}")

    if where_parts:
        sql += "\nWHERE " + "\n  AND ".join(where_parts)

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
    """Root endpoint with schema list"""
    return {
        "name": "SQL Query Generator API",
        "version": "5.0.0",
        "status": "running",
        "schemas": [s['name'] for s in db_manager.get_schemas()],
        "total_schemas": len(db_manager.get_schemas()),
        "total_tables": db_manager.total_tables,
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "schemas": "/schemas",
            "categories": "/categories",
            "tables": "/schemas/{schema}/tables",
            "table_info": "/schemas/{schema}/tables/{table}",
            "generate": "/query/generate",
            "union": "/query/union",
            "execute": "/query/execute",
            "search": "/search"
        }
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
    """Get columns, PKs and FKs for a table — frontend-compatible format"""
    try:
        table_info = db_manager.get_table_info(schema, table_name)
        columns = [
            {
                "name": c,
                "type": db_manager._infer_data_type(c),
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

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 SQL Query Generator API v5.0 (Schema-Based)")
    print("=" * 60)
    print()

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
        reload=True,
        log_level="info"
    )
