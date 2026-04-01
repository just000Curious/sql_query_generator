"""
api_improved.py
SQL Query Generator API with Schema-Based Navigation
"""

import os
import sys
import sqlite3
import json
import time
import traceback
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
    from pydantic import BaseModel, Field, validator
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

    @validator('operator')
    def validate_operator(cls, v):
        valid_ops = ['=', '!=', '<>', '>', '>=', '<', '<=', 'LIKE', 'NOT LIKE', 'IN', 'NOT IN']
        if v.upper() not in valid_ops:
            raise ValueError(f'Operator must be one of: {valid_ops}')
        return v

class GenerateRequest(BaseModel):
    schema: str
    table: str
    columns: Optional[List[str]] = None
    conditions: Optional[List[WhereCondition]] = None
    group_by: Optional[List[str]] = None
    order_by: Optional[List[str]] = None
    limit: Optional[int] = Field(100, ge=1, le=10000)

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
        self.table_ref = f"{schema}.{table}"
        if alias:
            self.table_ref += f" AS {alias}"

        self.selected_columns = []
        self.where_conditions = []
        self.group_by_cols = []
        self.order_by_cols = []
        self.limit_val = None
        self.offset_val = None
        self.distinct_flag = False

    def _format_value(self, value):
        """Properly format values for SQL"""
        if value is None:
            return "NULL"
        if isinstance(value, str):
            cleaned = value.strip("'").strip('"')
            try:
                float(cleaned)
                return cleaned
            except ValueError:
                escaped = cleaned.replace("'", "''")
                return f"'{escaped}'"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
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
                elif cond['operator'] == 'IN':
                    where_parts.append(f"{cond['column']} IN {cond['value']}")
                else:
                    where_parts.append(f"{cond['column']} {cond['operator']} {cond['value']}")
            parts.append("WHERE " + " AND ".join(where_parts))

        # GROUP BY clause
        if self.group_by_cols:
            parts.append("GROUP BY " + ", ".join(self.group_by_cols))

        # ORDER BY clause
        if self.order_by_cols:
            order_parts = [f"{o['column']} {o['direction']}" for o in self.order_by_cols]
            parts.append("ORDER BY " + ", ".join(order_parts))

        # LIMIT clause
        if self.limit_val:
            limit_clause = f"LIMIT {self.limit_val}"
            if self.offset_val:
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
            'order_by': self.order_by_cols,
            'limit': self.limit_val,
            'offset': self.offset_val
        }


# ============================================================
# DATABASE MANAGER WITH SCHEMA SUPPORT
# ============================================================

class SchemaDatabaseManager:
    """Database manager with schema awareness"""

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

                # Create full table name with schema prefix
                full_table_name = f"{schema_name}_{table_name}"

                # Create column definitions
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
                'description': self._get_schema_description(schema_name),
                'table_count': len(tables)
            })
        return result

    def _get_schema_description(self, schema_name: str) -> str:
        """Get description for schema"""
        descriptions = {
            'GM': 'General Management - Complaints, Forwarding, DMS',
            'HM': 'Healthcare Management - Medical Records, Lab Tests, Certificates',
            'PM': 'Personnel Management - Employee Data, Payroll, Leave',
            'SI': 'Stores & Inventory - Materials, Purchase, Tenders',
            'SA': 'Security & Administration - User Management, Roles',
            'TA': 'Traffic & Accounts - Ticketing, Freight, Accounting'
        }
        return descriptions.get(schema_name, f'{schema_name} Schema')

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


# ============================================================
# FASTAPI APPLICATION
# ============================================================

app = FastAPI(
    title="SQL Query Generator API",
    description="Complete SQL query generation API with schema-based navigation",
    version="4.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global database manager
JSON_PATH = r"G:\sql query generator\db_files\metadata.json"
db_manager = SchemaDatabaseManager(json_file_path=JSON_PATH)


# ============================================================
# EVENT HANDLERS
# ============================================================

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    print("=" * 60)
    print("🚀 Starting SQL Query Generator API v4.0")
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


# ============================================================
# SCHEMA ENDPOINTS
# ============================================================

@app.get("/")
async def root():
    """Root endpoint with schema list"""
    return {
        "name": "SQL Query Generator API",
        "version": "4.0.0",
        "status": "running",
        "schemas": [s['name'] for s in db_manager.get_schemas()],
        "total_schemas": len(db_manager.get_schemas()),
        "total_tables": db_manager.total_tables,
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "schemas": "/schemas",
            "tables": "/schemas/{schema}/tables",
            "table_info": "/schemas/{schema}/tables/{table}",
            "generate": "/query/generate",
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
    """List all available schemas with their table counts"""
    return {
        "schemas": db_manager.get_schemas(),
        "count": len(db_manager.get_schemas())
    }


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
    """Generate SQL query from parameters with schema support"""
    start_time = time.time()

    try:
        # Validate schema and table exist
        tables = db_manager.get_tables(request.schema)
        if not tables:
            return QueryResponse(
                success=False,
                error=f"Schema '{request.schema}' not found. Available: {[s['name'] for s in db_manager.get_schemas()]}",
                execution_time=time.time() - start_time
            )

        table_exists = any(t['name'] == request.table for t in tables)
        if not table_exists:
            return QueryResponse(
                success=False,
                error=f"Table '{request.table}' not found in schema '{request.schema}'",
                execution_time=time.time() - start_time
            )

        # Create query generator
        q = SchemaQueryGenerator(request.schema, request.table)

        # Add columns
        if request.columns:
            q.select(request.columns)
        else:
            q.select_all()

        # Add WHERE conditions
        if request.conditions:
            for cond in request.conditions:
                q.where(cond.column, cond.operator, cond.value)

        # Add GROUP BY
        if request.group_by:
            q.group_by(request.group_by)

        # Add ORDER BY
        if request.order_by:
            for order in request.order_by:
                parts = order.split()
                col = parts[0]
                direction = parts[1] if len(parts) > 1 else 'ASC'
                q.order_by(col, direction)

        # Add LIMIT
        if request.limit:
            q.limit(request.limit)

        sql = q.build()

        return QueryResponse(
            success=True,
            query=sql,
            execution_time=time.time() - start_time,
            row_count=request.limit
        )

    except Exception as e:
        logger.error(f"Query generation error: {e}")
        return QueryResponse(
            success=False,
            error=str(e),
            execution_time=time.time() - start_time
        )


@app.post("/query/execute", response_model=ExecutionResponse)
async def execute_query(request: SQLQueryRequest):
    """Execute SQL query"""
    start_time = time.time()

    data, columns, row_count, error = db_manager.execute_query(request.sql, request.limit)

    if error:
        return ExecutionResponse(
            success=False,
            execution_time=time.time() - start_time,
            message="Query execution failed",
            sql=request.sql,
            error_detail=error
        )

    return ExecutionResponse(
        success=True,
        data=data,
        columns=columns,
        row_count=row_count,
        execution_time=time.time() - start_time,
        message="Query executed successfully",
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
                "sql": "SELECT * FROM GM.gmtk_coms_hdr LIMIT 10"
            },
            {
                "name": "SELECT with WHERE",
                "description": "Filter records by condition",
                "sql": "SELECT complaint_no, emp_no, status FROM GM.gmtk_coms_hdr WHERE status = 'OPEN' LIMIT 10"
            },
            {
                "name": "Aggregate Query",
                "description": "Count records by status",
                "sql": "SELECT status, COUNT(*) as count FROM GM.gmtk_coms_hdr GROUP BY status"
            },
            {
                "name": "Date Range Filter",
                "description": "Filter by date range",
                "sql": "SELECT * FROM GM.gmtk_coms_hdr WHERE reg_date BETWEEN '2024-01-01' AND '2024-12-31' LIMIT 10"
            },
            {
                "name": "ORDER BY",
                "description": "Sort results",
                "sql": "SELECT complaint_no, reg_date FROM GM.gmtk_coms_hdr ORDER BY reg_date DESC LIMIT 10"
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


# ============================================================
# MAIN ENTRY POINT
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 SQL Query Generator API v4.0 (Schema-Based)")
    print("=" * 60)
    print()

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
        reload=True,
        log_level="info"
    )