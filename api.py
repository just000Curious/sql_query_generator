"""
FastAPI Backend for SQL Query Generator
Provides REST endpoints for all SQL generation functionality
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import uuid
import pandas as pd
from datetime import datetime
import traceback
import os
import csv
import io

# Import your modules - UPDATED to use CSVDBInfo
from db_information import CSVDBInfo, QueryValidator
from pypika_query_engine import QueryGenerator
from join_builder import JoinBuilder, build_join, build_join_chain, get_join_info
from cte_builder import CTEBuilder, build_cte, build_cte_query, list_ctes, reset_cte_builder
from temporary_table import TemporaryTable, TemporaryTableManager, create_temp_table, get_temp_table, list_temp_tables
from query_assembler import QueryAssembler, assemble, assemble_query, add_temp_table, add_cte
from query_validator import validate, validate_sql, get_validation_errors, get_validation_warnings
from query_engine import QueryEngine

# Initialize FastAPI
app = FastAPI(
    title="SQL Query Generator API",
    description="Generate complex SQL queries with CTEs, joins, and temporary tables",
    version="1.0.0"
)

# Enable CORS for frontend connections
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ========== DATA MODELS ==========

class TableInfo(BaseModel):
    table: str
    schema: Optional[str] = None
    alias: Optional[str] = None
    category: Optional[str] = None


class JoinDefinition(BaseModel):
    table1: str
    table2: str
    schema1: Optional[str] = None
    schema2: Optional[str] = None
    join_type: str = "INNER JOIN"
    condition: Optional[str] = None


class ColumnSelection(BaseModel):
    table: str
    column: str
    schema: Optional[str] = None
    alias: Optional[str] = None


class WhereCondition(BaseModel):
    table: str
    column: str
    schema: Optional[str] = None
    operator: str
    value: Any


class QueryRequest(BaseModel):
    tables: List[TableInfo]
    columns: Optional[List[ColumnSelection]] = None
    conditions: Optional[List[WhereCondition]] = None
    joins: Optional[List[JoinDefinition]] = None
    group_by: Optional[List[str]] = None
    order_by: Optional[List[str]] = None
    limit: Optional[int] = None
    offset: Optional[int] = 0
    use_cte: bool = False
    cte_name: Optional[str] = None
    create_temp_table: Optional[str] = None


class SQLQueryRequest(BaseModel):
    sql: str
    validate: bool = True


class TempTableRequest(BaseModel):
    name: str
    query: Optional[str] = None
    columns: Optional[List[str]] = None
    from_dataframe: Optional[Dict] = None


class JoinRequest(BaseModel):
    tables: List[TableInfo]
    join_type: str = "INNER JOIN"
    condition: Optional[str] = None


class CTERequest(BaseModel):
    name: str
    query: str
    is_final: bool = False


class ValidationResponse(BaseModel):
    valid: bool
    errors: List[str]
    warnings: List[str]


class QueryResponse(BaseModel):
    success: bool
    query: Optional[str] = None
    error: Optional[str] = None
    metadata: Optional[Dict] = None
    execution_time: float


# ========== ROOT ENDPOINT ==========

@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "message": "SQL Query Generator API",
        "version": "1.0.0",
        "endpoints": [
            "/sessions/create - Create a new session",
            "/sessions/{session_id} - Delete a session",
            "/categories - Get all business categories",
            "/categories/{category}/schemas - Get schemas in a category",
            "/categories/{category}/tables - Get tables in a category",
            "/schemas - Get all schemas",
            "/schemas/{schema_name}/tables - Get tables in a schema",
            "/schemas/{schema_name}/category - Get category for a schema",
            "/tables - Get table information (flat list for frontend)",
            "/tables/{table_name}/columns - Get table columns",
            "/relationships - Get table relationships (camelCase for frontend)",
            "/search/tables?q= - Search tables",
            "/search/columns?q= - Search columns",
            "/query/generate - Generate SQL query",
            "/query/validate - Validate SQL query",
            "/query/execute - Execute SQL query",
            "/join/build - Build joins",
            "/join/chain - Build join chain",
            "/cte/create - Create CTE",
            "/cte/list - List CTEs",
            "/cte/reset - Reset CTE builder",
            "/temp/create - Create temporary table",
            "/temp/list - List temporary tables",
            "/temp/{name} - Get temporary table info",
            "/temp/{name} - Drop temporary table",
            "/assemble - Assemble final query",
            "/export/{format} - Export query results",
            "/health - Health check"
        ]
    }


# ========== DATABASE CONNECTION ==========

# Global CSVDBInfo instance
db_info = None


@app.on_event("startup")
async def startup_event():
    """Initialize database info on startup"""
    global db_info
    try:
        # Try to load from CSV file
        csv_paths = [
            "master_db_schema.csv",
            "db_files/master_db_schema.csv",
            "data/master_db_schema.csv"
        ]

        csv_loaded = False
        for csv_path in csv_paths:
            if os.path.exists(csv_path):
                db_info = CSVDBInfo(csv_path)
                print(f"✅ Loaded schema from {csv_path}")
                csv_loaded = True
                break

        if not csv_loaded:
            # Use test mode with sample data
            db_info = CSVDBInfo()  # This will use test data
            print("✅ Using test database schema (no CSV file found)")

        # Verify tables were loaded
        tables = db_info.get_tables()
        schemas = db_info.get_schemas()
        categories = db_info.get_categories()

        print(f"📊 Loaded {len(tables)} tables")
        print(f"📁 Loaded {len(schemas)} schemas: {schemas[:5]}...")
        print(f"📋 Categories found: {categories}")

        # Print stats
        stats = db_info.get_stats()
        print(f"📈 Database stats: {stats}")

    except Exception as e:
        print(f"❌ Error loading schema: {e}")
        traceback.print_exc()
        db_info = CSVDBInfo()  # Fallback to test data
        print("✅ Falling back to test database schema")


# ========== SESSION MANAGEMENT ==========

# Store session data (in production, use Redis or database)
sessions = {}


class SessionManager:
    @staticmethod
    def create_session() -> str:
        """Create a new session with simplified initialization"""
        try:
            session_id = str(uuid.uuid4())

            # Create a simple session dict without complex objects
            sessions[session_id] = {
                'created_at': datetime.now(),
                'join_builder': None,
                'cte_builder': None,
                'temp_manager': {},  # Simple dict instead of TemporaryTableManager
                'query_assembler': None,
                'query_engine': None,
                'last_query': None,
                'selected_schema': None,
                'selected_category': None
            }

            print(f"✅ Session created: {session_id[:8]}...")
            print(f"📊 Active sessions: {len(sessions)}")
            return session_id

        except Exception as e:
            print(f"❌ Session creation error: {str(e)}")
            traceback.print_exc()
            raise Exception(f"Failed to create session: {str(e)}")

    @staticmethod
    def get_session(session_id: str):
        """Get session by ID"""
        if session_id not in sessions:
            print(f"❌ Session not found: {session_id}")
            raise HTTPException(status_code=404, detail=f"Session not found")
        return sessions[session_id]

    @staticmethod
    def delete_session(session_id: str):
        """Delete a session"""
        if session_id in sessions:
            del sessions[session_id]
            print(f"✅ Session deleted: {session_id[:8]}...")
            return True
        return False


# ========== SESSION ENDPOINTS ==========

@app.post("/sessions/create")
async def create_session_endpoint():
    """Create a new session"""
    try:
        print("📝 Creating new session...")
        session_id = SessionManager.create_session()
        return {
            "session_id": session_id,
            "message": "Session created successfully",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        print(f"❌ Session creation endpoint error: {str(e)}")
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Session creation failed: {str(e)}"
        )


@app.delete("/sessions/{session_id}")
async def delete_session_endpoint(session_id: str):
    """Delete a session"""
    try:
        SessionManager.delete_session(session_id)
        return {"message": "Session deleted successfully"}
    except Exception as e:
        print(f"❌ Session deletion error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== CATEGORY ENDPOINTS ==========

@app.get("/categories")
async def get_categories():
    """Get all business categories"""
    try:
        if db_info is None:
            raise HTTPException(status_code=500, detail="Database not initialized")

        categories = db_info.get_categories()

        # Build response with table counts
        result = []
        for category in categories:
            tables = db_info.get_tables_by_category(category)
            schemas = db_info.get_schemas_by_category(category)
            result.append({
                "name": category,
                "schemas": schemas,
                "table_count": len(tables)
            })

        return {
            "categories": result,
            "count": len(result)
        }
    except Exception as e:
        print(f"❌ Get categories error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/categories/{category}/schemas")
async def get_schemas_by_category(category: str):
    """Get all schemas in a specific category"""
    try:
        if db_info is None:
            raise HTTPException(status_code=500, detail="Database not initialized")

        # Find matching category (case-insensitive)
        categories = db_info.get_categories()
        matching_category = None
        for cat in categories:
            if cat.lower() == category.lower():
                matching_category = cat
                break

        if not matching_category:
            raise HTTPException(status_code=404, detail=f"Category '{category}' not found")

        schemas = db_info.get_schemas_by_category(matching_category)

        return {
            "category": matching_category,
            "schemas": schemas,
            "count": len(schemas)
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Get schemas by category error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/categories/{category}/tables")
async def get_tables_by_category(category: str):
    """Get all tables in a specific category"""
    try:
        if db_info is None:
            raise HTTPException(status_code=500, detail="Database not initialized")

        # Find matching category (case-insensitive)
        categories = db_info.get_categories()
        matching_category = None
        for cat in categories:
            if cat.lower() == category.lower():
                matching_category = cat
                break

        if not matching_category:
            raise HTTPException(status_code=404, detail=f"Category '{category}' not found")

        tables = db_info.get_tables_by_category(matching_category)

        # Format for frontend - return just table names with schema
        formatted_tables = [
            {
                "table": t['name'],
                "schema": t['schema'],
                "full_name": t['full_name']
            } for t in tables
        ]

        return {
            "category": matching_category,
            "tables": formatted_tables,
            "count": len(formatted_tables)
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Get tables by category error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== SCHEMA ENDPOINTS ==========

@app.get("/schemas")
async def get_schemas():
    """
    Get all schemas with enhanced metadata for frontend UI
    Returns schema name, table count, and business category
    """
    try:
        if db_info is None:
            raise HTTPException(status_code=500, detail="Database not initialized")

        schemas = db_info.get_schemas()

        # Build enhanced response with metadata
        result = []
        for schema_name in schemas:
            # Get table count for this schema
            tables = db_info.get_tables(schema_name)
            table_count = len(tables)

            # Get category for this schema (using the internal method)
            # Find a table with this schema to get its category
            category = "Other"
            tables_with_schema = db_info.get_tables_with_schema(schema_name)
            if tables_with_schema:
                category = tables_with_schema[0].get('category', 'Other')

            result.append({
                "name": schema_name,
                "tableCount": table_count,
                "category": category,
                "displayName": schema_name.upper() if len(schema_name) < 10 else schema_name.title()
            })

        return {
            "schemas": result,
            "count": len(result),
            "totalTables": sum(s["tableCount"] for s in result)
        }
    except Exception as e:
        print(f"❌ Get schemas error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/schemas/{schema_name}/category")
async def get_schema_category(schema_name: str):
    """Get the business category for a schema"""
    try:
        if db_info is None:
            raise HTTPException(status_code=500, detail="Database not initialized")

        # We need to find a table with this schema to get its category
        tables = db_info.get_tables_with_schema(schema_name)

        if not tables:
            raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found")

        # Get category from first table
        category = tables[0].get('category', 'Other')
        table_count = len(tables)

        return {
            "schema": schema_name,
            "category": category,
            "table_count": table_count
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Get schema category error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/schemas/{schema_name}/tables")
async def get_tables_by_schema(schema_name: str):
    """Get tables in a specific schema"""
    try:
        if db_info is None:
            raise HTTPException(status_code=500, detail="Database not initialized")

        tables = db_info.get_tables_with_schema(schema_name)

        # Format for frontend - simple list of table names
        table_names = [t['name'] for t in tables]

        return {
            "schema": schema_name,
            "tables": table_names,
            "count": len(table_names)
        }
    except Exception as e:
        print(f"❌ Get tables by schema error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== DATABASE INFO ENDPOINTS ==========

@app.get("/tables")
async def get_tables(schema: Optional[str] = None):
    """
    Get all tables in the database
    PROTECTED: Requires schema parameter to prevent loading all 2000+ tables at once
    """
    try:
        if db_info is None:
            raise HTTPException(status_code=500, detail="Database not initialized")

        # Protect the endpoint - require a schema to prevent frontend freeze
        if not schema:
            return {
                "message": "Please select a schema first",
                "tables": [],
                "count": 0,
                "requires_schema": True
            }

        all_tables = db_info.get_tables_with_schema(schema)

        # Return flat list of table names
        table_names = [t['name'] for t in all_tables]

        return {
            "tables": table_names,
            "count": len(table_names),
            "schema": schema
        }
    except Exception as e:
        print(f"❌ Get tables error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tables/{table_name}/columns")
async def get_table_columns(table_name: str, schema: Optional[str] = None):
    """
    Get columns for a specific table
    FIXED: Maps parent_table/parent_column to references_table/references_column
    FIXED: Converts data_type to uppercase for frontend compatibility
    """
    try:
        if db_info is None:
            raise HTTPException(status_code=500, detail="Database not initialized")

        if not db_info.table_exists(table_name, schema):
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

        raw_columns = db_info.get_columns(table_name, schema)

        # Get full table info
        full_name = db_info.get_full_table_name(table_name, schema)
        table_info = db_info.schema.tables.get(full_name, {})

        # Format columns for frontend - map CSV fields to expected format
        formatted_columns = []
        for col in raw_columns:
            # Convert data_type to uppercase for frontend syntax highlighting
            data_type = col.get('data_type', 'UNKNOWN').upper()

            formatted_columns.append({
                "column_name": col['column_name'],
                "data_type": data_type,  # Uppercase for frontend
                "is_primary_key": col.get('is_primary_key', False),
                "is_foreign_key": col.get('is_foreign_key', False),
                "is_nullable": col.get('is_nullable', True),
                # Map CSV 'references_table' to Frontend 'references_table'
                "references_table": col.get('references_table'),
                "references_column": col.get('references_column')
            })

        return {
            "table": table_name,
            "schema": schema or table_info.get('schema'),
            "category": table_info.get('category', 'Other'),
            "full_name": full_name,
            "columns": formatted_columns,
            "count": len(formatted_columns)
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Get columns error for {table_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/relationships")
async def get_relationships(table: Optional[str] = None, schema: Optional[str] = None):
    """
    Get all relationships or relationships for a specific table
    FIXED: Returns camelCase keys for frontend TypeScript interfaces
    """
    try:
        if db_info is None:
            raise HTTPException(status_code=500, detail="Database not initialized")

        if table:
            relationships = db_info.get_direct_relationships(table, schema)
            # Format for frontend with camelCase
            formatted_rels = []
            for rel in relationships:
                formatted_rels.append({
                    "fromTable": table,
                    "fromSchema": schema or rel.get('schema'),
                    "fromColumn": rel.get('via', {}).get('from_column'),
                    "toTable": rel['table'],
                    "toSchema": rel['schema'],
                    "toColumn": rel.get('via', {}).get('to_column'),
                    "type": rel['type']
                })
        else:
            raw_rels = db_info.get_all_relationships()
            # MAPPED TO CAMELCASE for frontend TypeScript interfaces
            formatted_rels = [
                {
                    "fromTable": r['from_table'],
                    "fromColumn": r['from_column'],
                    "toTable": r['to_table'],
                    "toColumn": r['to_column']
                } for r in raw_rels
            ]

        return {
            "relationships": formatted_rels,
            "count": len(formatted_rels)
        }
    except Exception as e:
        print(f"❌ Get relationships error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== SEARCH ENDPOINTS ==========

@app.get("/search/tables")
async def search_tables(q: str):
    """
    Search for tables matching query
    FIXED: Returns full table info for frontend canvas
    """
    try:
        if db_info is None:
            raise HTTPException(status_code=500, detail="Database not initialized")

        print(f"🔍 Searching tables for: {q}")

        results = db_info.search_tables(q)

        # Format for frontend - include schema and alias (alias defaults to table name)
        formatted_results = [
            {
                "table": r['name'],
                "schema": r['schema'],
                "alias": r['name'],  # Default alias is table name
                "category": r.get('category', 'Other'),
                "full_name": r['full_name']
            } for r in results
        ]

        return {
            "results": formatted_results,
            "count": len(formatted_results)
        }
    except Exception as e:
        print(f"❌ Table search error: {str(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search/columns")
async def search_columns(q: str):
    """Search for columns matching query"""
    try:
        if db_info is None:
            raise HTTPException(status_code=500, detail="Database not initialized")

        print(f"🔍 Searching columns for: {q}")

        results = db_info.search_columns(q)

        return {
            "results": results,
            "count": len(results)
        }
    except Exception as e:
        print(f"❌ Column search error: {str(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ========== QUERY GENERATION ENDPOINT ==========

@app.post("/query/generate", response_model=QueryResponse)
async def generate_query(request: QueryRequest, session_id: Optional[str] = None):
    """Generate SQL query from request parameters"""
    import time
    start_time = time.time()

    try:
        print(f"🔧 Generating query with {len(request.tables)} tables...")

        # Get or create session
        if session_id and session_id in sessions:
            session = sessions[session_id]
            print(f"📝 Using existing session: {session_id}")
        else:
            # Create new session
            session_id = str(uuid.uuid4())
            session = {
                'created_at': datetime.now(),
                'join_builder': None,
                'cte_builder': None,
                'temp_manager': None,
                'query_assembler': None,
                'query_engine': None,
                'last_query': None
            }
            sessions[session_id] = session
            print(f"✅ Created new session: {session_id}")

        if not request.tables:
            return QueryResponse(
                success=False,
                error="At least one table is required",
                execution_time=time.time() - start_time
            )

        # Simple but robust query generation with schema support
        main_table = request.tables[0]
        table_name = main_table.table
        schema_name = main_table.schema
        table_alias = main_table.alias or "t1"

        # Get fully qualified table name
        full_table_name = db_info.get_full_table_name(table_name,
                                                      schema_name) if db_info else f"{schema_name or 'public'}.{table_name}"

        # Build SELECT clause
        if request.columns and len(request.columns) > 0:
            select_parts = []
            for col in request.columns:
                col_table = col.table or table_name
                col_schema = col.schema or schema_name

                if col.column == "*":
                    select_parts.append(f"{col_table}.*")
                else:
                    col_expr = f"{col_table}.{col.column}"
                    if col.alias:
                        col_expr += f" AS {col.alias}"
                    select_parts.append(col_expr)
            select_clause = ", ".join(select_parts)
        else:
            select_clause = f"{table_name}.*"

        # Start building query
        query = f"SELECT {select_clause} FROM {full_table_name}"
        if table_alias != table_name:
            query += f" AS {table_alias}"

        # Add joins if any
        if request.joins:
            for join in request.joins:
                join_type = join.join_type or "INNER JOIN"

                # Get fully qualified table names
                table1_full = db_info.get_full_table_name(join.table1,
                                                          join.schema1) if db_info else f"{join.schema1 or 'public'}.{join.table1}"
                table2_full = db_info.get_full_table_name(join.table2,
                                                          join.schema2) if db_info else f"{join.schema2 or 'public'}.{join.table2}"

                condition = join.condition or f"{join.table1}.id = {join.table2}.id"
                query += f"\n{join_type} {table2_full} ON {condition}"

        # Add WHERE clause
        if request.conditions:
            where_parts = []
            for cond in request.conditions:
                cond_table = cond.table or table_name
                if isinstance(cond.value, str):
                    where_parts.append(f"{cond_table}.{cond.column} {cond.operator} '{cond.value}'")
                else:
                    where_parts.append(f"{cond_table}.{cond.column} {cond.operator} {cond.value}")
            query += "\nWHERE " + " AND ".join(where_parts)

        # Add GROUP BY
        if request.group_by:
            query += "\nGROUP BY " + ", ".join(request.group_by)

        # Add ORDER BY
        if request.order_by:
            query += "\nORDER BY " + ", ".join(request.order_by)

        # Add LIMIT and OFFSET
        if request.limit:
            query += f"\nLIMIT {request.limit}"
            if request.offset:
                query += f" OFFSET {request.offset}"

        # Handle CTE if requested
        if request.use_cte and request.cte_name:
            final_query = f"WITH {request.cte_name} AS (\n{query}\n)\nSELECT * FROM {request.cte_name}"
        else:
            final_query = query

        session['last_query'] = final_query
        print(f"✅ Query generated: {final_query[:200]}...")

        return QueryResponse(
            success=True,
            query=final_query,
            metadata={
                "tables": [t.table for t in request.tables],
                "schemas": [t.schema for t in request.tables if t.schema],
                "session_id": session_id
            },
            execution_time=time.time() - start_time
        )

    except Exception as e:
        print(f"❌ Query generation error: {str(e)}")
        traceback.print_exc()
        return QueryResponse(
            success=False,
            error=str(e),
            execution_time=time.time() - start_time
        )


# ========== QUERY VALIDATION ENDPOINT ==========

@app.post("/query/validate", response_model=ValidationResponse)
async def validate_query_endpoint(request: SQLQueryRequest):
    """Validate a SQL query"""
    try:
        # Use the QueryValidator if available
        if db_info:
            validator = QueryValidator(db_info)
            is_valid = validator.validate_sql(request.sql)
            errors = validator.get_errors()
            warnings = validator.get_warnings()
        else:
            # Simple validation - check if it's a SELECT statement
            sql_upper = request.sql.strip().upper()
            is_valid = sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")

            errors = []
            warnings = []

            if not is_valid:
                errors.append("Query must start with SELECT or WITH")

            # Check for basic SQL syntax
            if ";" in request.sql and not request.sql.strip().endswith(";"):
                warnings.append("Query contains semicolon in the middle")

        return ValidationResponse(
            valid=is_valid,
            errors=errors,
            warnings=warnings
        )
    except Exception as e:
        return ValidationResponse(
            valid=False,
            errors=[str(e)],
            warnings=[]
        )


# ========== QUERY EXECUTION ENDPOINT ==========

@app.post("/query/execute")
async def execute_query(request: SQLQueryRequest, session_id: str):
    """
    Execute a SQL query and return results
    FIXED: Ensures columns array matches data keys exactly
    """
    try:
        print(f"⚡ Executing query for session: {session_id}")
        session = SessionManager.get_session(session_id)

        # Mock data for testing with schema information
        # Ensure keys in 'data' match strings in 'columns' array
        mock_data = [
            {"id": 1, "name": "Sample Data 1", "data_type": "INTEGER", "created_at": "2024-01-01"},
            {"id": 2, "name": "Sample Data 2", "data_type": "VARCHAR", "created_at": "2024-01-02"},
            {"id": 3, "name": "Sample Data 3", "data_type": "DATE", "created_at": "2024-01-03"},
            {"id": 4, "name": "Sample Data 4", "data_type": "INTEGER", "created_at": "2024-01-04"},
            {"id": 5, "name": "Sample Data 5", "data_type": "VARCHAR", "created_at": "2024-01-05"},
        ]

        # Columns MUST match the keys in mock_data exactly
        columns = ["id", "name", "data_type", "created_at"]

        # Store the executed query
        session['last_query'] = request.sql

        return {
            "success": True,
            "data": mock_data,
            "columns": columns,
            "row_count": len(mock_data),
            "truncated": False,
            "message": "Query executed successfully (mock data)"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Query execution error: {str(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ========== JOIN BUILDING ENDPOINTS ==========

@app.post("/join/build")
async def build_join_endpoint(request: JoinRequest, session_id: Optional[str] = None):
    """Build a join between tables"""
    try:
        if session_id and session_id in sessions:
            session = sessions[session_id]
        else:
            session_id = str(uuid.uuid4())
            session = {
                'created_at': datetime.now(),
                'join_builder': None,
                'last_query': None
            }
            sessions[session_id] = session

        if len(request.tables) < 2:
            raise HTTPException(status_code=400, detail="At least 2 tables required for join")

        table1 = request.tables[0]
        table2 = request.tables[1]

        # Get fully qualified names
        table1_full = db_info.get_full_table_name(table1.table,
                                                  table1.schema) if db_info else f"{table1.schema or 'public'}.{table1.table}"
        table2_full = db_info.get_full_table_name(table2.table,
                                                  table2.schema) if db_info else f"{table2.schema or 'public'}.{table2.table}"

        # Try to find relationship
        relationship = None
        if db_info:
            relationship = db_info.find_relationship(
                table1.table, table2.table,
                table1.schema, table2.schema
            )

        # Create join representation
        join_info = {
            "type": request.join_type,
            "left_table": table1.table,
            "left_schema": table1.schema or 'public',
            "right_table": table2.table,
            "right_schema": table2.schema or 'public',
            "left_full_name": table1_full,
            "right_full_name": table2_full,
            "condition": request.condition or (
                f"{table1.table}.{relationship['from_column']} = {table2.table}.{relationship['to_column']}"
                if relationship else f"{table1.table}.id = {table2.table}.id"
            ),
            "relationship": relationship
        }

        session['join_builder'] = join_info

        return {
            "success": True,
            "join_info": join_info,
            "session_id": session_id
        }

    except Exception as e:
        print(f"❌ Join build error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/join/chain")
async def build_join_chain_endpoint(joins: List[JoinDefinition], session_id: str):
    """Build a chain of joins"""
    try:
        session = SessionManager.get_session(session_id)

        join_chain = []
        for j in joins:
            # Get fully qualified names
            table1_full = db_info.get_full_table_name(j.table1,
                                                      j.schema1) if db_info else f"{j.schema1 or 'public'}.{j.table1}"
            table2_full = db_info.get_full_table_name(j.table2,
                                                      j.schema2) if db_info else f"{j.schema2 or 'public'}.{j.table2}"

            join_chain.append({
                'table1': j.table1,
                'schema1': j.schema1 or 'public',
                'table2': j.table2,
                'schema2': j.schema2 or 'public',
                'table1_full': table1_full,
                'table2_full': table2_full,
                'type': j.join_type,
                'condition': j.condition or f"{j.table1}.id = {j.table2}.id"
            })

        session['join_builder'] = {"chain": join_chain}

        return {
            "success": True,
            "message": "Join chain built successfully",
            "joins": len(join_chain)
        }

    except Exception as e:
        print(f"❌ Join chain error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== CTE ENDPOINTS ==========

@app.post("/cte/create")
async def create_cte_endpoint(request: CTERequest, session_id: str):
    """Create a CTE"""
    try:
        session = SessionManager.get_session(session_id)

        cte_info = {
            "name": request.name,
            "query": request.query,
            "is_final": request.is_final,
            "created_at": datetime.now().isoformat()
        }

        session['cte_builder'] = cte_info

        return {
            "success": True,
            "message": f"CTE '{request.name}' created",
            "cte_query": f"WITH {request.name} AS (\n{request.query}\n)"
        }

    except Exception as e:
        print(f"❌ CTE creation error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cte/list")
async def list_ctes_endpoint(session_id: str):
    """List all CTEs in session"""
    try:
        session = SessionManager.get_session(session_id)
        cte = session.get('cte_builder')

        if cte:
            return {"ctes": [cte]}
        return {"ctes": []}

    except Exception as e:
        print(f"❌ List CTEs error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/cte/reset")
async def reset_cte_endpoint(session_id: str):
    """Reset CTE builder"""
    try:
        session = SessionManager.get_session(session_id)
        session['cte_builder'] = None
        return {"success": True, "message": "CTE builder reset"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========== TEMPORARY TABLE ENDPOINTS ==========

@app.post("/temp/create")
async def create_temp_table_endpoint(request: TempTableRequest, session_id: str):
    """Create a temporary table"""
    try:
        session = SessionManager.get_session(session_id)

        temp_table_info = {
            "name": request.name,
            "created_at": datetime.now().isoformat(),
            "type": "query" if request.query else "columns" if request.columns else "dataframe"
        }

        if 'temp_manager' not in session:
            session['temp_manager'] = {}

        session['temp_manager'][request.name] = temp_table_info

        return {
            "success": True,
            "message": f"Temporary table '{request.name}' created",
            "metadata": temp_table_info
        }

    except Exception as e:
        print(f"❌ Temp table creation error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/temp/list")
async def list_temp_tables_endpoint(session_id: str):
    """List all temporary tables in session"""
    try:
        session = SessionManager.get_session(session_id)
        tables = list(session.get('temp_manager', {}).keys())
        return {"temporary_tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/temp/{name}")
async def get_temp_table_endpoint(name: str, session_id: str):
    """Get information about a temporary table"""
    try:
        session = SessionManager.get_session(session_id)
        temp_table = session.get('temp_manager', {}).get(name)

        if not temp_table:
            raise HTTPException(status_code=404, detail=f"Temporary table '{name}' not found")

        return {"table_info": temp_table}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/temp/{name}")
async def drop_temp_table_endpoint(name: str, session_id: str):
    """Drop a temporary table"""
    try:
        session = SessionManager.get_session(session_id)
        if 'temp_manager' in session and name in session['temp_manager']:
            del session['temp_manager'][name]
        return {"success": True, "message": f"Temporary table '{name}' dropped"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========== QUERY ASSEMBLY ENDPOINTS ==========

@app.post("/assemble")
async def assemble_query_endpoint(session_id: str):
    """Assemble the final query from current session state"""
    try:
        session = SessionManager.get_session(session_id)

        # Get components
        join_builder = session.get('join_builder')
        cte_builder = session.get('cte_builder')

        if cte_builder:
            if isinstance(cte_builder, dict):
                query = f"WITH {cte_builder['name']} AS (\n{cte_builder['query']}\n)\nSELECT * FROM {cte_builder['name']}"
            else:
                query = str(cte_builder)
            session['last_query'] = query
            return {"query": query, "type": "cte"}

        elif join_builder:
            if isinstance(join_builder, dict):
                if 'chain' in join_builder:
                    # Build from chain
                    tables = []
                    for j in join_builder['chain']:
                        if j['table1'] not in tables:
                            tables.append(j['table1'])
                        if j['table2'] not in tables:
                            tables.append(j['table2'])

                    # Use fully qualified names
                    first_table = join_builder['chain'][0]
                    query = f"SELECT * FROM {first_table['table1_full']}"

                    for j in join_builder['chain']:
                        query += f"\n{j['type']} {j['table2_full']} ON {j['condition']}"
                else:
                    # Simple join
                    query = f"SELECT * FROM {join_builder['left_full_name']} {join_builder['type']} {join_builder['right_full_name']} ON {join_builder['condition']}"
            else:
                query = str(join_builder)

            session['last_query'] = query
            return {"query": query, "type": "join"}

        elif session.get('last_query'):
            return {"query": session['last_query'], "type": "existing"}

        else:
            raise HTTPException(status_code=400, detail="No query components found in session")

    except Exception as e:
        print(f"❌ Assemble error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== EXPORT ENDPOINTS ==========

@app.post("/export/{format}")
async def export_query_results(format: str, session_id: str, filename: Optional[str] = None):
    """Export query results in various formats"""
    try:
        print(f"📤 Exporting results in {format} format for session: {session_id}")

        session = SessionManager.get_session(session_id)

        # Mock data for export with schema information
        mock_data = [
            {"id": 1, "name": "Sample Data 1", "data_type": "INTEGER", "created_at": "2024-01-01"},
            {"id": 2, "name": "Sample Data 2", "data_type": "VARCHAR", "created_at": "2024-01-02"},
            {"id": 3, "name": "Sample Data 3", "data_type": "DATE", "created_at": "2024-01-03"},
            {"id": 4, "name": "Sample Data 4", "data_type": "INTEGER", "created_at": "2024-01-04"},
            {"id": 5, "name": "Sample Data 5", "data_type": "VARCHAR", "created_at": "2024-01-05"},
        ]

        # Generate filename if not provided
        if not filename:
            filename = f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Export in requested format
        if format.lower() == 'csv':
            # Convert to CSV string
            output = io.StringIO()
            if mock_data:
                writer = csv.DictWriter(output, fieldnames=mock_data[0].keys())
                writer.writeheader()
                writer.writerows(mock_data)
            csv_data = output.getvalue()

            return JSONResponse(
                content={
                    "success": True,
                    "data": csv_data,
                    "filename": f"{filename}.csv",
                    "format": "csv"
                }
            )

        elif format.lower() == 'json':
            return {
                "success": True,
                "data": mock_data,
                "filename": f"{filename}.json",
                "format": "json"
            }

        elif format.lower() == 'excel':
            # For Excel, return the data as JSON with a message
            return {
                "success": True,
                "data": mock_data,
                "filename": f"{filename}.xlsx",
                "format": "excel",
                "message": "Excel export - client-side conversion needed"
            }

        else:
            return {
                "success": False,
                "error": f"Format {format} not supported",
                "supported_formats": ["csv", "json", "excel"]
            }

    except Exception as e:
        print(f"❌ Export error: {str(e)}")
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e)
        }


@app.get("/schemas/quick")
async def get_schemas_quick():
    """
    Ultra-light schema info for quick loading
    Returns just names and counts for dashboard
    """
    try:
        if db_info is None:
            raise HTTPException(status_code=500, detail="Database not initialized")

        schemas = db_info.get_schemas()

        # Minimal response for quick loading
        result = []
        for schema_name in schemas[:10]:  # Limit to first 10 for demo
            tables = db_info.get_tables(schema_name)
            result.append({
                "name": schema_name,
                "count": len(tables)
            })

        return {
            "schemas": result,
            "total": len(schemas)
        }
    except Exception as e:
        print(f"❌ Get schemas quick error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== HEALTH CHECK ==========

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    FIXED: Returns 200 OK with {"status": "healthy"} for Lovable isHealthy state check
    """
    try:
        if db_info:
            tables_count = len(db_info.get_tables())
            schemas_count = len(db_info.get_schemas())
            categories = len(db_info.get_categories())
            stats = db_info.get_stats()
        else:
            tables_count = 0
            schemas_count = 0
            categories = 0
            stats = {}

        # Return 200 OK with status healthy - exactly what Lovable expects
        return {
            "status": "healthy",
            "database_loaded": db_info is not None,
            "tables_loaded": tables_count,
            "schemas_loaded": schemas_count,
            "categories_found": categories,
            "active_sessions": len(sessions),
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        # Even on error, return 200 with degraded status - don't throw HTTP exception
        return {
            "status": "degraded",
            "error": str(e),
            "database_loaded": False,
            "tables_loaded": 0,
            "active_sessions": len(sessions),
            "timestamp": datetime.now().isoformat()
        }


# ========== ERROR HANDLERS ==========

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail, "success": False}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    print(f"❌ Unhandled exception: {str(exc)}")
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={
            "error": str(exc),
            "traceback": traceback.format_exc().split("\n"),
            "success": False
        }
    )


if __name__ == "__main__":
    import uvicorn

    print("🚀 Starting SQL Query Generator API...")
    print("📡 Server will run on http://localhost:8000")
    print("🔧 Press Ctrl+C to stop")
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)