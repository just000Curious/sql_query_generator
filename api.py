"""
FastAPI Backend for SQL Query Generator
Provides REST endpoints for all SQL generation functionality
"""

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Union
import json
import uuid
import pandas as pd
from datetime import datetime
import traceback

# Import your modules
from db_information import DBInfo, create_db_info, get_test_db_info
from pypika_query_engine import QueryGenerator
from join_builder import JoinBuilder, build_join, build_join_chain, get_join_info
from cte_builder import CTEBuilder, build_cte, build_cte_query, list_ctes, reset_cte_builder
from temporary_table import TemporaryTable, TemporaryTableManager, create_temp_table, get_temp_table, list_temp_tables
from query_assembler import QueryAssembler, assemble, assemble_query, add_temp_table, add_cte
from query_validator import QueryValidator, validate, validate_sql, get_validation_errors, get_validation_warnings
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


class JoinDefinition(BaseModel):
    table1: str
    table2: str
    join_type: str = "INNER JOIN"
    condition: Optional[str] = None


class ColumnSelection(BaseModel):
    table: str
    column: str
    alias: Optional[str] = None


class WhereCondition(BaseModel):
    table: str
    column: str
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


# ========== DATABASE CONNECTION ==========

# Global DBInfo instance (can be configured with schema file)
db_info = None


@app.on_event("startup")
async def startup_event():
    """Initialize database info on startup"""
    global db_info
    try:
        # Try to load from a schema file if it exists
        import os
        if os.path.exists("schema.sql"):
            db_info = DBInfo("schema.sql")
        else:
            # Use test mode with sample data
            db_info = get_test_db_info()
            print("Using test database schema")
    except Exception as e:
        print(f"Error loading schema: {e}")
        db_info = get_test_db_info()


# ========== SESSION MANAGEMENT ==========

# Store session data (in production, use Redis or database)
sessions = {}


class SessionManager:
    @staticmethod
    def create_session() -> str:
        session_id = str(uuid.uuid4())
        sessions[session_id] = {
            'created_at': datetime.now(),
            'join_builder': None,
            'cte_builder': None,
            'temp_manager': TemporaryTableManager(),
            'query_assembler': QueryAssembler(),
            'query_engine': QueryEngine(db_info) if db_info else None,
            'last_query': None
        }
        return session_id

    @staticmethod
    def get_session(session_id: str):
        if session_id not in sessions:
            raise HTTPException(status_code=404, detail="Session not found")
        return sessions[session_id]

    @staticmethod
    def delete_session(session_id: str):
        if session_id in sessions:
            del sessions[session_id]


# ========== API ENDPOINTS ==========

@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "message": "SQL Query Generator API",
        "version": "1.0.0",
        "endpoints": [
            "/sessions - Manage sessions",
            "/tables - Get table information",
            "/query/generate - Generate SQL query",
            "/query/validate - Validate SQL query",
            "/query/execute - Execute SQL query",
            "/join/build - Build joins",
            "/cte/create - Create CTE",
            "/temp/create - Create temporary table",
            "/export - Export query results"
        ]
    }


# ========== SESSION ENDPOINTS ==========

@app.post("/sessions/create")
async def create_session():
    """Create a new session"""
    session_id = SessionManager.create_session()
    return {"session_id": session_id, "message": "Session created successfully"}


@app.delete("/sessions/{session_id}")
async def delete_session_endpoint(session_id: str):
    """Delete a session"""
    SessionManager.delete_session(session_id)
    return {"message": "Session deleted successfully"}


# ========== DATABASE INFO ENDPOINTS ==========

@app.get("/tables")
async def get_tables(schema: Optional[str] = None):
    """Get all tables in the database"""
    try:
        tables = db_info.get_tables(schema)
        return {"tables": tables, "count": len(tables)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tables/{table_name}/columns")
async def get_table_columns(table_name: str, schema: Optional[str] = None):
    """Get columns for a specific table"""
    try:
        columns = db_info.get_columns(table_name, schema)
        return {
            "table": table_name,
            "schema": schema,
            "columns": columns,
            "count": len(columns)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/relationships")
async def get_relationships(table: Optional[str] = None):
    """Get all relationships or relationships for a specific table"""
    try:
        if table:
            relationships = db_info.get_direct_relationships(table)
        else:
            relationships = db_info.get_all_relationships()
        return {"relationships": relationships}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search/tables")
async def search_tables(pattern: str):
    """Search for tables matching pattern"""
    try:
        results = db_info.search_tables(pattern)
        return {"results": results, "count": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search/columns")
async def search_columns(pattern: str):
    """Search for columns matching pattern"""
    try:
        results = db_info.search_columns(pattern)
        return {"results": results, "count": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========== QUERY GENERATION ENDPOINTS ==========

@app.post("/query/generate", response_model=QueryResponse)
async def generate_query(request: QueryRequest, session_id: Optional[str] = None):
    """Generate SQL query from request parameters"""
    import time
    start_time = time.time()

    try:
        # Get or create session
        if session_id:
            session = SessionManager.get_session(session_id)
        else:
            session_id = SessionManager.create_session()
            session = sessions[session_id]

        # Create query generator
        if not request.tables:
            raise HTTPException(status_code=400, detail="At least one table is required")

        main_table = request.tables[0]
        query_gen = QueryGenerator(main_table.table, main_table.schema)

        # Add columns
        if request.columns:
            for col in request.columns:
                if col.column == "*":
                    query_gen.select_all()
                else:
                    query_gen.select_column(col.column, col.alias)

        # Add conditions
        if request.conditions:
            for cond in request.conditions:
                query_gen.where(cond.column, cond.operator, cond.value)

        # Add group by
        if request.group_by:
            query_gen.group_by(request.group_by)

        # Add order by
        if request.order_by:
            for order in request.order_by:
                query_gen.order_by(order)

        # Add limit/offset
        if request.limit:
            query_gen.limit(request.limit, request.offset or 0)

        # Build the query
        if request.use_cte and request.cte_name:
            # Create CTE
            cte_builder = CTEBuilder()
            cte_builder.add_stage(request.cte_name, query_gen)
            final_query = cte_builder.build()
        elif request.create_temp_table:
            # Create temporary table
            temp_table = create_temp_table(request.create_temp_table)
            temp_table.create(query_gen.build())
            final_query = f"SELECT * FROM {request.create_temp_table}"
        else:
            # Simple query
            final_query = query_gen.build()

        session['last_query'] = final_query

        return QueryResponse(
            success=True,
            query=final_query,
            metadata={"tables": [t.table for t in request.tables]},
            execution_time=time.time() - start_time
        )

    except Exception as e:
        return QueryResponse(
            success=False,
            error=str(e),
            execution_time=time.time() - start_time
        )


@app.post("/query/validate", response_model=ValidationResponse)
async def validate_query_endpoint(request: SQLQueryRequest):
    """Validate a SQL query"""
    try:
        if request.validate:
            is_valid = validate_sql(request.sql)
            errors = get_validation_errors()
            warnings = get_validation_warnings()

            return ValidationResponse(
                valid=is_valid,
                errors=errors,
                warnings=warnings
            )
        return ValidationResponse(valid=True, errors=[], warnings=[])
    except Exception as e:
        return ValidationResponse(
            valid=False,
            errors=[str(e)],
            warnings=[]
        )


@app.post("/query/execute")
async def execute_query(request: SQLQueryRequest, session_id: str, background_tasks: BackgroundTasks):
    """Execute a SQL query and return results"""
    try:
        session = SessionManager.get_session(session_id)
        engine = session['query_engine']

        if not engine:
            raise HTTPException(status_code=400, detail="Query engine not initialized")

        # Execute query
        result = engine.execute_query(request.sql)

        # Convert to dict for JSON response
        if isinstance(result, pd.DataFrame):
            # For large results, we might want to limit rows
            data = result.head(1000).to_dict(orient='records')
            return {
                "success": True,
                "data": data,
                "columns": list(result.columns),
                "row_count": len(result),
                "truncated": len(result) > 1000
            }
        else:
            return {"success": True, "result": str(result)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========== JOIN BUILDING ENDPOINTS ==========

@app.post("/join/build")
async def build_join_endpoint(request: JoinRequest, session_id: Optional[str] = None):
    """Build a join between tables"""
    try:
        if session_id:
            session = SessionManager.get_session(session_id)
        else:
            session_id = SessionManager.create_session()
            session = sessions[session_id]

        if len(request.tables) < 2:
            raise HTTPException(status_code=400, detail="At least 2 tables required for join")

        # Build join
        table1 = request.tables[0]
        table2 = request.tables[1]

        join_builder = build_join(
            table1.table,
            table2.table,
            request.join_type,
            request.condition
        )

        session['join_builder'] = join_builder

        # Get join info
        join_info = get_join_info()

        return {
            "success": True,
            "join_info": join_info,
            "session_id": session_id
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/join/chain")
async def build_join_chain_endpoint(joins: List[JoinDefinition], session_id: str):
    """Build a chain of joins"""
    try:
        session = SessionManager.get_session(session_id)

        join_defs = []
        for j in joins:
            join_defs.append({
                'table1': j.table1,
                'table2': j.table2,
                'type': j.join_type,
                'condition': j.condition
            })

        join_builder = build_join_chain(join_defs)
        session['join_builder'] = join_builder

        return {"success": True, "message": "Join chain built successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========== CTE ENDPOINTS ==========

@app.post("/cte/create")
async def create_cte_endpoint(request: CTERequest, session_id: str):
    """Create a CTE"""
    try:
        session = SessionManager.get_session(session_id)

        cte_builder = build_cte(request.name, request.query)
        session['cte_builder'] = cte_builder

        return {
            "success": True,
            "message": f"CTE '{request.name}' created",
            "cte_query": build_cte_query()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cte/list")
async def list_ctes_endpoint(session_id: str):
    """List all CTEs in session"""
    try:
        session = SessionManager.get_session(session_id)
        ctes = list_ctes()
        return {"ctes": ctes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/cte/reset")
async def reset_cte_endpoint(session_id: str):
    """Reset CTE builder"""
    try:
        reset_cte_builder()
        return {"success": True, "message": "CTE builder reset"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========== TEMPORARY TABLE ENDPOINTS ==========

@app.post("/temp/create")
async def create_temp_table_endpoint(request: TempTableRequest, session_id: str):
    """Create a temporary table"""
    try:
        session = SessionManager.get_session(session_id)

        if request.query:
            temp_table = create_temp_table(request.name, request.query)
        elif request.columns:
            temp_table = create_temp_table(request.name, request.columns)
        elif request.from_dataframe:
            # Convert dict to DataFrame
            df = pd.DataFrame(request.from_dataframe)
            temp_table = TemporaryTable(request.name)
            temp_table.from_dataframe(df)
        else:
            raise HTTPException(status_code=400, detail="Either query, columns, or from_dataframe is required")

        session['temp_manager'].temp_tables[request.name] = temp_table

        return {
            "success": True,
            "message": f"Temporary table '{request.name}' created",
            "metadata": temp_table.describe()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/temp/list")
async def list_temp_tables_endpoint(session_id: str):
    """List all temporary tables in session"""
    try:
        session = SessionManager.get_session(session_id)
        tables = session['temp_manager'].list_temp_tables()
        return {"temporary_tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/temp/{name}")
async def get_temp_table_endpoint(name: str, session_id: str):
    """Get information about a temporary table"""
    try:
        session = SessionManager.get_session(session_id)
        temp_table = session['temp_manager'].get_temp_table(name)

        if not temp_table:
            raise HTTPException(status_code=404, detail=f"Temporary table '{name}' not found")

        return {"table_info": temp_table.describe()}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/temp/{name}")
async def drop_temp_table_endpoint(name: str, session_id: str):
    """Drop a temporary table"""
    try:
        session = SessionManager.get_session(session_id)
        session['temp_manager'].drop_temp_table(name)
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
        temp_manager = session['temp_manager']

        # Build CTEs first if they exist
        if cte_builder:
            cte_query = cte_builder.build()
            session['last_query'] = cte_query
            return {"query": cte_query, "type": "cte"}

        # Otherwise build from joins
        elif join_builder:
            query = join_builder.build()
            session['last_query'] = query
            return {"query": query, "type": "join"}

        else:
            raise HTTPException(status_code=400, detail="No query components found in session")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========== EXPORT ENDPOINTS ==========

@app.post("/export/{format}")
async def export_query_results(format: str, session_id: str, filename: Optional[str] = None):
    """Export query results in various formats"""
    try:
        session = SessionManager.get_session(session_id)

        if not session['last_query']:
            raise HTTPException(status_code=400, detail="No query to export")

        engine = session['query_engine']
        if not engine:
            raise HTTPException(status_code=400, detail="Query engine not initialized")

        # Execute query
        result = engine.execute_query(session['last_query'])

        if not isinstance(result, pd.DataFrame):
            raise HTTPException(status_code=400, detail="Results cannot be exported in this format")

        # Generate filename if not provided
        if not filename:
            filename = f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Export in requested format
        if format.lower() == 'csv':
            csv_data = result.to_csv(index=False)
            return JSONResponse(
                content={"data": csv_data, "filename": f"{filename}.csv"},
                headers={"Content-Disposition": f"attachment; filename={filename}.csv"}
            )
        elif format.lower() == 'json':
            return {"data": result.to_dict(orient='records'), "filename": f"{filename}.json"}
        elif format.lower() == 'excel':
            # For Excel, we'd need to create a file and return it
            excel_file = f"{filename}.xlsx"
            result.to_excel(excel_file, index=False)
            return {"message": f"File saved as {excel_file}"}
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported format: {format}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========== HEALTH CHECK ==========

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "database_loaded": db_info is not None,
        "tables_loaded": len(db_info.get_tables()) if db_info else 0,
        "active_sessions": len(sessions)
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
    return JSONResponse(
        status_code=500,
        content={
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "success": False
        }
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)