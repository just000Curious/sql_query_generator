"""
Streamlit UI for SQL Query Generator
Connects to FastAPI backend and provides interactive interface
"""

import streamlit as st
import requests
import pandas as pd
import json
import time
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# ========== CONFIGURATION ==========

API_URL = "http://localhost:8000"  # FastAPI backend URL

# Page config
st.set_page_config(
    page_title="SQL Query Generator",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .stButton > button {
        width: 100%;
        background-color: #4CAF50;
        color: white;
        font-weight: bold;
    }
    .success-box {
        padding: 10px;
        border-radius: 5px;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .error-box {
        padding: 10px;
        border-radius: 5px;
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
    }
    .warning-box {
        padding: 10px;
        border-radius: 5px;
        background-color: #fff3cd;
        border: 1px solid #ffeeba;
        color: #856404;
    }
    .info-box {
        padding: 10px;
        border-radius: 5px;
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        color: #0c5460;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 5px;
        text-align: center;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# ========== SESSION STATE INITIALIZATION ==========

if 'session_id' not in st.session_state:
    st.session_state.session_id = None
if 'tables' not in st.session_state:
    st.session_state.tables = []
if 'columns' not in st.session_state:
    st.session_state.columns = {}
if 'relationships' not in st.session_state:
    st.session_state.relationships = []
if 'last_query' not in st.session_state:
    st.session_state.last_query = None
if 'query_results' not in st.session_state:
    st.session_state.query_results = None
if 'temp_tables' not in st.session_state:
    st.session_state.temp_tables = []
if 'ctes' not in st.session_state:
    st.session_state.ctes = []

# ========== HELPER FUNCTIONS ==========

def check_api_health():
    """Check if API is running"""
    try:
        response = requests.get(f"{API_URL}/health", timeout=2)
        return response.status_code == 200
    except:
        return False

def create_session():
    """Create a new session"""
    try:
        response = requests.post(f"{API_URL}/sessions/create")
        if response.status_code == 200:
            st.session_state.session_id = response.json()['session_id']
            return True
    except:
        return False
    return False

def load_tables():
    """Load tables from database"""
    try:
        response = requests.get(f"{API_URL}/tables")
        if response.status_code == 200:
            st.session_state.tables = response.json()['tables']
            return True
    except:
        return False
    return False

def load_columns(table_name):
    """Load columns for a specific table"""
    try:
        response = requests.get(f"{API_URL}/tables/{table_name}/columns")
        if response.status_code == 200:
            st.session_state.columns[table_name] = response.json()['columns']
            return response.json()['columns']
    except:
        return []
    return []

def load_relationships(table=None):
    """Load relationships"""
    try:
        url = f"{API_URL}/relationships"
        if table:
            url += f"?table={table}"
        response = requests.get(url)
        if response.status_code == 200:
            st.session_state.relationships = response.json()['relationships']
            return True
    except:
        return False
    return False

def generate_query(params):
    """Generate SQL query"""
    try:
        response = requests.post(
            f"{API_URL}/query/generate",
            json=params,
            params={"session_id": st.session_state.session_id}
        )
        if response.status_code == 200:
            data = response.json()
            st.session_state.last_query = data.get('query')
            return data
    except Exception as e:
        st.error(f"Error: {str(e)}")
    return None

def validate_query(sql):
    """Validate SQL query"""
    try:
        response = requests.post(
            f"{API_URL}/query/validate",
            json={"sql": sql, "validate": True}
        )
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

def execute_query(sql):
    """Execute SQL query"""
    try:
        response = requests.post(
            f"{API_URL}/query/execute",
            json={"sql": sql},
            params={"session_id": st.session_state.session_id}
        )
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

def create_temp_table(name, query=None, columns=None):
    """Create temporary table"""
    try:
        data = {"name": name}
        if query:
            data["query"] = query
        if columns:
            data["columns"] = columns

        response = requests.post(
            f"{API_URL}/temp/create",
            json=data,
            params={"session_id": st.session_state.session_id}
        )
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

def build_join(table1, table2, join_type="INNER JOIN", condition=None):
    """Build a join"""
    try:
        data = {
            "tables": [
                {"table": table1},
                {"table": table2}
            ],
            "join_type": join_type
        }
        if condition:
            data["condition"] = condition

        response = requests.post(
            f"{API_URL}/join/build",
            json=data,
            params={"session_id": st.session_state.session_id}
        )
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

def create_cte(name, query):
    """Create a CTE"""
    try:
        response = requests.post(
            f"{API_URL}/cte/create",
            json={"name": name, "query": query},
            params={"session_id": st.session_state.session_id}
        )
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

def assemble_query():
    """Assemble final query"""
    try:
        response = requests.post(
            f"{API_URL}/assemble",
            params={"session_id": st.session_state.session_id}
        )
        if response.status_code == 200:
            data = response.json()
            st.session_state.last_query = data.get('query')
            return data
    except:
        pass
    return None

# ========== MAIN APP ==========

def main():
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/color/96/000000/sql.png", width=80)
        st.title("SQL Query Generator")
        st.markdown("---")

        # API Connection Status
        api_healthy = check_api_health()
        if api_healthy:
            st.success("✅ API Connected")
        else:
            st.error("❌ API Not Connected")
            st.info("Run: `python api.py` to start the backend")
            st.stop()

        # Session Management
        st.subheader("📊 Session")
        if st.button("🆕 Create New Session"):
            if create_session():
                st.success("Session created!")
                st.rerun()
            else:
                st.error("Failed to create session")

        if st.session_state.session_id:
            st.info(f"Session ID: `{st.session_state.session_id[:8]}...`")

        st.markdown("---")

        # Navigation
        st.subheader("🧭 Navigation")
        page = st.radio(
            "Go to",
            ["🏠 Dashboard",
             "📝 Query Builder",
             "🔗 Join Builder",
             "🔄 CTE Builder",
             "📦 Temp Tables",
             "✅ Validator",
             "📊 Results",
             "🔍 Schema Explorer"]
        )

        st.markdown("---")

        # Quick Actions
        st.subheader("⚡ Quick Actions")
        if st.button("📥 Load Tables"):
            if load_tables():
                st.success(f"Loaded {len(st.session_state.tables)} tables")
                st.rerun()

        if st.button("🔄 Load Relationships"):
            if load_relationships():
                st.success(f"Loaded {len(st.session_state.relationships)} relationships")

        st.markdown("---")
        st.caption(f"v1.0.0 | {datetime.now().strftime('%Y-%m-%d')}")

    # Main content area
    if page == "🏠 Dashboard":
        show_dashboard()
    elif page == "📝 Query Builder":
        show_query_builder()
    elif page == "🔗 Join Builder":
        show_join_builder()
    elif page == "🔄 CTE Builder":
        show_cte_builder()
    elif page == "📦 Temp Tables":
        show_temp_tables()
    elif page == "✅ Validator":
        show_validator()
    elif page == "📊 Results":
        show_results()
    elif page == "🔍 Schema Explorer":
        show_schema_explorer()

# ========== DASHBOARD ==========

def show_dashboard():
    st.title("🏠 SQL Query Generator Dashboard")

    # Metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        with st.container():
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("📊 Tables", len(st.session_state.tables))
            st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        with st.container():
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            total_columns = sum(len(cols) for cols in st.session_state.columns.values())
            st.metric("📋 Columns", total_columns)
            st.markdown('</div>', unsafe_allow_html=True)

    with col3:
        with st.container():
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("🔗 Relationships", len(st.session_state.relationships))
            st.markdown('</div>', unsafe_allow_html=True)

    with col4:
        with st.container():
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("📦 Temp Tables", len(st.session_state.temp_tables))
            st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("---")

    # Quick Start Guide
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🚀 Quick Start")
        st.markdown("""
        1. **Load Tables** from sidebar
        2. Go to **Query Builder** to create SQL
        3. Use **Join Builder** for complex joins
        4. Create **CTEs** for multi-stage queries
        5. **Validate** your query
        6. **Execute** and see results
        """)

        st.subheader("🎯 Features")
        st.markdown("""
        - ✅ Interactive SQL generation
        - ✅ Automatic join detection
        - ✅ CTE support
        - ✅ Temporary tables
        - ✅ Query validation
        - ✅ Export results (CSV/JSON)
        """)

    with col2:
        st.subheader("📊 Database Overview")
        if st.session_state.tables:
            df = pd.DataFrame({
                "Table": st.session_state.tables,
                "Columns": [len(st.session_state.columns.get(t, [])) for t in st.session_state.tables],
                "Relationships": [sum(1 for r in st.session_state.relationships if r.get('table') == t)
                                 for t in st.session_state.tables]
            })
            st.dataframe(df, use_container_width=True)
        else:
            st.info("Click 'Load Tables' in sidebar to view database schema")

# ========== QUERY BUILDER ==========

def show_query_builder():
    st.title("📝 Query Builder")

    if not st.session_state.tables:
        st.warning("Please load tables first from the sidebar")
        return

    col1, col2 = st.columns([2, 1])

    with col1:
        # Table Selection
        selected_tables = st.multiselect(
            "Select Tables",
            st.session_state.tables,
            help="Choose tables to query"
        )

        if selected_tables:
            # Column Selection
            st.subheader("Select Columns")
            columns_to_select = []

            for table in selected_tables:
                if table not in st.session_state.columns:
                    load_columns(table)

                cols = st.session_state.columns.get(table, [])
                col_names = [c['column_name'] for c in cols]

                selected_cols = st.multiselect(
                    f"Columns from {table}",
                    ["*"] + col_names,
                    key=f"cols_{table}"
                )

                for col in selected_cols:
                    columns_to_select.append({
                        "table": table,
                        "column": col
                    })

            # Where Conditions
            st.subheader("Where Conditions")
            num_conditions = st.number_input("Number of conditions", 0, 10, 0)

            conditions = []
            for i in range(num_conditions):
                with st.expander(f"Condition {i+1}"):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        table = st.selectbox("Table", selected_tables, key=f"cond_table_{i}")
                    with col2:
                        if table in st.session_state.columns:
                            cols = [c['column_name'] for c in st.session_state.columns.get(table, [])]
                            column = st.selectbox("Column", cols, key=f"cond_col_{i}")
                    with col3:
                        operator = st.selectbox("Operator",
                                               ["=", ">", "<", ">=", "<=", "!=", "LIKE", "IN"],
                                               key=f"cond_op_{i}")
                    value = st.text_input("Value", key=f"cond_val_{i}")

                    conditions.append({
                        "table": table,
                        "column": column,
                        "operator": operator,
                        "value": value
                    })

            # Advanced Options
            with st.expander("Advanced Options"):
                col1, col2 = st.columns(2)
                with col1:
                    limit = st.number_input("Limit", 0, 10000, 0)
                with col2:
                    offset = st.number_input("Offset", 0, 10000, 0)

                order_by = st.text_input("Order By (comma-separated columns)")
                group_by = st.text_input("Group By (comma-separated columns)")

                use_cte = st.checkbox("Use CTE")
                cte_name = None
                if use_cte:
                    cte_name = st.text_input("CTE Name", "my_cte")

                create_temp = st.checkbox("Create as Temporary Table")
                temp_name = None
                if create_temp:
                    temp_name = st.text_input("Temporary Table Name", "temp_results")

    with col2:
        st.subheader("Query Preview")

        if st.button("🚀 Generate Query", type="primary"):
            params = {
                "tables": [{"table": t} for t in selected_tables],
                "columns": columns_to_select if columns_to_select else None,
                "conditions": conditions if conditions else None,
                "limit": limit if limit > 0 else None,
                "offset": offset if offset > 0 else None,
                "use_cte": use_cte,
                "cte_name": cte_name if use_cte else None,
                "create_temp_table": temp_name if create_temp else None
            }

            with st.spinner("Generating query..."):
                result = generate_query(params)
                if result and result.get('success'):
                    st.success("Query generated successfully!")
                    st.session_state.last_query = result['query']
                else:
                    st.error(result.get('error', "Failed to generate query"))

        if st.session_state.last_query:
            st.code(st.session_state.last_query, language="sql")

            # Action buttons
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Validate"):
                    validation = validate_query(st.session_state.last_query)
                    if validation:
                        if validation['valid']:
                            st.success("Query is valid!")
                        else:
                            st.error("Query has errors")
                            for err in validation['errors']:
                                st.warning(err)

            with col2:
                if st.button("▶️ Execute"):
                    with st.spinner("Executing query..."):
                        results = execute_query(st.session_state.last_query)
                        if results:
                            st.session_state.query_results = results
                            st.success("Query executed!")
                            st.rerun()

# ========== JOIN BUILDER ==========

def show_join_builder():
    st.title("🔗 Join Builder")

    if len(st.session_state.tables) < 2:
        st.warning("Need at least 2 tables to build joins")
        return

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Build Join")

        table1 = st.selectbox("First Table", st.session_state.tables, key="join_table1")
        table2 = st.selectbox("Second Table", st.session_state.tables, key="join_table2")

        join_type = st.selectbox(
            "Join Type",
            ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN"]
        )

        # Auto-detect relationship
        rel = None
        for r in st.session_state.relationships:
            if (r.get('from_table') == table1 and r.get('to_table') == table2) or \
               (r.get('from_table') == table2 and r.get('to_table') == table1):
                rel = r
                break

        if rel:
            st.info(f"Detected relationship: {rel.get('from_column')} = {rel.get('to_column')}")
            condition = f"{rel.get('from_table')}.{rel.get('from_column')} = {rel.get('to_table')}.{rel.get('to_column')}"
        else:
            condition = st.text_input("Join Condition (e.g., table1.id = table2.user_id)")

        if st.button("🔨 Build Join", type="primary"):
            with st.spinner("Building join..."):
                result = build_join(table1, table2, join_type, condition)
                if result:
                    st.success("Join built successfully!")
                    st.json(result.get('join_info', {}))

    with col2:
        st.subheader("Available Relationships")
        if st.session_state.relationships:
            for rel in st.session_state.relationships[:5]:
                st.info(f"{rel.get('from_table')}.{rel.get('from_column')} → "
                       f"{rel.get('to_table')}.{rel.get('to_column')}")
        else:
            st.info("No relationships loaded")

# ========== CTE BUILDER ==========

def show_cte_builder():
    st.title("🔄 CTE Builder")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Create CTE")

        cte_name = st.text_input("CTE Name", "stage_1")
        cte_query = st.text_area("CTE Query", height=150)

        if st.button("➕ Add CTE", type="primary"):
            if cte_name and cte_query:
                result = create_cte(cte_name, cte_query)
                if result:
                    st.success(f"CTE '{cte_name}' created!")
                    if cte_name not in st.session_state.ctes:
                        st.session_state.ctes.append(cte_name)

        st.markdown("---")

        st.subheader("Final Query")
        final_query = st.text_area("Final Query (using CTEs)", height=150)

        if st.button("🔨 Build Final Query"):
            if final_query:
                # This would need to be implemented in the API
                st.session_state.last_query = final_query
                st.success("Final query set!")

    with col2:
        st.subheader("Active CTEs")
        if st.session_state.ctes:
            for cte in st.session_state.ctes:
                st.info(f"📄 {cte}")
        else:
            st.info("No CTEs created yet")

# ========== TEMP TABLES ==========

def show_temp_tables():
    st.title("📦 Temporary Tables")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Create Temporary Table")

        temp_name = st.text_input("Table Name", "temp_data")

        creation_method = st.radio(
            "Creation Method",
            ["From Query", "From Columns", "From DataFrame"]
        )

        if creation_method == "From Query":
            temp_query = st.text_area("SQL Query", height=150)
            if st.button("📊 Create Temp Table", type="primary"):
                if temp_name and temp_query:
                    result = create_temp_table(temp_name, query=temp_query)
                    if result:
                        st.success(f"Temporary table '{temp_name}' created!")
                        if temp_name not in st.session_state.temp_tables:
                            st.session_state.temp_tables.append(temp_name)

        elif creation_method == "From Columns":
            col_defs = st.text_area("Column Definitions (one per line)",
                                    "id INT\nname VARCHAR(100)\ncreated_at DATE")
            if st.button("📊 Create Temp Table", type="primary"):
                columns = [c.strip() for c in col_defs.split('\n') if c.strip()]
                result = create_temp_table(temp_name, columns=columns)
                if result:
                    st.success(f"Temporary table '{temp_name}' created!")
                    if temp_name not in st.session_state.temp_tables:
                        st.session_state.temp_tables.append(temp_name)

        else:  # From DataFrame
            st.info("Upload a CSV to create temporary table")
            uploaded_file = st.file_uploader("Choose CSV file", type="csv")
            if uploaded_file and st.button("📊 Create Temp Table", type="primary"):
                df = pd.read_csv(uploaded_file)
                # This would need to be implemented in the API
                st.success(f"DataFrame loaded with {len(df)} rows")

    with col2:
        st.subheader("Active Temp Tables")
        if st.session_state.temp_tables:
            for temp in st.session_state.temp_tables:
                st.info(f"📋 {temp}")
        else:
            st.info("No temporary tables created")

# ========== VALIDATOR ==========

def show_validator():
    st.title("✅ Query Validator")

    sql_to_validate = st.text_area(
        "Enter SQL to validate",
        value=st.session_state.last_query if st.session_state.last_query else "",
        height=200
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("🔍 Validate", type="primary"):
            if sql_to_validate:
                with st.spinner("Validating..."):
                    validation = validate_query(sql_to_validate)
                    if validation:
                        if validation['valid']:
                            st.success("✅ Query is valid!")
                        else:
                            st.error("❌ Query has errors")

                        if validation.get('errors'):
                            st.subheader("Errors")
                            for err in validation['errors']:
                                st.markdown(f'<div class="error-box">{err}</div>',
                                          unsafe_allow_html=True)

                        if validation.get('warnings'):
                            st.subheader("Warnings")
                            for warn in validation['warnings']:
                                st.markdown(f'<div class="warning-box">{warn}</div>',
                                          unsafe_allow_html=True)

    with col2:
        if st.button("📋 Load Last Query"):
            if st.session_state.last_query:
                sql_to_validate = st.session_state.last_query
                st.rerun()

    with col3:
        if st.button("🧹 Clear"):
            st.rerun()

    # Validation Rules
    with st.expander("📖 Validation Rules"):
        st.markdown("""
        - **Tables must exist** in the database
        - **Columns must exist** in their respective tables
        - **JOIN conditions** must reference valid columns
        - **SQL syntax** must be valid
        - **Aggregate functions** must be used with GROUP BY
        """)

# ========== RESULTS ==========

def show_results():
    st.title("📊 Query Results")

    if st.session_state.query_results:
        data = st.session_state.query_results

        # Display results info
        if isinstance(data, dict):
            if 'data' in data:
                df = pd.DataFrame(data['data'])

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Rows", data.get('row_count', len(df)))
                with col2:
                    st.metric("Columns", len(df.columns))
                with col3:
                    if data.get('truncated'):
                        st.warning("Results truncated to 1000 rows")

                # Display the data
                st.dataframe(df, use_container_width=True)

                # Export options
                st.subheader("Export Results")
                export_format = st.radio("Format", ["CSV", "JSON"], horizontal=True)

                if export_format == "CSV":
                    csv = df.to_csv(index=False)
                    st.download_button(
                        "📥 Download CSV",
                        csv,
                        f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        "text/csv"
                    )
                else:
                    json_str = df.to_json(orient='records', indent=2)
                    st.download_button(
                        "📥 Download JSON",
                        json_str,
                        f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        "application/json"
                    )

                # Data Visualization
                if len(df.columns) >= 2:
                    st.subheader("Quick Visualization")
                    chart_type = st.selectbox("Chart Type",
                                             ["Bar Chart", "Line Chart", "Scatter Plot"])

                    x_axis = st.selectbox("X Axis", df.columns)
                    y_axis = st.selectbox("Y Axis", [c for c in df.columns if c != x_axis])

                    if chart_type == "Bar Chart":
                        fig = px.bar(df, x=x_axis, y=y_axis)
                    elif chart_type == "Line Chart":
                        fig = px.line(df, x=x_axis, y=y_axis)
                    else:
                        fig = px.scatter(df, x=x_axis, y=y_axis)

                    st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No query results yet. Generate and execute a query to see results here.")

# ========== SCHEMA EXPLORER ==========

def show_schema_explorer():
    st.title("🔍 Schema Explorer")

    if not st.session_state.tables:
        st.warning("Please load tables first from the sidebar")
        return

    # Table selector
    selected_table = st.selectbox("Select Table", st.session_state.tables)

    if selected_table:
        # Load columns if not already loaded
        if selected_table not in st.session_state.columns:
            load_columns(selected_table)

        cols = st.session_state.columns.get(selected_table, [])

        if cols:
            # Display column information
            df_columns = pd.DataFrame(cols)
            st.subheader(f"Columns in {selected_table}")
            st.dataframe(df_columns, use_container_width=True)

        # Display relationships
        st.subheader("Relationships")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Foreign Keys (Outgoing)**")
            outgoing = [r for r in st.session_state.relationships
                       if r.get('from_table') == selected_table]
            if outgoing:
                for rel in outgoing:
                    st.info(f"→ {rel.get('to_table')}.{rel.get('to_column')}")
            else:
                st.info("No outgoing relationships")

        with col2:
            st.markdown("**Referenced By (Incoming)**")
            incoming = [r for r in st.session_state.relationships
                       if r.get('to_table') == selected_table]
            if incoming:
                for rel in incoming:
                    st.info(f"← {rel.get('from_table')}.{rel.get('from_column')}")
            else:
                st.info("No incoming relationships")

        # Sample query
        with st.expander("📝 Sample Query"):
            sample_query = f"SELECT * FROM {selected_table} LIMIT 10"
            st.code(sample_query, language="sql")
            if st.button("Try Sample Query"):
                params = {
                    "tables": [{"table": selected_table}],
                    "limit": 10
                }
                result = generate_query(params)
                if result:
                    st.success("Sample query generated!")

# ========== RUN APP ==========

if __name__ == "__main__":
    main()