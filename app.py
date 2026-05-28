"""
streamlit_app.py
Simple Streamlit UI for testing the SQL Query Generator API
"""

import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime, date, timedelta
import plotly.express as px
import plotly.graph_objects as go

# API Configuration
API_URL = "http://127.0.0.1:8000"

# Page configuration
st.set_page_config(
    page_title="SQL Query Generator",
    page_icon="🗄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
    }
    .query-box {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 5px;
        font-family: monospace;
        margin: 1rem 0;
    }
    .success-box {
        background-color: #d4edda;
        color: #155724;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        color: #721c24;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)


def check_api_health():
    """Check if API is running"""
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False


def get_tables():
    """Get list of tables from API"""
    try:
        response = requests.get(f"{API_URL}/tables")
        if response.status_code == 200:
            return response.json().get("tables", [])
    except:
        pass
    return []


def get_table_schema(table_name):
    """Get schema for a specific table"""
    try:
        response = requests.get(f"{API_URL}/tables/{table_name}")
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None


def generate_query(table, columns, conditions, group_by, order_by, limit):
    """Generate SQL query"""
    payload = {
        "table": table,
        "columns": columns,
        "conditions": conditions,
        "group_by": group_by,
        "order_by": order_by,
        "limit": limit
    }
    try:
        response = requests.post(f"{API_URL}/query/generate", json=payload)
        return response.json()
    except Exception as e:
        return {"success": False, "error": str(e)}


def execute_query(sql, limit=1000):
    """Execute SQL query"""
    payload = {
        "sql": sql,
        "limit": limit
    }
    try:
        response = requests.post(f"{API_URL}/query/execute", json=payload)
        return response.json()
    except Exception as e:
        return {"success": False, "message": str(e)}


def get_sample_queries():
    """Get sample queries from API"""
    try:
        response = requests.get(f"{API_URL}/samples")
        if response.status_code == 200:
            return response.json().get("samples", [])
    except:
        pass
    return []


def search_columns(search_term):
    """Search for columns"""
    try:
        response = requests.get(f"{API_URL}/search/columns", params={"q": search_term})
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None


def main():
    # Header
    st.markdown('<div class="main-header"><h1>🗄️ SQL Query Generator</h1><p>Generate and execute SQL queries with ease</p></div>', unsafe_allow_html=True)

    # Check API health
    if not check_api_health():
        st.error("❌ Cannot connect to API. Please make sure the API server is running.")
        st.info("Run the API first: python api_improved.py")
        return

    st.success("✅ Connected to API Server")

    # Sidebar
    with st.sidebar:
        st.header("📊 Database Info")

        # Get tables
        tables = get_tables()
        st.write(f"**Tables:** {len(tables)} tables found")

        # Table selector
        selected_table = st.selectbox("Select Table", tables if tables else ["No tables found"])

        if selected_table and selected_table != "No tables found":
            schema = get_table_schema(selected_table)
            if schema:
                st.write(f"**Columns in {selected_table}:**")
                columns_list = schema.get("columns_list", [])
                for col in columns_list[:10]:
                    st.write(f"- {col}")
                if len(columns_list) > 10:
                    st.write(f"... and {len(columns_list) - 10} more")

        st.divider()

        # Search
        st.header("🔍 Search")
        search_term = st.text_input("Search columns", placeholder="Enter column name...")
        if search_term:
            results = search_columns(search_term)
            if results:
                st.write(f"Found {results.get('count', 0)} columns:")
                for col in results.get('results', [])[:10]:
                    st.write(f"- {col.get('table', '')}.{col.get('column', '')}")

        st.divider()

        # API Status
        st.header("📡 API Info")
        st.write(f"**API URL:** {API_URL}")
        st.write(f"**Docs:** {API_URL}/docs")

    # Main content
    tab1, tab2, tab3, tab4 = st.tabs(["✍️ Query Builder", "📝 SQL Editor", "📊 Sample Queries", "📈 Analytics"])

    # Tab 1: Query Builder
    with tab1:
        st.header("Build Your Query")

        col1, col2 = st.columns(2)

        with col1:
            table = st.selectbox("Table", tables, key="builder_table")

            # Get columns for selected table
            schema = get_table_schema(table) if table and table != "No tables found" else None
            columns = schema.get("columns_list", []) if schema else []

            selected_columns = st.multiselect("Select Columns", columns, default=columns[:3] if columns else [])

            # Conditions
            st.subheader("WHERE Conditions")
            conditions = []
            num_conditions = st.number_input("Number of conditions", min_value=0, max_value=5, value=0)

            for i in range(num_conditions):
                st.write(f"Condition {i+1}")
                col1_cond, col2_cond, col3_cond = st.columns(3)
                with col1_cond:
                    cond_col = st.selectbox("Column", columns, key=f"cond_col_{i}")
                with col2_cond:
                    cond_op = st.selectbox("Operator", ["=", "!=", ">", ">=", "<", "<=", "LIKE"], key=f"cond_op_{i}")
                with col3_cond:
                    cond_val = st.text_input("Value", key=f"cond_val_{i}")
                conditions.append({"column": cond_col, "operator": cond_op, "value": cond_val})

        with col2:
            # GROUP BY
            group_by = st.multiselect("GROUP BY", columns)

            # ORDER BY
            st.subheader("ORDER BY")
            order_by_cols = []
            num_order = st.number_input("Number of order by clauses", min_value=0, max_value=3, value=0)

            for i in range(num_order):
                col1_order, col2_order = st.columns(2)
                with col1_order:
                    order_col = st.selectbox("Column", columns, key=f"order_col_{i}")
                with col2_order:
                    order_dir = st.selectbox("Direction", ["ASC", "DESC"], key=f"order_dir_{i}")
                order_by_cols.append(f"{order_col} {order_dir}")

            # LIMIT
            limit = st.number_input("LIMIT", min_value=1, max_value=10000, value=100)

        # Generate button
        if st.button("🚀 Generate Query", type="primary"):
            if not table or table == "No tables found":
                st.error("Please select a table")
            else:
                with st.spinner("Generating query..."):
                    result = generate_query(
                        table=table,
                        columns=selected_columns,
                        conditions=conditions,
                        group_by=group_by,
                        order_by=order_by_cols,
                        limit=limit
                    )

                    if result.get("success"):
                        st.markdown('<div class="success-box">✅ Query generated successfully!</div>', unsafe_allow_html=True)

                        # Display query
                        st.code(result["query"], language="sql")

                        # Execute button
                        if st.button("▶️ Execute Query"):
                            with st.spinner("Executing query..."):
                                exec_result = execute_query(result["query"], limit)
                                if exec_result.get("success"):
                                    st.success(f"✅ Query executed! {exec_result.get('row_count', 0)} rows returned in {exec_result.get('execution_time', 0):.3f}s")

                                    # Display results
                                    data = exec_result.get("data", [])
                                    if data:
                                        df = pd.DataFrame(data)
                                        st.dataframe(df, use_container_width=True)

                                        # Download button
                                        csv = df.to_csv(index=False)
                                        st.download_button(
                                            label="📥 Download as CSV",
                                            data=csv,
                                            file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                            mime="text/csv"
                                        )
                                    else:
                                        st.info("No results returned")
                                else:
                                    st.error(f"Execution failed: {exec_result.get('message', 'Unknown error')}")
                    else:
                        st.error(f"Query generation failed: {result.get('error', 'Unknown error')}")

    # Tab 2: SQL Editor
    with tab2:
        st.header("SQL Editor")

        # SQL Input
        sql_query = st.text_area("Enter SQL Query", height=200, placeholder="SELECT * FROM employees LIMIT 10")

        col1, col2 = st.columns([1, 3])
        with col1:
            exec_limit = st.number_input("Limit", min_value=1, max_value=10000, value=1000)
            execute_btn = st.button("▶️ Execute", type="primary")

        with col2:
            if st.button("📋 Clear"):
                st.session_state.sql_query = ""
                st.rerun()

        if execute_btn and sql_query:
            with st.spinner("Executing query..."):
                result = execute_query(sql_query, exec_limit)
                if result.get("success"):
                    st.success(f"✅ Query executed! {result.get('row_count', 0)} rows returned in {result.get('execution_time', 0):.3f}s")

                    data = result.get("data", [])
                    if data:
                        df = pd.DataFrame(data)
                        st.dataframe(df, use_container_width=True)

                        # Download button
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download as CSV",
                            data=csv,
                            file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.info("No results returned")
                else:
                    st.error(f"Execution failed: {result.get('message', 'Unknown error')}")

    # Tab 3: Sample Queries
    with tab3:
        st.header("Sample Queries")

        sample_queries = get_sample_queries()

        for i, sample in enumerate(sample_queries):
            with st.expander(f"{sample.get('name', 'Sample')} - {sample.get('description', '')}"):
                st.code(sample.get('sql', ''), language="sql")

                if st.button(f"Run Sample {i+1}", key=f"sample_{i}"):
                    with st.spinner("Executing..."):
                        result = execute_query(sample.get('sql', ''), 100)
                        if result.get("success"):
                            data = result.get("data", [])
                            if data:
                                df = pd.DataFrame(data)
                                st.dataframe(df, use_container_width=True)
                            else:
                                st.info("No results returned")
                        else:
                            st.error(f"Execution failed: {result.get('message', 'Unknown error')}")

    # Tab 4: Analytics
    with tab4:
        st.header("Database Analytics")

        # Get stats
        try:
            stats_response = requests.get(f"{API_URL}/stats")
            if stats_response.status_code == 200:
                stats = stats_response.json()

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Tables", stats.get("total_tables", 0))
                with col2:
                    st.metric("Total Columns", stats.get("total_columns", 0))

                # Tables chart
                tables_data = stats.get("tables", {})
                if tables_data:
                    table_names = list(tables_data.keys())
                    column_counts = [tables_data[t]["columns"] for t in table_names]

                    fig = px.bar(
                        x=table_names, y=column_counts,
                        title="Columns per Table",
                        labels={"x": "Table", "y": "Number of Columns"}
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    # Table details
                    st.subheader("Table Details")
                    for table, info in list(tables_data.items())[:10]:
                        with st.expander(f"{table} ({info['columns']} columns)"):
                            st.write("Columns:", ", ".join(info['columns_list'][:20]))
                            if len(info['columns_list']) > 20:
                                st.write(f"... and {len(info['columns_list']) - 20} more")
        except Exception as e:
            st.error(f"Could not fetch stats: {e}")

        # Quick queries
        st.subheader("Quick Analytics Queries")
        quick_queries = [
            ("Count all employees", "SELECT COUNT(*) as total FROM employees"),
            ("Departments with employee count", "SELECT emp_dept_cd, COUNT(*) as count FROM employees GROUP BY emp_dept_cd"),
            ("Complaint status summary", "SELECT status, COUNT(*) as count FROM complaints GROUP BY status"),
            ("Average salary by department", "SELECT emp_dept_cd, AVG(salary) as avg_salary FROM employees GROUP BY emp_dept_cd"),
        ]

        for name, query in quick_queries:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write(f"**{name}**")
                st.code(query, language="sql")
            with col2:
                if st.button(f"Run", key=f"quick_{name}"):
                    with st.spinner("Executing..."):
                        result = execute_query(query, 100)
                        if result.get("success"):
                            data = result.get("data", [])
                            if data:
                                df = pd.DataFrame(data)
                                st.dataframe(df, use_container_width=True)
                            else:
                                st.info("No results returned")
                        else:
                            st.error(f"Execution failed: {result.get('message', 'Unknown error')}")


if __name__ == "__main__":
    main()