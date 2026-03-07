import streamlit as st
import pandas as pd
import sqlite3
import os
from db_information import DBInfo
from pypika_query_engine import QueryGenerator
from join_builder import JoinBuilder
from temporary_table import TemporaryTable, TemporaryTableManager
import plotly.express as px
import plotly.graph_objects as go

# Try to import networkx, but don't fail if it's not available
try:
    import networkx as nx

    NETWORKX_AVAILABLE = True
except ImportError:
    NETWORKX_AVAILABLE = False
    nx = None

# Page config
st.set_page_config(
    page_title="Advanced SQL Query Generator",
    page_icon="🔍",
    layout="wide"
)

# Initialize session state
if 'schema_infos' not in st.session_state:
    st.session_state.schema_infos = {}

if 'selected_tables' not in st.session_state:
    st.session_state.selected_tables = []

if 'join_builders' not in st.session_state:
    st.session_state.join_builders = {}

if 'temp_manager' not in st.session_state:
    st.session_state.temp_manager = TemporaryTableManager()

if 'query_history' not in st.session_state:
    st.session_state.query_history = []

if 'connection' not in st.session_state:
    # Create in-memory SQLite database for demo
    st.session_state.connection = sqlite3.connect(':memory:', check_same_thread=False)


# Auto-load schemas from db_files directory if it exists
def auto_load_schemas():
    """Auto-load schemas from db_files directory"""
    schema_dir = 'db_files'
    if os.path.exists(schema_dir) and os.path.isdir(schema_dir):
        for file in os.listdir(schema_dir):
            if file.endswith('.csv'):
                schema_name = file.replace('extracted_', '').replace('_schema.csv', '').replace('.csv', '')
                filepath = os.path.join(schema_dir, file)
                try:
                    df = pd.read_csv(filepath)
                    # Ensure required columns exist
                    required_cols = ['table_name', 'column_name', 'data_type']
                    if all(col in df.columns for col in required_cols):
                        st.session_state.schema_infos[schema_name] = DBInfo(df, schema_name)
                        st.sidebar.success(f"✅ Auto-loaded {schema_name}")
                    else:
                        st.sidebar.warning(f"⚠️ {file} missing required columns")
                except Exception as e:
                    st.sidebar.error(f"Error loading {file}: {e}")


# Auto-load schemas on startup
auto_load_schemas()

# Title and description
st.title("🔍 Advanced SQL Query Generator")
st.markdown("Build complex queries with multiple schemas, joins, CTEs, and temporary tables")

# Sidebar
with st.sidebar:
    st.header("📁 Schema Management")

    # File upload for multiple schemas
    uploaded_files = st.file_uploader(
        "Upload schema CSV files",
        type=['csv'],
        accept_multiple_files=True,
        key="schema_uploader"
    )

    if uploaded_files:
        for uploaded_file in uploaded_files:
            schema_name = st.text_input(
                f"Name for {uploaded_file.name}",
                value=uploaded_file.name.replace('.csv', '').replace('extracted_', '').replace('_schema', ''),
                key=f"schema_name_{uploaded_file.name}"
            )

            if st.button(f"Load {uploaded_file.name}", key=f"load_{uploaded_file.name}"):
                try:
                    df = pd.read_csv(uploaded_file)
                    # Validate required columns
                    required_cols = ['table_name', 'column_name', 'data_type']
                    missing_cols = [col for col in required_cols if col not in df.columns]

                    if missing_cols:
                        st.error(f"Missing required columns: {missing_cols}")
                    else:
                        st.session_state.schema_infos[schema_name] = DBInfo(df, schema_name)
                        st.success(f"✅ Loaded {schema_name}")
                        st.rerun()
                except Exception as e:
                    st.error(f"Error loading {uploaded_file.name}: {e}")

    # Show loaded schemas
    if st.session_state.schema_infos:
        st.markdown("---")
        st.subheader("📊 Loaded Schemas")
        for schema_name in st.session_state.schema_infos.keys():
            col1, col2 = st.columns([3, 1])
            with col1:
                st.info(f"📁 {schema_name}")
            with col2:
                if st.button("❌", key=f"remove_{schema_name}"):
                    del st.session_state.schema_infos[schema_name]
                    st.rerun()

    # Temporary tables management
    st.markdown("---")
    st.subheader("🗂️ Temporary Tables")

    temp_tables = st.session_state.temp_manager.list_temp_tables()
    if temp_tables:
        for temp_name in temp_tables:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.text(f"📄 {temp_name}")
            with col2:
                if st.button("❌", key=f"drop_{temp_name}"):
                    st.session_state.temp_manager.drop_temp_table(temp_name)
                    st.rerun()
    else:
        st.info("No temporary tables created")

    # Query history
    st.markdown("---")
    st.subheader("📜 Query History")
    if st.session_state.query_history:
        for i, query in enumerate(st.session_state.query_history[-5:]):
            with st.expander(f"Query {i + 1}"):
                st.code(query, language="sql")
                if st.button("📋 Use", key=f"use_query_{i}"):
                    st.session_state.current_query = query
    else:
        st.info("No queries generated yet")

# Main content area with tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "🔗 Join Builder",
    "📊 Query Generator",
    "🔄 CTE Architecture",
    "📈 Schema Visualization"
])

with tab1:
    st.header("Multi-Table Join Builder")

    if not st.session_state.schema_infos:
        st.warning("Please load at least one schema to begin")
    else:
        # Schema and table selection
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("1. Select Tables")

            # Schema selector
            selected_schemas = st.multiselect(
                "Select schemas",
                options=list(st.session_state.schema_infos.keys()),
                default=list(st.session_state.schema_infos.keys())[:1] if st.session_state.schema_infos else []
            )

            # Table selector with schema context
            all_tables = []
            for schema in selected_schemas:
                db_info = st.session_state.schema_infos[schema]
                for table in db_info.get_tables():
                    if table and pd.notna(table):  # Skip None/NaN values
                        all_tables.append({
                            'schema': schema,
                            'table': str(table),  # Convert to string
                            'display': f"{schema}.{table}"
                        })

            selected_displays = st.multiselect(
                "Select tables to join (2 or more)",
                options=[t['display'] for t in all_tables],
                key="join_table_select"
            )

            # Convert selected displays to table info
            selected_tables = []
            for display in selected_displays:
                if '.' in display:
                    schema, table = display.split('.', 1)
                    selected_tables.append({
                        'schema': schema,
                        'table': table,
                        'display': display
                    })

            st.session_state.selected_tables = selected_tables

        with col2:
            st.subheader("2. Join Configuration")

            if len(selected_tables) >= 2:
                # Auto-join or manual join
                join_mode = st.radio(
                    "Join mode",
                    ["Auto-detect joins", "Manual joins"]
                )

                if join_mode == "Auto-detect joins":
                    if st.button("🔍 Auto-detect Join Path", type="primary"):
                        try:
                            # Use first table's schema for DBInfo
                            db_info = st.session_state.schema_infos[selected_tables[0]['schema']]
                            join_builder = JoinBuilder(db_info)

                            # Auto-join
                            join_builder.auto_join(selected_tables)

                            # Store in session state
                            join_key = "_".join([t['table'] for t in selected_tables])
                            st.session_state.join_builders[join_key] = join_builder

                            st.success("✅ Join path detected!")
                        except Exception as e:
                            st.error(f"Error detecting joins: {e}")

                else:  # Manual joins
                    st.markdown("Add join conditions:")

                    # Create join condition
                    from_idx = st.selectbox(
                        "From table",
                        range(len(selected_tables)),
                        format_func=lambda i: selected_tables[i]['display']
                    )

                    to_options = [i for i in range(len(selected_tables)) if i != from_idx]
                    if to_options:
                        to_idx = st.selectbox(
                            "To table",
                            to_options,
                            format_func=lambda i: selected_tables[i]['display']
                        )

                        from_table = selected_tables[from_idx]
                        to_table = selected_tables[to_idx]

                        # Get columns
                        db_info_from = st.session_state.schema_infos[from_table['schema']]
                        db_info_to = st.session_state.schema_infos[to_table['schema']]

                        from_cols = db_info_from.get_column_names(from_table['table'])
                        to_cols = db_info_to.get_column_names(to_table['table'])

                        # Filter out None values
                        from_cols = [col for col in from_cols if col and pd.notna(col)]
                        to_cols = [col for col in to_cols if col and pd.notna(col)]

                        if from_cols and to_cols:
                            from_col = st.selectbox(f"Column from {from_table['display']}", from_cols)
                            to_col = st.selectbox(f"Column from {to_table['display']}", to_cols)

                            join_type = st.selectbox(
                                "Join type",
                                ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN"]
                            )

                            if st.button("➕ Add Join Condition"):
                                # Create or update join builder
                                join_key = "_".join([t['table'] for t in selected_tables])

                                if join_key not in st.session_state.join_builders:
                                    db_info = st.session_state.schema_infos[selected_tables[0]['schema']]
                                    st.session_state.join_builders[join_key] = JoinBuilder(db_info)

                                join_builder = st.session_state.join_builders[join_key]

                                # Add tables if not already added
                                for table_info in selected_tables:
                                    join_builder.add_table(
                                        table_info['table'],
                                        table_info['schema']
                                    )

                                # Add join condition
                                join_builder.add_join(
                                    from_table['table'], to_table['table'],
                                    from_col, to_col,
                                    from_table['schema'], to_table['schema'],
                                    join_type
                                )

                                st.success("✅ Join condition added")
                        else:
                            st.warning("One of the selected tables has no columns")
                    else:
                        st.warning("Need at least one other table to join with")

        # Join preview and column selection
        if len(selected_tables) >= 2:
            join_key = "_".join([t['table'] for t in selected_tables])

            if join_key in st.session_state.join_builders:
                join_builder = st.session_state.join_builders[join_key]

                st.markdown("---")
                st.subheader("3. Select Columns")

                # Column selection interface
                col_selections = []

                for table_info in selected_tables:
                    db_info = st.session_state.schema_infos[table_info['schema']]
                    columns = db_info.get_column_names(table_info['table'])
                    # Filter out None values
                    columns = [col for col in columns if col and pd.notna(col)]

                    if columns:
                        st.markdown(f"**{table_info['display']}**")

                        # Option to select all
                        select_all = st.checkbox(
                            f"Select all from {table_info['table']}",
                            key=f"select_all_{table_info['display']}"
                        )

                        if select_all:
                            for col in columns:
                                col_selections.append({
                                    'table': table_info['table'],
                                    'schema': table_info['schema'],
                                    'column': col,
                                    'alias': None
                                })
                        else:
                            selected_cols = st.multiselect(
                                f"Columns from {table_info['table']}",
                                options=columns,
                                key=f"cols_{table_info['display']}"
                            )

                            for col in selected_cols:
                                alias = st.text_input(
                                    f"Alias for {col}",
                                    key=f"alias_{table_info['display']}_{col}"
                                )
                                col_selections.append({
                                    'table': table_info['table'],
                                    'schema': table_info['schema'],
                                    'column': col,
                                    'alias': alias if alias else None
                                })

                # Apply column selection
                if col_selections and st.button("Apply Column Selection", type="primary"):
                    join_builder.select(col_selections)
                    st.success("✅ Columns selected")

                st.markdown("---")
                st.subheader("4. Query Preview")

                # Show join configuration
                with st.expander("Join Configuration"):
                    preview = join_builder.preview()
                    # Convert any non-serializable values
                    preview_clean = {
                        'join_path': preview['join_path'],
                        'selected_columns': preview['selected_columns'],
                        'conditions': preview['conditions'],
                        'temp_tables': [str(t) for t in preview['temp_tables']],
                        'ctes': preview['ctes']
                    }
                    st.json(preview_clean)

                # Build and display query
                try:
                    query = join_builder.build()
                    st.code(query, language="sql")

                    # Save to history
                    if st.button("📋 Copy to History"):
                        st.session_state.query_history.append(query)
                        st.success("Query saved to history")

                    # Create temporary table option
                    st.markdown("---")
                    st.subheader("5. Save as Temporary Table")

                    temp_name = st.text_input(
                        "Temporary table name",
                        value="temp_result"
                    )

                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("💾 Create Temporary Table"):
                            temp_table = join_builder.create_temp_table(temp_name)
                            st.session_state.temp_manager.create_temp_table(temp_name, query)
                            st.success(f"✅ Created temporary table: {temp_name}")

                    with col2:
                        if st.button("📥 Preview Data (first 10 rows)"):
                            try:
                                # Execute query and show preview
                                df = pd.read_sql_query(f"{query} LIMIT 10", st.session_state.connection)
                                st.dataframe(df, use_container_width=True)
                            except Exception as e:
                                st.error(f"Error previewing data: {e}")

                except Exception as e:
                    st.error(f"Error building query: {e}")

with tab2:
    st.header("Single Table Query Generator")

    if not st.session_state.schema_infos:
        st.warning("Please load a schema first")
    else:
        col1, col2 = st.columns([1, 2])

        with col1:
            st.subheader("Select Table")

            schema_name = st.selectbox(
                "Schema",
                options=list(st.session_state.schema_infos.keys()),
                key="query_schema"
            )

            db_info = st.session_state.schema_infos[schema_name]

            tables = db_info.get_tables()
            # Filter out None values
            tables = [t for t in tables if t and pd.notna(t)]

            if tables:
                table_name = st.selectbox(
                    "Table",
                    options=tables,
                    key="query_table"
                )

                if table_name:
                    # Initialize query generator
                    query_gen = QueryGenerator(table_name, schema_name)

                    st.subheader("Select Columns")

                    columns = db_info.get_columns(table_name)
                    col_names = [c['column_name'] for c in columns if c['column_name'] and pd.notna(c['column_name'])]

                    select_type = st.radio(
                        "Selection type",
                        ["All columns", "Specific columns", "Aggregations"]
                    )

                    if select_type == "All columns":
                        query_gen.select_all()
                        selected_cols = ['*']

                    elif select_type == "Specific columns":
                        selected_cols = st.multiselect(
                            "Choose columns",
                            options=col_names
                        )
                        if selected_cols:
                            query_gen.select(selected_cols)

                    else:  # Aggregations
                        if col_names:
                            agg_col = st.selectbox("Column for aggregation", col_names)
                            agg_func = st.selectbox(
                                "Aggregation function",
                                ["COUNT", "SUM", "AVG", "MIN", "MAX"]
                            )

                            if agg_func == "COUNT":
                                query_gen.count(agg_col)
                            elif agg_func == "SUM":
                                query_gen.sum(agg_col)
                            elif agg_func == "AVG":
                                query_gen.avg(agg_col)
                            elif agg_func == "MIN":
                                query_gen.min(agg_col)
                            elif agg_func == "MAX":
                                query_gen.max(agg_col)

                            selected_cols = [f"{agg_func}({agg_col})"]
                        else:
                            st.warning("No columns available")
                            selected_cols = []

        with col2:
            if table_name and 'query_gen' in locals():
                st.subheader("Query Conditions")

                # WHERE conditions
                with st.expander("WHERE Conditions", expanded=True):
                    add_where = st.checkbox("Add WHERE condition")

                    if add_where and col_names:
                        where_col = st.selectbox(
                            "Column",
                            options=col_names,
                            key="where_col"
                        )

                        where_op = st.selectbox(
                            "Operator",
                            options=["=", "!=", ">", ">=", "<", "<=", "LIKE", "IN"],
                            key="where_op"
                        )

                        where_val = st.text_input(
                            "Value (comma-separated for IN)",
                            key="where_val"
                        )

                        if st.button("Apply WHERE"):
                            if where_op == "IN":
                                values = [v.strip() for v in where_val.split(',')]
                                query_gen.where(where_col, where_op, values)
                            else:
                                query_gen.where(where_col, where_op, where_val)
                            st.success("WHERE condition added")

                # GROUP BY
                with st.expander("GROUP BY"):
                    add_group = st.checkbox("Add GROUP BY")

                    if add_group and col_names:
                        group_cols = st.multiselect(
                            "Group by columns",
                            options=col_names,
                            key="group_cols"
                        )

                        if group_cols and st.button("Apply GROUP BY"):
                            query_gen.group_by(group_cols)
                            st.success("GROUP BY added")

                # ORDER BY
                with st.expander("ORDER BY"):
                    add_order = st.checkbox("Add ORDER BY")

                    if add_order and col_names:
                        order_col = st.selectbox(
                            "Order by column",
                            options=col_names,
                            key="order_col"
                        )

                        order_dir = st.radio(
                            "Direction",
                            ["ASC", "DESC"],
                            horizontal=True,
                            key="order_dir"
                        )

                        if st.button("Apply ORDER BY"):
                            query_gen.order_by(order_col, order_dir)
                            st.success("ORDER BY added")

                # LIMIT
                with st.expander("LIMIT"):
                    add_limit = st.checkbox("Add LIMIT")

                    if add_limit:
                        limit_val = st.number_input(
                            "Limit",
                            min_value=1,
                            value=10,
                            key="limit_val"
                        )

                        if st.button("Apply LIMIT"):
                            query_gen.limit(limit_val)
                            st.success(f"LIMIT {limit_val} added")

                st.markdown("---")
                st.subheader("Generated Query")

                try:
                    query = query_gen.build()
                    st.code(query, language="sql")

                    # Metadata
                    with st.expander("Query Metadata"):
                        st.json(query_gen.get_metadata())

                    # Save to history
                    if st.button("💾 Save to History"):
                        st.session_state.query_history.append(query)
                        st.success("Query saved to history")

                except Exception as e:
                    st.error(f"Error building query: {e}")

with tab3:
    st.header("CTE Architecture Builder")

    st.markdown("""
    Build complex queries using Common Table Expressions (CTEs).
    CTEs allow you to create temporary result sets that can be referenced within a query.
    """)

    if not st.session_state.schema_infos:
        st.warning("Please load a schema first")
    else:
        # Initialize CTE builder in session state
        if 'cte_queries' not in st.session_state:
            st.session_state.cte_queries = []

        col1, col2 = st.columns([1, 1])

        with col1:
            st.subheader("Create CTE")

            cte_name = st.text_input("CTE Name", value="cte_1")

            # Table selection for CTE
            schema_name = st.selectbox(
                "Schema for CTE",
                options=list(st.session_state.schema_infos.keys()),
                key="cte_schema"
            )

            db_info = st.session_state.schema_infos[schema_name]

            tables = db_info.get_tables()
            tables = [t for t in tables if t and pd.notna(t)]

            if tables:
                table_name = st.selectbox(
                    "Base table for CTE",
                    options=tables,
                    key="cte_table"
                )

                if table_name:
                    # Create query generator for CTE
                    cte_query_gen = QueryGenerator(table_name, schema_name)

                    # Simple CTE configuration
                    columns = db_info.get_column_names(table_name)
                    columns = [col for col in columns if col and pd.notna(col)]

                    if columns:
                        select_cols = st.multiselect(
                            "Select columns for CTE",
                            options=columns,
                            default=columns[:3] if columns else []
                        )

                        if select_cols:
                            cte_query_gen.select(select_cols)

                            # Optional WHERE clause
                            add_where_cte = st.checkbox("Add WHERE clause to CTE")

                            if add_where_cte and select_cols:
                                where_col = st.selectbox(
                                    "Column for WHERE",
                                    options=select_cols,
                                    key="cte_where_col"
                                )

                                where_val = st.text_input("Value", key="cte_where_val")

                                if where_val:
                                    cte_query_gen.where(where_col, "=", where_val)

                            if st.button("➕ Add CTE"):
                                st.session_state.cte_queries.append({
                                    'name': cte_name,
                                    'query_gen': cte_query_gen
                                })
                                st.success(f"CTE '{cte_name}' added")

        with col2:
            st.subheader("CTE List")

            if st.session_state.cte_queries:
                for i, cte in enumerate(st.session_state.cte_queries):
                    with st.expander(f"📄 {cte['name']}"):
                        st.code(cte['query_gen'].build(), language="sql")

                        if st.button("Remove", key=f"remove_cte_{i}"):
                            st.session_state.cte_queries.pop(i)
                            st.rerun()
            else:
                st.info("No CTEs created yet")

        # Main query builder
        if st.session_state.cte_queries:
            st.markdown("---")
            st.subheader("Build Main Query with CTEs")

            # Table selection for main query
            schema_name_main = st.selectbox(
                "Schema for main query",
                options=list(st.session_state.schema_infos.keys()),
                key="main_schema"
            )

            db_info_main = st.session_state.schema_infos[schema_name_main]

            tables_main = db_info_main.get_tables()
            tables_main = [t for t in tables_main if t and pd.notna(t)]

            if tables_main:
                table_name_main = st.selectbox(
                    "Main table",
                    options=tables_main,
                    key="main_table"
                )

                if table_name_main:
                    main_query_gen = QueryGenerator(table_name_main, schema_name_main)

                    # Add all CTEs to main query
                    for cte in st.session_state.cte_queries:
                        main_query_gen.with_cte(cte['name'], cte['query_gen'])

                    # Simple SELECT for main query
                    columns_main = db_info_main.get_column_names(table_name_main)
                    columns_main = [col for col in columns_main if col and pd.notna(col)]

                    if columns_main:
                        select_main = st.multiselect(
                            "Select columns for main query",
                            options=columns_main,
                            default=columns_main[:3] if columns_main else []
                        )

                        if select_main:
                            main_query_gen.select(select_main)

                            if st.button("🚀 Build Final Query"):
                                final_query = main_query_gen.build()

                                st.markdown("---")
                                st.subheader("Final Query with CTEs")
                                st.code(final_query, language="sql")

                                # Save to history
                                if st.button("💾 Save Final Query to History"):
                                    st.session_state.query_history.append(final_query)
                                    st.success("Final query saved to history")

with tab4:
    st.header("Schema Visualization")

    if not st.session_state.schema_infos:
        st.warning("Please load a schema to visualize")
    else:
        # Schema selector
        viz_schema = st.selectbox(
            "Select schema to visualize",
            options=list(st.session_state.schema_infos.keys())
        )

        if viz_schema:
            db_info = st.session_state.schema_infos[viz_schema]

            # Get all relationships
            relationships = db_info.get_all_relationships()

            # Filter out relationships with None values
            valid_relationships = []
            for rel in relationships:
                if (rel.get('from_table') and rel.get('to_table') and
                        pd.notna(rel.get('from_table')) and pd.notna(rel.get('to_table')) and
                        rel.get('from_column') and pd.notna(rel.get('from_column'))):
                    valid_relationships.append(rel)

            if valid_relationships and NETWORKX_AVAILABLE:
                try:
                    # Create network graph
                    G = nx.DiGraph()

                    # Add nodes (tables)
                    tables = db_info.get_tables()
                    for table in tables:
                        if table and pd.notna(table):
                            G.add_node(str(table))

                    # Add edges (relationships)
                    for rel in valid_relationships:
                        from_table = str(rel['from_table'])
                        to_table = str(rel['to_table'])
                        if from_table in G.nodes and to_table in G.nodes:
                            G.add_edge(
                                from_table,
                                to_table,
                                label=f"{rel['from_column']} → {rel.get('to_column', '?')}"
                            )

                    if len(G.nodes()) > 0 and len(G.edges()) > 0:
                        # Create position layout
                        pos = nx.spring_layout(G)

                        # Create edge trace
                        edge_trace = []
                        for edge in G.edges(data=True):
                            if edge[0] in pos and edge[1] in pos:
                                x0, y0 = pos[edge[0]]
                                x1, y1 = pos[edge[1]]

                                edge_trace.append(
                                    go.Scatter(
                                        x=[x0, x1, None],
                                        y=[y0, y1, None],
                                        mode='lines',
                                        line=dict(width=1, color='#888'),
                                        hoverinfo='none'
                                    )
                                )

                        # Create node trace
                        node_x = []
                        node_y = []
                        node_text = []

                        for node in G.nodes():
                            if node in pos:
                                x, y = pos[node]
                                node_x.append(x)
                                node_y.append(y)
                                node_text.append(node)

                        node_trace = go.Scatter(
                            x=node_x,
                            y=node_y,
                            mode='markers+text',
                            text=node_text,
                            textposition="top center",
                            hoverinfo='text',
                            marker=dict(
                                size=20,
                                color='lightblue',
                                line=dict(color='darkblue', width=2)
                            )
                        )

                        # Create figure
                        fig = go.Figure(data=[node_trace] + edge_trace,
                                        layout=go.Layout(
                                            title='Table Relationships',
                                            showlegend=False,
                                            hovermode='closest',
                                            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                            height=600
                                        ))

                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No valid relationships to display in the graph")
                except Exception as e:
                    st.error(f"Error creating graph: {e}")
            elif not NETWORKX_AVAILABLE:
                st.warning("NetworkX not installed. Install with: pip install networkx")

            # Show relationship table
            if valid_relationships:
                st.subheader("Relationship Details")

                rel_data = []
                for rel in valid_relationships[:20]:  # Limit to 20 for display
                    rel_data.append({
                        'From Table': rel['from_table'],
                        'From Column': rel['from_column'],
                        'To Table': rel['to_table'],
                        'To Column': rel.get('to_column', 'N/A')
                    })

                st.dataframe(pd.DataFrame(rel_data), use_container_width=True)
            else:
                st.info("No valid relationships found in this schema")

            # Table statistics
            st.subheader("Table Statistics")

            stats_data = []
            for table in db_info.get_tables()[:10]:  # Limit to 10 tables
                if table and pd.notna(table):
                    columns = db_info.get_columns(table)
                    pk_count = len([c for c in columns if c.get('is_primary_key', False)])
                    fk_count = len([c for c in columns if c.get('is_foreign_key', False)])

                    stats_data.append({
                        'Table': table,
                        'Columns': len(columns),
                        'Primary Keys': pk_count,
                        'Foreign Keys': fk_count
                    })

            if stats_data:
                st.dataframe(pd.DataFrame(stats_data), use_container_width=True)

# Footer
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Loaded Schemas", len(st.session_state.schema_infos))
with col2:
    st.metric("Temporary Tables", len(st.session_state.temp_manager.list_temp_tables()))
with col3:
    st.metric("Queries in History", len(st.session_state.query_history))

st.markdown("Built with ❤️ using Streamlit, PyPika, and Plotly")