import streamlit as st
import pandas as pd
from db_information import DBInfo
from pypika_query_engine import  SQLGeneratorFactory, PypikaSQLGenerator
import plotly.graph_objects as go
import networkx as nx
from datetime import datetime
import json

# Page config
st.set_page_config(
    page_title="SQL Query Builder - Schema Explorer",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E88E5;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #0D47A1;
        margin-top: 1rem;
        margin-bottom: 1rem;
    }
    .table-card {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 0.5rem;
        padding: 1rem;
        margin-bottom: 1rem;
    }
    .table-header {
        font-size: 1.2rem;
        font-weight: 600;
        color: #1E88E5;
        margin-bottom: 0.5rem;
    }
    .column-chip {
        background-color: #e3f2fd;
        padding: 0.25rem 0.5rem;
        border-radius: 1rem;
        display: inline-block;
        margin: 0.25rem;
        font-size: 0.9rem;
        cursor: pointer;
    }
    .column-chip:hover {
        background-color: #bbdefb;
    }
    .column-chip.selected {
        background-color: #1E88E5;
        color: white;
    }
    .pk-badge {
        background-color: #ffd700;
        color: #000;
        padding: 0.2rem 0.5rem;
        border-radius: 0.25rem;
        font-size: 0.8rem;
        font-weight: 600;
    }
    .fk-badge {
        background-color: #90caf9;
        color: #000;
        padding: 0.2rem 0.5rem;
        border-radius: 0.25rem;
        font-size: 0.8rem;
        font-weight: 600;
    }
    .join-indicator {
        background-color: #4caf50;
        color: white;
        padding: 0.2rem 0.5rem;
        border-radius: 0.25rem;
        font-size: 0.8rem;
        margin-left: 0.5rem;
    }
    .query-card {
        background-color: #1e1e1e;
        color: #d4d4d4;
        padding: 1rem;
        border-radius: 0.5rem;
        font-family: monospace;
        overflow-x: auto;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize session state
if 'schemas' not in st.session_state:
    st.session_state.schemas = {}
if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = None
if 'selected_tables' not in st.session_state:
    st.session_state.selected_tables = []  # Now a list for multiple tables
if 'db_info_objects' not in st.session_state:
    st.session_state.db_info_objects = {}  # Dict of table_name -> DBInfo
if 'sql_generators' not in st.session_state:
    st.session_state.sql_generators = {}  # Dict of table_name -> SQLGenerator
if 'selected_columns' not in st.session_state:
    st.session_state.selected_columns = {}  # Dict of table_name -> list of columns
if 'table_joins' not in st.session_state:
    st.session_state.table_joins = []  # List of join configurations
if 'join_conditions' not in st.session_state:
    st.session_state.join_conditions = {}  # Dict of (table1,table2) -> condition
if 'where_conditions' not in st.session_state:
    st.session_state.where_conditions = []  # List of where conditions
if 'group_by_cols' not in st.session_state:
    st.session_state.group_by_cols = []
if 'order_by_cols' not in st.session_state:
    st.session_state.order_by_cols = []
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'generated_query' not in st.session_state:
    st.session_state.generated_query = ""

# Header
st.markdown('<p class="main-header">🔍 SQL Query Builder - Multi-Table Schema Explorer</p>', unsafe_allow_html=True)
st.markdown(
    "Select multiple tables, choose columns, and build complex SQL queries with automatic relationship detection")

# Create two main columns
left_col, right_col = st.columns([1, 1.5])

with left_col:
    st.markdown('<p class="sub-header">📁 Schema & Table Selection</p>', unsafe_allow_html=True)

    # File uploader
    uploaded_files = st.file_uploader(
        "Upload Schema CSV Files",
        type=['csv'],
        accept_multiple_files=True
    )

    # Process uploaded files
    if uploaded_files:
        for uploaded_file in uploaded_files:
            if uploaded_file.name not in st.session_state.schemas:
                try:
                    df = pd.read_csv(uploaded_file)

                    # Validate required columns
                    required_cols = ['table_name', 'column_name', 'data_type', 'is_primary_key',
                                     'is_foreign_key', 'parent_table', 'parent_column']
                    missing_cols = [col for col in required_cols if col not in df.columns]

                    if missing_cols:
                        st.error(f"Missing columns in {uploaded_file.name}: {missing_cols}")
                    else:
                        st.session_state.schemas[uploaded_file.name] = {
                            'dataframe': df,
                            'tables': sorted(df['table_name'].unique().tolist()),
                            'upload_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'row_count': len(df)
                        }
                except Exception as e:
                    st.error(f"Error loading {uploaded_file.name}: {str(e)}")

    # Schema selection
    if st.session_state.schemas:
        schema_options = list(st.session_state.schemas.keys())
        selected_schema = st.selectbox(
            "📁 Select Schema",
            options=schema_options,
            index=0 if schema_options else None
        )

        if selected_schema != st.session_state.selected_schema:
            st.session_state.selected_schema = selected_schema
            st.session_state.selected_tables = []
            st.session_state.db_info_objects = {}
            st.session_state.sql_generators = {}
            st.session_state.selected_columns = {}
            st.session_state.table_joins = []
            st.rerun()

        if st.session_state.selected_schema:
            schema_data = st.session_state.schemas[st.session_state.selected_schema]

            # Table multi-select
            st.markdown("### 📋 Select Tables")
            selected_tables = st.multiselect(
                "Choose tables to include in your query",
                options=schema_data['tables'],
                default=st.session_state.selected_tables
            )

            # Update selected tables
            if selected_tables != st.session_state.selected_tables:
                st.session_state.selected_tables = selected_tables

                # Create DBInfo and SQL Generator for new tables
                for table in selected_tables:
                    if table not in st.session_state.db_info_objects:
                        db_info = DBInfo(schema_data['dataframe'], table)
                        st.session_state.db_info_objects[table] = db_info
                        sql_gen = SQLGeneratorFactory.create_generator(db_info, backend='pypika')
                        st.session_state.sql_generators[table] = sql_gen

                        # Initialize selected columns for this table
                        if table not in st.session_state.selected_columns:
                            st.session_state.selected_columns[table] = []

                # Remove objects for deselected tables
                for table in list(st.session_state.db_info_objects.keys()):
                    if table not in selected_tables:
                        del st.session_state.db_info_objects[table]
                        del st.session_state.sql_generators[table]
                        if table in st.session_state.selected_columns:
                            del st.session_state.selected_columns[table]

                st.rerun()

            # Quick action buttons
            if st.session_state.selected_tables:
                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button("Select All Columns", use_container_width=True):
                        for table in st.session_state.selected_tables:
                            db_info = st.session_state.db_info_objects[table]
                            all_columns = [col['column_name'] for col in db_info.get_columns()]
                            st.session_state.selected_columns[table] = all_columns
                        st.rerun()

                with col2:
                    if st.button("Clear All Columns", use_container_width=True):
                        for table in st.session_state.selected_tables:
                            st.session_state.selected_columns[table] = []
                        st.rerun()

                with col3:
                    if st.button("Auto-Join All", use_container_width=True):
                        # Automatically create joins based on relationships
                        st.session_state.table_joins = []
                        for i, table1 in enumerate(st.session_state.selected_tables):
                            for table2 in st.session_state.selected_tables[i + 1:]:
                                # Check if there's a relationship
                                db_info1 = st.session_state.db_info_objects[table1]
                                all_info1 = db_info1.get_all_info()

                                # Check if table1 has FK to table2
                                for fk in all_info1['foreign_keys']:
                                    if fk['references_table'] == table2:
                                        st.session_state.table_joins.append({
                                            'from_table': table1,
                                            'to_table': table2,
                                            'from_column': fk['column'],
                                            'to_column': fk['references_column'],
                                            'join_type': 'INNER'
                                        })

                                # Check if table2 has FK to table1
                                db_info2 = st.session_state.db_info_objects[table2]
                                all_info2 = db_info2.get_all_info()
                                for fk in all_info2['foreign_keys']:
                                    if fk['references_table'] == table1:
                                        st.session_state.table_joins.append({
                                            'from_table': table2,
                                            'to_table': table1,
                                            'from_column': fk['column'],
                                            'to_column': fk['references_column'],
                                            'join_type': 'INNER'
                                        })
                        st.rerun()

# Right Column - Multi-Table Column Selection
with right_col:
    if st.session_state.selected_tables:
        st.markdown('<p class="sub-header">📊 Column Selection by Table</p>', unsafe_allow_html=True)

        # Create tabs for each selected table
        table_tabs = st.tabs([f"📋 {table}" for table in st.session_state.selected_tables])

        for i, table in enumerate(st.session_state.selected_tables):
            with table_tabs[i]:
                db_info = st.session_state.db_info_objects[table]
                all_info = db_info.get_all_info()

                # Table metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Columns", len(all_info['columns']))
                with col2:
                    st.metric("Primary Keys", len(all_info['primary_keys']))
                with col3:
                    st.metric("Foreign Keys", len(all_info['foreign_keys']))
                with col4:
                    rel_count = len(all_info['parent_tables']) + len(all_info['child_tables'])
                    st.metric("Relationships", rel_count)

                # Column selection
                st.markdown("#### Select Columns")

                column_options = [col['column_name'] for col in all_info['columns']]

                # Quick select buttons for this table
                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button(f"Select All", key=f"select_all_{table}", use_container_width=True):
                        st.session_state.selected_columns[table] = column_options
                        st.rerun()
                with col2:
                    if st.button(f"Clear All", key=f"clear_all_{table}", use_container_width=True):
                        st.session_state.selected_columns[table] = []
                        st.rerun()
                with col3:
                    if st.button(f"Select PKs", key=f"select_pks_{table}", use_container_width=True):
                        st.session_state.selected_columns[table] = all_info['primary_keys']
                        st.rerun()

                # Multi-select for columns
                selected = st.multiselect(
                    f"Columns from {table}",
                    options=column_options,
                    default=st.session_state.selected_columns.get(table, []),
                    key=f"col_select_{table}"
                )
                st.session_state.selected_columns[table] = selected

                # Display columns with badges
                st.markdown("#### Column Details")
                col_data = []
                for col in all_info['columns']:
                    col_name = col['column_name']
                    badges = []
                    if col_name in all_info['primary_keys']:
                        badges.append('🔑 PK')
                    if col_name in [fk['column'] for fk in all_info['foreign_keys']]:
                        badges.append('🔗 FK')

                    col_data.append({
                        'Column': col_name,
                        'Type': col['data_type'],
                        'Keys': ', '.join(badges),
                        'Selected': '✅' if col_name in st.session_state.selected_columns.get(table, []) else '❌'
                    })

                col_df = pd.DataFrame(col_data)
                st.dataframe(col_df, use_container_width=True, hide_index=True)

        # Join Configuration Section
        if len(st.session_state.selected_tables) > 1:
            st.markdown('<p class="sub-header">🔗 Join Configuration</p>', unsafe_allow_html=True)

            # Display existing joins
            for i, join in enumerate(st.session_state.table_joins):
                with st.container():
                    cols = st.columns([2, 1, 2, 1, 1, 0.5])
                    with cols[0]:
                        st.markdown(f"**{join['from_table']}**")
                    with cols[1]:
                        st.markdown("→")
                    with cols[2]:
                        st.markdown(f"**{join['to_table']}**")
                    with cols[3]:
                        st.markdown(f"ON {join['from_column']} = {join['to_column']}")
                    with cols[4]:
                        join['join_type'] = st.selectbox(
                            "Type",
                            ["INNER", "LEFT", "RIGHT", "FULL"],
                            index=0,
                            key=f"join_type_{i}",
                            label_visibility="collapsed"
                        )
                    with cols[5]:
                        if st.button("❌", key=f"remove_join_{i}"):
                            st.session_state.table_joins.pop(i)
                            st.rerun()

            # Add new join
            st.markdown("#### Add New Join")

            if len(st.session_state.selected_tables) >= 2:
                col1, col2, col3, col4 = st.columns([2, 1, 2, 2])

                with col1:
                    from_table = st.selectbox(
                        "From Table",
                        options=st.session_state.selected_tables,
                        key="join_from"
                    )

                with col2:
                    st.markdown("<br>→", unsafe_allow_html=True)

                with col3:
                    to_table = st.selectbox(
                        "To Table",
                        options=[t for t in st.session_state.selected_tables if t != from_table],
                        key="join_to"
                    )

                if from_table and to_table:
                    # Get columns for both tables
                    from_db_info = st.session_state.db_info_objects[from_table]
                    to_db_info = st.session_state.db_info_objects[to_table]

                    from_columns = [col['column_name'] for col in from_db_info.get_columns()]
                    to_columns = [col['column_name'] for col in to_db_info.get_columns()]

                    col1, col2, col3, col4 = st.columns([2, 1, 2, 2])

                    with col1:
                        from_col = st.selectbox(
                            f"Column from {from_table}",
                            options=from_columns,
                            key="join_from_col"
                        )

                    with col2:
                        st.markdown("<br>=", unsafe_allow_html=True)

                    with col3:
                        to_col = st.selectbox(
                            f"Column from {to_table}",
                            options=to_columns,
                            key="join_to_col"
                        )

                    with col4:
                        join_type = st.selectbox(
                            "Join Type",
                            ["INNER", "LEFT", "RIGHT", "FULL"],
                            key="join_type_new"
                        )

                    if st.button("➕ Add Join", use_container_width=True):
                        st.session_state.table_joins.append({
                            'from_table': from_table,
                            'to_table': to_table,
                            'from_column': from_col,
                            'to_column': to_col,
                            'join_type': join_type
                        })
                        st.rerun()

        # WHERE Conditions Section
        st.markdown('<p class="sub-header">⚖️ WHERE Conditions</p>', unsafe_allow_html=True)

        # Display existing conditions
        for i, cond in enumerate(st.session_state.where_conditions):
            cols = st.columns([1.5, 1, 1.5, 1, 2, 0.5])
            with cols[0]:
                st.markdown(f"**{cond['table']}**")
            with cols[1]:
                st.markdown(f"**{cond['column']}**")
            with cols[2]:
                cond['operator'] = st.selectbox(
                    "Op",
                    ["=", "!=", ">", "<", ">=", "<=", "LIKE", "IN"],
                    index=["=", "!=", ">", "<", ">=", "<=", "LIKE", "IN"].index(cond['operator']),
                    key=f"cond_op_{i}",
                    label_visibility="collapsed"
                )
            with cols[3]:
                cond['logical_op'] = st.selectbox(
                    "Logic",
                    ["AND", "OR"],
                    index=0 if cond.get('logical_op', 'AND') == 'AND' else 1,
                    key=f"cond_logic_{i}",
                    label_visibility="collapsed"
                )
            with cols[4]:
                cond['value'] = st.text_input(
                    "Value",
                    value=cond.get('value', ''),
                    key=f"cond_val_{i}",
                    label_visibility="collapsed"
                )
            with cols[5]:
                if st.button("❌", key=f"remove_cond_{i}"):
                    st.session_state.where_conditions.pop(i)
                    st.rerun()

        # Add new condition
        st.markdown("#### Add New Condition")

        if st.session_state.selected_tables:
            col1, col2, col3, col4, col5 = st.columns([1.5, 1.5, 1, 1.5, 2])

            with col1:
                cond_table = st.selectbox(
                    "Table",
                    options=st.session_state.selected_tables,
                    key="cond_table"
                )

            if cond_table:
                db_info = st.session_state.db_info_objects[cond_table]
                columns = [col['column_name'] for col in db_info.get_columns()]

                with col2:
                    cond_column = st.selectbox(
                        "Column",
                        options=columns,
                        key="cond_column"
                    )

                with col3:
                    cond_operator = st.selectbox(
                        "Operator",
                        ["=", "!=", ">", "<", ">=", "<=", "LIKE", "IN"],
                        key="cond_operator"
                    )

                with col4:
                    cond_logical = st.selectbox(
                        "Logical",
                        ["AND", "OR"],
                        key="cond_logical"
                    )

                with col5:
                    cond_value = st.text_input(
                        "Value",
                        key="cond_value",
                        placeholder="value or comma,separated,for IN"
                    )

                if st.button("➕ Add Condition", use_container_width=True) and cond_value:
                    st.session_state.where_conditions.append({
                        'table': cond_table,
                        'column': cond_column,
                        'operator': cond_operator,
                        'value': cond_value if cond_operator != "IN" else [v.strip() for v in cond_value.split(',')],
                        'logical_op': cond_logical
                    })
                    st.rerun()

        # GROUP BY and ORDER BY
        st.markdown('<p class="sub-header">📊 GROUP BY & ORDER BY</p>', unsafe_allow_html=True)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### GROUP BY")
            all_columns = []
            column_table_map = {}
            for table in st.session_state.selected_tables:
                for col in st.session_state.selected_columns.get(table, []):
                    qualified_name = f"{table}.{col}"
                    all_columns.append(qualified_name)
                    column_table_map[qualified_name] = table

            group_by_cols = st.multiselect(
                "Select columns for GROUP BY",
                options=all_columns,
                default=st.session_state.group_by_cols
            )
            st.session_state.group_by_cols = group_by_cols

        with col2:
            st.markdown("#### ORDER BY")
            order_by_cols = st.multiselect(
                "Select columns for ORDER BY",
                options=all_columns,
                default=[c for c, _ in st.session_state.order_by_cols]
            )

            order_directions = {}
            for col in order_by_cols:
                direction = st.selectbox(
                    f"Direction for {col}",
                    ["ASC", "DESC"],
                    key=f"order_dir_{col}"
                )
                order_directions[col] = direction

            st.session_state.order_by_cols = [(col, order_directions[col]) for col in order_by_cols]

        # LIMIT
        limit_val = st.number_input("LIMIT", min_value=0, value=100, step=100)

        # Generate Query Button
        if st.button("🚀 Generate SQL Query", use_container_width=True, type="primary"):
            try:
                # Get the first table as base
                base_table = st.session_state.selected_tables[0]
                sql_gen = st.session_state.sql_generators[base_table]

                # Reset the generator
                sql_gen = SQLGeneratorFactory.create_generator(
                    st.session_state.db_info_objects[base_table],
                    backend='pypika'
                )

                # Add selected columns with table qualification
                select_columns = []
                for table in st.session_state.selected_tables:
                    for col in st.session_state.selected_columns.get(table, []):
                        select_columns.append(f"{table}.{col}")

                if select_columns:
                    sql_gen.select(*select_columns)
                else:
                    sql_gen.select('*')

                # Add JOINs
                for join in st.session_state.table_joins:
                    sql_gen.join_on(
                        join['to_table'],
                        join['from_column'],
                        join['to_column'],
                        join_type=join['join_type']
                    )

                # Add WHERE conditions
                for condition in st.session_state.where_conditions:
                    if condition.get('logical_op', 'AND') == 'AND':
                        sql_gen.where((
                            f"{condition['table']}.{condition['column']}",
                            condition['operator'],
                            condition['value']
                        ))
                    else:
                        sql_gen.or_where((
                            f"{condition['table']}.{condition['column']}",
                            condition['operator'],
                            condition['value']
                        ))

                # Add GROUP BY
                if st.session_state.group_by_cols:
                    sql_gen.group_by(*st.session_state.group_by_cols)

                # Add ORDER BY
                for col, direction in st.session_state.order_by_cols:
                    sql_gen.order_by(col, direction)

                # Add LIMIT
                if limit_val > 0:
                    sql_gen.limit(limit_val)

                # Build query
                st.session_state.generated_query = sql_gen.build()

            except Exception as e:
                st.error(f"Error generating query: {str(e)}")

        # Display generated query
        if st.session_state.generated_query:
            st.markdown("#### 📤 Generated SQL Query")
            st.markdown(f'<div class="query-card">{st.session_state.generated_query}</div>',
                        unsafe_allow_html=True)

            # Query actions
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("📋 Copy to Clipboard", use_container_width=True):
                    st.write("Query copied!")
                    st.session_state.query_history.append({
                        'timestamp': datetime.now().strftime("%H:%M:%S"),
                        'query': st.session_state.generated_query,
                        'tables': st.session_state.selected_tables
                    })

            with col2:
                if st.button("💾 Save to History", use_container_width=True):
                    st.session_state.query_history.append({
                        'timestamp': datetime.now().strftime("%H:%M:%S"),
                        'query': st.session_state.generated_query,
                        'tables': st.session_state.selected_tables
                    })
                    st.success("Query saved!")

            with col3:
                if st.button("🔄 Reset Query", use_container_width=True):
                    st.session_state.generated_query = ""
                    st.rerun()

        # Query History
        if st.session_state.query_history:
            with st.expander("📜 Query History"):
                for q in st.session_state.query_history[-5:]:
                    st.markdown(f"**{q['timestamp']}** - Tables: {', '.join(q['tables'])}")
                    st.code(q['query'], language="sql")

    else:
        st.info("👈 Select at least one table from the left panel to start building queries")

# Footer
st.markdown("---")
st.markdown("""
    <div style="text-align: center; color: #666; padding: 1rem;">
        SQL Query Builder v3.0 | Multi-Table Support | Select columns from multiple tables | Automatic JOIN detection
    </div>
""", unsafe_allow_html=True)