import streamlit as st
import pandas as pd
from db_information import *
from join_builder import *

# Page config
st.set_page_config(page_title="DB Schema Tester", layout="wide")

# Title
st.title("🔍 Database Schema Explorer & Join Tester")

# Initialize session state
if 'schema_df' not in st.session_state:
    st.session_state.schema_df = None
if 'selected_tables' not in st.session_state:
    st.session_state.selected_tables = []
if 'join_info' not in st.session_state:
    st.session_state.join_info = None

# Sidebar for file upload
with st.sidebar:
    st.header("📁 Load Schema")
    uploaded_file = st.file_uploader("Upload schema CSV", type=['csv'])

    if uploaded_file is not None:
        st.session_state.schema_df = pd.read_csv(uploaded_file)
        st.success(f"✅ Loaded {len(st.session_state.schema_df)} rows")

        # Show preview
        with st.expander("Preview Schema"):
            st.dataframe(st.session_state.schema_df.head())

# Main content
if st.session_state.schema_df is not None:
    schema_df = st.session_state.schema_df

    # Get all unique table names
    all_tables = schema_df['table_name'].unique().tolist()

    # Create two columns for table selection
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Select Tables to Explore")

        # Table selector
        selected_table = st.selectbox(
            "Choose a table to view details",
            options=all_tables,
            key="table_selector"
        )

        if selected_table:
            # Create DBInfo object for selected table
            table_info = DBInfo(schema_df, selected_table)

            # Display table information
            st.markdown("---")
            st.subheader(f"📋 Table: {selected_table}")

            # Columns with PK/FK indicators
            col_data = []
            for col in table_info.get_columns():
                is_pk = col['column_name'] in table_info.get_primary_keys()
                is_fk = any(fk['column'] == col['column_name']
                            for fk in table_info.get_foreign_keys())

                status = []
                if is_pk:
                    status.append("🔑 PK")
                if is_fk:
                    status.append("🔗 FK")

                col_data.append({
                    "Column": col['column_name'],
                    "Type": col['data_type'],
                    "Keys": ", ".join(status) if status else "─"
                })

            st.dataframe(
                pd.DataFrame(col_data),
                use_container_width=True,
                hide_index=True
            )

            # Foreign Keys details
            fks = table_info.get_foreign_keys()
            if fks:
                with st.expander("🔗 Foreign Key Relationships"):
                    fk_data = []
                    for fk in fks:
                        fk_data.append({
                            "Column": fk['column'],
                            "References": f"{fk['references_table']}.{fk['references_column']}"
                        })
                    st.dataframe(pd.DataFrame(fk_data), hide_index=True)

            # Parent/Child tables
            col_parent, col_child = st.columns(2)
            with col_parent:
                parents = table_info.get_parent_tables()
                if parents:
                    st.info(f"⬆️ **Parent Tables:** {', '.join(parents)}")

            with col_child:
                children = table_info.get_child_tables()
                if children:
                    st.info(f"⬇️ **Child Tables:** {', '.join(children)}")

    with col2:
        st.subheader("🔗 Join Builder")

        # Multi-table selector for joins
        st.markdown("Select tables to join (2 or more):")

        # Table checkboxes for join selection
        join_tables = []
        for table in all_tables[:10]:  # Limit to 10 for UI
            if st.checkbox(table, key=f"join_{table}"):
                join_tables.append(table)

        if len(join_tables) >= 2:
            st.session_state.selected_tables = join_tables

            # Join configuration
            st.markdown("---")
            st.markdown("**Configure Join**")

            # Table selection for join
            join_from = st.selectbox(
                "From table",
                options=join_tables,
                key="join_from"
            )

            join_to = st.selectbox(
                "To table",
                options=[t for t in join_tables if t != join_from],
                key="join_to"
            )

            # Create JoinBuilder
            join_builder = JoinBuilder(DBInfo(schema_df, join_from))

            # Auto-detect join
            if st.button("🔍 Auto-detect Join", use_container_width=True):
                join_info = join_builder.find_join_path(join_from, join_to)
                if join_info:
                    st.session_state.join_info = join_info
                    st.success("✅ Join found!")
                else:
                    st.warning("⚠️ No direct relationship found")

            # Display join info
            if st.session_state.join_info:
                st.markdown("---")
                st.subheader("🔗 Join Information")

                join_info = st.session_state.join_info

                # Show join details
                col_j1, col_j2 = st.columns(2)
                with col_j1:
                    st.info(f"**{join_info['from_table']}**")
                    st.code(f"Column: {join_info['from_column']}")

                with col_j2:
                    st.info(f"**{join_info['to_table']}**")
                    st.code(f"Column: {join_info['to_column']}")

                # Manual override
                with st.expander("✏️ Manual Join Override"):
                    st.markdown("Specify columns manually:")

                    manual_col1 = st.selectbox(
                        f"Column from {join_from}",
                        options=[c['column_name'] for c in DBInfo(schema_df, join_from).get_columns()]
                    )

                    manual_col2 = st.selectbox(
                        f"Column from {join_to}",
                        options=[c['column_name'] for c in DBInfo(schema_df, join_to).get_columns()]
                    )

                    if st.button("Apply Manual Join"):
                        st.session_state.join_info = join_builder.manual_join(
                            join_from, join_to, manual_col1, manual_col2
                        )
                        st.rerun()

                # Build query button
                if st.button("🔨 Build Join Query", type="primary", use_container_width=True):
                    try:
                        # Build sample query
                        query = join_builder.build_join_query(
                            join_from,
                            join_to
                        )
                        st.code(query, language="sql")

                        # Show join visualization
                        st.markdown("**Join Visualization:**")

                        # Create a visual representation of the join
                        viz_html = f"""
                        <div style="background-color: #f0f2f6; padding: 15px; border-radius: 5px; font-family: monospace;">
                            <div style="display: flex; align-items: center; justify-content: center; gap: 20px;">
                                <div style="background-color: #4CAF50; color: white; padding: 10px; border-radius: 5px;">
                                    {join_from}<br>
                                    <small>{join_info['from_column']}</small>
                                </div>
                                <div style="font-size: 24px;">→</div>
                                <div style="background-color: #2196F3; color: white; padding: 10px; border-radius: 5px;">
                                    {join_to}<br>
                                    <small>{join_info['to_column']}</small>
                                </div>
                            </div>
                        </div>
                        """
                        st.markdown(viz_html, unsafe_allow_html=True)

                    except Exception as e:
                        st.error(f"Error building query: {e}")

        elif len(join_tables) > 0:
            st.info(f"Select {2 - len(join_tables)} more table(s) to enable joins")

    # Bottom section for exploring relationships
    st.markdown("---")
    st.subheader("🕸️ Table Relationships")

    # Create a simple relationship visualization
    rel_data = []
    for table in all_tables[:5]:  # Limit to first 5 tables for demo
        table_info = DBInfo(schema_df, table)
        for fk in table_info.get_foreign_keys():
            rel_data.append({
                "From Table": table,
                "From Column": fk['column'],
                "To Table": fk['references_table'],
                "To Column": fk['references_column']
            })

    if rel_data:
        st.dataframe(pd.DataFrame(rel_data), use_container_width=True, hide_index=True)

        # Simple network visualization
        with st.expander("📊 Relationship Graph"):
            # Create a simple text-based graph
            graph_text = "```\n"
            for rel in rel_data[:10]:  # Limit to 10 for readability
                graph_text += f"{rel['From Table']}.{rel['From Column']} → {rel['To Table']}.{rel['To Column']}\n"
            graph_text += "```"
            st.markdown(graph_text)
    else:
        st.info("No relationships found in the schema")

else:
    # Welcome message when no file is uploaded
    st.info("👈 Please upload a schema CSV file to get started")

    # Show expected format
    with st.expander("📋 Expected CSV Format"):
        st.markdown("""
        Your CSV should contain these columns:
        - **table_name**: Name of the table
        - **column_name**: Name of the column
        - **data_type**: Data type of the column
        - **is_primary_key**: True/False
        - **is_foreign_key**: True/False
        - **parent_table**: For FKs, the referenced table
        - **parent_column**: For FKs, the referenced column

        Example:""")

# Footer
st.markdown("---")
st.markdown("Built with ❤️ using Streamlit")