import pandas as pd


class DBInfo:
    """
    A class to extract database schema information from a pandas DataFrame

    Attributes:
        schema_df (pd.DataFrame): DataFrame containing the database schema
        table_name (str): Name of the table to analyze
    """

    def __init__(self, schema_df, table_name):
        """
        Initialize DBInfo with schema DataFrame and table name

        Args:
            schema_df (pd.DataFrame): DataFrame with columns: table_name, column_name,
                                     data_type, is_primary_key, is_foreign_key,
                                     parent_table, parent_column
            table_name (str): Name of the table to analyze
        """
        self.schema_df = schema_df
        self.table_name = table_name
        self._table_data = None  # Will be populated when needed

    def _get_table_data(self):
        """Lazy load table data"""
        if self._table_data is None:
            self._table_data = self.schema_df[self.schema_df["table_name"] == self.table_name]
        return self._table_data

    def get_columns(self):
        """
        Extract column names with their data types

        Returns:
            list: List of dictionaries with column_name and data_type
        """
        table_data = self._get_table_data()
        return table_data[["column_name", "data_type"]].to_dict(orient="records")

    def get_primary_keys(self):
        """
        Extract primary key columns

        Returns:
            list: List of primary key column names
        """
        table_data = self._get_table_data()
        pk_data = table_data[table_data["is_primary_key"] == True]
        return pk_data["column_name"].tolist()

    def get_foreign_keys(self):
        """
        Extract foreign key details

        Returns:
            list: List of dictionaries with column, references_table, references_column
        """
        table_data = self._get_table_data()
        fk_data = table_data[table_data["is_foreign_key"] == True]

        foreign_keys = []
        for _, row in fk_data.iterrows():
            foreign_keys.append({
                "column": row["column_name"],
                "references_table": row["parent_table"],
                "references_column": row["parent_column"]
            })

        return foreign_keys

    def get_parent_tables(self):
        """
        Find all tables that this table references (parents)

        Returns:
            list: List of parent table names
        """
        foreign_keys = self.get_foreign_keys()
        parents = list(set([fk["references_table"] for fk in foreign_keys if fk["references_table"]]))
        return parents

    def get_child_tables(self):
        """
        Find all tables that reference this table (children)

        Returns:
            list: List of child table names
        """
        child_data = self.schema_df[
            (self.schema_df["is_foreign_key"] == True) &
            (self.schema_df["parent_table"] == self.table_name)
            ]
        return child_data["table_name"].unique().tolist()

    def get_all_info(self):
        """
        Get complete information about the table in one call

        Returns:
            dict: Dictionary containing all table information
        """
        return {
            "table_name": self.table_name,
            "columns": self.get_columns(),
            "primary_keys": self.get_primary_keys(),
            "foreign_keys": self.get_foreign_keys(),
            "parent_tables": self.get_parent_tables(),
            "child_tables": self.get_child_tables()
        }

    def display_info(self):
        """
        Pretty print the table information
        """
        info = self.get_all_info()

        print(f"\n{'=' * 60}")
        print(f"📋 TABLE: {info['table_name']}")
        print(f"{'=' * 60}")

        print(f"\n📊 COLUMNS ({len(info['columns'])}):")
        for col in info['columns']:
            pk_marker = "🔑" if col['column_name'] in info['primary_keys'] else "  "
            print(f"   {pk_marker} {col['column_name']} ({col['data_type']})")

        if info['primary_keys']:
            print(f"\n🔑 PRIMARY KEYS:")
            for pk in info['primary_keys']:
                print(f"   • {pk}")

        if info['foreign_keys']:
            print(f"\n🔗 FOREIGN KEYS (references to parent tables):")
            for fk in info['foreign_keys']:
                print(f"   • {fk['column']} -> {fk['references_table']}.{fk['references_column']}")

        if info['parent_tables']:
            print(f"\n⬆️  PARENT TABLES (tables this depends on):")
            for parent in info['parent_tables']:
                print(f"   • {parent}")

        if info['child_tables']:
            print(f"\n⬇️  CHILD TABLES (tables that depend on this):")
            for child in info['child_tables']:
                print(f"   • {child}")

# example use case

# Load your schema
df = pd.read_csv("db_files/extracted_gm_schema.csv")

# Create DBInfo object for a specific table
table_info = DBInfo(df, "public.gmhk_appointment")

# Get specific information
columns = table_info.get_columns()
primary_keys = table_info.get_primary_keys()
foreign_keys = table_info.get_foreign_keys()
parents = table_info.get_parent_tables()
children = table_info.get_child_tables()

# Get everything at once
all_info = table_info.get_all_info()

# Pretty print everything
table_info.display_info()