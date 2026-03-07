from db_information import *

class JoinBuilder:
    def __init__(self, db_info):
        self.db_info = db_info
        self.schema = db_info.schema_df

    def find_join_path(self, table1, table2):
        """
        Automatically find a join path between two tables
        Returns join information if tables are directly related
        """
        # Check if table1 references table2 (table1 -> table2)
        t1_info = DBInfo(self.schema, table1)
        for fk in t1_info.get_foreign_keys():
            if fk['references_table'] == table2:
                return {
                    "from_table": table1,
                    "to_table": table2,
                    "from_column": fk['column'],
                    "to_column": fk['references_column'],
                    "join_type": "left_to_right"
                }

        # Check if table2 references table1 (table2 -> table1)
        t2_info = DBInfo(self.schema, table2)
        for fk in t2_info.get_foreign_keys():
            if fk['references_table'] == table1:
                return {
                    "from_table": table1,
                    "to_table": table2,
                    "from_column": fk['references_column'],
                    "to_column": fk['column'],
                    "join_type": "right_to_left"
                }

        return None  # No direct relationship found

    def build_join_query(self, main_table, join_table, columns=None):
        """
        Build a join query between two tables
        """
        from pypika import Table, Query

        join_info = self.find_join_path(main_table, join_table)
        if not join_info:
            raise ValueError(f"No direct relationship found between {main_table} and {join_table}")

        # Create tables
        t1 = Table(main_table)
        t2 = Table(join_table)

        # Build query with join
        query = Query.from_(t1)

        if join_info['join_type'] == "left_to_right":
            query = query.join(t2).on(
                getattr(t1, join_info['from_column']) == getattr(t2, join_info['to_column'])
            )
        else:
            query = query.join(t2).on(
                getattr(t1, join_info['from_column']) == getattr(t2, join_info['to_column'])
            )

        # Select columns if specified
        if columns:
            fields = []
            for table, col in columns:
                if table == main_table:
                    fields.append(getattr(t1, col))
                else:
                    fields.append(getattr(t2, col))
            query = query.select(*fields)

        return str(query)

    def manual_join(self, table1, table2, column1, column2):
        """
        Create a manual join specification
        """
        return {
            "table1": table1,
            "table2": table2,
            "table1_column": column1,
            "table2_column": column2
        }