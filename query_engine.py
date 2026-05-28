from typing import Dict, List, Optional, Union, Any
from datetime import date, datetime  # Add this import

from db_information import DBInfo
from pypika_query_engine import QueryGenerator
from join_builder import JoinBuilder
from query_assembler import QueryAssembler
from cte_builder import CTEBuilder
from query_validator import QueryValidator
from temporary_table import TemporaryTable, TemporaryTableManager
from union_builder import UnionBuilder, union_all, union, intersect, except_

from filter_templates import DateRangeBuilder, FilterTemplate
from union_builder import UnionBuilder

class QueryEngine:
    """
    Main Query Engine - Orchestrates the entire query generation process
    Follows the project architecture flowchart
    """

    def __init__(self, schema_file_path: str = None):  # Make schema_file_path optional
        """
        Initialize query engine with database schema

        Args:
            schema_file_path: Path to SQL schema file (optional)
        """
        # Step 1: DBInfo Module - Schema Intelligence Layer
        if schema_file_path:
            self.db_info = DBInfo(schema_file_path)
        else:
            self.db_info = DBInfo()  # Use test data

        # Initialize other components
        self.query_generator = None
        self.join_builder = None
        self.query_assembler = None
        self.cte_builder = None
        self.validator = QueryValidator(self.db_info)
        self.temp_table_manager = TemporaryTableManager()

        # Query state
        self.current_stage = 0
        self.stages = []

    # -------------------------
    # STAGE 1: Base Query Builder
    # -------------------------

    def create_query(self, table_name: str, schema_name: Optional[str] = None,
                     alias: Optional[str] = None) -> QueryGenerator:
        """
        Create a base query for a single table

        This is the Query Builder module in the flowchart
        """
        self.query_generator = QueryGenerator(table_name, schema_name, alias)
        self.current_stage = 1
        return self.query_generator

    # -------------------------
    # STAGE 2: Join Builder
    # -------------------------

    def create_join_builder(self) -> JoinBuilder:
        """Create a new join builder"""
        self.join_builder = JoinBuilder(self.db_info)
        self.current_stage = 2
        return self.join_builder

    def auto_join_tables(self, tables: List[Dict]) -> JoinBuilder:
        """
        Automatically build joins between tables

        Args:
            tables: List of dicts with keys:
                   - table: table name
                   - schema: schema name (optional)
                   - alias: alias (optional)
        """
        self.join_builder = JoinBuilder(self.db_info)
        self.join_builder.auto_join(tables)
        self.current_stage = 2
        return self.join_builder

    # -------------------------
    # STAGE 3: Query Assembler
    # -------------------------

    def assemble(self) -> QueryAssembler:
        """
        Assemble base query with joins

        This is the Query Assembler module in the flowchart
        """
        self.query_assembler = QueryAssembler()

        if self.query_generator:
            self.query_assembler.set_base_query(self.query_generator)

        if self.join_builder:
            self.query_assembler.set_joins(self.join_builder)

        self.current_stage = 3
        return self.query_assembler

    # -------------------------
    # STAGE 4: Temporary Table Builder
    # -------------------------

    def create_temp_table(self, name: str, query: Union[str, QueryGenerator]) -> TemporaryTable:
        """
        Create a temporary table

        This is the Temporary Table Builder module in the flowchart
        """
        if isinstance(query, QueryGenerator):
            query_str = query.build()
        else:
            query_str = query

        temp_table = self.temp_table_manager.create_temp_table(name, query_str)
        self.current_stage = 4
        return temp_table

    def get_temp_table(self, name: str) -> Optional[TemporaryTable]:
        """Get a temporary table by name"""
        return self.temp_table_manager.get_temp_table(name)

    # -------------------------
    # STAGE 5: CTE Builder
    # -------------------------

    def create_cte_builder(self) -> CTEBuilder:
        """Create a new CTE builder"""
        self.cte_builder = CTEBuilder()
        self.current_stage = 5
        return self.cte_builder

    def build_cte_query(self, stages: List[QueryGenerator],
                        final_query: QueryGenerator) -> str:
        """
        Build a query with multiple CTE stages

        This is the CTE Builder module in the flowchart
        """
        cte_builder = CTEBuilder()

        for stage in stages:
            cte_builder.add_stage(query_generator=stage)

        cte_builder.set_final_query(final_query)
        self.cte_builder = cte_builder
        self.current_stage = 5

        return cte_builder.build()

    # -------------------------
    # STAGE 6: Query Validator
    # -------------------------

    def validate_current(self) -> bool:
        """
        Validate the current query state

        This is the Query Validator module in the flowchart
        """
        self.current_stage = 6

        if self.cte_builder:
            return self.validator.validate_cte_builder(self.cte_builder)
        elif self.query_assembler:
            # Would need to convert assembler to something validatable
            return True
        elif self.join_builder:
            return self.validator.validate_join_builder(self.join_builder)
        elif self.query_generator:
            return self.validator.validate_query_generator(self.query_generator)
        else:
            self.validator.errors.append("No query to validate")
            return False

    def validate_sql(self, sql: str) -> bool:
        """Validate a raw SQL string"""
        return self.validator.validate_sql(sql)

    def get_validation_errors(self) -> List[str]:
        """Get validation errors"""
        return self.validator.get_errors()

    def get_validation_warnings(self) -> List[str]:
        """Get validation warnings"""
        return self.validator.get_warnings()

    # -------------------------
    # FINAL OUTPUT
    # -------------------------

    def generate(self) -> str:
        """
        Generate the final SQL query by going through all stages

        This executes the complete workflow:
        1. Validate
        2. Assemble if needed
        3. Build CTEs
        4. Return final SQL
        """
        # Validate first
        if not self.validate_current():
            error_msg = "Validation failed:\n" + "\n".join(self.get_validation_errors())
            raise ValueError(error_msg)

        # If we have CTE builder, use it
        if self.cte_builder:
            return self.cte_builder.build()

        # If we have assembler, use it
        if self.query_assembler:
            assembled = self.query_assembler.assemble()

            # Add any temp tables as CTEs
            if self.temp_table_manager.ctes or self.temp_table_manager.temp_tables:
                # This would need to be integrated
                pass

            return assembled

        # If we have join builder, use it
        if self.join_builder:
            return self.join_builder.build_with_ctes()

        # If we have query generator, use it
        if self.query_generator:
            return self.query_generator.build()

        raise ValueError("No query configured")

    def get_current_stage(self) -> int:
        """Get current workflow stage"""
        return self.current_stage

    def reset(self):
        """Reset the engine state"""
        self.query_generator = None
        self.join_builder = None
        self.query_assembler = None
        self.cte_builder = None
        self.temp_table_manager = TemporaryTableManager()
        self.validator.clear()
        self.current_stage = 0

    def combine_queries(self, queries: List[Union[QueryGenerator, str]],
                        operation: str = "UNION ALL") -> UnionBuilder:
        """
        Combine multiple queries using set operations

        Args:
            queries: List of QueryGenerator objects or SQL strings
            operation: UNION, UNION ALL, INTERSECT, EXCEPT

        Returns:
            UnionBuilder instance
        """
        builder = UnionBuilder()

        for i, q in enumerate(queries):
            op = operation if i < len(queries) - 1 else None
            if op:
                builder.add_query(q, op)
            else:
                builder.add_query(q)

        return builder

    def create_union_all(self, queries: List[Union[QueryGenerator, str]]) -> str:
        """Create UNION ALL of multiple queries"""
        return union_all(*queries).build()

    def create_union(self, queries: List[Union[QueryGenerator, str]]) -> str:
        """Create UNION of multiple queries"""
        return union(*queries).build()

    def create_intersect(self, queries: List[Union[QueryGenerator, str]]) -> str:
        """Create INTERSECT of multiple queries"""
        return intersect(*queries).build()

    def apply_date_range(self, query: QueryGenerator, column: str,
                         start: Union[str, date, datetime],
                         end: Union[str, date, datetime]) -> QueryGenerator:
        """
        Apply a date range filter to a query
        """
        return DateRangeBuilder().add_range(column, start, end).apply_to(query)

    def create_filter_template(self) -> FilterTemplate:
        """Get filter template helper"""
        return FilterTemplate()

    def create_date_range_builder(self) -> DateRangeBuilder:
        """Create a new date range builder"""
        return DateRangeBuilder()

    def union_queries(self, queries: List[Union[QueryGenerator, str]],
                      all_: bool = True) -> str:
        """Create UNION or UNION ALL of multiple queries"""
        builder = UnionBuilder()
        operation = "UNION ALL" if all_ else "UNION"

        for i, q in enumerate(queries):
            if i < len(queries) - 1:
                builder.add_query(q, operation)
            else:
                builder.add_query(q)

        return builder.build()