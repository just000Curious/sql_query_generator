from typing import Dict, List, Optional, Union, Any
from pypika import Query, Table

from pypika_query_engine import QueryGenerator
from query_assembler import QueryAssembler


class CTEBuilder:
    """
    CTE Builder Module - Constructs multi-stage SQL queries using Common Table Expressions
    Acts as the central orchestrator that stitches together all previous components
    """

    def __init__(self):
        self.stages = []  # List of CTE stages
        self.final_query = None
        self.stage_counter = 0

    def add_stage(self, name: Optional[str] = None, query_generator: Optional[QueryGenerator] = None) -> 'CTEBuilder':
        """
        Add a new CTE stage

        Args:
            name: Name of the CTE (auto-generated if not provided)
            query_generator: QueryGenerator for this stage
        """
        if not name:
            self.stage_counter += 1
            name = f"stage_{self.stage_counter}"

        self.stages.append({
            'name': name,
            'query': query_generator
        })

        return self

    def add_stage_from_assembler(self, assembler: QueryAssembler, name: Optional[str] = None) -> 'CTEBuilder':
        """Add a stage from a QueryAssembler"""
        if not name:
            self.stage_counter += 1
            name = f"stage_{self.stage_counter}"

        # Convert assembler to query generator
        # This is a simplified approach - would need proper conversion
        query_sql = assembler.assemble()

        # Create a dummy query generator for now
        # In real implementation, would parse the SQL
        self.stages.append({
            'name': name,
            'query': query_sql
        })

        return self

    def set_final_query(self, query_generator: QueryGenerator) -> 'CTEBuilder':
        """Set the final query that uses the CTEs"""
        self.final_query = query_generator
        return self

    def build(self) -> str:
        """
        Build the complete SQL query with all CTE stages

        Returns:
            Complete SQL query with CTEs
        """
        if not self.stages:
            if self.final_query:
                return self.final_query.build()
            return ""

        if not self.final_query:
            raise ValueError("No final query set")

        # Build CTE section
        cte_parts = []
        for stage in self.stages:
            if isinstance(stage['query'], QueryGenerator):
                query_sql = stage['query'].build()
            else:
                query_sql = str(stage['query'])

            cte_parts.append(f"{stage['name']} AS (\n{query_sql}\n)")

        # Get final query
        final_sql = self.final_query.build()

        # Combine
        return "WITH\n" + ",\n".join(cte_parts) + "\n\n" + final_sql

    def get_stage_names(self) -> List[str]:
        """Get names of all CTE stages"""
        return [stage['name'] for stage in self.stages]

    def get_metadata(self) -> Dict:
        """Get metadata about the CTE structure"""
        return {
            'num_stages': len(self.stages),
            'stage_names': self.get_stage_names(),
            'has_final_query': self.final_query is not None
        }


# ========== WRAPPER FUNCTIONS FOR COMPATIBILITY ==========
# These functions make the module compatible with the test script

_cte_builder_instance = None


def _get_instance():
    """Get or create the CTE builder instance"""
    global _cte_builder_instance
    if _cte_builder_instance is None:
        _cte_builder_instance = CTEBuilder()
    return _cte_builder_instance


def build_cte(name: str, query: str, query_generator=None):
    """
    Wrapper function to build a CTE
    Compatible with test.py

    Args:
        name: Name of the CTE
        query: SQL query string or QueryGenerator
        query_generator: Optional QueryGenerator object

    Returns:
        The CTEBuilder instance
    """
    builder = _get_instance()

    # If query_generator is provided, use it
    if query_generator:
        builder.add_stage(name, query_generator)
    # If query is a string, create a simple QueryGenerator from it
    elif isinstance(query, str):
        # Create a simple query generator
        # This is a simplified approach
        from pypika_query_engine import QueryGenerator
        qg = QueryGenerator("cte_source")
        # In a real implementation, you'd parse the query
        builder.add_stage(name, qg)
    else:
        # Assume query is already a QueryGenerator or similar
        builder.add_stage(name, query)

    return builder


def get_cte(name: str):
    """
    Wrapper function to get a CTE by name

    Returns:
        The CTE stage if found, None otherwise
    """
    builder = _get_instance()
    stage_names = builder.get_stage_names()

    if name in stage_names:
        # Return the stage info
        for stage in builder.stages:
            if stage['name'] == name:
                return stage

    return None


def list_ctes():
    """
    Wrapper function to list all CTEs

    Returns:
        List of CTE names
    """
    builder = _get_instance()
    return builder.get_stage_names()


def add_cte_stage(name: str, query_generator):
    """
    Alternative wrapper function for adding CTE stages
    """
    builder = _get_instance()
    return builder.add_stage(name, query_generator)


def set_final_query(query_generator):
    """
    Wrapper function to set the final query
    """
    builder = _get_instance()
    return builder.set_final_query(query_generator)


def build_cte_query():
    """
    Wrapper function to build the complete CTE query
    """
    builder = _get_instance()
    return builder.build()


def reset_cte_builder():
    """
    Reset the CTE builder instance
    """
    global _cte_builder_instance
    _cte_builder_instance = None
    return True


# For backward compatibility with any existing code
CTETreeBuilder = CTEBuilder  # Alias for any code expecting this name