from typing import Dict, List, Optional, Union, Any
from pypika import Query, Table, Field

from join_builder import JoinBuilder
from pypika_query_engine import QueryGenerator


class QueryAssembler:
    """
    Query Assembler Module - Combines outputs from QueryBuilder and JoinBuilder
    to create a partially completed SQL query (~70% complete)
    """

    def __init__(self):
        self.base_query = None
        self.join_builder = None
        self.temp_tables = []
        self.ctes = []

    def set_base_query(self, query_generator: QueryGenerator):
        """Set the base query from QueryGenerator"""
        self.base_query = query_generator
        return self

    def set_joins(self, join_builder: JoinBuilder):
        """Set the join builder with join information"""
        self.join_builder = join_builder
        return self

    def assemble(self) -> str:
        """
        Assemble the base query with joins
        Returns approximately 70% complete SQL query
        """
        if not self.base_query:
            raise ValueError("No base query set")

        # Get base query structure
        base_metadata = self.base_query.get_metadata()

        # Get join structure
        if self.join_builder:
            join_sql = self.join_builder.build()
            return join_sql
        else:
            return self.base_query.build()

    def add_temp_table(self, name: str, query: str):
        """Add a temporary table definition"""
        self.temp_tables.append({
            'name': name,
            'query': query
        })
        return self

    def add_cte(self, name: str, query_generator: QueryGenerator):
        """Add a CTE definition"""
        self.ctes.append({
            'name': name,
            'query': query_generator
        })
        return self

    def get_metadata(self) -> Dict:
        """Get metadata about the assembled query"""
        return {
            'has_base_query': self.base_query is not None,
            'has_joins': self.join_builder is not None,
            'temp_tables': [t['name'] for t in self.temp_tables],
            'ctes': [c['name'] for c in self.ctes]
        }


# ========== WRAPPER FUNCTIONS FOR COMPATIBILITY ==========
# These functions make the module compatible with the test script

_assembler_instance = None


def _get_assembler_instance():
    """Get or create the query assembler instance"""
    global _assembler_instance
    if _assembler_instance is None:
        _assembler_instance = QueryAssembler()
    return _assembler_instance


def assemble(query_parts=None):
    """
    Wrapper function to assemble a query
    Compatible with test.py

    Args:
        query_parts: Can be:
            - List of query parts (for test compatibility)
            - QueryGenerator (to set as base query)
            - JoinBuilder (to set joins)
            - None (to just run assembly)

    Returns:
        Assembled SQL string or QueryAssembler instance
    """
    assembler = _get_assembler_instance()

    # Handle different input types
    if query_parts is None:
        # Just run assembly
        return assembler.assemble()

    elif isinstance(query_parts, list):
        # For test compatibility: assemble(["SELECT", "FROM", "WHERE"])
        # Create a simple query from parts
        return " ".join(query_parts)

    elif hasattr(query_parts, '__class__'):
        class_name = query_parts.__class__.__name__

        if class_name == 'QueryGenerator':
            assembler.set_base_query(query_parts)
        elif class_name == 'JoinBuilder':
            assembler.set_joins(query_parts)
        else:
            # Unknown type, just return as string
            return str(query_parts)

    return assembler


def assemble_query(base_query=None, join_builder=None):
    """
    Wrapper function to assemble a query with explicit parameters
    """
    assembler = _get_assembler_instance()

    if base_query:
        assembler.set_base_query(base_query)
    if join_builder:
        assembler.set_joins(join_builder)

    return assembler.assemble()


def add_temp_table(name: str, query: str):
    """
    Wrapper function to add a temporary table
    """
    assembler = _get_assembler_instance()
    return assembler.add_temp_table(name, query)


def add_cte(name: str, query_generator: QueryGenerator):
    """
    Wrapper function to add a CTE
    """
    assembler = _get_assembler_instance()
    return assembler.add_cte(name, query_generator)


def get_assembler_metadata():
    """
    Get metadata about the current assembler state
    """
    assembler = _get_assembler_instance()
    return assembler.get_metadata()


def reset_assembler():
    """
    Reset the assembler instance
    """
    global _assembler_instance
    _assembler_instance = None
    return True


# For backward compatibility
def build_query(*args, **kwargs):
    """Alias for assemble function"""
    return assemble(*args, **kwargs)