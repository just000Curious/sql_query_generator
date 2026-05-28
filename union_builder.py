"""
Union Builder Module - Handles UNION, UNION ALL, INTERSECT, and EXCEPT operations
"""
from __future__ import annotations
from typing import List, Optional, Dict, Any, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from pypika_query_engine import QueryGenerator


class UnionBuilder:
    """
    Builds queries with set operations (UNION, UNION ALL, INTERSECT, EXCEPT)
    """

    def __init__(self):
        self.queries: List[Union[QueryGenerator, str]] = []
        self.operations: List[str] = []

    def add_query(self, query: Union[QueryGenerator, str],
                  operation: str = "UNION ALL") -> UnionBuilder:
        """
        Add a query to the union

        Args:
            query: QueryGenerator instance or SQL string
            operation: UNION, UNION ALL, INTERSECT, EXCEPT
        """
        self.queries.append(query)

        if len(self.queries) > 1 and len(self.operations) < len(self.queries) - 1:
            self.operations.append(operation)

        return self

    def _wrap_subquery_with_limit(self, query: Union[QueryGenerator, str]) -> str:
        """Wrap a query that has LIMIT in a subquery for UNION compatibility"""
        query_str = query.build() if hasattr(query, 'build') else str(query)

        if 'LIMIT' in query_str.upper():
            return f"({query_str})"

        return query_str

    def build(self) -> str:
        """Build the complete set operation query"""
        if not self.queries:
            raise ValueError("No queries added to UnionBuilder")

        query_parts = []
        for q in self.queries:
            query_str = self._wrap_subquery_with_limit(q)
            query_parts.append(query_str)

        result = query_parts[0]
        for i, op in enumerate(self.operations):
            if i + 1 < len(query_parts):
                result += f"\n{op}\n{query_parts[i + 1]}"

        return result

    def build_with_ctes(self, cte_name: str = "combined_results") -> str:
        """Build query with the union as a CTE"""
        union_query = self.build()
        return f"WITH {cte_name} AS (\n{union_query}\n)\nSELECT * FROM {cte_name}"


# Helper functions
def union_all(*queries) -> UnionBuilder:
    """Create UNION ALL of multiple queries"""
    builder = UnionBuilder()
    for i, q in enumerate(queries):
        op = "UNION ALL" if i > 0 else None
        builder.add_query(q, op)
    return builder


def union(*queries) -> UnionBuilder:
    """Create UNION of multiple queries"""
    builder = UnionBuilder()
    for i, q in enumerate(queries):
        op = "UNION" if i > 0 else None
        builder.add_query(q, op)
    return builder


def intersect(*queries) -> UnionBuilder:
    """Create INTERSECT of multiple queries"""
    builder = UnionBuilder()
    for i, q in enumerate(queries):
        op = "INTERSECT" if i > 0 else None
        builder.add_query(q, op)
    return builder


def except_(query1, query2) -> UnionBuilder:
    """Create EXCEPT of two queries"""
    builder = UnionBuilder()
    builder.add_query(query1)
    builder.add_query(query2, "EXCEPT")
    return builder