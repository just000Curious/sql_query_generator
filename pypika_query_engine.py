from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from pypika import Query, Table, Field, Order, functions as fn
from pypika.terms import Term
from pypika.enums import JoinType


# ==================== CORE ABSTRACT CLASS ====================

class BaseSQLQueryGenerator(ABC):
    """
    Abstract base class for SQL query generation.
    Contains core logic independent of any specific SQL builder library.
    """

    def __init__(self, db_info):
        """
        Args:
            db_info: Instance of your DBInfo class
        """
        self.db_info = db_info
        self.base_table_name = db_info.table_name

        # Query components
        self.selected_columns = []  # List of column names or expressions
        self.joins = []  # List of join configurations
        self.where_conditions = []  # List of condition tuples (column, operator, value, logical_op)
        self.group_by_columns = []  # List of column names for GROUP BY
        self.having_conditions = []  # List of condition tuples for HAVING
        self.order_by_columns = []  # List of (column, direction) tuples
        self.limit_value = None
        self.offset_value = None
        self.distinct_flag = False
        self.table_aliases = {}  # Dictionary to store table aliases

        # Supported operators mapping
        self.supported_operators = {
            '=': 'eq',
            '!=': 'ne',
            '>': 'gt',
            '>=': 'gte',
            '<': 'lt',
            '<=': 'lte',
            'LIKE': 'like',
            'ILIKE': 'ilike',
            'IN': 'in',
            'NOT IN': 'not_in',
            'IS NULL': 'is_null',
            'IS NOT NULL': 'is_not_null',
            'BETWEEN': 'between'
        }

    def _validate_column(self, table_name: str, column_name: str) -> bool:
        """Validate if column exists in the given table"""
        if table_name == self.base_table_name:
            valid_columns = [col["column_name"] for col in self.db_info.get_columns()]
        else:
            # Check if table exists and get its columns
            table_data = self.db_info.schema_df[
                self.db_info.schema_df["table_name"] == table_name
                ]
            if table_data.empty:
                raise ValueError(f"Table '{table_name}' does not exist")
            valid_columns = table_data["column_name"].unique().tolist()

        if column_name not in valid_columns:
            raise ValueError(f"Column '{column_name}' does not exist in table '{table_name}'")
        return True

    def _parse_condition(self, condition: Union[Dict, tuple]) -> Dict:
        """Parse condition from various input formats"""
        if isinstance(condition, dict):
            return {
                'column': condition.get('column'),
                'operator': condition.get('operator', '='),
                'value': condition.get('value'),
                'logical_op': condition.get('logical_op', 'AND')
            }
        elif isinstance(condition, (list, tuple)) and len(condition) >= 3:
            return {
                'column': condition[0],
                'operator': condition[1] if len(condition) > 1 else '=',
                'value': condition[2] if len(condition) > 2 else None,
                'logical_op': condition[3] if len(condition) > 3 else 'AND'
            }
        else:
            raise ValueError("Condition must be a dict or tuple of (column, operator, value, logical_op)")

    # ==================== SELECT METHODS ====================

    def select(self, *columns: str) -> 'BaseSQLQueryGenerator':
        """
        Add columns to SELECT clause

        Args:
            *columns: Column names to select
        """
        for col in columns:
            if col == '*':
                self.selected_columns = ['*']
                break
            self._validate_column(self.base_table_name, col)
            self.selected_columns.append(col)
        return self

    def select_expr(self, expression: str, alias: Optional[str] = None) -> 'BaseSQLQueryGenerator':
        """
        Add a custom SQL expression to SELECT clause

        Args:
            expression: SQL expression (e.g., 'COUNT(*)', 'salary * 12')
            alias: Optional alias for the expression
        """
        expr_dict = {'expression': expression}
        if alias:
            expr_dict['alias'] = alias
        self.selected_columns.append(expr_dict)
        return self

    def distinct(self) -> 'BaseSQLQueryGenerator':
        """Add DISTINCT to SELECT clause"""
        self.distinct_flag = True
        return self

    # ==================== AGGREGATION METHODS ====================

    def count(self, column: str = '*', alias: Optional[str] = None) -> 'BaseSQLQueryGenerator':
        """Add COUNT aggregation"""
        expr = f"COUNT({column})"
        if alias:
            expr += f" AS {alias}"
        return self.select_expr(expr)

    def sum(self, column: str, alias: Optional[str] = None) -> 'BaseSQLQueryGenerator':
        """Add SUM aggregation"""
        self._validate_column(self.base_table_name, column)
        expr = f"SUM({column})"
        if alias:
            expr += f" AS {alias}"
        return self.select_expr(expr)

    def avg(self, column: str, alias: Optional[str] = None) -> 'BaseSQLQueryGenerator':
        """Add AVG aggregation"""
        self._validate_column(self.base_table_name, column)
        expr = f"AVG({column})"
        if alias:
            expr += f" AS {alias}"
        return self.select_expr(expr)

    def min(self, column: str, alias: Optional[str] = None) -> 'BaseSQLQueryGenerator':
        """Add MIN aggregation"""
        self._validate_column(self.base_table_name, column)
        expr = f"MIN({column})"
        if alias:
            expr += f" AS {alias}"
        return self.select_expr(expr)

    def max(self, column: str, alias: Optional[str] = None) -> 'BaseSQLQueryGenerator':
        """Add MAX aggregation"""
        self._validate_column(self.base_table_name, column)
        expr = f"MAX({column})"
        if alias:
            expr += f" AS {alias}"
        return self.select_expr(expr)

    # ==================== WHERE METHODS ====================

    def where(self, *conditions) -> 'BaseSQLQueryGenerator':
        """
        Add WHERE conditions

        Args:
            *conditions: Can be:
                - (column, value) -> defaults to '=' operator
                - (column, operator, value)
                - (column, operator, value, logical_op)
                - Dict with keys: column, operator, value, logical_op
        """
        for condition in conditions:
            parsed = self._parse_condition(condition)
            self._validate_column(self.base_table_name, parsed['column'])
            self.where_conditions.append(parsed)
        return self

    def and_where(self, *conditions) -> 'BaseSQLQueryGenerator':
        """Add WHERE conditions with AND (alias for where)"""
        return self.where(*conditions)

    def or_where(self, *conditions) -> 'BaseSQLQueryGenerator':
        """
        Add WHERE conditions with OR
        """
        for condition in conditions:
            parsed = self._parse_condition(condition)
            parsed['logical_op'] = 'OR'
            self._validate_column(self.base_table_name, parsed['column'])
            self.where_conditions.append(parsed)
        return self

    # ==================== JOIN METHODS ====================

    def join(self, target_table: str, join_type: str = 'INNER',
             alias: Optional[str] = None) -> 'BaseSQLQueryGenerator':
        """
        Add a JOIN clause automatically based on foreign key relationship

        Args:
            target_table: Name of the table to join
            join_type: Type of join (INNER, LEFT, RIGHT, FULL)
            alias: Optional alias for the target table
        """
        relationship = self._find_relationship(target_table)

        join_config = {
            'table': target_table,
            'type': join_type,
            'alias': alias,
            'relationship': relationship
        }
        self.joins.append(join_config)

        if alias:
            self.table_aliases[target_table] = alias

        return self

    def join_on(self, target_table: str, left_column: str, right_column: str,
                join_type: str = 'INNER', alias: Optional[str] = None) -> 'BaseSQLQueryGenerator':
        """
        Add a JOIN clause with explicit join condition

        Args:
            target_table: Name of the table to join
            left_column: Column from base table
            right_column: Column from target table
            join_type: Type of join (INNER, LEFT, RIGHT, FULL)
            alias: Optional alias for the target table
        """
        self._validate_column(self.base_table_name, left_column)

        # Validate target table column
        target_data = self.db_info.schema_df[
            self.db_info.schema_df["table_name"] == target_table
            ]
        if target_data.empty:
            raise ValueError(f"Table '{target_table}' does not exist")

        if right_column not in target_data["column_name"].values:
            raise ValueError(f"Column '{right_column}' does not exist in table '{target_table}'")

        join_config = {
            'table': target_table,
            'type': join_type,
            'alias': alias,
            'on': {
                'left_column': left_column,
                'right_column': right_column
            }
        }
        self.joins.append(join_config)

        if alias:
            self.table_aliases[target_table] = alias

        return self

    def _find_relationship(self, target_table: str) -> Dict:
        """Find relationship between base table and target table"""
        # Case 1: Base table has FK to target
        for fk in self.db_info.get_foreign_keys():
            if fk["references_table"] == target_table:
                return {
                    'type': 'base_to_target',
                    'base_column': fk["column"],
                    'target_column': fk["references_column"]
                }

        # Case 2: Target table has FK to base
        child_data = self.db_info.schema_df[
            (self.db_info.schema_df["table_name"] == target_table) &
            (self.db_info.schema_df["is_foreign_key"] == True) &
            (self.db_info.schema_df["parent_table"] == self.base_table_name)
            ]

        if not child_data.empty:
            row = child_data.iloc[0]
            return {
                'type': 'target_to_base',
                'base_column': row["parent_column"],
                'target_column': row["column_name"]
            }

        raise ValueError(f"No relationship found between {self.base_table_name} and {target_table}")

    # ==================== GROUP BY METHODS ====================

    def group_by(self, *columns: str) -> 'BaseSQLQueryGenerator':
        """Add GROUP BY clause"""
        for col in columns:
            self._validate_column(self.base_table_name, col)
            self.group_by_columns.append(col)
        return self

    # ==================== HAVING METHODS ====================

    def having(self, *conditions) -> 'BaseSQLQueryGenerator':
        """Add HAVING clause"""
        for condition in conditions:
            parsed = self._parse_condition(condition)
            # HAVING conditions typically use aggregate functions
            # We'll validate later in the builder
            self.having_conditions.append(parsed)
        return self

    # ==================== ORDER BY METHODS ====================

    def order_by(self, column: str, direction: str = 'ASC') -> 'BaseSQLQueryGenerator':
        """Add ORDER BY clause"""
        self._validate_column(self.base_table_name, column)
        direction = direction.upper()
        if direction not in ['ASC', 'DESC']:
            raise ValueError("Direction must be 'ASC' or 'DESC'")
        self.order_by_columns.append((column, direction))
        return self

    # ==================== LIMIT/OFFSET METHODS ====================

    def limit(self, limit: int) -> 'BaseSQLQueryGenerator':
        """Add LIMIT clause"""
        if limit < 0:
            raise ValueError("Limit must be non-negative")
        self.limit_value = limit
        return self

    def offset(self, offset: int) -> 'BaseSQLQueryGenerator':
        """Add OFFSET clause"""
        if offset < 0:
            raise ValueError("Offset must be non-negative")
        self.offset_value = offset
        return self

    # ==================== ALIAS METHODS ====================

    def alias_table(self, table_name: str, alias: str) -> 'BaseSQLQueryGenerator':
        """Set alias for a table"""
        if table_name != self.base_table_name and table_name not in [j['table'] for j in self.joins]:
            raise ValueError(f"Table '{table_name}' is not part of the query")
        self.table_aliases[table_name] = alias
        return self

    def alias_column(self, column: str, alias: str) -> 'BaseSQLQueryGenerator':
        """Add column alias to SELECT clause"""
        self._validate_column(self.base_table_name, column)
        # Remove column if already in selected_columns
        self.selected_columns = [c for c in self.selected_columns if c != column]
        # Add as expression with alias
        return self.select_expr(column, alias)

    # ==================== ABSTRACT METHODS TO BE IMPLEMENTED ====================

    @abstractmethod
    def build(self) -> str:
        """Build and return the final SQL query string"""
        pass


# ==================== PYPIKA IMPLEMENTATION ====================

class PypikaSQLGenerator(BaseSQLQueryGenerator):
    """
    Concrete implementation of SQL query generator using Pypika library
    """

    def __init__(self, db_info):
        super().__init__(db_info)
        self.query = Query
        self._table_cache = {}  # Cache for Table objects

    def _get_table(self, table_name: str, force_no_alias: bool = False) -> Table:
        """Get or create a Table object, with alias if defined"""
        cache_key = table_name
        if cache_key in self._table_cache:
            return self._table_cache[cache_key]

        table = Table(table_name)

        # Apply alias if defined and not forcing no alias
        if not force_no_alias and table_name in self.table_aliases:
            table = table.as_(self.table_aliases[table_name])

        self._table_cache[cache_key] = table
        return table

    def _get_field(self, table_name: str, column: str) -> Field:
        """Get a Field object for a specific table and column"""
        table = self._get_table(table_name)
        return getattr(table, column)

    def _build_condition(self, condition: Dict, table_name: Optional[str] = None) -> Term:
        """Build a Pypika condition from a condition dictionary"""
        table = table_name or self.base_table_name
        field = self._get_field(table, condition['column'])
        operator = condition['operator']
        value = condition['value']

        if operator == '=':
            return field == value
        elif operator == '!=':
            return field != value
        elif operator == '>':
            return field > value
        elif operator == '>=':
            return field >= value
        elif operator == '<':
            return field < value
        elif operator == '<=':
            return field <= value
        elif operator.upper() == 'LIKE':
            return field.like(value)
        elif operator.upper() == 'ILIKE':
            return field.ilike(value)
        elif operator.upper() == 'IN':
            return field.isin(value)
        elif operator.upper() == 'NOT IN':
            return field.notin(value)
        elif operator.upper() == 'IS NULL':
            return field.isnull()
        elif operator.upper() == 'IS NOT NULL':
            return field.isnotnull()
        elif operator.upper() == 'BETWEEN':
            if not isinstance(value, (list, tuple)) or len(value) != 2:
                raise ValueError("BETWEEN requires a list/tuple of two values")
            return field.between(value[0], value[1])
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def _build_select_clause(self, query):
        """Build the SELECT clause"""
        if not self.selected_columns:
            # Default to all columns if none specified
            query = query.select('*')
        else:
            select_fields = []
            for col in self.selected_columns:
                if isinstance(col, dict) and 'expression' in col:
                    # Handle custom expression
                    if col['expression'] == '*':
                        select_fields.append('*')
                    else:
                        expr = col['expression']
                        if 'alias' in col:
                            # Parse expression and add alias
                            # This is simplified; complex expressions might need different handling
                            field = expr
                            if '(' in expr and ')' in expr:
                                # It's a function, keep as is
                                select_fields.append(f"{expr} AS {col['alias']}")
                            else:
                                # It's a column, get the field
                                field = self._get_field(self.base_table_name, expr)
                                select_fields.append(field.as_(col['alias']))
                        else:
                            select_fields.append(expr)
                else:
                    # Regular column
                    select_fields.append(self._get_field(self.base_table_name, col))
            query = query.select(*select_fields)

        if self.distinct_flag:
            query = query.distinct()

        return query

    def _build_joins(self, query):
        """Build JOIN clauses"""
        for join_config in self.joins:
            target_table = join_config['table']
            join_type = join_config['type'].upper()
            alias = join_config.get('alias')

            target_table_obj = self._get_table(target_table)

            # Determine join condition
            if 'on' in join_config:
                # Explicit join condition
                left_field = self._get_field(self.base_table_name, join_config['on']['left_column'])
                right_field = getattr(target_table_obj, join_config['on']['right_column'])
                condition = left_field == right_field
            else:
                # Auto-detect from relationship
                rel = join_config['relationship']
                if rel['type'] == 'base_to_target':
                    left_field = self._get_field(self.base_table_name, rel['base_column'])
                    right_field = getattr(target_table_obj, rel['target_column'])
                else:  # target_to_base
                    left_field = self._get_field(self.base_table_name, rel['base_column'])
                    right_field = getattr(target_table_obj, rel['target_column'])
                condition = left_field == right_field

            # Apply join based on type
            if join_type == 'INNER':
                query = query.join(target_table_obj).on(condition)
            elif join_type == 'LEFT':
                query = query.left_join(target_table_obj).on(condition)
            elif join_type == 'RIGHT':
                query = query.right_join(target_table_obj).on(condition)
            elif join_type == 'FULL':
                query = query.full_join(target_table_obj).on(condition)
            else:
                raise ValueError(f"Unsupported join type: {join_type}")

        return query

    def _build_where(self, query):
        """Build WHERE clause"""
        if not self.where_conditions:
            return query

        # Build conditions with proper AND/OR nesting
        where_conditions = []
        current_group = []
        current_op = 'AND'

        for condition in self.where_conditions:
            cond_term = self._build_condition(condition)
            logical_op = condition.get('logical_op', 'AND')

            if logical_op != current_op and current_group:
                # Group completed, add to where_conditions
                if current_op == 'AND':
                    where_conditions.append(Query._and(*current_group))
                else:
                    where_conditions.append(Query._or(*current_group))
                current_group = []
                current_op = logical_op

            current_group.append(cond_term)

        # Add the last group
        if current_group:
            if current_op == 'AND':
                where_conditions.append(Query._and(*current_group))
            else:
                where_conditions.append(Query._or(*current_group))

        # Combine all groups with AND
        if len(where_conditions) > 1:
            final_condition = Query._and(*where_conditions)
        else:
            final_condition = where_conditions[0]

        return query.where(final_condition)

    def _build_group_by(self, query):
        """Build GROUP BY clause"""
        if self.group_by_columns:
            group_fields = [self._get_field(self.base_table_name, col)
                            for col in self.group_by_columns]
            query = query.groupby(*group_fields)
        return query

    def _build_having(self, query):
        """Build HAVING clause"""
        if self.having_conditions:
            # HAVING conditions are applied after GROUP BY
            having_conditions = [self._build_condition(cond)
                                 for cond in self.having_conditions]
            query = query.having(*having_conditions)
        return query

    def _build_order_by(self, query):
        """Build ORDER BY clause"""
        if self.order_by_columns:
            order_fields = []
            for col, direction in self.order_by_columns:
                field = self._get_field(self.base_table_name, col)
                if direction.upper() == 'DESC':
                    order_fields.append(field.desc())
                else:
                    order_fields.append(field.asc())
            query = query.orderby(*order_fields)
        return query

    def _build_limit_offset(self, query):
        """Build LIMIT and OFFSET clauses"""
        if self.limit_value is not None:
            query = query.limit(self.limit_value)
        if self.offset_value is not None:
            query = query.offset(self.offset_value)
        return query

    def build(self) -> str:
        """Build and return the final SQL query string"""
        # Start with base table
        base_table = self._get_table(self.base_table_name)
        query = self.query.from_(base_table)

        # Build all clauses
        query = self._build_select_clause(query)
        query = self._build_joins(query)
        query = self._build_where(query)
        query = self._build_group_by(query)
        query = self._build_having(query)
        query = self._build_order_by(query)
        query = self._build_limit_offset(query)

        return str(query)


# ==================== USAGE EXAMPLES ====================

def example_usage(db_info):
    """Example usage of the SQL query generator"""

    # Create generator instance
    sql_gen = PypikaSQLGenerator(db_info)

    # Example 1: Simple select
    query1 = (sql_gen
              .select('emp_no', 'emp_firstname', 'emp_lastname')
              .where(('emp_no', '>', 1000))
              .order_by('emp_no')
              .limit(10)
              ).build()
    print("Query 1:", query1)

    # Example 2: With joins
    sql_gen2 = PypikaSQLGenerator(db_info)
    query2 = (sql_gen2
              .select('emp_no', 'emp_firstname', 'emp_lastname', 'emp_desig_desc')
              .join('pmm_designation')
              .where(('emp_curr_stat', '=', 'ACTIVE'))
              .build()
              )
    print("Query 2:", query2)

    # Example 3: With aggregations
    sql_gen3 = PypikaSQLGenerator(db_info)
    query3 = (sql_gen3
              .select('emp_dept_cd')
              .count('*', 'emp_count')
              .group_by('emp_dept_cd')
              .having(('emp_count', '>', 5))
              .build()
              )
    print("Query 3:", query3)

    # Example 4: Complex where with multiple conditions
    sql_gen4 = PypikaSQLGenerator(db_info)
    query4 = (sql_gen4
              .select('*')
              .where(('emp_dept_cd', '=', 10))
              .or_where(('emp_dept_cd', '=', 20))
              .and_where(('emp_curr_stat', '=', 'ACTIVE'))
              .build()
              )
    print("Query 4:", query4)

    return {
        'query1': query1,
        'query2': query2,
        'query3': query3,
        'query4': query4
    }


# ==================== FACTORY CLASS (Optional) ====================

class SQLGeneratorFactory:
    """
    Factory class to create different SQL generator implementations
    """

    @staticmethod
    def create_generator(db_info, backend='pypika'):
        """
        Create a SQL generator instance

        Args:
            db_info: DBInfo instance
            backend: SQL builder backend ('pypika' or others)
        """
        if backend == 'pypika':
            return PypikaSQLGenerator(db_info)
        else:
            raise ValueError(f"Unsupported backend: {backend}")