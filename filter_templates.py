"""
Filter Templates - Predefined filter patterns for common query patterns
"""
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Union
from pypika_query_engine import QueryGenerator, RawExpression

class DateRangeFilter:
    """Helper for date range filters"""

    def __init__(self, column: str, start_date: Union[str, date, datetime],
                 end_date: Union[str, date, datetime]):
        self.column = column
        self.start_date = start_date
        self.end_date = end_date

    def apply(self, query: QueryGenerator) -> QueryGenerator:
        """Apply the date range filter to a query"""
        start = self._format_date(self.start_date)
        end = self._format_date(self.end_date)

        # Use BETWEEN for simple date ranges
        query.where_between(self.column, f"'{start}'", f"'{end}'")

        return query

    def _format_date(self, d: Union[str, date, datetime]) -> str:
        """Format date for SQL"""
        if isinstance(d, str):
            return d
        elif isinstance(d, datetime):
            return d.strftime('%Y-%m-%d')
        else:
            return d.strftime('%Y-%m-%d')


class DateRangeBuilder:
    """Builder for complex date range filters"""

    def __init__(self):
        self.filters: List[DateRangeFilter] = []
        self.custom_filters: List[Dict] = []

    def add_range(self, column: str, start: Union[str, date, datetime],
                  end: Union[str, date, datetime]) -> 'DateRangeBuilder':
        """Add a date range filter"""
        self.filters.append(DateRangeFilter(column, start, end))
        return self

    def add_month(self, column: str, year: int, month: int) -> 'DateRangeBuilder':
        """Add a filter for a specific month"""
        start = f"{year}-{month:02d}-01"
        # Get last day of month
        if month == 12:
            end = f"{year}-12-31"
        else:
            end = f"{year}-{month + 1:02d}-01"
        return self.add_range(column, start, end)

    def add_year(self, column: str, year: int) -> 'DateRangeBuilder':
        """Add a filter for a specific year"""
        start = f"{year}-01-01"
        end = f"{year}-12-31"
        return self.add_range(column, start, end)

    def add_quarter(self, column: str, year: int, quarter: int) -> 'DateRangeBuilder':
        """Add a filter for a specific quarter"""
        quarter_months = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}
        start_month, end_month = quarter_months[quarter]
        start = f"{year}-{start_month:02d}-01"
        end = f"{year}-{end_month:02d}-{self._days_in_month(year, end_month)}"
        return self.add_range(column, start, end)

    def apply_to(self, query: QueryGenerator) -> QueryGenerator:
        """Apply all filters to a query"""
        for f in self.filters:
            f.apply(query)
        for custom in self.custom_filters:
            if custom.get('operator') == 'RAW':
                query.where_raw(custom['condition'])
            else:
                query.where(custom['column'], custom['operator'], custom['value'])
        return query

    def _days_in_month(self, year: int, month: int) -> int:
        """Get number of days in a month"""
        if month == 2:
            # Leap year check
            if (year % 400 == 0) or (year % 4 == 0 and year % 100 != 0):
                return 29
            return 28
        elif month in [4, 6, 9, 11]:
            return 30
        return 31


class FilterTemplate:
    """Common filter templates for reporting queries"""

    @staticmethod
    def current_year(column: str) -> DateRangeBuilder:
        """Filter for current year"""
        current_year = datetime.now().year
        return DateRangeBuilder().add_year(column, current_year)

    @staticmethod
    def last_n_days(column: str, days: int) -> DateRangeBuilder:
        """Filter for last N days"""
        end = datetime.now()
        start = end - timedelta(days=days)
        return DateRangeBuilder().add_range(column, start, end)

    @staticmethod
    def last_n_months(column: str, months: int) -> DateRangeBuilder:
        """Filter for last N months"""
        end = datetime.now()
        start = end - timedelta(days=months * 30)
        return DateRangeBuilder().add_range(column, start, end)

    @staticmethod
    def financial_year(column: str, year: int, start_month: int = 4) -> DateRangeBuilder:
        """Filter for financial year (default: April-March)"""
        start = f"{year}-{start_month:02d}-01"
        end_month = start_month - 1 if start_month > 1 else 12
        end_year = year + 1 if start_month > 1 else year
        end_day = DateRangeBuilder()._days_in_month(end_year, end_month)
        end = f"{end_year}-{end_month:02d}-{end_day}"
        return DateRangeBuilder().add_range(column, start, end)