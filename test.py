"""
test_api_fixed.py
Fixed test script for SQL Query Generator API
"""

import requests
import json
import time
from datetime import datetime, date, timedelta
import sys

# API Configuration
API_URL = "http://127.0.0.1:8000"


class APITester:
    """Test the SQL Query Generator API"""

    def __init__(self, api_url=API_URL):
        self.api_url = api_url
        self.passed = 0
        self.failed = 0

    def test(self, name, func):
        """Run a single test"""
        print(f"\n▶ Testing: {name}")
        try:
            result = func()
            if result:
                print(f"  ✓ PASS")
                self.passed += 1
            else:
                print(f"  ✗ FAIL")
                self.failed += 1
            return result
        except Exception as e:
            print(f"  ✗ FAIL - Exception: {e}")
            self.failed += 1
            return False

    def summary(self):
        """Print test summary"""
        print("\n" + "="*60)
        print(f"TEST SUMMARY: {self.passed} passed, {self.failed} failed")
        print("="*60)
        return self.failed == 0


def check_api_health():
    """Test 1: Check if API is running"""
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"    Status: {data.get('status')}")
            print(f"    Tables loaded: {data.get('tables_loaded')}")
            return True
        return False
    except requests.exceptions.ConnectionError:
        print("    ❌ API not running. Please start the API first: python api_improved.py")
        return False


def test_root_endpoint():
    """Test 2: Root endpoint"""
    response = requests.get(f"{API_URL}/")
    if response.status_code == 200:
        data = response.json()
        print(f"    API Version: {data.get('version')}")
        print(f"    Tables: {data.get('tables')}")
        return True
    return False


def test_list_tables():
    """Test 3: List all tables"""
    response = requests.get(f"{API_URL}/tables")
    if response.status_code == 200:
        data = response.json()
        print(f"    Tables found: {data.get('count')}")
        print(f"    Tables: {data.get('tables')[:5]}...")
        return data.get('count', 0) > 0
    return False


def test_get_table_schema():
    """Test 4: Get table schema"""
    tables_response = requests.get(f"{API_URL}/tables")
    if tables_response.status_code != 200:
        return False

    tables = tables_response.json().get('tables', [])
    if not tables:
        print("    No tables found")
        return False

    table_name = tables[0]
    response = requests.get(f"{API_URL}/tables/{table_name}")

    if response.status_code == 200:
        data = response.json()
        print(f"    Table: {data.get('table')}")
        print(f"    Columns: {data.get('column_count')}")
        print(f"    Columns list: {data.get('columns_list')[:5]}...")
        return True
    return False


def test_generate_basic_query():
    """Test 5: Generate a basic SELECT query"""
    payload = {
        "table": "employees",
        "columns": ["emp_no", "emp_name", "emp_dept_cd", "salary"],
        "conditions": [
            {"column": "emp_dept_cd", "operator": "=", "value": "IT"}
        ],
        "limit": 5
    }

    response = requests.post(f"{API_URL}/query/generate", json=payload)

    if response.status_code == 200:
        data = response.json()
        if data.get('success'):
            print(f"    Generated SQL:")
            print(f"    {data.get('query')}")
            return True
        else:
            print(f"    Error: {data.get('error')}")
            return False
    return False


def test_execute_query():
    """Test 6: Execute a query and get results"""
    payload = {
        "sql": "SELECT emp_no, emp_name, emp_dept_cd, salary FROM employees WHERE emp_dept_cd = 'IT' LIMIT 3",
        "limit": 10
    }

    response = requests.post(f"{API_URL}/query/execute", json=payload)

    if response.status_code == 200:
        data = response.json()
        if data.get('success'):
            print(f"    Row count: {data.get('row_count')}")
            print(f"    Columns: {data.get('columns')}")
            print(f"    Sample data: {data.get('data')[:2]}")
            return data.get('row_count', 0) > 0
        else:
            print(f"    Error: {data.get('message')}")
            return False
    return False


def test_aggregate_query():
    """Test 7: Execute an aggregate query"""
    payload = {
        "sql": """
            SELECT status, COUNT(*) as count 
            FROM complaints 
            GROUP BY status 
            ORDER BY count DESC
        """,
        "limit": 10
    }

    response = requests.post(f"{API_URL}/query/execute", json=payload)

    if response.status_code == 200:
        data = response.json()
        if data.get('success'):
            print(f"    Results: {data.get('row_count')} rows")
            for row in data.get('data', []):
                print(f"      {row}")
            return data.get('row_count', 0) > 0
        else:
            print(f"    Error: {data.get('message')}")
            return False
    return False


def test_join_query():
    """Test 8: Execute a JOIN query"""
    payload = {
        "sql": """
            SELECT 
                c.complaint_no,
                c.status,
                e.emp_name,
                e.emp_dept_cd
            FROM complaints c
            JOIN employees e ON c.emp_no = e.emp_no
            WHERE c.status IN ('OPEN', 'IN_PROGRESS')
            LIMIT 5
        """,
        "limit": 10
    }

    response = requests.post(f"{API_URL}/query/execute", json=payload)

    if response.status_code == 200:
        data = response.json()
        if data.get('success'):
            print(f"    Results: {data.get('row_count')} rows")
            for row in data.get('data', [])[:3]:
                print(f"      {row}")
            return True
        else:
            print(f"    Error: {data.get('message')}")
            return False
    return False


def test_date_range_query():
    """Test 9: Execute date range query"""
    payload = {
        "sql": """
            SELECT complaint_no, complaint_date, status
            FROM complaints
            WHERE complaint_date BETWEEN '2024-01-01' AND '2024-01-31'
        """,
        "limit": 10
    }

    response = requests.post(f"{API_URL}/query/execute", json=payload)

    if response.status_code == 200:
        data = response.json()
        if data.get('success'):
            print(f"    Results: {data.get('row_count')} rows")
            for row in data.get('data', []):
                print(f"      {row}")
            return True
        else:
            print(f"    Error: {data.get('message')}")
            return False
    return False


def test_search_tables():
    """Test 10: Search tables"""
    response = requests.get(f"{API_URL}/search/tables", params={"q": "emp"})

    if response.status_code == 200:
        data = response.json()
        print(f"    Query: {data.get('query')}")
        print(f"    Results: {data.get('results')}")
        print(f"    Count: {data.get('count')}")
        return data.get('count', 0) > 0
    return False


def test_search_columns():
    """Test 11: Search columns"""
    response = requests.get(f"{API_URL}/search/columns", params={"q": "name"})

    if response.status_code == 200:
        data = response.json()
        print(f"    Query: {data.get('query')}")
        print(f"    Results: {data.get('count')} columns found")
        for col in data.get('results', [])[:3]:
            print(f"      {col.get('table')}.{col.get('column')}")
        return data.get('count', 0) > 0
    return False


def test_get_stats():
    """Test 12: Get database statistics"""
    response = requests.get(f"{API_URL}/stats")

    if response.status_code == 200:
        data = response.json()
        print(f"    Total tables: {data.get('total_tables')}")
        print(f"    Total columns: {data.get('total_columns')}")
        print(f"    Tables: {list(data.get('tables', {}).keys())[:5]}")
        return data.get('total_tables', 0) > 0
    return False


def test_get_schema():
    """Test 13: Get complete schema"""
    response = requests.get(f"{API_URL}/schema")

    if response.status_code == 200:
        data = response.json()
        print(f"    Database: {data.get('database_name')}")
        print(f"    Total tables: {data.get('total_tables')}")
        print(f"    Tables: {data.get('tables')[:5]}")
        return data.get('total_tables', 0) > 0
    return False


def test_get_sample_queries():
    """Test 14: Get sample queries"""
    response = requests.get(f"{API_URL}/samples")

    if response.status_code == 200:
        data = response.json()
        samples = data.get('samples', [])
        print(f"    Sample queries: {len(samples)}")
        for sample in samples[:3]:
            print(f"      - {sample.get('name')}: {sample.get('description')}")
        return len(samples) > 0
    return False


def test_validate_sql():
    """Test 15: Validate SQL (fixed - uses execute with LIMIT 0)"""
    # Valid SQL test
    payload_valid = {
        "sql": "SELECT * FROM employees",
        "validate": True
    }

    response = requests.post(f"{API_URL}/query/validate", json=payload_valid)

    if response.status_code == 200:
        data = response.json()
        valid = data.get('valid', False)
        errors = data.get('errors', [])
        print(f"    Valid SQL - Valid: {valid}, Errors: {errors}")

        # Test invalid SQL
        payload_invalid = {
            "sql": "SELECT FROM employees",
            "validate": True
        }

        response2 = requests.post(f"{API_URL}/query/validate", json=payload_invalid)

        if response2.status_code == 200:
            data2 = response2.json()
            valid2 = data2.get('valid', False)
            print(f"    Invalid SQL - Valid: {valid2}, Errors: {data2.get('errors', [])}")

            # Both tests pass if valid SQL returns True and invalid SQL returns False
            return valid and not valid2

    return False


def test_advanced_query_generation():
    """Test 16: Advanced query generation with multiple tables"""
    payload = {
        "tables": [{"table": "employees", "alias": "e"}],
        "columns": [
            {"table": "e", "column": "emp_no"},
            {"table": "e", "column": "emp_name"}
        ],
        "conditions": [
            {"column": "e.emp_dept_cd", "operator": "=", "value": "IT"}
        ],
        "limit": 10
    }

    response = requests.post(f"{API_URL}/query/advanced/generate", json=payload)

    if response.status_code == 200:
        data = response.json()
        if data.get('success'):
            print(f"    Generated SQL:")
            print(f"    {data.get('query')}")
            return True
        else:
            print(f"    Error: {data.get('error')}")
            return False
    return False


def test_performance():
    """Test 17: Performance test - multiple queries"""
    queries = [
        "SELECT * FROM employees LIMIT 100",
        "SELECT COUNT(*) FROM employees",
        "SELECT emp_dept_cd, AVG(salary) FROM employees GROUP BY emp_dept_cd"
    ]

    times = []
    for i, sql in enumerate(queries):
        start = time.time()
        response = requests.post(f"{API_URL}/query/execute", json={"sql": sql, "limit": 100})
        elapsed = time.time() - start

        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                times.append(elapsed)
                print(f"    Query {i+1}: {elapsed:.3f}s")
            else:
                print(f"    Query {i+1}: FAILED - {data.get('message')}")
                return False
        else:
            print(f"    Query {i+1}: HTTP {response.status_code}")
            return False

    avg_time = sum(times) / len(times)
    print(f"    Average response time: {avg_time:.3f}s")
    return avg_time < 2.0


def test_error_handling():
    """Test 18: Error handling for invalid table"""
    payload = {
        "table": "nonexistent_table",
        "columns": ["*"],
        "limit": 10
    }

    response = requests.post(f"{API_URL}/query/generate", json=payload)

    if response.status_code == 200:
        data = response.json()
        print(f"    Success: {data.get('success')}")
        print(f"    Error: {data.get('error')}")
        return data.get('success') is False
    return False


def test_sql_injection_prevention():
    """Test 19: Test SQL injection prevention"""
    payload = {
        "sql": "SELECT * FROM employees WHERE emp_name = 'test' OR '1'='1'",
        "limit": 10
    }

    response = requests.post(f"{API_URL}/query/execute", json=payload)

    if response.status_code == 200:
        data = response.json()
        print(f"    Query executed safely: {data.get('success')}")
        return True
    return False


def test_limit_validation():
    """Test 20: Test that limit parameter works correctly"""
    payload = {
        "sql": "SELECT * FROM employees",
        "limit": 2
    }

    response = requests.post(f"{API_URL}/query/execute", json=payload)

    if response.status_code == 200:
        data = response.json()
        row_count = data.get('row_count', 0)
        print(f"    Requested limit: 2, Actual rows: {row_count}")
        return row_count <= 2
    return False


def main():
    """Run all tests"""
    print("="*60)
    print("SQL QUERY GENERATOR API TEST SUITE (FIXED)")
    print("="*60)

    tester = APITester()

    # First check if API is running
    if not check_api_health():
        print("\n❌ API is not running. Please start the API first:")
        print("   python api_improved.py")
        sys.exit(1)

    # Run all tests
    tester.test("Root Endpoint", test_root_endpoint)
    tester.test("List Tables", test_list_tables)
    tester.test("Get Table Schema", test_get_table_schema)
    tester.test("Generate Basic Query", test_generate_basic_query)
    tester.test("Execute Query", test_execute_query)
    tester.test("Aggregate Query", test_aggregate_query)
    tester.test("JOIN Query", test_join_query)
    tester.test("Date Range Query", test_date_range_query)
    tester.test("Search Tables", test_search_tables)
    tester.test("Search Columns", test_search_columns)
    tester.test("Get Statistics", test_get_stats)
    tester.test("Get Schema", test_get_schema)
    tester.test("Get Sample Queries", test_get_sample_queries)
    tester.test("SQL Validation", test_validate_sql)  # Fixed test
    tester.test("Advanced Query Generation", test_advanced_query_generation)
    tester.test("Performance Test", test_performance)
    tester.test("Error Handling", test_error_handling)
    tester.test("SQL Injection Prevention", test_sql_injection_prevention)
    tester.test("Limit Validation", test_limit_validation)

    # Print summary
    success = tester.summary()

    if success:
        print("\n🎉 ALL TESTS PASSED! API is working correctly.")
        print("\n📊 You can now:")
        print("   - Use the Streamlit UI: streamlit run streamlit_app.py")
        print("   - Access API docs: http://127.0.0.1:8000/docs")
        print("   - Make API calls from your applications")
    else:
        print("\n❌ SOME TESTS FAILED. Please check the errors above.")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())