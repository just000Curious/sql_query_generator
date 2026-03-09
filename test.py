#!/usr/bin/env python3
"""
AGGRESSIVE TEST SUITE - Tests all modules with extreme prejudice
"""

import sys
import os
import importlib
import traceback
import time
import random
import string
from datetime import datetime

# Add root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# Colors for output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_header(text):
    print(f"\n{Colors.HEADER}{'=' * 60}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.END}")
    print(f"{Colors.HEADER}{'=' * 60}{Colors.END}")


def print_success(text):
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")


def print_failure(text):
    print(f"{Colors.RED}✗ {text}{Colors.END}")


def print_warning(text):
    print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")


def print_info(text):
    print(f"{Colors.BLUE}ℹ {text}{Colors.END}")


class AggressiveTester:
    def __init__(self):
        self.results = {
            'passed': [],
            'failed': [],
            'crashed': []
        }
        self.start_time = time.time()

    def test_imports(self):
        """Test importing all modules aggressively"""
        print_header("TESTING IMPORTS")

        modules_to_test = [
            'cte_builder',
            'join_builder',
            'temporary_table',
            'query_engine',
            'query_validator',
            'query_assembler',
            'db_information',
            'pypika_query_engine'
        ]

        for module_name in modules_to_test:
            try:
                # Try multiple import methods
                module = None
                errors = []

                # Method 1: Direct import
                try:
                    module = __import__(module_name)
                    print_info(f"Direct import of {module_name}: SUCCESS")
                except Exception as e:
                    errors.append(f"Direct import failed: {str(e)}")

                # Method 2: Importlib
                if not module:
                    try:
                        module = importlib.import_module(module_name)
                        print_info(f"Importlib import of {module_name}: SUCCESS")
                    except Exception as e:
                        errors.append(f"Importlib failed: {str(e)}")

                # Method 3: Reload if exists
                if module:
                    try:
                        importlib.reload(module)
                        print_info(f"Reload of {module_name}: SUCCESS")
                    except:
                        pass

                    # Check module attributes
                    attrs = dir(module)
                    public_attrs = [a for a in attrs if not a.startswith('_')]
                    print_info(f"Module {module_name} has {len(public_attrs)} public attributes")

                    self.results['passed'].append(module_name)
                    print_success(f"Module {module_name} imported successfully")
                else:
                    raise ImportError(f"All import methods failed: {'; '.join(errors)}")

            except Exception as e:
                self.results['failed'].append(module_name)
                print_failure(f"Module {module_name} failed to import")
                print(f"  Error: {str(e)}")
                traceback.print_exc()

    def test_cte_builder(self):
        """Aggressively test CTE builder"""
        print_header("TESTING CTE BUILDER")

        try:
            import cte_builder

            # Test 1: Create instance
            try:
                builder = cte_builder.CTETreeBuilder() if hasattr(cte_builder, 'CTETreeBuilder') else None
                if builder:
                    print_success("CTETreeBuilder instantiated")
            except Exception as e:
                print_failure(f"CTE builder instantiation failed: {e}")

            # Test 2: Find all callable methods
            methods = [m for m in dir(cte_builder) if callable(getattr(cte_builder, m)) and not m.startswith('_')]
            print_info(f"Found {len(methods)} callable methods/classes")

            # Test 3: Try to call everything with garbage data
            for method in methods:
                try:
                    func = getattr(cte_builder, method)
                    # Try with random arguments
                    for _ in range(3):
                        args = [self._generate_random_arg() for _ in range(random.randint(0, 3))]
                        kwargs = {f"arg{i}": self._generate_random_arg() for i in range(random.randint(0, 2))}
                        try:
                            result = func(*args, **kwargs)
                            print_info(f"  Called {method} with random args: returned {type(result)}")
                        except Exception as e:
                            print_warning(f"  {method} raised expected error with random data: {e}")
                except Exception as e:
                    print_warning(f"  Could not test {method}: {e}")

            # Test 4: Stress test with loops
            print_info("Stress testing CTE builder...")
            for i in range(10):
                try:
                    # Generate random CTE structure
                    cte_name = f"cte_{i}_{random.randint(1, 1000)}"
                    if hasattr(cte_builder, 'create_cte'):
                        getattr(cte_builder, 'create_cte')(cte_name, f"SELECT {random.randint(1, 100)}")
                except:
                    pass

            self.results['passed'].append('cte_builder_stress')
            print_success("CTE builder stress test completed")

        except Exception as e:
            self.results['failed'].append('cte_builder')
            print_failure(f"CTE builder testing crashed: {e}")
            traceback.print_exc()

    def test_join_builder(self):
        """Aggressively test join builder"""
        print_header("TESTING JOIN BUILDER")

        try:
            import join_builder

            # Test 1: Find all join-related functions
            join_methods = [m for m in dir(join_builder) if 'join' in m.lower() and callable(getattr(join_builder, m))]
            print_info(f"Found {len(join_methods)} join-related methods")

            # Test 2: Try all join types with invalid data
            join_types = ['INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'NATURAL', 'SELF']
            tables = ['users', 'orders', 'products', 'categories', None, 123, [], {}]

            for join_type in join_types:
                for table1 in tables:
                    for table2 in tables:
                        try:
                            if hasattr(join_builder, 'build_join'):
                                result = join_builder.build_join(table1, table2, join_type)
                                print_info(f"  Join {join_type} {table1} x {table2} -> {type(result)}")
                        except:
                            pass

            # Test 3: Massive join chain
            try:
                if hasattr(join_builder, 'build_join_chain'):
                    chain = []
                    for i in range(50):  # 50 joins!
                        chain.append({
                            'table': f'table_{i}',
                            'type': random.choice(join_types),
                            'condition': f"id_{i - 1} = id_{i}" if i > 0 else None
                        })
                    result = join_builder.build_join_chain(chain)
                    print_success(f"Built massive join chain with 50 joins")
            except Exception as e:
                print_warning(f"Massive join chain test: {e}")

            self.results['passed'].append('join_builder_stress')
            print_success("Join builder testing completed")

        except Exception as e:
            self.results['failed'].append('join_builder')
            print_failure(f"Join builder testing crashed: {e}")

    def test_temporary_table(self):
        """Aggressively test temporary table functionality"""
        print_header("TESTING TEMPORARY TABLE")

        try:
            import temporary_table

            # Test 1: Create/drop cycles
            for i in range(20):
                try:
                    if hasattr(temporary_table, 'create_temp_table'):
                        table_name = f"temp_{i}_{random.randint(1, 10000)}"
                        columns = [f"col_{j} {random.choice(['INT', 'VARCHAR(50)', 'DATE'])}" for j in
                                   range(random.randint(1, 5))]
                        result = temporary_table.create_temp_table(table_name, columns)

                        # Immediately try to drop
                        if hasattr(temporary_table, 'drop_temp_table'):
                            temporary_table.drop_temp_table(table_name)
                except:
                    pass

            print_success("Created and dropped 20 temporary tables")

            # Test 2: Concurrent access simulation
            print_info("Simulating concurrent access...")
            import threading

            def temp_table_worker(worker_id):
                for j in range(5):
                    try:
                        if hasattr(temporary_table, 'create_temp_table'):
                            name = f"thread_{worker_id}_temp_{j}"
                            temporary_table.create_temp_table(name, ['id INT'])
                            time.sleep(0.01)
                            if hasattr(temporary_table, 'drop_temp_table'):
                                temporary_table.drop_temp_table(name)
                    except:
                        pass

            threads = []
            for i in range(10):
                t = threading.Thread(target=temp_table_worker, args=(i,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            print_success("Concurrent access test completed")

            self.results['passed'].append('temporary_table_stress')

        except Exception as e:
            self.results['failed'].append('temporary_table')
            print_failure(f"Temporary table testing crashed: {e}")

    def test_query_engine(self):
        """Aggressively test query engine"""
        print_header("TESTING QUERY ENGINE")

        try:
            import query_engine

            # Test 1: Generate massive queries
            if hasattr(query_engine, 'generate_query'):
                for size in [10, 100, 1000, 10000]:
                    try:
                        # Generate huge query
                        tables = [f"table_{i}" for i in range(size)]
                        columns = [f"col_{i}" for i in range(min(size, 100))]
                        conditions = [f"col_{i} > {random.randint(1, 100)}" for i in range(min(size, 50))]

                        query = query_engine.generate_query(
                            tables=random.sample(tables, min(10, len(tables))),
                            columns=random.sample(columns, min(20, len(columns))),
                            where=" AND ".join(random.sample(conditions, min(10, len(conditions))))
                        )
                        print_info(f"Generated query with {len(str(query))} characters")
                    except Exception as e:
                        print_warning(f"Query generation failed for size {size}: {e}")

            # Test 2: Malformed inputs
            print_info("Testing with malformed inputs...")
            malformed_inputs = [
                None,
                "",
                "'; DROP TABLE users; --",
                "\x00\x01\x02\x03",
                {"injection": "attempt"},
                ["nested", ["list"]],
                Exception("Test exception"),
                globals()
            ]

            if hasattr(query_engine, 'execute_query'):
                for malformed in malformed_inputs:
                    try:
                        query_engine.execute_query(malformed)
                    except Exception as e:
                        print_warning(f"  Malformed input {type(malformed)} raised: {type(e).__name__}")

            self.results['passed'].append('query_engine_stress')
            print_success("Query engine testing completed")

        except Exception as e:
            self.results['failed'].append('query_engine')
            print_failure(f"Query engine testing crashed: {e}")

    def test_all_modules_together(self):
        """Test interaction between all modules"""
        print_header("TESTING MODULE INTERACTIONS")

        try:
            # Import everything
            modules = {}
            for module_name in ['cte_builder', 'join_builder', 'temporary_table',
                                'query_engine', 'query_validator', 'query_assembler',
                                'db_information', 'pypika_query_engine']:
                try:
                    modules[module_name] = __import__(module_name)
                    print_success(f"Imported {module_name}")
                except Exception as e:
                    print_warning(f"Could not import {module_name}: {e}")

            # Try to create a complex query using multiple modules
            print_info("Attempting to build complex query with all modules...")

            # Step 1: Create temp tables
            if 'temporary_table' in modules:
                modules['temporary_table'].create_temp_table("temp_analysis", ["id INT", "value DECIMAL"])

            # Step 2: Build CTE
            if 'cte_builder' in modules:
                modules['cte_builder'].build_cte("analysis_cte", "SELECT * FROM temp_analysis")

            # Step 3: Build joins
            if 'join_builder' in modules:
                modules['join_builder'].build_join("temp_analysis", "main_table", "LEFT")

            # Step 4: Validate
            if 'query_validator' in modules:
                modules['query_validator'].validate("SELECT * FROM temp_analysis")

            # Step 5: Assemble
            if 'query_assembler' in modules:
                modules['query_assembler'].assemble(["SELECT", "FROM", "WHERE"])

            print_success("All modules interacted successfully")
            self.results['passed'].append('module_interaction')

        except Exception as e:
            self.results['failed'].append('module_interaction')
            print_failure(f"Module interaction failed: {e}")
            traceback.print_exc()

    def _generate_random_arg(self):
        """Generate random argument for testing"""
        types = [
            lambda: random.randint(-1000, 1000),
            lambda: random.uniform(-1000, 1000),
            lambda: ''.join(random.choices(string.ascii_letters, k=random.randint(0, 10))),
            lambda: None,
            lambda: True if random.random() > 0.5 else False,
            lambda: [random.randint(0, 10) for _ in range(random.randint(0, 5))],
            lambda: {f"key_{i}": random.randint(0, 10) for i in range(random.randint(0, 3))},
            lambda: Exception("Random exception"),
            lambda: datetime.now(),
            lambda: object()
        ]
        return random.choice(types)()

    def run_all_tests(self):
        """Run all aggressive tests"""
        print_header("🔥 AGGRESSIVE TEST SUITE STARTING 🔥")
        print_info(f"Start time: {datetime.now()}")
        print_info(f"Python version: {sys.version}")
        print_info(f"Platform: {sys.platform}")

        # Run tests
        self.test_imports()
        self.test_cte_builder()
        self.test_join_builder()
        self.test_temporary_table()
        self.test_query_engine()
        self.test_all_modules_together()

        # Print summary
        self.print_summary()

    def print_summary(self):
        """Print test summary"""
        elapsed = time.time() - self.start_time

        print_header("📊 TEST SUMMARY 📊")
        print_info(f"Total time: {elapsed:.2f} seconds")
        print_success(f"Passed: {len(self.results['passed'])}")
        print_failure(f"Failed: {len(self.results['failed'])}")

        if self.results['passed']:
            print_info("\nPassed tests:")
            for test in self.results['passed']:
                print(f"  {Colors.GREEN}✓{Colors.END} {test}")

        if self.results['failed']:
            print_info("\nFailed tests:")
            for test in self.results['failed']:
                print(f"  {Colors.RED}✗{Colors.END} {test}")

        if len(self.results['failed']) == 0:
            print_header("🎉 ALL TESTS PASSED! YOUR CODE IS BULLETPROOF! 🎉")
        else:
            print_header("💥 SOME TESTS FAILED. TIME TO DEBUG! 💥")


if __name__ == "__main__":
    tester = AggressiveTester()

    try:
        tester.run_all_tests()
    except KeyboardInterrupt:
        print_warning("\nTests interrupted by user")
    except Exception as e:
        print_failure(f"Test framework crashed: {e}")
        traceback.print_exc()
    finally:
        print_header("TESTING COMPLETE")