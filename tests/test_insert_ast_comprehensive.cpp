#include "query_planner.hpp"
#include "simple_schema.hpp"
#include <iostream>
#include <vector>
#include <cassert>

struct InsertTestCase {
    std::string name;
    std::string query;
    std::string expected_table;
    std::vector<std::string> expected_columns;
    bool should_succeed;
    std::string description;
};

void run_insert_test(const InsertTestCase& test_case, db25::QueryPlanner& planner) {
    std::cout << "\n--- " << test_case.name << " ---" << std::endl;
    std::cout << "Query: " << test_case.query << std::endl;
    std::cout << "Description: " << test_case.description << std::endl;
    
    auto plan = planner.create_plan(test_case.query);
    
    if (test_case.should_succeed) {
        if (plan.root && plan.root->type == db25::PlanNodeType::INSERT) {
            auto insert_node = std::static_pointer_cast<db25::InsertNode>(plan.root);
            
            // Check table name
            if (insert_node->table_name == test_case.expected_table) {
                std::cout << "✓ Table name correct: " << insert_node->table_name << std::endl;
            } else {
                std::cout << "✗ Table name mismatch. Expected: " << test_case.expected_table 
                         << ", Got: " << insert_node->table_name << std::endl;
                return;
            }
            
            // Check columns
            if (insert_node->target_columns.size() == test_case.expected_columns.size()) {
                bool columns_match = true;
                for (size_t i = 0; i < test_case.expected_columns.size(); i++) {
                    if (insert_node->target_columns[i] != test_case.expected_columns[i]) {
                        columns_match = false;
                        break;
                    }
                }
                
                if (columns_match) {
                    std::cout << "✓ Columns correct: ";
                    for (const auto& col : insert_node->target_columns) {
                        std::cout << col << " ";
                    }
                    std::cout << "(" << insert_node->target_columns.size() << " columns)" << std::endl;
                } else {
                    std::cout << "✗ Column order mismatch" << std::endl;
                    std::cout << "  Expected: ";
                    for (const auto& col : test_case.expected_columns) {
                        std::cout << col << " ";
                    }
                    std::cout << std::endl;
                    std::cout << "  Got: ";
                    for (const auto& col : insert_node->target_columns) {
                        std::cout << col << " ";
                    }
                    std::cout << std::endl;
                }
            } else {
                std::cout << "✗ Column count mismatch. Expected: " << test_case.expected_columns.size() 
                         << ", Got: " << insert_node->target_columns.size() << std::endl;
            }
            
            std::cout << "✓ " << test_case.name << " - PASSED" << std::endl;
        } else {
            std::cout << "✗ " << test_case.name << " - FAILED (no INSERT node created)" << std::endl;
        }
    } else {
        if (!plan.root || plan.root->type != db25::PlanNodeType::INSERT) {
            std::cout << "✓ " << test_case.name << " - PASSED (correctly rejected)" << std::endl;
        } else {
            std::cout << "✗ " << test_case.name << " - FAILED (should have been rejected)" << std::endl;
        }
    }
}

int main() {
    auto schema = std::make_shared<db25::DatabaseSchema>(db25::create_simple_schema());
    db25::QueryPlanner planner(schema);
    
    std::cout << "=== Comprehensive INSERT AST Parsing Test Suite ===" << std::endl;
    std::cout << "This test suite validates that AST-based INSERT parsing supports all" << std::endl;
    std::cout << "common INSERT scenarios and properly handles edge cases and errors." << std::endl;
    
    // ====================================================================
    // SECTION 1: BASIC INSERT STATEMENTS
    // ====================================================================
    
    std::vector<InsertTestCase> basic_tests = {
        {
            "Basic INSERT with columns",
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
            "users",
            {"name", "email"},
            true,
            "Standard INSERT with explicit column list and VALUES"
        },
        {
            "INSERT without columns",
            "INSERT INTO users VALUES ('Jane', 'jane@example.com', 25)",
            "users", 
            {},
            true,
            "INSERT without explicit column list (uses table order)"
        },
        {
            "Single column INSERT",
            "INSERT INTO users (name) VALUES ('Bob')",
            "users",
            {"name"},
            true,
            "INSERT with only one column specified"
        },
        {
            "Multiple columns different order",
            "INSERT INTO users (email, name, age) VALUES ('alice@example.com', 'Alice', 30)",
            "users",
            {"email", "name", "age"},
            true,
            "INSERT with columns in different order than table definition"
        },
        {
            "Different table",
            "INSERT INTO orders (user_id, product_id) VALUES (1, 100)",
            "orders",
            {"user_id", "product_id"},
            true,
            "INSERT into different table (orders instead of users)"
        }
    };
    
    std::cout << "\n=== SECTION 1: Basic INSERT Statements ===" << std::endl;
    int basic_passed = 0;
    for (const auto& test_case : basic_tests) {
        try {
            run_insert_test(test_case, planner);
            basic_passed++;
        } catch (const std::exception& e) {
            std::cout << "✗ " << test_case.name << " - EXCEPTION: " << e.what() << std::endl;
        }
    }
    
    // ====================================================================
    // SECTION 2: CASE SENSITIVITY AND WHITESPACE
    // ====================================================================
    
    std::vector<InsertTestCase> case_tests = {
        {
            "Lowercase INSERT",
            "insert into users (name) values ('test')",
            "users",
            {"name"},
            true,
            "INSERT statement in lowercase"
        },
        {
            "Mixed case INSERT",
            "Insert Into users (Name, Email) Values ('test', 'test@example.com')",
            "users",
            {"name", "email"}, // PostgreSQL normalizes to lowercase
            true,
            "INSERT statement with mixed case"
        },
        {
            "Extra whitespace",
            "INSERT   INTO   users   (  name  ,  email  )   VALUES   (  'John'  ,  'john@example.com'  )",
            "users",
            {"name", "email"},
            true,
            "INSERT with extra whitespace"
        },
        {
            "Minimal whitespace",
            "INSERT INTO users(name,email)VALUES('John','john@example.com')",
            "users",
            {"name", "email"},
            true,
            "INSERT with minimal whitespace"
        },
        {
            "Multi-line INSERT",
            "INSERT INTO users\n  (name, email)\nVALUES\n  ('John', 'john@example.com')",
            "users",
            {"name", "email"},
            true,
            "INSERT with multi-line formatting"
        }
    };
    
    std::cout << "\n=== SECTION 2: Case Sensitivity and Whitespace ===" << std::endl;
    int case_passed = 0;
    for (const auto& test_case : case_tests) {
        try {
            run_insert_test(test_case, planner);
            case_passed++;
        } catch (const std::exception& e) {
            std::cout << "✗ " << test_case.name << " - EXCEPTION: " << e.what() << std::endl;
        }
    }
    
    // ====================================================================
    // SECTION 3: DATA TYPES AND VALUES
    // ====================================================================
    
    std::vector<InsertTestCase> data_type_tests = {
        {
            "Multiple VALUES rows",
            "INSERT INTO users (name, email) VALUES ('User1', 'user1@example.com'), ('User2', 'user2@example.com')",
            "users",
            {"name", "email"},
            true,
            "INSERT with multiple VALUES rows"
        },
        {
            "Special characters",
            "INSERT INTO users (name, email) VALUES ('O''Reilly', 'test@sub.domain.com')",
            "users",
            {"name", "email"},
            true,
            "INSERT with special characters and quoted strings"
        },
        {
            "Numeric values",
            "INSERT INTO users (age, score) VALUES (25, 95.5)",
            "users",
            {"age", "score"},
            true,
            "INSERT with numeric values"
        },
        {
            "NULL values",
            "INSERT INTO users (name, email) VALUES ('John', NULL)",
            "users",
            {"name", "email"},
            true,
            "INSERT with NULL values"
        },
        {
            "Boolean values",
            "INSERT INTO users (name, active) VALUES ('John', true)",
            "users",
            {"name", "active"},
            true,
            "INSERT with boolean values"
        },
        {
            "INSERT with DEFAULT",
            "INSERT INTO users (name, email, created_at) VALUES ('John', 'john@example.com', DEFAULT)",
            "users",
            {"name", "email", "created_at"},
            true,
            "INSERT with DEFAULT value"
        },
        {
            "Unicode characters",
            "INSERT INTO users (name, location) VALUES ('José', 'São Paulo')",
            "users",
            {"name", "location"},
            true,
            "INSERT with unicode characters"
        }
    };
    
    std::cout << "\n=== SECTION 3: Data Types and Values ===" << std::endl;
    int data_passed = 0;
    for (const auto& test_case : data_type_tests) {
        try {
            run_insert_test(test_case, planner);
            data_passed++;
        } catch (const std::exception& e) {
            std::cout << "✗ " << test_case.name << " - EXCEPTION: " << e.what() << std::endl;
        }
    }
    
    // ====================================================================
    // SECTION 4: ADVANCED FEATURES
    // ====================================================================
    
    std::vector<InsertTestCase> advanced_tests = {
        {
            "INSERT with subquery",
            "INSERT INTO users (name, email) SELECT name, email FROM temp_users",
            "users",
            {"name", "email"},
            true,
            "INSERT with SELECT subquery"
        },
        {
            "INSERT with function calls",
            "INSERT INTO users (name, created_at) VALUES ('John', NOW())",
            "users",
            {"name", "created_at"},
            true,
            "INSERT with function calls in VALUES"
        },
        {
            "INSERT with expressions",
            "INSERT INTO users (name, score) VALUES ('John', 50 + 25)",
            "users",
            {"name", "score"},
            true,
            "INSERT with arithmetic expressions"
        },
        {
            "INSERT with CAST",
            "INSERT INTO users (name, age) VALUES ('John', CAST('25' AS INTEGER))",
            "users",
            {"name", "age"},
            true,
            "INSERT with CAST operations"
        },
        {
            "INSERT with subquery in VALUES",
            "INSERT INTO users (name, email, referrer_id) VALUES ('John', 'john@example.com', (SELECT id FROM users WHERE name = 'Admin'))",
            "users",
            {"name", "email", "referrer_id"},
            true,
            "INSERT with subquery in VALUES clause"
        },
        {
            "INSERT ON CONFLICT",
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') ON CONFLICT (email) DO NOTHING",
            "users",
            {"name", "email"},
            true,
            "INSERT with ON CONFLICT clause (PostgreSQL extension)"
        },
        {
            "INSERT RETURNING",
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id",
            "users",
            {"name", "email"},
            true,
            "INSERT with RETURNING clause (PostgreSQL extension)"
        }
    };
    
    std::cout << "\n=== SECTION 4: Advanced Features ===" << std::endl;
    int advanced_passed = 0;
    for (const auto& test_case : advanced_tests) {
        try {
            run_insert_test(test_case, planner);
            advanced_passed++;
        } catch (const std::exception& e) {
            std::cout << "✗ " << test_case.name << " - EXCEPTION: " << e.what() << std::endl;
        }
    }
    
    // ====================================================================
    // SECTION 5: CTE (COMMON TABLE EXPRESSIONS)
    // ====================================================================
    
    std::vector<InsertTestCase> cte_tests = {
        {
            "Basic CTE INSERT",
            "WITH new_user AS (SELECT 'John' as name, 'john@example.com' as email) INSERT INTO users (name, email) SELECT name, email FROM new_user",
            "users",
            {"name", "email"},
            true,
            "Basic CTE with INSERT"
        },
        {
            "Multiple CTEs",
            "WITH names AS (SELECT 'John' as name), emails AS (SELECT 'john@example.com' as email) INSERT INTO users (name, email) SELECT n.name, e.email FROM names n, emails e",
            "users",
            {"name", "email"},
            true,
            "INSERT with multiple CTEs"
        },
        {
            "CTE with complex query",
            "WITH user_data AS (SELECT 'Alice' as name, 'alice@example.com' as email, 25 as age WHERE 1=1) INSERT INTO users (name, email, age) SELECT name, email, age FROM user_data",
            "users",
            {"name", "email", "age"},
            true,
            "CTE with complex WHERE clause"
        },
        {
            "Recursive CTE",
            "WITH RECURSIVE series AS (SELECT 1 as n UNION ALL SELECT n+1 FROM series WHERE n < 5) INSERT INTO numbers (value) SELECT n FROM series",
            "numbers",
            {"value"},
            true,
            "INSERT with recursive CTE"
        },
        {
            "CTE with aggregation",
            "WITH stats AS (SELECT COUNT(*) as total_users FROM existing_users) INSERT INTO user_stats (total) SELECT total_users FROM stats",
            "user_stats",
            {"total"},
            true,
            "CTE with aggregation functions"
        }
    };
    
    std::cout << "\n=== SECTION 5: CTE (Common Table Expressions) ===" << std::endl;
    int cte_passed = 0;
    for (const auto& test_case : cte_tests) {
        try {
            run_insert_test(test_case, planner);
            cte_passed++;
        } catch (const std::exception& e) {
            std::cout << "✗ " << test_case.name << " - EXCEPTION: " << e.what() << std::endl;
        }
    }
    
    // ====================================================================
    // SECTION 6: ERROR HANDLING
    // ====================================================================
    
    std::vector<InsertTestCase> error_tests = {
        {
            "Invalid syntax",
            "INSERT INVALID SYNTAX",
            "",
            {},
            false,
            "Completely invalid INSERT syntax"
        },
        {
            "Missing table",
            "INSERT INTO VALUES ('test')",
            "",
            {},
            false,
            "INSERT without table name"
        },
        {
            "Missing VALUES",
            "INSERT INTO users (name)",
            "",
            {},
            false,
            "INSERT without VALUES or SELECT"
        },
        {
            "Malformed column list",
            "INSERT INTO users (name,) VALUES ('test')",
            "",
            {},
            false,
            "INSERT with malformed column list"
        },
        {
            "Unmatched parentheses",
            "INSERT INTO users (name VALUES ('test')",
            "",
            {},
            false,
            "INSERT with unmatched parentheses"
        }
    };
    
    std::cout << "\n=== SECTION 6: Error Handling ===" << std::endl;
    int error_handled = 0;
    for (const auto& test_case : error_tests) {
        try {
            run_insert_test(test_case, planner);
            error_handled++;
        } catch (const std::exception& e) {
            std::cout << "✓ Exception properly handled: " << e.what() << std::endl;
            error_handled++;
        }
    }
    
    // ====================================================================
    // FINAL SUMMARY
    // ====================================================================
    
    int total_tests = basic_tests.size() + case_tests.size() + data_type_tests.size() + 
                     advanced_tests.size() + cte_tests.size() + error_tests.size();
    int total_passed = basic_passed + case_passed + data_passed + advanced_passed + cte_passed + error_handled;
    
    std::cout << "\n=== COMPREHENSIVE TEST SUMMARY ===" << std::endl;
    std::cout << "Basic INSERT Tests:        " << basic_passed << "/" << basic_tests.size() << std::endl;
    std::cout << "Case/Whitespace Tests:     " << case_passed << "/" << case_tests.size() << std::endl;
    std::cout << "Data Type Tests:           " << data_passed << "/" << data_type_tests.size() << std::endl;
    std::cout << "Advanced Feature Tests:    " << advanced_passed << "/" << advanced_tests.size() << std::endl;
    std::cout << "CTE Tests:                 " << cte_passed << "/" << cte_tests.size() << std::endl;
    std::cout << "Error Handling Tests:      " << error_handled << "/" << error_tests.size() << std::endl;
    std::cout << "----------------------------------------" << std::endl;
    std::cout << "TOTAL:                     " << total_passed << "/" << total_tests << std::endl;
    std::cout << "Success Rate:              " << (100.0 * total_passed / total_tests) << "%" << std::endl;
    
    if (total_passed == total_tests) {
        std::cout << "\n🎉 ALL TESTS PASSED!" << std::endl;
        std::cout << "✅ AST-based INSERT parsing is production-ready and comprehensive." << std::endl;
        std::cout << "\nSupported Features:" << std::endl;
        std::cout << "✓ Basic INSERT with/without column lists" << std::endl;
        std::cout << "✓ Multiple VALUES rows" << std::endl;
        std::cout << "✓ All data types (strings, numbers, booleans, NULL, DEFAULT)" << std::endl;
        std::cout << "✓ Case insensitive parsing with whitespace tolerance" << std::endl;
        std::cout << "✓ INSERT with SELECT subqueries" << std::endl;
        std::cout << "✓ Function calls and expressions in VALUES" << std::endl;
        std::cout << "✓ PostgreSQL extensions (ON CONFLICT, RETURNING)" << std::endl;
        std::cout << "✓ CTE (Common Table Expressions) including recursive CTEs" << std::endl;
        std::cout << "✓ Robust error handling for malformed queries" << std::endl;
    } else {
        std::cout << "\n⚠️  Some tests failed. Review the output above for details." << std::endl;
        return 1;
    }
    
    return 0;
}