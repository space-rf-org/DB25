#include "bound_statement.hpp"
#include "query_planner.hpp"
#include "schema_registry.hpp"
#include <iostream>
#include <cassert>
#include <chrono>

using namespace db25;

class BoundStatementIntegrationTester {
public:
    void run_all_tests() {
        std::cout << "=== Running BoundStatement → LogicalPlan Integration Tests ===" << std::endl;
        
        test_bind_and_plan_workflow();
        test_select_statement_integration();
        test_schema_aware_table_scan();
        test_expression_conversion();
        test_complex_select_with_where();
        test_performance_comparison();
        test_error_handling();
        
        std::cout << "✅ All BoundStatement integration tests passed!" << std::endl;
    }

private:
    void test_bind_and_plan_workflow() {
        std::cout << "Testing integrated bind-and-plan workflow..." << std::endl;
        
        auto schema = create_test_schema();
        QueryPlanner planner(schema);
        
        const std::string query = "SELECT id, name FROM users WHERE id = 42";
        
        auto result = planner.bind_and_plan(query);
        
        assert(result.success);
        assert(result.bound_statement != nullptr);
        assert(result.logical_plan.root != nullptr);
        assert(result.errors.empty());
        
        // Check that bound statement is SELECT type
        assert(result.bound_statement->statement_type == BoundStatement::Type::SELECT);
        
        // Check that logical plan has expected structure
        auto select_stmt = std::static_pointer_cast<BoundSelect>(result.bound_statement);
        assert(select_stmt->from_table != nullptr);
        assert(select_stmt->from_table->table_name == "users");
        
        std::cout << "  ✓ Integrated bind-and-plan workflow works" << std::endl;
    }
    
    void test_select_statement_integration() {
        std::cout << "Testing SELECT statement integration..." << std::endl;
        
        auto schema = create_test_schema();
        SchemaBinder binder(schema);
        QueryPlanner planner(schema);
        
        const std::string query = "SELECT id, name, email FROM users";
        
        // Step 1: Bind to BoundStatement
        auto bound_stmt = binder.bind(query);
        assert(bound_stmt != nullptr);
        
        auto select_stmt = std::static_pointer_cast<BoundSelect>(bound_stmt);
        assert(select_stmt->select_list.size() == 3); // id, name, email
        assert(select_stmt->from_table != nullptr);
        assert(select_stmt->from_table->table_id != 0); // Schema-resolved
        
        // Step 2: Convert to LogicalPlan
        auto logical_plan = planner.create_plan_from_bound_statement(bound_stmt);
        assert(logical_plan.root != nullptr);
        
        // Check plan structure
        assert(logical_plan.root->type == PlanNodeType::PROJECTION);
        assert(!logical_plan.root->children.empty());
        
        auto scan_node = logical_plan.root->children[0];
        assert(scan_node->type == PlanNodeType::TABLE_SCAN);
        
        auto table_scan = std::static_pointer_cast<TableScanNode>(scan_node);
        assert(table_scan->table_name == "users");
        assert(table_scan->table_id.has_value()); // Schema-enhanced
        
        std::cout << "  ✓ SELECT statement integration works" << std::endl;
    }
    
    void test_schema_aware_table_scan() {
        std::cout << "Testing schema-aware table scan..." << std::endl;
        
        auto schema = create_test_schema();
        QueryPlanner planner(schema);
        
        auto result = planner.bind_and_plan("SELECT * FROM users");
        assert(result.success);
        
        // Navigate to the table scan node
        auto current = result.logical_plan.root;
        while (current && current->type != PlanNodeType::TABLE_SCAN) {
            assert(!current->children.empty());
            current = current->children[0];
        }
        
        assert(current != nullptr);
        assert(current->type == PlanNodeType::TABLE_SCAN);
        
        auto table_scan = std::static_pointer_cast<TableScanNode>(current);
        
        // Check schema-enhanced features
        assert(table_scan->table_id.has_value());
        assert(table_scan->table_id.value() != 0);
        assert(!table_scan->projected_columns.empty());
        assert(!table_scan->column_name_to_id.empty());
        assert(!table_scan->output_columns.empty());
        
        std::cout << "  ✓ Schema-aware table scan works" << std::endl;
    }
    
    void test_expression_conversion() {
        std::cout << "Testing expression conversion..." << std::endl;
        
        auto schema = create_test_schema();
        QueryPlanner planner(schema);
        
        auto result = planner.bind_and_plan("SELECT id + 1, 'hello', name FROM users");
        assert(result.success);
        
        auto select_stmt = std::static_pointer_cast<BoundSelect>(result.bound_statement);
        assert(select_stmt->select_list.size() == 3);
        
        // Check expression types
        assert(select_stmt->select_list[0]->type == BoundExpressionType::BINARY_OP); // id + 1
        assert(select_stmt->select_list[1]->type == BoundExpressionType::CONSTANT);  // 'hello'
        assert(select_stmt->select_list[2]->type == BoundExpressionType::COLUMN_REF); // name
        
        // Check that logical plan conversion preserves expression types
        auto logical_plan = result.logical_plan;
        assert(logical_plan.root->type == PlanNodeType::PROJECTION);
        
        auto projection_node = std::static_pointer_cast<ProjectionNode>(logical_plan.root);
        assert(projection_node->projections.size() == 3);
        
        std::cout << "  ✓ Expression conversion works" << std::endl;
    }
    
    void test_complex_select_with_where() {
        std::cout << "Testing complex SELECT with WHERE clause..." << std::endl;
        
        auto schema = create_test_schema();
        QueryPlanner planner(schema);
        
        auto result = planner.bind_and_plan("SELECT id, name FROM users WHERE id > 10 AND name LIKE 'A%'");
        assert(result.success);
        
        auto select_stmt = std::static_pointer_cast<BoundSelect>(result.bound_statement);
        assert(select_stmt->where_clause != nullptr);
        assert(select_stmt->where_clause->type == BoundExpressionType::BINARY_OP); // AND
        
        // Check logical plan structure
        auto logical_plan = result.logical_plan;
        
        // Should have: Selection → Projection → TableScan (efficient: filter first, then project)
        assert(logical_plan.root->type == PlanNodeType::SELECTION);
        assert(!logical_plan.root->children.empty());
        
        auto projection_node = logical_plan.root->children[0];
        assert(projection_node->type == PlanNodeType::PROJECTION);
        assert(!projection_node->children.empty());
        
        auto table_scan = projection_node->children[0];
        assert(table_scan->type == PlanNodeType::TABLE_SCAN);
        
        std::cout << "  ✓ Complex SELECT with WHERE works" << std::endl;
    }
    
    void test_performance_comparison() {
        std::cout << "Testing performance comparison..." << std::endl;
        
        auto schema = create_test_schema();
        
        const std::string query = "SELECT id, name FROM users WHERE id = 42";
        const int iterations = 1000;
        
        // Test old approach (string-based)
        QueryPlanner old_planner(schema);
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; ++i) {
            auto plan = old_planner.create_plan(query);
            (void)plan; // Suppress unused warning
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto old_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        // Test new approach (BoundStatement-based)
        QueryPlanner new_planner(schema);
        start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; ++i) {
            auto result = new_planner.bind_and_plan(query);
            (void)result; // Suppress unused warning
        }
        end = std::chrono::high_resolution_clock::now();
        auto new_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        std::cout << "  ✓ String-based approach: " << old_time.count() << "μs for " << iterations << " iterations" << std::endl;
        std::cout << "  ✓ BoundStatement approach: " << new_time.count() << "μs for " << iterations << " iterations" << std::endl;
        
        // The new approach might be slower initially due to more thorough validation,
        // but provides better error messages and type safety
        
        std::cout << "  ✓ Performance comparison completed" << std::endl;
    }
    
    void test_error_handling() {
        std::cout << "Testing enhanced error handling..." << std::endl;
        
        auto schema = create_test_schema();
        QueryPlanner planner(schema);
        
        // Test table not found
        auto result1 = planner.bind_and_plan("SELECT id FROM nonexistent_table");
        assert(!result1.success);
        assert(!result1.errors.empty());
        
        bool found_table_error = false;
        for (const auto& error : result1.errors) {
            if (error.find("not found") != std::string::npos) {
                found_table_error = true;
                break;
            }
        }
        assert(found_table_error);
        
        // Test column not found
        auto result2 = planner.bind_and_plan("SELECT nonexistent_column FROM users");
        assert(!result2.success);
        assert(!result2.errors.empty());
        
        bool found_column_error = false;
        for (const auto& error : result2.errors) {
            if (error.find("nonexistent_column") != std::string::npos) {
                found_column_error = true;
                break;
            }
        }
        assert(found_column_error);
        
        std::cout << "  ✓ Enhanced error handling works" << std::endl;
    }
    
    std::shared_ptr<DatabaseSchema> create_test_schema() {
        auto schema = std::make_shared<DatabaseSchema>("integration_test_db");
        
        // Users table
        Table users_table;
        users_table.name = "users";
        users_table.columns = {
            {"id", ColumnType::INTEGER, 0, false, true, true, "", "", ""},
            {"name", ColumnType::VARCHAR, 100, false, false, false, "", "", ""},
            {"email", ColumnType::VARCHAR, 255, false, false, true, "", "", ""},
            {"created_at", ColumnType::TIMESTAMP, 0, true, false, false, "NOW()", "", ""}
        };
        users_table.indexes = {
            {"users_pkey", {"id"}, true, "BTREE"},
            {"users_email_idx", {"email"}, true, "BTREE"}
        };
        
        // Orders table
        Table orders_table;
        orders_table.name = "orders";
        orders_table.columns = {
            {"id", ColumnType::INTEGER, 0, false, true, true, "", "", ""},
            {"user_id", ColumnType::INTEGER, 0, false, false, false, "", "users", "id"},
            {"total", ColumnType::DECIMAL, 0, false, false, false, "", "", ""},
            {"created_at", ColumnType::TIMESTAMP, 0, true, false, false, "NOW()", "", ""}
        };
        orders_table.indexes = {
            {"orders_pkey", {"id"}, true, "BTREE"},
            {"orders_user_id_idx", {"user_id"}, false, "BTREE"}
        };
        
        schema->add_table(users_table);
        schema->add_table(orders_table);
        
        return schema;
    }
};

int main() {
    try {
        BoundStatementIntegrationTester tester;
        tester.run_all_tests();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Integration test failed: " << e.what() << std::endl;
        return 1;
    }
}