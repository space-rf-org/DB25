#include "bound_statement.hpp"
#include "query_planner.hpp"
#include "schema_registry.hpp"
#include <iostream>

using namespace db25;

std::shared_ptr<DatabaseSchema> create_integration_schema() {
    auto schema = std::make_shared<DatabaseSchema>("integration_test_db");
    
    // Users table (exact copy from test)
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

void print_plan_structure(const LogicalPlanNodePtr& node, int depth = 0) {
    if (!node) return;
    
    std::string indent(depth * 2, ' ');
    std::cout << indent << "Node type: " << static_cast<int>(node->type) << " (";
    
    switch (node->type) {
        case PlanNodeType::TABLE_SCAN: std::cout << "TABLE_SCAN"; break;
        case PlanNodeType::INDEX_SCAN: std::cout << "INDEX_SCAN"; break;
        case PlanNodeType::PROJECTION: std::cout << "PROJECTION"; break;
        case PlanNodeType::SELECTION: std::cout << "SELECTION"; break;
        case PlanNodeType::AGGREGATION: std::cout << "AGGREGATION"; break;
        case PlanNodeType::SORT: std::cout << "SORT"; break;
        case PlanNodeType::LIMIT: std::cout << "LIMIT"; break;
        default: std::cout << "OTHER"; break;
    }
    std::cout << ")" << std::endl;
    
    for (const auto& child : node->children) {
        print_plan_structure(child, depth + 1);
    }
}

int main() {
    try {
        std::cout << "=== Debug Complex WHERE Plan Structure ===" << std::endl;
        
        auto schema = create_integration_schema();
        QueryPlanner planner(schema);
        
        // The exact query from the failing test
        const std::string query = "SELECT id, name FROM users WHERE id > 10 AND name LIKE 'A%'";
        std::cout << "Query: " << query << std::endl;
        
        auto result = planner.bind_and_plan(query);
        std::cout << "bind_and_plan success: " << result.success << std::endl;
        
        if (!result.success) {
            std::cout << "❌ Binding failed. Errors:" << std::endl;
            for (const auto& error : result.errors) {
                std::cout << "  - " << error << std::endl;
            }
            return 1;
        }
        
        if (!result.logical_plan.root) {
            std::cout << "❌ No logical plan created" << std::endl;
            return 1;
        }
        
        std::cout << "✅ Logical plan created successfully" << std::endl;
        std::cout << "\nPlan structure:" << std::endl;
        print_plan_structure(result.logical_plan.root);
        
        std::cout << "\nExpected structure:" << std::endl;
        std::cout << "  PROJECTION" << std::endl;
        std::cout << "    SELECTION" << std::endl;
        std::cout << "      TABLE_SCAN" << std::endl;
        
        // Test expectations
        std::cout << "\nRoot node type: " << static_cast<int>(result.logical_plan.root->type) << std::endl;
        std::cout << "Expected PROJECTION: " << static_cast<int>(PlanNodeType::PROJECTION) << std::endl;
        std::cout << "Assertion would " << (result.logical_plan.root->type == PlanNodeType::PROJECTION ? "PASS" : "FAIL") << std::endl;
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Exception: " << e.what() << std::endl;
        return 1;
    }
}