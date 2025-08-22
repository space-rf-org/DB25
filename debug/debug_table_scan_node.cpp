#include "bound_statement.hpp"
#include "query_planner.hpp" 
#include "schema_registry.hpp"
#include <iostream>
#include <cassert>

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

int main() {
    try {
        std::cout << "=== Debug TableScanNode table_id Issue ===" << std::endl;
        
        auto schema = create_integration_schema();
        SchemaBinder binder(schema);
        QueryPlanner planner(schema);
        
        const std::string query = "SELECT id, name, email FROM users";
        std::cout << "Query: " << query << std::endl;
        
        // Step 1: Check bound statement
        auto bound_stmt = binder.bind(query);
        if (!bound_stmt) {
            std::cout << "❌ Binding failed" << std::endl;
            return 1;
        }
        
        auto select_stmt = std::static_pointer_cast<BoundSelect>(bound_stmt);
        std::cout << "✅ Bound statement created" << std::endl;
        std::cout << "From table name: " << select_stmt->from_table->table_name << std::endl;
        std::cout << "From table ID: " << select_stmt->from_table->table_id << std::endl;
        
        // Step 2: Convert to logical plan
        auto logical_plan = planner.create_plan_from_bound_statement(bound_stmt);
        if (!logical_plan.root) {
            std::cout << "❌ Logical plan creation failed" << std::endl;
            return 1;
        }
        
        std::cout << "✅ Logical plan created" << std::endl;
        std::cout << "Root node type: " << static_cast<int>(logical_plan.root->type) << std::endl;
        
        // Step 3: Navigate to table scan node
        auto current = logical_plan.root;
        while (current && current->type != PlanNodeType::TABLE_SCAN) {
            std::cout << "Navigating through node type: " << static_cast<int>(current->type) << std::endl;
            if (current->children.empty()) {
                std::cout << "❌ No children to navigate to" << std::endl;
                return 1;
            }
            current = current->children[0];
        }
        
        if (!current) {
            std::cout << "❌ No table scan node found" << std::endl;
            return 1;
        }
        
        std::cout << "✅ Found table scan node" << std::endl;
        auto table_scan = std::static_pointer_cast<TableScanNode>(current);
        std::cout << "Table scan name: " << table_scan->table_name << std::endl;
        std::cout << "Table scan has table_id: " << table_scan->table_id.has_value() << std::endl;
        if (table_scan->table_id.has_value()) {
            std::cout << "Table scan table_id value: " << table_scan->table_id.value() << std::endl;
        }
        
        // This is the exact assertion that's failing
        if (table_scan->table_id.has_value()) {
            std::cout << "✅ ASSERTION WOULD PASS: table_id has value" << std::endl;
        } else {
            std::cout << "❌ ASSERTION WOULD FAIL: table_id has no value" << std::endl;
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Exception: " << e.what() << std::endl;
        return 1;
    }
}