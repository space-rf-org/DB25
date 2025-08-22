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
        std::cout << "=== Integration Test Verbose Debug ===" << std::endl;
        
        auto schema = create_integration_schema();
        SchemaBinder binder(schema);
        QueryPlanner planner(schema);
        
        const std::string query = "SELECT id, name, email FROM users";
        std::cout << "Query: " << query << std::endl;
        
        // Step 1: Direct SchemaBinder test (same as debug_integration_issue.cpp)
        std::cout << "\n=== Step 1: Direct SchemaBinder Test ===\n" << std::endl;
        auto bound_stmt = binder.bind(query);
        if (!bound_stmt) {
            std::cout << "❌ Direct binding failed. Errors:" << std::endl;
            for (const auto& error : binder.get_errors()) {
                std::cout << "  - " << error.message << std::endl;
            }
            return 1;
        }
        
        auto select_stmt = std::static_pointer_cast<BoundSelect>(bound_stmt);
        std::cout << "✅ Direct binding succeeded!" << std::endl;
        std::cout << "Select list size: " << select_stmt->select_list.size() << std::endl;
        std::cout << "From table: " << (select_stmt->from_table ? select_stmt->from_table->table_name : "NULL") << std::endl;
        std::cout << "From table ID: " << (select_stmt->from_table ? std::to_string(select_stmt->from_table->table_id) : "NULL") << std::endl;
        
        // Show each bound expression
        for (size_t i = 0; i < select_stmt->select_list.size(); ++i) {
            const auto& expr = select_stmt->select_list[i];
            std::cout << "  [" << i << "] Type: " << static_cast<int>(expr->type) 
                      << ", Original: '" << expr->original_text << "'" << std::endl;
        }
        
        // Step 2: Test exact same thing as integration test fails on
        std::cout << "\n=== Step 2: Reproducing Integration Test Scenario ===" << std::endl;
        
        // Use fresh objects like the integration test
        auto fresh_schema = create_integration_schema();  
        SchemaBinder fresh_binder(fresh_schema);
        QueryPlanner fresh_planner(fresh_schema);
        
        auto fresh_bound_stmt = fresh_binder.bind(query);
        std::cout << "Fresh binding result: " << (fresh_bound_stmt ? "SUCCESS" : "FAILED") << std::endl;
        
        if (fresh_bound_stmt) {
            auto fresh_select_stmt = std::static_pointer_cast<BoundSelect>(fresh_bound_stmt);
            std::cout << "Fresh select list size: " << fresh_select_stmt->select_list.size() << std::endl;
            
            // This is the exact assertion that fails:
            std::cout << "Testing assertion: select_list.size() == 3" << std::endl;
            std::cout << "Actual size: " << fresh_select_stmt->select_list.size() << std::endl;
            std::cout << "Assertion would " << (fresh_select_stmt->select_list.size() == 3 ? "PASS" : "FAIL") << std::endl;
            
            if (fresh_select_stmt->select_list.size() != 3) {
                std::cout << "❌ Size mismatch! Let's check errors:" << std::endl;
                for (const auto& error : fresh_binder.get_errors()) {
                    std::cout << "  - " << error.message << std::endl;
                }
            }
        } else {
            std::cout << "❌ Fresh binding failed. Errors:" << std::endl;
            for (const auto& error : fresh_binder.get_errors()) {
                std::cout << "  - " << error.message << std::endl;
            }
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Exception: " << e.what() << std::endl;
        return 1;
    }
}