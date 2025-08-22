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
    
    schema->add_table(users_table);
    return schema;
}

int main() {
    try {
        std::cout << "=== Debug Column Error Handling ===" << std::endl;
        
        auto schema = create_integration_schema();
        QueryPlanner planner(schema);
        
        // Test query with non-existent column
        const std::string query = "SELECT nonexistent_column FROM users";
        std::cout << "Query: " << query << std::endl;
        
        auto result = planner.bind_and_plan(query);
        std::cout << "bind_and_plan success: " << result.success << std::endl;
        
        if (result.success) {
            std::cout << "❌ Query unexpectedly succeeded - should have failed for nonexistent column" << std::endl;
            if (result.bound_statement) {
                auto select_stmt = std::static_pointer_cast<BoundSelect>(result.bound_statement);
                std::cout << "Select list size: " << select_stmt->select_list.size() << std::endl;
                for (size_t i = 0; i < select_stmt->select_list.size(); ++i) {
                    const auto& expr = select_stmt->select_list[i];
                    std::cout << "  [" << i << "] Type: " << static_cast<int>(expr->type) 
                              << ", Original: '" << expr->original_text << "'" << std::endl;
                }
            }
        } else {
            std::cout << "✅ Query correctly failed" << std::endl;
        }
        
        std::cout << "Errors (" << result.errors.size() << "):" << std::endl;
        for (const auto& error : result.errors) {
            std::cout << "  - " << error << std::endl;
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Exception: " << e.what() << std::endl;
        return 1;
    }
}