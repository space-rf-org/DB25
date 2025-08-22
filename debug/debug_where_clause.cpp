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

int main() {
    try {
        std::cout << "=== Debug WHERE Clause Binding ===" << std::endl;
        
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
        
        if (!result.bound_statement) {
            std::cout << "❌ No bound statement" << std::endl;
            return 1;
        }
        
        auto select_stmt = std::static_pointer_cast<BoundSelect>(result.bound_statement);
        std::cout << "✅ Bound statement created" << std::endl;
        std::cout << "Has WHERE clause: " << (select_stmt->where_clause != nullptr) << std::endl;
        
        if (select_stmt->where_clause) {
            std::cout << "WHERE clause type: " << static_cast<int>(select_stmt->where_clause->type) << std::endl;
            std::cout << "WHERE clause original_text: '" << select_stmt->where_clause->original_text << "'" << std::endl;
            
            // Expected types:
            std::cout << "\nExpected type (BINARY_OP): " << static_cast<int>(BoundExpressionType::BINARY_OP) << std::endl;
            std::cout << "Other types for reference:" << std::endl;
            std::cout << "  COLUMN_REF: " << static_cast<int>(BoundExpressionType::COLUMN_REF) << std::endl;
            std::cout << "  CONSTANT: " << static_cast<int>(BoundExpressionType::CONSTANT) << std::endl;
            std::cout << "  PARAMETER: " << static_cast<int>(BoundExpressionType::PARAMETER) << std::endl;
            std::cout << "  FUNCTION_CALL: " << static_cast<int>(BoundExpressionType::FUNCTION_CALL) << std::endl;
            std::cout << "  SUBQUERY: " << static_cast<int>(BoundExpressionType::SUBQUERY) << std::endl;
            
            // Check if it's the expected AND operation
            if (select_stmt->where_clause->type == BoundExpressionType::BINARY_OP) {
                std::cout << "✅ WHERE clause is BINARY_OP as expected" << std::endl;
            } else {
                std::cout << "❌ WHERE clause is not BINARY_OP" << std::endl;
            }
        } else {
            std::cout << "❌ WHERE clause is null" << std::endl;
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Exception: " << e.what() << std::endl;
        return 1;
    }
}