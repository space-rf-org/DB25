#include "bound_statement.hpp"
#include "schema_registry.hpp"
#include <iostream>

using namespace db25;

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

int main() {
    try {
        std::cout << "=== Testing Direct SchemaBinder Creation ===" << std::endl;
        
        auto schema = create_test_schema();
        SchemaBinder binder(schema);  // Same as the failing test
        
        const std::string query = "SELECT id, name, email FROM users";
        std::cout << "Query: " << query << std::endl;
        
        // Step 1: Bind to BoundStatement
        auto bound_stmt = binder.bind(query);
        if (!bound_stmt) {
            std::cout << "❌ Binding failed. Errors:" << std::endl;
            for (const auto& error : binder.get_errors()) {
                std::cout << "  - " << error.message << std::endl;
            }
            return 1;
        }
        
        auto select_stmt = std::static_pointer_cast<BoundSelect>(bound_stmt);
        std::cout << "✅ Binding succeeded!" << std::endl;
        std::cout << "Select list size: " << select_stmt->select_list.size() << std::endl;
        std::cout << "From table: " << (select_stmt->from_table ? select_stmt->from_table->table_name : "NULL") << std::endl;
        std::cout << "From table ID: " << (select_stmt->from_table ? std::to_string(select_stmt->from_table->table_id) : "NULL") << std::endl;
        
        // Check each expression
        for (size_t i = 0; i < select_stmt->select_list.size(); ++i) {
            const auto& expr = select_stmt->select_list[i];
            std::cout << "  [" << i << "] Type: " << static_cast<int>(expr->type) 
                      << ", Original: '" << expr->original_text << "'" << std::endl;
        }
        
        // Run the exact same assertions as the test
        assert(bound_stmt != nullptr);
        assert(select_stmt->select_list.size() == 3); // This is the failing assertion
        assert(select_stmt->from_table != nullptr);
        assert(select_stmt->from_table->table_id != 0);
        
        std::cout << "✅ All assertions passed!" << std::endl;
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "❌ Exception: " << e.what() << std::endl;
        return 1;
    }
}