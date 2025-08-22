#include "bound_statement.hpp"
#include "query_planner.hpp"
#include "schema_registry.hpp"
#include <iostream>

using namespace db25;

std::shared_ptr<DatabaseSchema> create_integration_schema() {
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
    
    schema->add_table(users_table);
    return schema;
}

int main() {
    try {
        auto schema = create_integration_schema();
        SchemaBinder binder(schema);
        QueryPlanner planner(schema);
        
        const std::string query = "SELECT id, name, email FROM users";
        std::cout << "Testing query: " << query << std::endl;
        
        // Step 1: Direct SchemaBinder test
        std::cout << "\n=== Step 1: Direct SchemaBinder Test ===" << std::endl;
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
        
        // Step 2: QueryPlanner integration test  
        std::cout << "\n=== Step 2: QueryPlanner Integration Test ===" << std::endl;
        auto result = planner.bind_and_plan(query);
        if (!result.success) {
            std::cout << "❌ Integration binding failed. Errors:" << std::endl;
            for (const auto& error : result.errors) {
                std::cout << "  - " << error << std::endl;
            }
            return 1;
        }
        
        if (!result.bound_statement) {
            std::cout << "❌ No bound statement returned" << std::endl;
            return 1;
        }
        
        auto integrated_select = std::static_pointer_cast<BoundSelect>(result.bound_statement);
        std::cout << "✅ Integration binding succeeded!" << std::endl;
        std::cout << "Select list size: " << integrated_select->select_list.size() << std::endl;
        std::cout << "From table: " << (integrated_select->from_table ? integrated_select->from_table->table_name : "NULL") << std::endl;
        std::cout << "From table ID: " << (integrated_select->from_table ? std::to_string(integrated_select->from_table->table_id) : "NULL") << std::endl;
        
        // Step 3: Check logical plan
        std::cout << "\n=== Step 3: Logical Plan Check ===" << std::endl;
        if (result.logical_plan.root) {
            std::cout << "✅ Logical plan created successfully!" << std::endl;
            std::cout << "Root node type: " << static_cast<int>(result.logical_plan.root->type) << std::endl;
        } else {
            std::cout << "❌ No logical plan created" << std::endl;
            return 1;
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Exception: " << e.what() << std::endl;
        return 1;
    }
}