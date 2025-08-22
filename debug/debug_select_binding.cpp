#include "bound_statement.hpp"
#include "schema_registry.hpp"
#include <iostream>

using namespace db25;

std::shared_ptr<DatabaseSchema> create_debug_schema() {
    auto schema = std::make_shared<DatabaseSchema>("debug_db");
    
    Table users_table;
    users_table.name = "users";
    users_table.columns = {
        {"id", ColumnType::INTEGER, 0, false, true, true, "", "", ""},
        {"name", ColumnType::VARCHAR, 100, false, false, false, "", "", ""},
        {"email", ColumnType::VARCHAR, 255, false, false, true, "", "", ""}
    };
    
    schema->add_table(users_table);
    return schema;
}

int main() {
    try {
        auto schema = create_debug_schema();
        SchemaBinder binder(schema);
        
        const std::string query = "SELECT id, name, email FROM users";
        std::cout << "Binding query: " << query << std::endl;
        
        auto bound_stmt = binder.bind(query);
        
        if (!bound_stmt) {
            std::cout << "❌ Binding failed. Errors:" << std::endl;
            for (const auto& error : binder.get_errors()) {
                std::cout << "  - " << error.message << std::endl;
            }
            return 1;
        }
        
        std::cout << "✅ Binding succeeded!" << std::endl;
        std::cout << "Statement type: " << static_cast<int>(bound_stmt->statement_type) << std::endl;
        
        if (bound_stmt->statement_type == BoundStatement::Type::SELECT) {
            auto select_stmt = std::static_pointer_cast<BoundSelect>(bound_stmt);
            
            std::cout << "From table: " << (select_stmt->from_table ? select_stmt->from_table->table_name : "NULL") << std::endl;
            std::cout << "From table ID: " << (select_stmt->from_table ? std::to_string(select_stmt->from_table->table_id) : "NULL") << std::endl;
            std::cout << "Select list size: " << select_stmt->select_list.size() << std::endl;
            
            for (size_t i = 0; i < select_stmt->select_list.size(); ++i) {
                const auto& expr = select_stmt->select_list[i];
                std::cout << "  [" << i << "] Type: " << static_cast<int>(expr->type) 
                          << ", Original: '" << expr->original_text << "'" << std::endl;
            }
            
            // Debug: check table refs
            std::cout << "Table refs size: " << select_stmt->table_refs.size() << std::endl;
            for (const auto& [name, table_ref] : select_stmt->table_refs) {
                std::cout << "  Table ref: " << name << " -> " << table_ref->table_name 
                          << " (ID: " << table_ref->table_id << ")" << std::endl;
            }
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Debug failed: " << e.what() << std::endl;
        return 1;
    }
}