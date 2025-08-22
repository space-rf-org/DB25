#include "bound_statement.hpp"
#include "schema_registry.hpp"
#include <iostream>

using namespace db25;

std::shared_ptr<DatabaseSchema> create_test_schema() {
    auto schema = std::make_shared<DatabaseSchema>("test_db");
    
    Table users_table;
    users_table.name = "users";
    users_table.columns = {
        {"id", ColumnType::INTEGER, 0, false, true, true, "", "", ""},
        {"name", ColumnType::VARCHAR, 100, false, false, false, "", "", ""}
    };
    
    Table products_table;
    products_table.name = "products"; 
    products_table.columns = {
        {"id", ColumnType::INTEGER, 0, false, true, true, "", "", ""},
        {"name", ColumnType::VARCHAR, 100, false, false, false, "", "", ""}
    };
    
    schema->add_table(users_table);
    schema->add_table(products_table);
    
    return schema;
}

int main() {
    try {
        std::cout << "=== Debug Error Suggestions ===" << std::endl;
        
        auto schema = create_test_schema();
        SchemaBinder binder(schema);
        
        // Test with typo in table name (same as the failing test)
        const std::string query = "SELECT id FROM user"; // Missing 's'
        std::cout << "Query: " << query << std::endl;
        
        auto bound_stmt = binder.bind(query);
        std::cout << "Bound statement result: " << (bound_stmt ? "SUCCESS" : "FAILED") << std::endl;
        
        const auto& errors = binder.get_errors();
        std::cout << "Number of errors: " << errors.size() << std::endl;
        
        for (size_t i = 0; i < errors.size(); ++i) {
            const auto& error = errors[i];
            std::cout << "Error " << i << ": " << error.message << std::endl;
            std::cout << "Number of suggestions: " << error.suggestions.size() << std::endl;
            
            for (size_t j = 0; j < error.suggestions.size(); ++j) {
                std::cout << "  Suggestion " << j << ": " << error.suggestions[j] << std::endl;
            }
        }
        
        // Test the underlying SchemaRegistry directly
        std::cout << "\n=== Direct SchemaRegistry Test ===" << std::endl;
        SchemaRegistry registry(schema);
        auto suggestions = registry.suggest_table_names("user");
        std::cout << "Direct registry suggestions for 'user': " << suggestions.size() << std::endl;
        for (const auto& suggestion : suggestions) {
            std::cout << "  - " << suggestion << std::endl;
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Exception: " << e.what() << std::endl;
        return 1;
    }
}