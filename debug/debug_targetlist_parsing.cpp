#include "pg_query_wrapper.hpp"
#include "bound_statement.hpp"
#include "schema_registry.hpp"
#include <iostream>
#include <nlohmann/json.hpp>

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
        const std::string query = "SELECT id, name, email FROM users";
        
        // Parse the query
        const auto parse_result = pg_query_parse(query.c_str());
        if (parse_result.error) {
            std::cerr << "Parse error: " << parse_result.error->message << std::endl;
            pg_query_free_parse_result(parse_result);
            return 1;
        }
        
        const auto ast = nlohmann::json::parse(parse_result.parse_tree);
        pg_query_free_parse_result(parse_result);
        
        // Navigate to SELECT statement
        const auto& select_stmt = ast["stmts"][0]["stmt"]["SelectStmt"];
        
        std::cout << "=== Debug Target List Parsing ===" << std::endl;
        std::cout << "Has targetList: " << select_stmt.contains("targetList") << std::endl;
        
        if (select_stmt.contains("targetList")) {
            const auto& target_list = select_stmt["targetList"];
            std::cout << "targetList is array: " << target_list.is_array() << std::endl;
            std::cout << "targetList size: " << target_list.size() << std::endl;
            
            for (size_t i = 0; i < target_list.size(); ++i) {
                const auto& target = target_list[i];
                std::cout << "\n--- Target " << i << " ---" << std::endl;
                std::cout << "Contains ResTarget: " << target.contains("ResTarget") << std::endl;
                
                if (target.contains("ResTarget")) {
                    const auto& res_target = target["ResTarget"];
                    std::cout << "ResTarget contains val: " << res_target.contains("val") << std::endl;
                    
                    if (res_target.contains("val")) {
                        const auto& val = res_target["val"];
                        std::cout << "val structure: " << val.dump(1) << std::endl;
                        
                        // This is what bind_expression should receive
                        std::cout << "Contains ColumnRef: " << val.contains("ColumnRef") << std::endl;
                        if (val.contains("ColumnRef")) {
                            const auto& col_ref = val["ColumnRef"];
                            std::cout << "ColumnRef fields: " << col_ref["fields"].dump(1) << std::endl;
                        }
                    }
                }
            }
        }
        
        // Now test actual binding
        std::cout << "\n=== Testing SchemaBinder ===" << std::endl;
        auto schema = create_debug_schema();
        SchemaBinder binder(schema);
        
        auto bound_stmt = binder.bind(query);
        if (bound_stmt) {
            auto select_bound = std::static_pointer_cast<BoundSelect>(bound_stmt);
            std::cout << "Bound select list size: " << select_bound->select_list.size() << std::endl;
        } else {
            std::cout << "Binding failed" << std::endl;
            for (const auto& error : binder.get_errors()) {
                std::cout << "Error: " << error.message << std::endl;
            }
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}