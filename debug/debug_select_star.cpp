#include "pg_query_wrapper.hpp"
#include "bound_statement.hpp"
#include "query_planner.hpp"
#include "schema_registry.hpp"
#include <iostream>
#include <nlohmann/json.hpp>

using namespace db25;

std::shared_ptr<DatabaseSchema> create_test_schema() {
    auto schema = std::make_shared<DatabaseSchema>("integration_test_db");
    
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
        std::cout << "=== Debug SELECT * Parsing ===" << std::endl;
        
        const std::string query = "SELECT * FROM users";
        std::cout << "Query: " << query << std::endl;
        
        // Parse the query to see AST structure
        const auto parse_result = pg_query_parse(query.c_str());
        if (parse_result.error) {
            std::cerr << "Parse error: " << parse_result.error->message << std::endl;
            pg_query_free_parse_result(parse_result);
            return 1;
        }
        
        const auto ast = nlohmann::json::parse(parse_result.parse_tree);
        pg_query_free_parse_result(parse_result);
        
        const auto& select_stmt = ast["stmts"][0]["stmt"]["SelectStmt"];
        
        std::cout << "Has targetList: " << select_stmt.contains("targetList") << std::endl;
        if (select_stmt.contains("targetList")) {
            const auto& target_list = select_stmt["targetList"];
            std::cout << "targetList size: " << target_list.size() << std::endl;
            
            for (size_t i = 0; i < target_list.size(); ++i) {
                std::cout << "\nTarget " << i << ":" << std::endl;
                std::cout << target_list[i].dump(2) << std::endl;
            }
        }
        
        std::cout << "\n=== Testing Schema Binder ===" << std::endl;
        auto schema = create_test_schema();
        QueryPlanner planner(schema);
        
        auto result = planner.bind_and_plan(query);
        std::cout << "bind_and_plan success: " << result.success << std::endl;
        
        if (!result.success) {
            std::cout << "❌ Binding failed. Errors:" << std::endl;
            for (const auto& error : result.errors) {
                std::cout << "  - " << error << std::endl;
            }
        } else {
            std::cout << "✅ Binding succeeded" << std::endl;
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Exception: " << e.what() << std::endl;
        return 1;
    }
}