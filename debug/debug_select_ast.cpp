#include "pg_query_wrapper.hpp"
#include <iostream>
#include <nlohmann/json.hpp>

using namespace db25;

int main() {
    const std::string query = "SELECT id, name, email FROM users";
    
    try {
        const auto parse_result = pg_query_parse(query.c_str());
        if (parse_result.error) {
            std::cerr << "Parse error: " << parse_result.error->message << std::endl;
            pg_query_free_parse_result(parse_result);
            return 1;
        }
        
        const auto ast = nlohmann::json::parse(parse_result.parse_tree);
        pg_query_free_parse_result(parse_result);
        
        std::cout << "=== Full AST Structure ===" << std::endl;
        std::cout << ast.dump(2) << std::endl;
        
        // Navigate to the SELECT statement
        if (ast.contains("stmts") && ast["stmts"].is_array() && !ast["stmts"].empty()) {
            const auto& stmt = ast["stmts"][0];
            if (stmt.contains("stmt") && stmt["stmt"].contains("SelectStmt")) {
                const auto& select_stmt = stmt["stmt"]["SelectStmt"];
                
                std::cout << "\n=== SELECT Statement Structure ===" << std::endl;
                std::cout << select_stmt.dump(2) << std::endl;
                
                if (select_stmt.contains("targetList")) {
                    std::cout << "\n=== Target List Structure ===" << std::endl;
                    std::cout << select_stmt["targetList"].dump(2) << std::endl;
                }
            }
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}