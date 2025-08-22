#include <iostream>
#include "pg_query_wrapper.hpp"
#include <nlohmann/json.hpp>

using namespace db25;

int main() {
    std::string query = "SELECT * FROM users WHERE id = 123";
    
    std::cout << "Testing AST parsing for: " << query << std::endl;
    
    // Parse with pg_query directly
    PgQueryParseResult parse_result = pg_query_parse(query.c_str());
    
    if (parse_result.error) {
        std::cout << "Parse error: " << parse_result.error->message << std::endl;
        pg_query_free_parse_result(parse_result);
        return 1;
    }
    
    if (!parse_result.parse_tree) {
        std::cout << "No parse tree!" << std::endl;
        pg_query_free_parse_result(parse_result);
        return 1;
    }
    
    try {
        // Parse JSON AST
        nlohmann::json ast = nlohmann::json::parse(parse_result.parse_tree);
        
        std::cout << "AST structure:" << std::endl;
        std::cout << ast.dump(2) << std::endl;
        
        // Look for WHERE clause specifically
        if (ast.contains("stmts") && ast["stmts"].is_array() && !ast["stmts"].empty()) {
            const auto& stmt = ast["stmts"][0];
            std::cout << "\nFound stmt" << std::endl;
            
            if (stmt.contains("stmt") && stmt["stmt"].contains("SelectStmt")) {
                const auto& select_stmt = stmt["stmt"]["SelectStmt"];
                std::cout << "Found SelectStmt" << std::endl;
                
                if (select_stmt.contains("whereClause")) {
                    std::cout << "Found whereClause:" << std::endl;
                    std::cout << select_stmt["whereClause"].dump(2) << std::endl;
                } else {
                    std::cout << "No whereClause found" << std::endl;
                    std::cout << "SelectStmt contains:" << std::endl;
                    for (const auto& [key, value] : select_stmt.items()) {
                        std::cout << "  - " << key << std::endl;
                    }
                }
            } else {
                std::cout << "No SelectStmt found" << std::endl;
            }
        } else {
            std::cout << "No stmts found" << std::endl;
        }
        
    } catch (const std::exception& e) {
        std::cout << "JSON parse error: " << e.what() << std::endl;
    }
    
    pg_query_free_parse_result(parse_result);
    return 0;
}