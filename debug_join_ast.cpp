#include "../include/pg_query_wrapper.hpp"
#include <iostream>
#include <nlohmann/json.hpp>

using namespace db25;

int main() {
    std::string query = "SELECT u.name, p.name FROM users u JOIN products p ON u.id = p.user_id";
    
    std::cout << "Parsing query: " << query << std::endl;
    
    const auto parse_result = pg_query_parse(query.c_str());
    if (parse_result.error) {
        std::cerr << "Parse error: " << parse_result.error->message << std::endl;
        pg_query_free_parse_result(parse_result);
        return 1;
    }
    
    const auto ast = nlohmann::json::parse(parse_result.parse_tree);
    pg_query_free_parse_result(parse_result);
    
    std::cout << "\n=== FULL AST ===" << std::endl;
    std::cout << ast.dump(2) << std::endl;
    
    // Focus on the fromClause
    if (ast.contains("stmts") && ast["stmts"].is_array() && !ast["stmts"].empty()) {
        const auto& stmt = ast["stmts"][0];
        if (stmt.contains("stmt") && stmt["stmt"].contains("SelectStmt")) {
            const auto& select_stmt = stmt["stmt"]["SelectStmt"];
            if (select_stmt.contains("fromClause")) {
                std::cout << "\n=== FROM CLAUSE ===" << std::endl;
                std::cout << select_stmt["fromClause"].dump(2) << std::endl;
            }
        }
    }
    
    return 0;
}