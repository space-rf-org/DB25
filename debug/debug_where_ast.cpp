#include "pg_query_wrapper.hpp"
#include <iostream>
#include <nlohmann/json.hpp>

using namespace db25;

int main() {
    try {
        std::cout << "=== Debug WHERE Clause AST ===" << std::endl;
        
        const std::string query = "SELECT id, name FROM users WHERE id > 10 AND name LIKE 'A%'";
        std::cout << "Query: " << query << std::endl;
        
        // Parse the query
        const auto parse_result = pg_query_parse(query.c_str());
        if (parse_result.error) {
            std::cerr << "Parse error: " << parse_result.error->message << std::endl;
            pg_query_free_parse_result(parse_result);
            return 1;
        }
        
        const auto ast = nlohmann::json::parse(parse_result.parse_tree);
        pg_query_free_parse_result(parse_result);
        
        // Navigate to the WHERE clause
        const auto& select_stmt = ast["stmts"][0]["stmt"]["SelectStmt"];
        
        std::cout << "Has whereClause: " << select_stmt.contains("whereClause") << std::endl;
        
        if (select_stmt.contains("whereClause")) {
            const auto& where_clause = select_stmt["whereClause"];
            std::cout << "\nWHERE clause AST structure:" << std::endl;
            std::cout << where_clause.dump(2) << std::endl;
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Exception: " << e.what() << std::endl;
        return 1;
    }
}