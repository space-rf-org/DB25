#include <iostream>
#include "pg_query_wrapper.hpp"
#include <nlohmann/json.hpp>

using namespace db25;

int main() {
    std::string query = "SELECT * FROM users WHERE id = 123 AND status = 'active'";
    
    std::cout << "Tracing parse steps for: " << query << std::endl;
    
    // Parse with pg_query directly
    PgQueryParseResult parse_result = pg_query_parse(query.c_str());
    
    if (parse_result.error || !parse_result.parse_tree) {
        std::cout << "Parse failed" << std::endl;
        pg_query_free_parse_result(parse_result);
        return 1;
    }
    
    try {
        // Parse JSON AST
        nlohmann::json ast = nlohmann::json::parse(parse_result.parse_tree);
        
        std::cout << "Step 1: Check for stmts" << std::endl;
        if (!ast.contains("stmts") || !ast["stmts"].is_array() || ast["stmts"].empty()) {
            std::cout << "No stmts found" << std::endl;
            return 1;
        }
        std::cout << "Found stmts" << std::endl;
        
        const auto& stmt = ast["stmts"][0];
        std::cout << "Step 2: Check for stmt.SelectStmt" << std::endl;
        if (!stmt.contains("stmt") || !stmt["stmt"].contains("SelectStmt")) {
            std::cout << "No SelectStmt found" << std::endl;
            return 1;
        }
        std::cout << "Found SelectStmt" << std::endl;
        
        const auto& select_stmt = stmt["stmt"]["SelectStmt"];
        std::cout << "Step 3: Check for whereClause" << std::endl;
        if (!select_stmt.contains("whereClause") || select_stmt["whereClause"].is_null()) {
            std::cout << "No whereClause found" << std::endl;
            return 1;
        }
        std::cout << "Found whereClause" << std::endl;
        
        const auto& where_clause = select_stmt["whereClause"];
        std::cout << "Step 4: Check whereClause type" << std::endl;
        
        // Show what type of node we have
        for (const auto& [key, value] : where_clause.items()) {
            std::cout << "whereClause contains: " << key << std::endl;
        }
        
        std::cout << "Step 5: Check if it's BoolExpr" << std::endl;
        if (where_clause.contains("BoolExpr")) {
            std::cout << "Found BoolExpr!" << std::endl;
            const auto& bool_expr = where_clause["BoolExpr"];
            
            std::cout << "BoolExpr boolop: " << bool_expr["boolop"] << std::endl;
            std::cout << "BoolExpr args count: " << bool_expr["args"].size() << std::endl;
        } else {
            std::cout << "No BoolExpr found in whereClause" << std::endl;
        }
        
    } catch (const std::exception& e) {
        std::cout << "Exception: " << e.what() << std::endl;
    }
    
    pg_query_free_parse_result(parse_result);
    return 0;
}