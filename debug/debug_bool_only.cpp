#include <iostream>
#include "query_planner.hpp"
#include "database.hpp" 
#include "simple_schema.hpp"
#include "pg_query_wrapper.hpp"
#include <nlohmann/json.hpp>

using namespace db25;

int main() {
    std::string query = "SELECT * FROM users WHERE id = 123 AND status = 'active'";
    
    std::cout << "Testing boolean expression parsing directly" << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner planner(schema);
    
    // Parse query to get AST
    PgQueryParseResult parse_result = pg_query_parse(query.c_str());
    
    if (parse_result.error || !parse_result.parse_tree) {
        std::cout << "Parse failed" << std::endl;
        pg_query_free_parse_result(parse_result);
        return 1;
    }
    
    try {
        nlohmann::json ast = nlohmann::json::parse(parse_result.parse_tree);
        
        // Navigate to WHERE clause
        const auto& where_clause = ast["stmts"][0]["stmt"]["SelectStmt"]["whereClause"];
        
        std::cout << "Calling parse_where_expression_ast directly..." << std::endl;
        
        // Call the method that should parse the BoolExpr
        auto where_expr = planner.parse_where_expression_ast(where_clause);
        
        if (!where_expr) {
            std::cout << "parse_where_expression_ast returned NULL!" << std::endl;
        } else {
            std::cout << "parse_where_expression_ast returned expression:" << std::endl;
            std::cout << "  Type: " << static_cast<int>(where_expr->type) << std::endl;
            std::cout << "  Value: '" << where_expr->value << "'" << std::endl;
            std::cout << "  Children: " << where_expr->children.size() << std::endl;
            
            for (size_t i = 0; i < where_expr->children.size(); ++i) {
                if (where_expr->children[i]) {
                    std::cout << "    Child " << i << ": Type=" << static_cast<int>(where_expr->children[i]->type) 
                              << ", Value='" << where_expr->children[i]->value << "'" << std::endl;
                } else {
                    std::cout << "    Child " << i << ": NULL" << std::endl;
                }
            }
        }
        
        // Now test flatten_condition_tree
        if (where_expr) {
            std::cout << "\nCalling flatten_condition_tree..." << std::endl;
            std::vector<ExpressionPtr> conditions;
            planner.flatten_condition_tree(where_expr, conditions);
            
            std::cout << "flatten_condition_tree returned " << conditions.size() << " conditions" << std::endl;
            for (size_t i = 0; i < conditions.size(); ++i) {
                if (conditions[i]) {
                    std::cout << "  Condition " << i << ": Type=" << static_cast<int>(conditions[i]->type) 
                              << ", Value='" << conditions[i]->value << "'" << std::endl;
                } else {
                    std::cout << "  Condition " << i << ": NULL" << std::endl;
                }
            }
        }
        
    } catch (const std::exception& e) {
        std::cout << "Exception: " << e.what() << std::endl;
    }
    
    pg_query_free_parse_result(parse_result);
    return 0;
}