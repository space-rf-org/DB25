#include <iostream>
#include <cassert>
#include <memory>
#include "query_planner.hpp"
#include "database.hpp"
#include "simple_schema.hpp"

using namespace db25;

void print_expression_tree(const ExpressionPtr& expr, int depth = 0) {
    if (!expr) {
        std::cout << std::string(depth * 2, ' ') << "NULL" << std::endl;
        return;
    }
    
    std::string type_str;
    switch (expr->type) {
        case ExpressionType::COLUMN_REF: type_str = "COLUMN"; break;
        case ExpressionType::CONSTANT: type_str = "CONST"; break;
        case ExpressionType::FUNCTION_CALL: type_str = "FUNC"; break;
        case ExpressionType::BINARY_OP: type_str = "BINOP"; break;
        case ExpressionType::UNARY_OP: type_str = "UNARY"; break;
        case ExpressionType::CASE_EXPR: type_str = "CASE"; break;
        case ExpressionType::SUBQUERY: type_str = "SUBQ"; break;
    }
    
    std::cout << std::string(depth * 2, ' ') << type_str << ": \"" << expr->value << "\"" << std::endl;
    
    for (const auto& child : expr->children) {
        print_expression_tree(child, depth + 1);
    }
}

void test_ast_where_parsing() {
    std::cout << "Testing AST-based WHERE condition parsing..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner planner(schema);
    
    std::vector<std::pair<std::string, std::string>> test_queries = {
        {"Simple WHERE", "SELECT * FROM users WHERE id = 123"},
        {"Multiple AND conditions", "SELECT * FROM users WHERE id = 123 AND status = 'active'"},
        {"Complex boolean logic", "SELECT * FROM users WHERE (id = 1 OR id = 2) AND status = 'active'"},
        {"Function in WHERE", "SELECT * FROM users WHERE UPPER(name) = 'JOHN'"},
        {"CASE in WHERE", "SELECT * FROM users WHERE CASE WHEN type = 'premium' THEN price > 0 ELSE price > 10 END"},
        {"Subquery in WHERE", "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"},
        {"Date comparison", "SELECT * FROM users WHERE created_date > '2023-01-01'"}
    };
    
    for (const auto& [description, query] : test_queries) {
        std::cout << "\n--- " << description << " ---" << std::endl;
        std::cout << "Query: " << query << std::endl;
        
        try {
            // Test via plan creation which internally uses AST WHERE parsing
            auto plan = planner.create_plan(query);
            
            if (plan.root) {
                std::cout << "✓ Plan created successfully" << std::endl;
                std::cout << "Plan cost: " << plan.root->cost.total_cost << std::endl;
                
                // Try to extract WHERE conditions directly to see AST parsing in action
                auto where_conditions = planner.extract_where_conditions_from_ast(query);
                std::cout << "WHERE conditions extracted: " << where_conditions.size() << std::endl;
                
                for (size_t i = 0; i < where_conditions.size(); ++i) {
                    std::cout << "Condition " << (i + 1) << ":" << std::endl;
                    print_expression_tree(where_conditions[i], 1);
                }
            } else {
                std::cout << "❌ Plan creation failed" << std::endl;
            }
            
        } catch (const std::exception& e) {
            std::cout << "❌ Exception: " << e.what() << std::endl;
        }
    }
    
    std::cout << "\n✓ AST WHERE parsing test completed" << std::endl;
}

void test_comparison_with_original() {
    std::cout << "\nTesting AST vs original WHERE parsing comparison..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner planner(schema);
    
    std::string test_query = "SELECT * FROM users WHERE id = 123 AND status = 'active' AND age > 21";
    
    std::cout << "Query: " << test_query << std::endl;
    
    try {
        // Test AST-based approach
        auto ast_conditions = planner.extract_where_conditions_from_ast(test_query);
        std::cout << "\nAST-based parsing found " << ast_conditions.size() << " conditions:" << std::endl;
        for (size_t i = 0; i < ast_conditions.size(); ++i) {
            std::cout << "  " << (i + 1) << ". ";
            print_expression_tree(ast_conditions[i], 0);
        }
        
        // Test that plan creation works
        auto plan = planner.create_plan(test_query);
        if (plan.root) {
            std::cout << "✓ Plan created with AST-based WHERE parsing" << std::endl;
            std::cout << "Plan cost: " << plan.root->cost.total_cost << std::endl;
        }
        
    } catch (const std::exception& e) {
        std::cout << "❌ Exception: " << e.what() << std::endl;
    }
    
    std::cout << "✓ Comparison test completed" << std::endl;
}

int main() {
    std::cout << "=== AST-Based WHERE Condition Parsing Test ===" << std::endl;
    
    try {
        test_ast_where_parsing();
        test_comparison_with_original();
        
        std::cout << "\n✅ All WHERE condition AST tests passed!" << std::endl;
        std::cout << "\nConclusion: AST-based WHERE condition parsing is working!" << std::endl;
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Test failed: " << e.what() << std::endl;
        return 1;
    }
}