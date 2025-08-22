#include "query_planner.hpp"
#include "simple_schema.hpp"
#include <iostream>

using namespace db25;

int main() {
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner planner(schema);
    
    std::cout << "=== Testing Specific CTE Scenarios ===" << std::endl;
    
    // Test the simplest possible CTE SELECT
    std::string query = "WITH user_stats AS (SELECT COUNT(*) as total FROM users) SELECT total FROM user_stats";
    std::cout << "\nQuery: " << query << std::endl;
    
    // Test bind_and_plan directly
    std::cout << "\n--- Testing bind_and_plan ---" << std::endl;
    auto result = planner.bind_and_plan(query);
    std::cout << "Success: " << (result.success ? "YES" : "NO") << std::endl;
    if (!result.success) {
        std::cout << "Errors:" << std::endl;
        for (const auto& error : result.errors) {
            std::cout << "  - " << error << std::endl;
        }
    } else {
        std::cout << "Bound statement type: " << (result.bound_statement ? "EXISTS" : "NULL") << std::endl;
        std::cout << "Logical plan root: " << (result.logical_plan.root ? "EXISTS" : "NULL") << std::endl;
    }
    
    // Test the SchemaBinder directly
    std::cout << "\n--- Testing SchemaBinder directly ---" << std::endl;
    auto* binder = planner.get_schema_binder();
    if (binder) {
        auto bound_stmt = binder->bind(query);
        std::cout << "Direct binding success: " << (bound_stmt ? "YES" : "NO") << std::endl;
        
        const auto& errors = binder->get_errors();
        if (!errors.empty()) {
            std::cout << "Binding errors:" << std::endl;
            for (const auto& error : errors) {
                std::cout << "  - " << error.message << std::endl;
                if (!error.suggestions.empty()) {
                    std::cout << "    Suggestions: ";
                    for (const auto& suggestion : error.suggestions) {
                        std::cout << suggestion << " ";
                    }
                    std::cout << std::endl;
                }
            }
        }
    } else {
        std::cout << "SchemaBinder is null!" << std::endl;
    }
    
    // Test create_plan
    std::cout << "\n--- Testing create_plan ---" << std::endl;
    auto plan = planner.create_plan(query);
    std::cout << "Plan root: " << (plan.root ? "EXISTS" : "NULL") << std::endl;
    
    return 0;
}