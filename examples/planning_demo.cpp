#include <iostream>
#include <memory>
#include <iomanip>
#include "pg_query_wrapper.hpp"
#include "database.hpp"
#include "simple_schema.hpp"
#include "query_executor.hpp"
#include "query_planner.hpp"

void print_separator(const std::string& title) {
    std::cout << "\n" << std::string(60, '=') << std::endl;
    std::cout << title << std::endl;
    std::cout << std::string(60, '=') << std::endl;
}

void demonstrate_basic_planning() {
    print_separator("Basic Logical Planning");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryPlanner planner(schema);
    
    // Configure planner with some statistics
    pg::TableStats user_stats;
    user_stats.row_count = 10000;
    user_stats.avg_row_size = 120.0;
    user_stats.column_selectivity["email"] = 0.8;
    planner.set_table_stats("users", user_stats);
    
    pg::TableStats product_stats;
    product_stats.row_count = 50000;
    product_stats.avg_row_size = 200.0;
    product_stats.column_selectivity["name"] = 0.6;
    planner.set_table_stats("products", product_stats);
    
    std::vector<std::string> test_queries = {
        "SELECT * FROM users WHERE id = 123",
        "SELECT name FROM products WHERE price > 100",
        "SELECT u.name, p.name FROM users u JOIN products p ON u.id = p.id",
        "SELECT * FROM users ORDER BY name LIMIT 10"
    };
    
    for (const auto& query : test_queries) {
        std::cout << "\nQuery: " << query << std::endl;
        std::cout << std::string(50, '-') << std::endl;
        
        auto plan = planner.create_plan(query);
        if (plan.root) {
            std::cout << plan.to_string() << std::endl;
        } else {
            std::cout << "Failed to generate plan" << std::endl;
        }
    }
}

void demonstrate_plan_optimization() {
    print_separator("Plan Optimization");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryPlanner planner(schema);
    
    // Set up some table statistics
    pg::TableStats stats;
    stats.row_count = 100000;
    stats.avg_row_size = 150.0;
    planner.set_table_stats("users", stats);
    planner.set_table_stats("products", stats);
    
    std::string complex_query = "SELECT u.name, p.name FROM users u JOIN products p ON u.id = p.id WHERE u.name LIKE 'John%' AND p.price > 50";
    
    std::cout << "\nComplex Query: " << complex_query << std::endl;
    std::cout << std::string(50, '-') << std::endl;
    
    // Generate original plan
    auto original_plan = planner.create_plan(complex_query);
    std::cout << "Original Plan:" << std::endl;
    if (original_plan.root) {
        std::cout << original_plan.to_string() << std::endl;
    }
    
    // Generate optimized plan
    auto optimized_plan = planner.optimize_plan(original_plan);
    std::cout << "Optimized Plan:" << std::endl;
    if (optimized_plan.root) {
        std::cout << optimized_plan.to_string() << std::endl;
    }
    
    // Compare costs
    if (original_plan.root && optimized_plan.root) {
        std::cout << "Cost Comparison:" << std::endl;
        std::cout << "Original:  " << original_plan.root->cost.total_cost << " (rows: " << original_plan.root->cost.estimated_rows << ")" << std::endl;
        std::cout << "Optimized: " << optimized_plan.root->cost.total_cost << " (rows: " << optimized_plan.root->cost.estimated_rows << ")" << std::endl;
        
        if (optimized_plan.root->cost.total_cost < original_plan.root->cost.total_cost) {
            double improvement = ((original_plan.root->cost.total_cost - optimized_plan.root->cost.total_cost) / original_plan.root->cost.total_cost) * 100;
            std::cout << "Improvement: " << std::fixed << std::setprecision(2) << improvement << "%" << std::endl;
        }
    }
}

void demonstrate_alternative_plans() {
    print_separator("Alternative Plan Generation");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryPlanner planner(schema);
    
    // Configure planner for different join algorithms
    pg::PlannerConfig config;
    config.enable_hash_joins = true;
    config.enable_merge_joins = true;
    planner.set_config(config);
    
    std::string join_query = "SELECT u.name, p.name FROM users u JOIN products p ON u.id = p.id";
    
    std::cout << "\nJoin Query: " << join_query << std::endl;
    std::cout << std::string(50, '-') << std::endl;
    
    auto alternative_plans = planner.generate_alternative_plans(join_query);
    
    std::cout << "Generated " << alternative_plans.size() << " alternative plans:" << std::endl;
    
    for (size_t i = 0; i < alternative_plans.size(); ++i) {
        std::cout << "\nPlan " << (i + 1) << ":" << std::endl;
        if (alternative_plans[i].root) {
            std::cout << alternative_plans[i].to_string() << std::endl;
        }
    }
}

void demonstrate_dml_planning() {
    print_separator("DML Query Planning");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryPlanner planner(schema);
    
    std::vector<std::string> dml_queries = {
        "INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com')",
        "UPDATE users SET name = 'Jane Doe' WHERE id = 123",
        "DELETE FROM users WHERE email LIKE '%@spam.com'"
    };
    
    for (const auto& query : dml_queries) {
        std::cout << "\nDML Query: " << query << std::endl;
        std::cout << std::string(50, '-') << std::endl;
        
        auto plan = planner.create_plan(query);
        if (plan.root) {
            std::cout << plan.to_string() << std::endl;
        } else {
            std::cout << "Failed to generate plan" << std::endl;
        }
    }
}

void demonstrate_cost_estimation() {
    print_separator("Cost Estimation Analysis");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryPlanner planner(schema);
    
    // Set up different table sizes to see cost differences
    std::vector<size_t> table_sizes = {1000, 10000, 100000, 1000000};
    std::string test_query = "SELECT * FROM users WHERE name LIKE 'A%'";
    
    std::cout << "\nCost analysis for different table sizes:" << std::endl;
    std::cout << "Query: " << test_query << std::endl;
    std::cout << std::string(50, '-') << std::endl;
    
    std::cout << std::left << std::setw(12) << "Table Size" 
              << std::setw(15) << "Startup Cost" 
              << std::setw(15) << "Total Cost" 
              << std::setw(12) << "Est. Rows" << std::endl;
    std::cout << std::string(54, '-') << std::endl;
    
    for (size_t table_size : table_sizes) {
        pg::TableStats stats;
        stats.row_count = table_size;
        stats.avg_row_size = 100.0;
        stats.column_selectivity["name"] = 0.1; // 10% selectivity for LIKE 'A%'
        planner.set_table_stats("users", stats);
        
        auto plan = planner.create_plan(test_query);
        if (plan.root) {
            std::cout << std::left 
                      << std::setw(12) << table_size 
                      << std::setw(15) << std::fixed << std::setprecision(2) << plan.root->cost.startup_cost
                      << std::setw(15) << std::fixed << std::setprecision(2) << plan.root->cost.total_cost
                      << std::setw(12) << plan.root->cost.estimated_rows << std::endl;
        }
    }
}

void demonstrate_join_algorithms() {
    print_separator("Join Algorithm Selection");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryPlanner planner(schema);
    
    std::string join_query = "SELECT u.name, p.name FROM users u JOIN products p ON u.id = p.id";
    
    // Test different configurations
    struct TestCase {
        std::string name;
        size_t users_rows;
        size_t products_rows;
        bool enable_hash;
        bool enable_merge;
    };
    
    std::vector<TestCase> test_cases = {
        {"Small x Small (NL only)", 100, 100, false, false},
        {"Small x Large (Hash)", 100, 100000, true, false},
        {"Large x Small (Hash)", 100000, 100, true, false},
        {"Large x Large (Hash)", 100000, 100000, true, false}
    };
    
    for (const auto& test_case : test_cases) {
        std::cout << "\nTest Case: " << test_case.name << std::endl;
        std::cout << std::string(40, '-') << std::endl;
        
        // Configure table stats
        pg::TableStats user_stats;
        user_stats.row_count = test_case.users_rows;
        user_stats.avg_row_size = 100.0;
        planner.set_table_stats("users", user_stats);
        
        pg::TableStats product_stats;
        product_stats.row_count = test_case.products_rows;
        product_stats.avg_row_size = 150.0;
        planner.set_table_stats("products", product_stats);
        
        // Configure planner
        pg::PlannerConfig config;
        config.enable_hash_joins = test_case.enable_hash;
        config.enable_merge_joins = test_case.enable_merge;
        planner.set_config(config);
        
        auto plan = planner.create_plan(join_query);
        if (plan.root) {
            std::cout << plan.to_string() << std::endl;
        }
    }
}

int main() {
    try {
        std::cout << "PostgreSQL Logical Planning Demo" << std::endl;
        std::cout << "C++17 Project with Advanced Query Planning" << std::endl;
        
        demonstrate_basic_planning();
        demonstrate_plan_optimization();
        demonstrate_alternative_plans();
        demonstrate_dml_planning();
        demonstrate_cost_estimation();
        demonstrate_join_algorithms();
        
        std::cout << "\nLogical Planning Demo completed successfully!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}