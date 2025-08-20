#include <iostream>
#include <memory>
#include <iomanip>
#include "pg_query_wrapper.hpp"
#include "database.hpp"
#include "simple_schema.hpp"
#include "query_planner.hpp"
#include "physical_planner.hpp"

void print_separator(const std::string& title) {
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << title << std::endl;
    std::cout << std::string(70, '=') << std::endl;
}

std::string format_memory_bytes(size_t bytes) {
    if (bytes < 1024) return std::to_string(bytes) + " B";
    else if (bytes < 1024 * 1024) return std::to_string(bytes / 1024) + " KB";
    else return std::to_string(bytes / (1024 * 1024)) + " MB";
}

void demonstrate_physical_planning() {
    print_separator("Physical Plan Generation");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryPlanner logical_planner(schema);
    pg::PhysicalPlanner physical_planner(schema);
    
    // Configure table statistics
    pg::TableStats user_stats;
    user_stats.row_count = 50000;
    user_stats.avg_row_size = 120.0;
    logical_planner.set_table_stats("users", user_stats);
    physical_planner.set_table_stats("users", user_stats);
    
    pg::TableStats product_stats;
    product_stats.row_count = 100000;
    product_stats.avg_row_size = 200.0;
    logical_planner.set_table_stats("products", product_stats);
    physical_planner.set_table_stats("products", product_stats);
    
    std::vector<std::string> test_queries = {
        "SELECT * FROM users WHERE id = 123",
        "SELECT name FROM products WHERE price > 100",
        "SELECT u.name, p.name FROM users u JOIN products p ON u.id = p.id",
        "SELECT * FROM users ORDER BY name LIMIT 10"
    };
    
    for (const auto& query : test_queries) {
        std::cout << "\nQuery: " << query << std::endl;
        std::cout << std::string(60, '-') << std::endl;
        
        // Generate logical plan
        auto logical_plan = logical_planner.create_plan(query);
        std::cout << "LOGICAL PLAN:" << std::endl;
        std::cout << logical_plan.to_string() << std::endl;
        
        // Convert to physical plan
        auto physical_plan = physical_planner.create_physical_plan(logical_plan);
        std::cout << "PHYSICAL PLAN:" << std::endl;
        std::cout << physical_plan.to_string() << std::endl;
    }
}

void demonstrate_physical_execution() {
    print_separator("Physical Plan Execution");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryPlanner logical_planner(schema);
    pg::PhysicalPlanner physical_planner(schema);
    
    // Configure smaller dataset for execution demo
    pg::TableStats user_stats;
    user_stats.row_count = 1000;
    user_stats.avg_row_size = 120.0;
    logical_planner.set_table_stats("users", user_stats);
    physical_planner.set_table_stats("users", user_stats);
    
    std::vector<std::string> execution_queries = {
        "SELECT * FROM users LIMIT 5",
        "SELECT name FROM users WHERE id = 1"
    };
    
    for (const auto& query : execution_queries) {
        std::cout << "\nExecuting Query: " << query << std::endl;
        std::cout << std::string(50, '-') << std::endl;
        
        // Generate and execute physical plan
        auto logical_plan = logical_planner.create_plan(query);
        auto physical_plan = physical_planner.create_physical_plan(logical_plan);
        
        std::cout << "Physical Plan:" << std::endl;
        std::cout << physical_plan.to_string() << std::endl;
        
        // Execute the plan
        auto start_time = std::chrono::high_resolution_clock::now();
        auto results = physical_plan.execute();
        auto end_time = std::chrono::high_resolution_clock::now();
        
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        double execution_time_ms = duration.count() / 1000.0;
        
        std::cout << "Execution Results:" << std::endl;
        std::cout << "Rows returned: " << results.size() << std::endl;
        std::cout << "Execution time: " << std::fixed << std::setprecision(3) 
                  << execution_time_ms << " ms" << std::endl;
        
        // Show first few results
        std::cout << "\nFirst " << std::min(5UL, results.size()) << " rows:" << std::endl;
        for (size_t i = 0; i < std::min(5UL, results.size()); ++i) {
            std::cout << "Row " << (i + 1) << ": ";
            for (size_t j = 0; j < results[i].values.size(); ++j) {
                if (j > 0) std::cout << ", ";
                std::cout << results[i].values[j];
            }
            std::cout << std::endl;
        }
        
        std::cout << "\nExecution Stats:" << std::endl;
        auto stats = physical_plan.get_execution_stats();
        std::cout << "Total execution time: " << stats.execution_time_ms << " ms" << std::endl;
        std::cout << "Rows processed: " << stats.rows_processed << std::endl;
        std::cout << "Rows returned: " << stats.rows_returned << std::endl;
    }
}

void demonstrate_join_execution() {
    print_separator("Join Algorithm Execution");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryPlanner logical_planner(schema);
    pg::PhysicalPlanner physical_planner(schema);
    
    // Configure for join demonstration
    pg::TableStats user_stats;
    user_stats.row_count = 100;
    user_stats.avg_row_size = 50.0;
    logical_planner.set_table_stats("users", user_stats);
    physical_planner.set_table_stats("users", user_stats);
    
    pg::TableStats product_stats;
    product_stats.row_count = 50;
    product_stats.avg_row_size = 75.0;
    logical_planner.set_table_stats("products", product_stats);
    physical_planner.set_table_stats("products", product_stats);
    
    std::string join_query = "SELECT u.name, p.name FROM users u JOIN products p ON u.id = p.id LIMIT 10";
    
    std::cout << "\nExecuting Join Query: " << join_query << std::endl;
    std::cout << std::string(50, '-') << std::endl;
    
    auto logical_plan = logical_planner.create_plan(join_query);
    auto physical_plan = physical_planner.create_physical_plan(logical_plan);
    
    std::cout << "Physical Join Plan:" << std::endl;
    std::cout << physical_plan.to_string() << std::endl;
    
    // Execute and measure
    auto start_time = std::chrono::high_resolution_clock::now();
    auto results = physical_plan.execute();
    auto end_time = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    
    std::cout << "Join Execution Results:" << std::endl;
    std::cout << "Joined rows: " << results.size() << std::endl;
    std::cout << "Execution time: " << std::fixed << std::setprecision(3) 
              << duration.count() / 1000.0 << " ms" << std::endl;
    
    // Show sample joined results
    std::cout << "\nSample joined rows:" << std::endl;
    for (size_t i = 0; i < std::min(5UL, results.size()); ++i) {
        if (results[i].values.size() >= 2) {
            std::cout << "User: " << results[i].values[0] 
                      << ", Product: " << results[i].values[1] << std::endl;
        }
    }
}

void demonstrate_parallel_execution() {
    print_separator("Parallel Execution");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryPlanner logical_planner(schema);
    pg::PhysicalPlanner physical_planner(schema);
    
    // Configure for parallel execution
    pg::PhysicalPlannerConfig config;
    config.enable_parallel_execution = true;
    config.max_parallel_workers = 4;
    config.work_mem = 1024 * 1024; // 1MB
    physical_planner.set_config(config);
    
    // Large table for parallel scan
    pg::TableStats large_table_stats;
    large_table_stats.row_count = 10000;
    large_table_stats.avg_row_size = 100.0;
    logical_planner.set_table_stats("users", large_table_stats);
    physical_planner.set_table_stats("users", large_table_stats);
    
    std::string parallel_query = "SELECT * FROM users WHERE id > 100 LIMIT 20";
    
    std::cout << "\nParallel Scan Query: " << parallel_query << std::endl;
    std::cout << std::string(50, '-') << std::endl;
    
    // Generate alternative plans (including parallel)
    auto logical_plan = logical_planner.create_plan(parallel_query);
    auto alternative_plans = physical_planner.generate_alternative_physical_plans(logical_plan);
    
    std::cout << "Generated " << alternative_plans.size() << " alternative physical plans:" << std::endl;
    
    for (size_t i = 0; i < alternative_plans.size(); ++i) {
        std::cout << "\nPlan " << (i + 1) << ":" << std::endl;
        std::cout << alternative_plans[i].to_string() << std::endl;
        
        // Execute and measure
        auto start_time = std::chrono::high_resolution_clock::now();
        auto results = alternative_plans[i].execute();
        auto end_time = std::chrono::high_resolution_clock::now();
        
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        std::cout << "Results: " << results.size() << " rows in " 
                  << std::fixed << std::setprecision(3) << duration.count() / 1000.0 << " ms" << std::endl;
    }
}

void demonstrate_physical_optimization() {
    print_separator("Physical Plan Optimization");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryPlanner logical_planner(schema);
    pg::PhysicalPlanner physical_planner(schema);
    
    // Configure for optimization demo
    pg::TableStats stats;
    stats.row_count = 100000;
    stats.avg_row_size = 200.0;
    logical_planner.set_table_stats("users", stats);
    physical_planner.set_table_stats("users", stats);
    
    std::string optimization_query = "SELECT * FROM users WHERE name LIKE 'A%' ORDER BY id LIMIT 100";
    
    std::cout << "\nOptimization Query: " << optimization_query << std::endl;
    std::cout << std::string(50, '-') << std::endl;
    
    auto logical_plan = logical_planner.create_plan(optimization_query);
    auto physical_plan = physical_planner.create_physical_plan(logical_plan);
    
    std::cout << "Original Physical Plan:" << std::endl;
    std::cout << physical_plan.to_string() << std::endl;
    
    // Apply optimizations
    auto optimized_plan = physical_planner.optimize_physical_plan(physical_plan);
    
    std::cout << "Optimized Physical Plan:" << std::endl;
    std::cout << optimized_plan.to_string() << std::endl;
    
    // Compare memory estimates
    size_t original_memory = physical_planner.estimate_memory_usage(physical_plan.root);
    size_t optimized_memory = physical_planner.estimate_memory_usage(optimized_plan.root);
    
    std::cout << "Memory Usage Comparison:" << std::endl;
    std::cout << "Original: " << format_memory_bytes(original_memory) << std::endl;
    std::cout << "Optimized: " << format_memory_bytes(optimized_memory) << std::endl;
    
    if (optimized_memory < original_memory) {
        double savings = ((double)(original_memory - optimized_memory) / original_memory) * 100.0;
        std::cout << "Memory savings: " << std::fixed << std::setprecision(1) << savings << "%" << std::endl;
    }
}

void demonstrate_batch_processing() {
    print_separator("Vectorized Batch Processing");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryPlanner logical_planner(schema);
    pg::PhysicalPlanner physical_planner(schema);
    
    pg::TableStats stats;
    stats.row_count = 5000;
    physical_planner.set_table_stats("users", stats);
    
    std::string batch_query = "SELECT * FROM users";
    
    std::cout << "\nBatch Processing Query: " << batch_query << std::endl;
    std::cout << std::string(50, '-') << std::endl;
    
    auto logical_plan = logical_planner.create_plan(batch_query);
    auto physical_plan = physical_planner.create_physical_plan(logical_plan);
    
    std::cout << "Processing in batches..." << std::endl;
    
    physical_plan.initialize();
    
    size_t total_rows = 0;
    size_t batch_count = 0;
    auto start_time = std::chrono::high_resolution_clock::now();
    
    while (physical_plan.root->has_more_data()) {
        auto batch = physical_plan.execute_batch();
        total_rows += batch.size();
        batch_count++;
        
        std::cout << "Batch " << batch_count << ": " << batch.size() << " rows" << std::endl;
        
        if (batch_count >= 10) break; // Limit output
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    
    std::cout << "\nBatch Processing Summary:" << std::endl;
    std::cout << "Total batches: " << batch_count << std::endl;
    std::cout << "Total rows: " << total_rows << std::endl;
    std::cout << "Processing time: " << std::fixed << std::setprecision(3) 
              << duration.count() / 1000.0 << " ms" << std::endl;
    std::cout << "Throughput: " << std::fixed << std::setprecision(0) 
              << (total_rows * 1000.0) / (duration.count() / 1000.0) << " rows/sec" << std::endl;
}

// Helper function moved to top of file

int main() {
    try {
        std::cout << "PostgreSQL Physical Planning and Execution Demo" << std::endl;
        std::cout << "C++17 Project with Advanced Physical Query Execution" << std::endl;
        
        demonstrate_physical_planning();
        demonstrate_physical_execution();
        demonstrate_join_execution();
        demonstrate_parallel_execution();
        demonstrate_physical_optimization();
        demonstrate_batch_processing();
        
        std::cout << "\nPhysical Planning Demo completed successfully!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}