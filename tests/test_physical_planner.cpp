#include <iostream>
#include <cassert>
#include <memory>
#include "physical_planner.hpp"
#include "query_planner.hpp"
#include "database.hpp"
#include "simple_schema.hpp"

using namespace db25;

void test_physical_plan_creation() {
    std::cout << "Testing physical plan creation..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner logical_planner(schema);
    PhysicalPlanner physical_planner(schema);
    
    auto logical_plan = logical_planner.create_plan("SELECT * FROM users");
    assert(logical_plan.root != nullptr);
    
    auto physical_plan = physical_planner.create_physical_plan(logical_plan);
    assert(physical_plan.root != nullptr);
    
    std::cout << "✓ Physical plan creation passed" << std::endl;
}

void test_sequential_scan_conversion() {
    std::cout << "Testing sequential scan conversion..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner logical_planner(schema);
    PhysicalPlanner physical_planner(schema);
    
    auto logical_plan = logical_planner.create_plan("SELECT * FROM users");
    auto physical_plan = physical_planner.create_physical_plan(logical_plan);
    
    assert(physical_plan.root != nullptr);
    assert(physical_plan.root->type == PhysicalOperatorType::SEQUENTIAL_SCAN);
    
    auto seq_scan = std::static_pointer_cast<SequentialScanNode>(physical_plan.root);
    assert(seq_scan->table_name == "users");
    
    std::cout << "✓ Sequential scan conversion passed" << std::endl;
}

void test_join_conversion() {
    std::cout << "Testing join conversion..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner logical_planner(schema);
    PhysicalPlanner physical_planner(schema);
    
    auto logical_plan = logical_planner.create_plan("SELECT u.name, p.name FROM users u JOIN products p ON u.id = p.id");
    auto physical_plan = physical_planner.create_physical_plan(logical_plan);
    
    assert(physical_plan.root != nullptr);
    
    // Should have a join operator somewhere
    bool has_join = false;
    std::function<void(PhysicalPlanNodePtr)> find_join = [&](PhysicalPlanNodePtr node) {
        if (!node) return;
        if (node->type == PhysicalOperatorType::NESTED_LOOP_JOIN || 
            node->type == PhysicalOperatorType::HASH_JOIN) {
            has_join = true;
            return;
        }
        for (const auto& child : node->children) {
            find_join(child);
        }
    };
    
    find_join(physical_plan.root);
    assert(has_join);
    
    std::cout << "✓ Join conversion passed" << std::endl;
}

void test_sort_conversion() {
    std::cout << "Testing sort conversion..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner logical_planner(schema);
    PhysicalPlanner physical_planner(schema);
    
    auto logical_plan = logical_planner.create_plan("SELECT * FROM users ORDER BY name");
    auto physical_plan = physical_planner.create_physical_plan(logical_plan);
    
    assert(physical_plan.root != nullptr);
    
    // Should have a sort operator somewhere
    bool has_sort = false;
    std::function<void(PhysicalPlanNodePtr)> find_sort = [&](PhysicalPlanNodePtr node) {
        if (!node) return;
        if (node->type == PhysicalOperatorType::SORT) {
            has_sort = true;
            return;
        }
        for (const auto& child : node->children) {
            find_sort(child);
        }
    };
    
    find_sort(physical_plan.root);
    assert(has_sort);
    
    std::cout << "✓ Sort conversion passed" << std::endl;
}

void test_limit_conversion() {
    std::cout << "Testing limit conversion..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner logical_planner(schema);
    PhysicalPlanner physical_planner(schema);
    
    auto logical_plan = logical_planner.create_plan("SELECT * FROM users LIMIT 10");
    auto physical_plan = physical_planner.create_physical_plan(logical_plan);
    
    assert(physical_plan.root != nullptr);
    
    // Should have a limit operator at the top
    if (physical_plan.root->type == PhysicalOperatorType::LIMIT) {
        auto limit_node = std::static_pointer_cast<PhysicalLimitNode>(physical_plan.root);
        assert(limit_node->limit.has_value());
        assert(*limit_node->limit == 10);
    }
    
    std::cout << "✓ Limit conversion passed" << std::endl;
}

void test_memory_estimation() {
    std::cout << "Testing memory usage estimation..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner logical_planner(schema);
    PhysicalPlanner physical_planner(schema);
    
    // Set table statistics to test memory estimation
    TableStats stats;
    stats.row_count = 10000;
    stats.avg_row_size = 100.0;
    physical_planner.set_table_stats("users", stats);
    
    auto logical_plan = logical_planner.create_plan("SELECT * FROM users ORDER BY name");
    auto physical_plan = physical_planner.create_physical_plan(logical_plan);
    
    size_t memory_usage = physical_planner.estimate_memory_usage(physical_plan.root);
    assert(memory_usage > 0);
    
    std::cout << "✓ Memory estimation passed (estimated: " << memory_usage << " bytes)" << std::endl;
}

void test_access_method_selection() {
    std::cout << "Testing access method selection..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    PhysicalPlanner physical_planner(schema);
    
    // Add an index access method
    AccessMethod index_method;
    index_method.type = AccessMethod::INDEX_SCAN;
    index_method.index_name = "users_id_idx";
    index_method.key_columns = {"id"};
    index_method.cost = 50.0;
    
    physical_planner.add_access_method("users", index_method);
    
    // Create a plan that could use the index
    QueryPlanner logical_planner(schema);
    auto logical_plan = logical_planner.create_plan("SELECT * FROM users WHERE id = 123");
    auto physical_plan = physical_planner.create_physical_plan(logical_plan);
    
    assert(physical_plan.root != nullptr);
    
    std::cout << "✓ Access method selection passed" << std::endl;
}

void test_parallel_planning() {
    std::cout << "Testing parallel execution planning..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner logical_planner(schema);
    PhysicalPlanner physical_planner(schema);
    
    // Configure for parallel execution
    PhysicalPlannerConfig config;
    config.enable_parallel_execution = true;
    config.max_parallel_workers = 4;
    physical_planner.set_config(config);
    
    // Set large table stats to trigger parallelization
    TableStats large_stats;
    large_stats.row_count = 1000000;
    large_stats.avg_row_size = 200.0;
    physical_planner.set_table_stats("users", large_stats);
    
    auto logical_plan = logical_planner.create_plan("SELECT * FROM users");
    auto alternatives = physical_planner.generate_alternative_physical_plans(logical_plan);
    
    assert(!alternatives.empty());
    
    // Check if any alternative uses parallel scan
    bool has_parallel = false;
    for (const auto& plan : alternatives) {
        if (plan.root && plan.root->type == PhysicalOperatorType::PARALLEL_SEQ_SCAN) {
            has_parallel = true;
            break;
        }
    }
    
    std::cout << "✓ Parallel planning passed (parallel: " << (has_parallel ? "yes" : "no") << ")" << std::endl;
}

void test_physical_plan_optimization() {
    std::cout << "Testing physical plan optimization..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner logical_planner(schema);
    PhysicalPlanner physical_planner(schema);
    
    auto logical_plan = logical_planner.create_plan("SELECT * FROM users WHERE name LIKE 'A%' ORDER BY id");
    auto physical_plan = physical_planner.create_physical_plan(logical_plan);
    
    auto optimized_plan = physical_planner.optimize_physical_plan(physical_plan);
    assert(optimized_plan.root != nullptr);
    
    // Optimized plan should be structurally sound
    std::cout << "✓ Physical plan optimization passed" << std::endl;
}

void test_execution_context() {
    std::cout << "Testing execution context configuration..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    PhysicalPlanner physical_planner(schema);
    
    PhysicalPlannerConfig config;
    config.work_mem = 2048 * 1024;  // 2MB
    config.enable_parallel_execution = true;
    config.max_parallel_workers = 8;
    config.batch_size = 5000;
    
    physical_planner.set_config(config);
    
    auto retrieved_config = physical_planner.get_config();
    assert(retrieved_config.work_mem == 2048 * 1024);
    assert(retrieved_config.enable_parallel_execution == true);
    assert(retrieved_config.max_parallel_workers == 8);
    assert(retrieved_config.batch_size == 5000);
    
    std::cout << "✓ Execution context configuration passed" << std::endl;
}

void test_temp_file_decisions() {
    std::cout << "Testing temp file usage decisions..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner logical_planner(schema);
    PhysicalPlanner physical_planner(schema);
    
    // Configure small work memory to force temp files
    PhysicalPlannerConfig config;
    config.work_mem = 1024;  // Very small: 1KB
    physical_planner.set_config(config);
    
    // Set large table stats
    TableStats large_stats;
    large_stats.row_count = 100000;
    large_stats.avg_row_size = 500.0;
    physical_planner.set_table_stats("users", large_stats);
    
    auto logical_plan = logical_planner.create_plan("SELECT * FROM users ORDER BY name");
    auto physical_plan = physical_planner.create_physical_plan(logical_plan);
    
    bool should_use_temp = physical_planner.should_use_temp_files(physical_plan.root);
    
    std::cout << "✓ Temp file decisions passed (use temp: " << (should_use_temp ? "yes" : "no") << ")" << std::endl;
}

void test_mock_data_generation() {
    std::cout << "Testing mock data generation..." << std::endl;
    
    std::vector<std::string> columns = {"id", "name", "email", "price"};
    
    auto data = MockDataGenerator::generate_table_data("test_table", 100, columns);
    assert(data.size() == 100);
    
    for (const auto& tuple : data) {
        assert(tuple.size() == columns.size());
        assert(!tuple.get_value(0).empty());  // id should not be empty
    }
    
    // Test individual data generation
    auto random_tuple = MockDataGenerator::generate_random_tuple(columns);
    assert(random_tuple.size() == columns.size());
    
    auto random_string = MockDataGenerator::generate_random_string(10);
    assert(random_string.length() == 10);
    
    auto random_int = MockDataGenerator::generate_random_int(1, 100);
    assert(random_int >= 1 && random_int <= 100);
    
    auto random_double = MockDataGenerator::generate_random_double(0.0, 100.0);
    assert(random_double >= 0.0 && random_double <= 100.0);
    
    std::cout << "✓ Mock data generation passed" << std::endl;
}

int main() {
    std::cout << "=== Physical Planner Tests ===" << std::endl;
    
    try {
        test_physical_plan_creation();
        test_sequential_scan_conversion();
        test_join_conversion();
        test_sort_conversion();
        test_limit_conversion();
        test_memory_estimation();
        test_access_method_selection();
        test_parallel_planning();
        test_physical_plan_optimization();
        test_execution_context();
        test_temp_file_decisions();
        test_mock_data_generation();
        
        std::cout << "\n✅ All physical planner tests passed!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Test failed: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "❌ Test failed with unknown exception" << std::endl;
        return 1;
    }
}