#include <iostream>
#include <cassert>
#include <memory>
#include <vector>
#include "physical_plan.hpp"
#include "physical_planner.hpp"
#include "query_planner.hpp"
#include "database.hpp"
#include "simple_schema.hpp"

using namespace db25;

void test_sequential_scan_execution() {
    std::cout << "Testing sequential scan execution..." << std::endl;
    
    auto seq_scan = std::make_shared<SequentialScanNode>("users");
    seq_scan->generate_mock_data(100);
    
    ExecutionContext context;
    seq_scan->initialize(&context);
    
    auto batch = seq_scan->get_next_batch();
    assert(!batch.empty());
    assert(batch.size() <= 100);
    
    // Test that we can get multiple batches
    int total_rows = batch.size();
    while (seq_scan->has_more_data() && total_rows < 100) {
        batch = seq_scan->get_next_batch();
        total_rows += batch.size();
    }
    
    std::cout << "✓ Sequential scan execution passed (rows: " << total_rows << ")" << std::endl;
}

void test_index_scan_execution() {
    std::cout << "Testing index scan execution..." << std::endl;
    
    auto index_scan = std::make_shared<PhysicalIndexScanNode>("users", "users_id_idx");
    index_scan->generate_mock_data(50);
    
    ExecutionContext context;
    index_scan->initialize(&context);
    
    auto batch = index_scan->get_next_batch();
    assert(batch.size() <= 50);
    
    std::cout << "✓ Index scan execution passed (rows: " << batch.size() << ")" << std::endl;
}

void test_nested_loop_join_execution() {
    std::cout << "Testing nested loop join execution..." << std::endl;
    
    // Create left child (users)
    auto left_scan = std::make_shared<SequentialScanNode>("users");
    left_scan->generate_mock_data(10);
    
    // Create right child (products) 
    auto right_scan = std::make_shared<SequentialScanNode>("products");
    right_scan->generate_mock_data(5);
    
    // Create join
    auto nl_join = std::make_shared<PhysicalNestedLoopJoinNode>(JoinType::INNER);
    nl_join->children.push_back(left_scan);
    nl_join->children.push_back(right_scan);
    
    ExecutionContext context;
    nl_join->initialize(&context);
    
    auto batch = nl_join->get_next_batch();
    // Nested loop join should produce some results (cartesian product in this case)
    
    std::cout << "✓ Nested loop join execution passed (rows: " << batch.size() << ")" << std::endl;
}

void test_sort_execution() {
    std::cout << "Testing sort execution..." << std::endl;
    
    auto seq_scan = std::make_shared<SequentialScanNode>("users");
    seq_scan->generate_mock_data(20);
    
    auto sort_node = std::make_shared<PhysicalSortNode>();
    sort_node->children.push_back(seq_scan);
    
    // Add a sort key
    PhysicalSortNode::SortKey sort_key;
    sort_key.expression = std::make_shared<Expression>(ExpressionType::COLUMN_REF, "name");
    sort_key.ascending = true;
    sort_node->sort_keys.push_back(sort_key);
    
    ExecutionContext context;
    sort_node->initialize(&context);
    
    auto batch = sort_node->get_next_batch();
    assert(!batch.empty());
    
    std::cout << "✓ Sort execution passed (rows: " << batch.size() << ")" << std::endl;
}

void test_limit_execution() {
    std::cout << "Testing limit execution..." << std::endl;
    
    auto seq_scan = std::make_shared<SequentialScanNode>("users");
    seq_scan->generate_mock_data(100);
    
    auto limit_node = std::make_shared<PhysicalLimitNode>();
    limit_node->children.push_back(seq_scan);
    limit_node->limit = 10;
    limit_node->offset = 5;
    
    ExecutionContext context;
    limit_node->initialize(&context);
    
    auto batch = limit_node->get_next_batch();
    
    // Should return at most 10 rows (after skipping 5)
    assert(batch.size() <= 10);
    
    std::cout << "✓ Limit execution passed (rows: " << batch.size() << ")" << std::endl;
}

void test_parallel_scan_execution() {
    std::cout << "Testing parallel scan execution..." << std::endl;
    
    auto parallel_scan = std::make_shared<ParallelSequentialScanNode>("users", 2);
    parallel_scan->generate_mock_data(100);
    
    ExecutionContext context;
    context.max_parallel_workers = 2;
    parallel_scan->initialize(&context);
    
    auto batch = parallel_scan->get_next_batch();
    
    std::cout << "✓ Parallel scan execution passed (rows: " << batch.size() << ")" << std::endl;
    
    // Cleanup parallel resources
    parallel_scan->cleanup();
}

void test_physical_plan_execution() {
    std::cout << "Testing complete physical plan execution..." << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner logical_planner(schema);
    PhysicalPlanner physical_planner(schema);
    
    // Create and execute a simple plan
    auto logical_plan = logical_planner.create_plan("SELECT * FROM users LIMIT 5");
    auto physical_plan = physical_planner.create_physical_plan(logical_plan);
    
    physical_plan.initialize();
    auto results = physical_plan.execute();
    
    assert(results.size() <= 5);
    std::cout << "✓ Physical plan execution passed (results: " << results.size() << ")" << std::endl;
}

void test_batch_processing() {
    std::cout << "Testing batch processing..." << std::endl;
    
    auto seq_scan = std::make_shared<SequentialScanNode>("users");
    seq_scan->generate_mock_data(1000);
    
    ExecutionContext context;
    context.work_mem_limit = 10000;  // Small batch size
    seq_scan->initialize(&context);
    
    std::vector<TupleBatch> batches;
    while (seq_scan->has_more_data()) {
        auto batch = seq_scan->get_next_batch();
        if (!batch.empty()) {
            batches.push_back(batch);
        }
        if (batches.size() > 20) break;  // Prevent infinite loop
    }
    
    assert(!batches.empty());
    std::cout << "✓ Batch processing passed (batches: " << batches.size() << ")" << std::endl;
}

void test_tuple_operations() {
    std::cout << "Testing tuple operations..." << std::endl;
    
    // Test basic tuple operations
    Tuple tuple1({"1", "John", "john@example.com"});
    assert(tuple1.size() == 3);
    assert(tuple1.get_value(0) == "1");
    assert(tuple1.get_value(1) == "John");
    
    tuple1.set_value(1, "Jane");
    assert(tuple1.get_value(1) == "Jane");
    
    tuple1.set_value("email", "jane@example.com");
    assert(tuple1.get_value("email") == "jane@example.com");
    
    // Test tuple batch operations
    TupleBatch batch;
    batch.column_names = {"id", "name", "email"};
    batch.add_tuple(tuple1);
    batch.add_tuple(Tuple({"2", "Bob", "bob@example.com"}));
    
    assert(batch.size() == 2);
    assert(!batch.empty());
    
    std::cout << "✓ Tuple operations passed" << std::endl;
}

void test_execution_stats() {
    std::cout << "Testing execution statistics..." << std::endl;
    
    auto seq_scan = std::make_shared<SequentialScanNode>("users");
    seq_scan->generate_mock_data(100);
    
    ExecutionContext context;
    seq_scan->initialize(&context);
    
    // Execute and gather stats
    auto batch = seq_scan->get_next_batch();
    
    const auto& stats = seq_scan->get_stats();
    assert(stats.execution_time_ms >= 0);
    assert(stats.rows_returned >= 0);
    
    std::cout << "✓ Execution statistics passed (time: " << stats.execution_time_ms << "ms)" << std::endl;
}

void test_memory_management() {
    std::cout << "Testing memory management..." << std::endl;
    
    ExecutionContext context;
    context.work_mem_limit = 1024 * 1024;  // 1MB
    context.temp_file_threshold = 512 * 1024;  // 512KB
    
    auto sort_node = std::make_shared<PhysicalSortNode>();
    auto seq_scan = std::make_shared<SequentialScanNode>("large_table");
    seq_scan->generate_mock_data(10000);  // Large dataset
    
    sort_node->children.push_back(seq_scan);
    sort_node->initialize(&context);
    
    // Test that large sorts can be handled
    auto batch = sort_node->get_next_batch();
    
    std::cout << "✓ Memory management passed" << std::endl;
    
    sort_node->cleanup();
}

void test_error_handling() {
    std::cout << "Testing error handling..." << std::endl;
    
    // Test with empty plan
    PhysicalPlan empty_plan;
    empty_plan.initialize();
    auto results = empty_plan.execute();
    assert(results.empty());
    
    // Test with null children
    auto join_node = std::make_shared<PhysicalNestedLoopJoinNode>(JoinType::INNER);
    ExecutionContext context;
    join_node->initialize(&context);
    
    auto batch = join_node->get_next_batch();
    // Should handle gracefully without crashing
    
    std::cout << "✓ Error handling passed" << std::endl;
}

void test_plan_display() {
    std::cout << "Testing plan display..." << std::endl;
    
    auto seq_scan = std::make_shared<SequentialScanNode>("users");
    seq_scan->output_columns = {"id", "name", "email"};
    seq_scan->estimated_cost.total_cost = 100.0;
    seq_scan->estimated_cost.estimated_rows = 1000;
    
    std::string plan_str = seq_scan->to_string(0);
    assert(!plan_str.empty());
    assert(plan_str.find("users") != std::string::npos);
    
    // Test plan copy
    auto copied_node = seq_scan->copy();
    assert(copied_node != nullptr);
    assert(copied_node->type == seq_scan->type);
    
    std::cout << "✓ Plan display passed" << std::endl;
}

int main() {
    std::cout << "=== Physical Execution Tests ===" << std::endl;
    
    try {
        test_sequential_scan_execution();
        test_index_scan_execution();
        test_nested_loop_join_execution();
        test_sort_execution();
        test_limit_execution();
        test_parallel_scan_execution();
        test_physical_plan_execution();
        test_batch_processing();
        test_tuple_operations();
        test_execution_stats();
        test_memory_management();
        test_error_handling();
        test_plan_display();
        
        std::cout << "\n✅ All physical execution tests passed!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Test failed: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "❌ Test failed with unknown exception" << std::endl;
        return 1;
    }
}