#pragma once

#include "logical_plan.hpp"
#include <memory>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <chrono>

namespace pg {

// Forward declarations
struct PhysicalPlanNode;
using PhysicalPlanNodePtr = std::shared_ptr<PhysicalPlanNode>;

// Execution context and state
struct ExecutionContext {
    size_t work_mem_limit = 1024 * 1024; // 1MB default
    size_t temp_file_threshold = 512 * 1024; // 512KB
    std::string temp_dir = "/tmp";
    bool enable_parallel = true;
    size_t max_parallel_workers = std::thread::hardware_concurrency();
};

// Tuple representation
struct Tuple {
    std::vector<std::string> values;
    std::unordered_map<std::string, std::string> column_map;
    
    Tuple() = default;
    Tuple(const std::vector<std::string>& vals) : values(vals) {}
    
    std::string get_value(size_t index) const {
        return index < values.size() ? values[index] : "";
    }
    
    std::string get_value(const std::string& column) const {
        auto it = column_map.find(column);
        return it != column_map.end() ? it->second : "";
    }
    
    void set_value(size_t index, const std::string& value) {
        if (index >= values.size()) {
            values.resize(index + 1);
        }
        values[index] = value;
    }
    
    void set_value(const std::string& column, const std::string& value) {
        column_map[column] = value;
    }
    
    size_t size() const { return values.size(); }
    bool empty() const { return values.empty() && column_map.empty(); }
};

// Batch of tuples for vectorized processing
struct TupleBatch {
    std::vector<Tuple> tuples;
    std::vector<std::string> column_names;
    size_t batch_size = 1000; // Default batch size
    
    void add_tuple(const Tuple& tuple) {
        tuples.push_back(tuple);
    }
    
    void clear() {
        tuples.clear();
    }
    
    size_t size() const { return tuples.size(); }
    bool empty() const { return tuples.empty(); }
    bool is_full() const { return tuples.size() >= batch_size; }
};

// Physical operator interface
enum class PhysicalOperatorType {
    SEQUENTIAL_SCAN,
    INDEX_SCAN,
    BITMAP_HEAP_SCAN,
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    MERGE_JOIN,
    SORT,
    HASH_AGGREGATE,
    GROUP_AGGREGATE,
    LIMIT,
    MATERIALIZE,
    GATHER,
    GATHER_MERGE,
    PARALLEL_SEQ_SCAN,
    PARALLEL_HASH_JOIN
};

// Execution statistics
struct ExecutionStats {
    size_t rows_processed = 0;
    size_t rows_returned = 0;
    double execution_time_ms = 0.0;
    size_t memory_used_bytes = 0;
    size_t disk_reads = 0;
    size_t disk_writes = 0;
    bool used_temp_files = false;
    
    void merge(const ExecutionStats& other) {
        rows_processed += other.rows_processed;
        rows_returned += other.rows_returned;
        execution_time_ms += other.execution_time_ms;
        memory_used_bytes = std::max(memory_used_bytes, other.memory_used_bytes);
        disk_reads += other.disk_reads;
        disk_writes += other.disk_writes;
        used_temp_files = used_temp_files || other.used_temp_files;
    }
};

// Base physical plan node
struct PhysicalPlanNode {
    PhysicalOperatorType type;
    std::vector<PhysicalPlanNodePtr> children;
    std::vector<std::string> output_columns;
    PlanCost estimated_cost;
    ExecutionStats actual_stats;
    ExecutionContext* context = nullptr;
    
    PhysicalPlanNode(PhysicalOperatorType t) : type(t) {}
    virtual ~PhysicalPlanNode() = default;
    
    // Execution interface
    virtual void initialize(ExecutionContext* ctx) { context = ctx; }
    virtual TupleBatch get_next_batch() = 0;
    virtual void reset() = 0;
    virtual void cleanup() {}
    
    // Plan display
    virtual std::string to_string(int indent = 0) const = 0;
    virtual PhysicalPlanNodePtr copy() const = 0;
    
    // Helper methods
    bool has_more_data() const { return has_more_data_; }
    const ExecutionStats& get_stats() const { return actual_stats; }
    
protected:
    bool has_more_data_ = true;
    std::chrono::high_resolution_clock::time_point start_time_;
    
    void start_timing() {
        start_time_ = std::chrono::high_resolution_clock::now();
    }
    
    void end_timing() {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time_);
        actual_stats.execution_time_ms += duration.count() / 1000.0;
    }
};

// Sequential scan operator
struct SequentialScanNode : PhysicalPlanNode {
    std::string table_name;
    std::string alias;
    std::vector<ExpressionPtr> filter_conditions;
    
    // Mock data source - in real implementation this would connect to storage
    std::vector<Tuple> mock_data;
    size_t current_position = 0;
    
    SequentialScanNode(const std::string& table) 
        : PhysicalPlanNode(PhysicalOperatorType::SEQUENTIAL_SCAN), table_name(table) {}
    
    void initialize(ExecutionContext* ctx) override;
    TupleBatch get_next_batch() override;
    void reset() override;
    
    std::string to_string(int indent = 0) const override;
    PhysicalPlanNodePtr copy() const override;
    
    void generate_mock_data(size_t num_rows);
};

// Index scan operator
struct PhysicalIndexScanNode : PhysicalPlanNode {
    std::string table_name;
    std::string index_name;
    std::string alias;
    std::vector<ExpressionPtr> index_conditions;
    std::vector<ExpressionPtr> filter_conditions;
    
    std::vector<Tuple> mock_data;
    size_t current_position = 0;
    
    PhysicalIndexScanNode(const std::string& table, const std::string& index) 
        : PhysicalPlanNode(PhysicalOperatorType::INDEX_SCAN), 
          table_name(table), index_name(index) {}
    
    void initialize(ExecutionContext* ctx) override;
    TupleBatch get_next_batch() override;
    void reset() override;
    
    std::string to_string(int indent = 0) const override;
    PhysicalPlanNodePtr copy() const override;
    
    void generate_mock_data(size_t num_rows);
};

// Nested loop join operator
struct PhysicalNestedLoopJoinNode : PhysicalPlanNode {
    JoinType join_type;
    std::vector<ExpressionPtr> join_conditions;
    
    TupleBatch outer_batch;
    TupleBatch inner_batch;
    size_t outer_index = 0;
    size_t inner_index = 0;
    bool outer_exhausted = false;
    bool inner_exhausted = false;
    
    PhysicalNestedLoopJoinNode(JoinType jt) 
        : PhysicalPlanNode(PhysicalOperatorType::NESTED_LOOP_JOIN), join_type(jt) {}
    
    void initialize(ExecutionContext* ctx) override;
    TupleBatch get_next_batch() override;
    void reset() override;
    
    std::string to_string(int indent = 0) const override;
    PhysicalPlanNodePtr copy() const override;
    
private:
    bool evaluate_join_condition(const Tuple& outer_tuple, const Tuple& inner_tuple);
    Tuple merge_tuples(const Tuple& outer_tuple, const Tuple& inner_tuple);
};

// Hash join operator
struct PhysicalHashJoinNode : PhysicalPlanNode {
    JoinType join_type;
    std::vector<ExpressionPtr> join_conditions;
    std::vector<ExpressionPtr> hash_keys_left;
    std::vector<ExpressionPtr> hash_keys_right;
    
    // Hash table for build phase
    std::unordered_map<std::string, std::vector<Tuple>> hash_table;
    std::vector<Tuple> probe_batch;
    size_t probe_index = 0;
    bool build_phase_complete = false;
    bool probe_phase_complete = false;
    
    PhysicalHashJoinNode(JoinType jt) 
        : PhysicalPlanNode(PhysicalOperatorType::HASH_JOIN), join_type(jt) {}
    
    void initialize(ExecutionContext* ctx) override;
    TupleBatch get_next_batch() override;
    void reset() override;
    void cleanup() override;
    
    std::string to_string(int indent = 0) const override;
    PhysicalPlanNodePtr copy() const override;
    
private:
    void build_hash_table();
    std::string compute_hash_key(const Tuple& tuple, const std::vector<ExpressionPtr>& keys);
    bool evaluate_join_condition(const Tuple& left_tuple, const Tuple& right_tuple);
    Tuple merge_tuples(const Tuple& left_tuple, const Tuple& right_tuple);
};

// Sort operator
struct PhysicalSortNode : PhysicalPlanNode {
    struct SortKey {
        ExpressionPtr expression;
        bool ascending = true;
        bool nulls_first = false;
    };
    
    std::vector<SortKey> sort_keys;
    std::vector<Tuple> sorted_data;
    size_t current_position = 0;
    bool sorting_complete = false;
    
    PhysicalSortNode() : PhysicalPlanNode(PhysicalOperatorType::SORT) {}
    
    void initialize(ExecutionContext* ctx) override;
    TupleBatch get_next_batch() override;
    void reset() override;
    void cleanup() override;
    
    std::string to_string(int indent = 0) const override;
    PhysicalPlanNodePtr copy() const override;
    
private:
    void perform_sort();
    bool compare_tuples(const Tuple& a, const Tuple& b);
    std::string extract_sort_value(const Tuple& tuple, const ExpressionPtr& expr);
};

// Hash aggregate operator
struct HashAggregateNode : PhysicalPlanNode {
    std::vector<ExpressionPtr> group_by_exprs;
    std::vector<ExpressionPtr> aggregate_exprs;
    std::vector<std::string> aggregate_functions;
    
    std::unordered_map<std::string, Tuple> groups;
    std::unordered_map<std::string, std::vector<double>> aggregate_values;
    std::vector<std::string> group_keys;
    size_t current_group_index = 0;
    bool aggregation_complete = false;
    
    HashAggregateNode() : PhysicalPlanNode(PhysicalOperatorType::HASH_AGGREGATE) {}
    
    void initialize(ExecutionContext* ctx) override;
    TupleBatch get_next_batch() override;
    void reset() override;
    void cleanup() override;
    
    std::string to_string(int indent = 0) const override;
    PhysicalPlanNodePtr copy() const override;
    
private:
    void perform_aggregation();
    std::string compute_group_key(const Tuple& tuple);
    void update_aggregates(const std::string& group_key, const Tuple& tuple);
    Tuple create_result_tuple(const std::string& group_key);
};

// Limit operator
struct PhysicalLimitNode : PhysicalPlanNode {
    std::optional<size_t> limit;
    std::optional<size_t> offset;
    size_t rows_returned = 0;
    size_t rows_skipped = 0;
    
    PhysicalLimitNode() : PhysicalPlanNode(PhysicalOperatorType::LIMIT) {}
    
    void initialize(ExecutionContext* ctx) override;
    TupleBatch get_next_batch() override;
    void reset() override;
    
    std::string to_string(int indent = 0) const override;
    PhysicalPlanNodePtr copy() const override;
};

// Physical plan container
struct PhysicalPlan {
    PhysicalPlanNodePtr root;
    ExecutionContext context;
    ExecutionStats total_stats;
    
    PhysicalPlan() = default;
    PhysicalPlan(PhysicalPlanNodePtr r) : root(r) {}
    
    // Execution interface
    void initialize();
    std::vector<Tuple> execute();
    TupleBatch execute_batch();
    void reset();
    void cleanup();
    
    // Display and analysis
    std::string to_string() const;
    std::string explain_analyze() const;
    ExecutionStats get_execution_stats() const;
    
    PhysicalPlan copy() const;
};

// Parallel execution support
struct ParallelContext {
    std::mutex result_mutex;
    std::condition_variable worker_cv;
    std::queue<TupleBatch> result_queue;
    std::atomic<size_t> active_workers{0};
    std::atomic<bool> execution_complete{false};
    
    void add_result_batch(const TupleBatch& batch);
    TupleBatch get_result_batch();
    bool has_results() const;
    void signal_completion();
    void wait_for_completion();
};

// Parallel sequential scan
struct ParallelSequentialScanNode : PhysicalPlanNode {
    std::string table_name;
    std::vector<ExpressionPtr> filter_conditions;
    size_t parallel_degree;
    
    std::shared_ptr<ParallelContext> parallel_ctx;
    std::vector<std::thread> worker_threads;
    std::vector<Tuple> mock_data;
    
    ParallelSequentialScanNode(const std::string& table, size_t degree) 
        : PhysicalPlanNode(PhysicalOperatorType::PARALLEL_SEQ_SCAN), 
          table_name(table), parallel_degree(degree) {}
    
    void initialize(ExecutionContext* ctx) override;
    TupleBatch get_next_batch() override;
    void reset() override;
    void cleanup() override;
    
    std::string to_string(int indent = 0) const override;
    PhysicalPlanNodePtr copy() const override;
    
private:
    void worker_scan(size_t worker_id, size_t start_row, size_t end_row);
    void generate_mock_data(size_t num_rows);
};

}