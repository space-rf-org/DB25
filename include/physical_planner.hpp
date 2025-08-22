#pragma once

#include "logical_plan.hpp"
#include "physical_plan.hpp"
#include "database.hpp"
#include "query_planner.hpp"
#include <memory>
#include <unordered_map>
#include <random>

namespace db25 {

// Physical planning configuration
struct PhysicalPlannerConfig {
    bool enable_parallel_execution = true;
    size_t max_parallel_workers = std::thread::hardware_concurrency();
    size_t work_mem = 1024 * 1024; // 1MB
    size_t hash_join_threshold = 10000; // Prefer hash join above this size
    size_t index_scan_threshold = 1000; // Prefer index scan below this size
    double parallel_threshold = 0.1; // Parallelize if cost > 10% of total
    bool enable_vectorization = true;
    size_t batch_size = 1000;
    std::string temp_dir = "/tmp";
};

// Table access method information
struct AccessMethod {
    enum Type {
        HEAP_SCAN,
        INDEX_SCAN,
        BITMAP_SCAN
    } type;
    
    std::string index_name;
    std::vector<std::string> key_columns;
    double selectivity = 1.0;
    double cost = 0.0;
};

// Physical plan statistics and metadata
struct PhysicalPlanMetadata {
    std::unordered_map<std::string, TableStats> table_stats;
    std::unordered_map<std::string, std::vector<AccessMethod>> access_methods;
    ExecutionContext execution_context;
};

// Main physical planner class
class PhysicalPlanner {
public:
    explicit PhysicalPlanner(std::shared_ptr<DatabaseSchema> schema);
    
    // Main planning interface
    PhysicalPlan create_physical_plan(const LogicalPlan& logical_plan);
    PhysicalPlanNodePtr convert_logical_node(LogicalPlanNodePtr logical_node);
    
    // Configuration
    void set_config(const PhysicalPlannerConfig& config) { config_ = config; }
    const PhysicalPlannerConfig& get_config() const { return config_; }
    
    // Statistics and metadata
    void set_table_stats(const std::string& table_name, const TableStats& stats);
    void add_access_method(const std::string& table_name, const AccessMethod& method);
    
    // Optimization and analysis
    PhysicalPlan optimize_physical_plan(const PhysicalPlan& plan);
    std::vector<PhysicalPlan> generate_alternative_physical_plans(const LogicalPlan& logical_plan);
    
    // Memory management
    size_t estimate_memory_usage(PhysicalPlanNodePtr node);
    bool should_use_temp_files(PhysicalPlanNodePtr node);
    
private:
    std::shared_ptr<DatabaseSchema> schema_;
    PhysicalPlannerConfig config_;
    PhysicalPlanMetadata metadata_;
    
    // Node conversion methods
    PhysicalPlanNodePtr convert_table_scan(std::shared_ptr<TableScanNode> logical_node);
    PhysicalPlanNodePtr convert_index_scan(std::shared_ptr<IndexScanNode> logical_node);
    PhysicalPlanNodePtr convert_join(LogicalPlanNodePtr logical_node);
    PhysicalPlanNodePtr convert_projection(std::shared_ptr<ProjectionNode> logical_node);
    PhysicalPlanNodePtr convert_selection(std::shared_ptr<SelectionNode> logical_node);
    PhysicalPlanNodePtr convert_aggregation(std::shared_ptr<AggregationNode> logical_node);
    PhysicalPlanNodePtr convert_sort(std::shared_ptr<SortNode> logical_node);
    PhysicalPlanNodePtr convert_limit(std::shared_ptr<LimitNode> logical_node);
    
    // Access method selection
    AccessMethod select_best_access_method(const std::string& table_name,
                                          const std::vector<ExpressionPtr>& conditions);
    std::vector<AccessMethod> get_available_access_methods(const std::string& table_name);
    
    // Join algorithm selection
    PhysicalPlanNodePtr select_join_algorithm(LogicalPlanNodePtr logical_join);
    bool should_use_hash_join(LogicalPlanNodePtr left, LogicalPlanNodePtr right);
    bool should_use_merge_join(LogicalPlanNodePtr left, LogicalPlanNodePtr right);
    
    // Parallelization decisions
    bool should_parallelize(LogicalPlanNodePtr node);
    size_t calculate_parallel_degree(LogicalPlanNodePtr node);
    PhysicalPlanNodePtr add_parallelization(PhysicalPlanNodePtr physical_node);
    
    // Memory and cost analysis
    double estimate_physical_cost(PhysicalPlanNodePtr node);
    size_t estimate_memory_for_hash_join(PhysicalPlanNodePtr build_side);
    size_t estimate_memory_for_sort(PhysicalPlanNodePtr input);
    
    // Optimization transformations
    PhysicalPlanNodePtr apply_vectorization(PhysicalPlanNodePtr node);
    PhysicalPlanNodePtr optimize_batch_sizes(PhysicalPlanNodePtr node);
    PhysicalPlanNodePtr add_materialization_nodes(PhysicalPlanNodePtr node);
    
    // Utility methods
    TableStats get_table_stats(const std::string& table_name) const;
    bool table_has_index(const std::string& table_name, const std::vector<std::string>& columns);
    double estimate_join_selectivity(const std::vector<ExpressionPtr>& conditions);
};

// Physical plan optimizer
class PhysicalPlanOptimizer {
public:
    explicit PhysicalPlanOptimizer(const PhysicalPlannerConfig& config);
    
    PhysicalPlan optimize(const PhysicalPlan& plan);
    
private:
    PhysicalPlannerConfig config_;
    
    // Optimization passes
    PhysicalPlanNodePtr optimize_memory_usage(PhysicalPlanNodePtr node);
    PhysicalPlanNodePtr optimize_parallelism(PhysicalPlanNodePtr node);
    PhysicalPlanNodePtr optimize_vectorization(PhysicalPlanNodePtr node);
    PhysicalPlanNodePtr add_buffer_nodes(PhysicalPlanNodePtr node);
    
    // Analysis methods
    bool is_memory_intensive(PhysicalPlanNodePtr node);
    bool can_benefit_from_parallelism(PhysicalPlanNodePtr node);
    size_t estimate_parallelism_overhead(PhysicalPlanNodePtr node);
};

// Execution engine interface
class ExecutionEngine {
public:
    explicit ExecutionEngine(const ExecutionContext& context);
    
    // Execution methods
    std::vector<Tuple> execute_plan(PhysicalPlan& plan);
    TupleBatch execute_batch(PhysicalPlan& plan);
    void execute_async(PhysicalPlan& plan, std::function<void(const TupleBatch&)> callback);
    
    // Execution control
    void pause_execution();
    void resume_execution();
    void cancel_execution();
    
    // Statistics and monitoring
    ExecutionStats get_execution_stats() const;
    std::string get_execution_profile() const;
    
private:
    ExecutionContext context_;
    ExecutionStats stats_;
    std::atomic<bool> paused_{false};
    std::atomic<bool> cancelled_{false};
    
    // Execution helpers
    void execute_node(PhysicalPlanNodePtr node);
    void monitor_memory_usage();
    void handle_execution_errors(const std::exception& e);
};

// Mock data generator for testing
class MockDataGenerator {
public:
    static std::vector<Tuple> generate_table_data(const std::string& table_name, 
                                                 size_t num_rows,
                                                 const std::vector<std::string>& columns);
    
    static Tuple generate_random_tuple(const std::vector<std::string>& columns);
    static std::string generate_random_string(size_t length = 10);
    static int generate_random_int(int min = 0, int max = 1000000);
    static double generate_random_double(double min = 0.0, double max = 1000.0);
    
private:
    static std::random_device rd_;
    static std::mt19937 gen_;
};

// Physical plan analysis utilities
class PhysicalPlanAnalyzer {
public:
    static std::string analyze_plan(const PhysicalPlan& plan);
    static std::string explain_execution_stats(const PhysicalPlan& plan);
    static std::vector<std::string> identify_bottlenecks(const PhysicalPlan& plan);
    static std::string suggest_optimizations(const PhysicalPlan& plan);
    
private:
    static std::string format_operator_stats(PhysicalPlanNodePtr node, int depth = 0);
    static std::string format_memory_usage(size_t bytes);
    static std::string format_duration(double milliseconds);
    static double calculate_cpu_efficiency(PhysicalPlanNodePtr node);
    static double calculate_io_efficiency(PhysicalPlanNodePtr node);
};

}