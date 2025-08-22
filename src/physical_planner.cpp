#include "physical_planner.hpp"
#include <algorithm>
#include <random>

namespace db25 {

// Mock data generator implementation
std::random_device MockDataGenerator::rd_;
std::mt19937 MockDataGenerator::gen_(rd_());

PhysicalPlanner::PhysicalPlanner(std::shared_ptr<DatabaseSchema> schema) 
    : schema_(schema) {
    
    // Initialize default table statistics
    for (const auto& table_name : schema_->get_table_names()) {
        TableStats stats;
        stats.row_count = 1000;
        stats.avg_row_size = 100.0;
        metadata_.table_stats[table_name] = stats;
    }
    
    // Set up default execution context
    metadata_.execution_context.work_mem_limit = config_.work_mem;
    metadata_.execution_context.enable_parallel = config_.enable_parallel_execution;
    metadata_.execution_context.max_parallel_workers = config_.max_parallel_workers;
}

PhysicalPlan PhysicalPlanner::create_physical_plan(const LogicalPlan& logical_plan) {
    if (!logical_plan.root) {
        return PhysicalPlan();
    }
    
    PhysicalPlanNodePtr physical_root = convert_logical_node(logical_plan.root);
    
    PhysicalPlan physical_plan(physical_root);
    physical_plan.context = metadata_.execution_context;
    
    return physical_plan;
}

PhysicalPlanNodePtr PhysicalPlanner::convert_logical_node(LogicalPlanNodePtr logical_node) {
    if (!logical_node) return nullptr;
    
    PhysicalPlanNodePtr physical_node;
    
    switch (logical_node->type) {
        case PlanNodeType::TABLE_SCAN: {
            auto table_scan = std::static_pointer_cast<TableScanNode>(logical_node);
            physical_node = convert_table_scan(table_scan);
            break;
        }
        
        case PlanNodeType::INDEX_SCAN: {
            auto index_scan = std::static_pointer_cast<IndexScanNode>(logical_node);
            physical_node = convert_index_scan(index_scan);
            break;
        }
        
        case PlanNodeType::NESTED_LOOP_JOIN:
        case PlanNodeType::HASH_JOIN:
        case PlanNodeType::MERGE_JOIN: {
            physical_node = convert_join(logical_node);
            break;
        }
        
        case PlanNodeType::PROJECTION: {
            auto projection = std::static_pointer_cast<ProjectionNode>(logical_node);
            physical_node = convert_projection(projection);
            break;
        }
        
        case PlanNodeType::SELECTION: {
            auto selection = std::static_pointer_cast<SelectionNode>(logical_node);
            physical_node = convert_selection(selection);
            break;
        }
        
        case PlanNodeType::AGGREGATION: {
            auto aggregation = std::static_pointer_cast<AggregationNode>(logical_node);
            physical_node = convert_aggregation(aggregation);
            break;
        }
        
        case PlanNodeType::SORT: {
            auto sort = std::static_pointer_cast<SortNode>(logical_node);
            physical_node = convert_sort(sort);
            break;
        }
        
        case PlanNodeType::LIMIT: {
            auto limit = std::static_pointer_cast<LimitNode>(logical_node);
            physical_node = convert_limit(limit);
            break;
        }
        
        default:
            // For unsupported node types, create a basic sequential scan
            physical_node = std::make_shared<SequentialScanNode>("unknown_table");
            break;
    }
    
    if (physical_node) {
        // Convert children
        for (const auto& logical_child : logical_node->children) {
            PhysicalPlanNodePtr physical_child = convert_logical_node(logical_child);
            if (physical_child) {
                physical_node->children.push_back(physical_child);
            }
        }
        
        // Copy cost and output information
        physical_node->estimated_cost = logical_node->cost;
        physical_node->output_columns = logical_node->output_columns;
    }
    
    return physical_node;
}

void PhysicalPlanner::set_table_stats(const std::string& table_name, const TableStats& stats) {
    metadata_.table_stats[table_name] = stats;
}

void PhysicalPlanner::add_access_method(const std::string& table_name, const AccessMethod& method) {
    metadata_.access_methods[table_name].push_back(method);
}

PhysicalPlan PhysicalPlanner::optimize_physical_plan(const PhysicalPlan& plan) {
    PhysicalPlanOptimizer optimizer(config_);
    return optimizer.optimize(plan);
}

std::vector<PhysicalPlan> PhysicalPlanner::generate_alternative_physical_plans(const LogicalPlan& logical_plan) {
    std::vector<PhysicalPlan> alternatives;
    
    // Generate base plan
    PhysicalPlan base_plan = create_physical_plan(logical_plan);
    alternatives.push_back(base_plan);
    
    // Generate parallelized version if applicable
    if (config_.enable_parallel_execution && should_parallelize(logical_plan.root)) {
        PhysicalPlan parallel_plan = base_plan.copy();
        if (parallel_plan.root) {
            parallel_plan.root = add_parallelization(parallel_plan.root);
            alternatives.push_back(parallel_plan);
        }
    }
    
    return alternatives;
}

size_t PhysicalPlanner::estimate_memory_usage(PhysicalPlanNodePtr node) {
    if (!node) return 0;
    
    size_t memory = 0;
    
    switch (node->type) {
        case PhysicalOperatorType::HASH_JOIN:
            memory += estimate_memory_for_hash_join(node->children.empty() ? nullptr : node->children[0]);
            break;
            
        case PhysicalOperatorType::SORT:
            memory += estimate_memory_for_sort(node->children.empty() ? nullptr : node->children[0]);
            break;
            
        case PhysicalOperatorType::HASH_AGGREGATE:
            memory += node->estimated_cost.estimated_rows * 50; // Rough estimate
            break;
            
        default:
            memory += 1024; // Base memory usage
            break;
    }
    
    // Add memory for children
    for (const auto& child : node->children) {
        memory += estimate_memory_usage(child);
    }
    
    return memory;
}

bool PhysicalPlanner::should_use_temp_files(PhysicalPlanNodePtr node) {
    return estimate_memory_usage(node) > config_.work_mem;
}

// Private implementation methods
PhysicalPlanNodePtr PhysicalPlanner::convert_table_scan(std::shared_ptr<TableScanNode> logical_node) {
    // Check if we should use index scan instead
    AccessMethod best_method = select_best_access_method(logical_node->table_name, logical_node->filter_conditions);
    
    if (best_method.type == AccessMethod::INDEX_SCAN && !best_method.index_name.empty()) {
        auto index_scan = std::make_shared<PhysicalIndexScanNode>(logical_node->table_name, best_method.index_name);
        index_scan->alias = logical_node->alias;
        index_scan->filter_conditions = logical_node->filter_conditions;
        return index_scan;
    } else {
        auto seq_scan = std::make_shared<SequentialScanNode>(logical_node->table_name);
        seq_scan->alias = logical_node->alias;
        seq_scan->filter_conditions = logical_node->filter_conditions;
        return seq_scan;
    }
}

PhysicalPlanNodePtr PhysicalPlanner::convert_index_scan(std::shared_ptr<IndexScanNode> logical_node) {
    auto physical_index_scan = std::make_shared<PhysicalIndexScanNode>(logical_node->table_name, logical_node->index_name);
    physical_index_scan->alias = logical_node->alias;
    physical_index_scan->index_conditions = logical_node->index_conditions;
    physical_index_scan->filter_conditions = logical_node->filter_conditions;
    return physical_index_scan;
}

PhysicalPlanNodePtr PhysicalPlanner::convert_join(LogicalPlanNodePtr logical_node) {
    if (logical_node->children.size() != 2) {
        return nullptr;
    }
    
    PhysicalPlanNodePtr join_node = select_join_algorithm(logical_node);
    return join_node;
}

PhysicalPlanNodePtr PhysicalPlanner::convert_projection(std::shared_ptr<ProjectionNode> logical_node) {
    // Physical projection is typically implicit - just pass through the child
    // In a real implementation, might add explicit projection operators for complex expressions
    if (!logical_node->children.empty()) {
        return convert_logical_node(logical_node->children[0]);
    }
    return nullptr;
}

PhysicalPlanNodePtr PhysicalPlanner::convert_selection(std::shared_ptr<SelectionNode> logical_node) {
    // Selection is typically pushed down to scans in physical planning
    // Return the converted child with filters applied
    if (!logical_node->children.empty()) {
        PhysicalPlanNodePtr child = convert_logical_node(logical_node->children[0]);
        
        // If child is a scan node, add the selection conditions to it
        if (auto seq_scan = std::dynamic_pointer_cast<SequentialScanNode>(child)) {
            seq_scan->filter_conditions.insert(seq_scan->filter_conditions.end(),
                                              logical_node->conditions.begin(),
                                              logical_node->conditions.end());
        } else if (auto index_scan = std::dynamic_pointer_cast<PhysicalIndexScanNode>(child)) {
            index_scan->filter_conditions.insert(index_scan->filter_conditions.end(),
                                                 logical_node->conditions.begin(),
                                                 logical_node->conditions.end());
        }
        
        return child;
    }
    return nullptr;
}

PhysicalPlanNodePtr PhysicalPlanner::convert_aggregation(std::shared_ptr<AggregationNode> logical_node) {
    // TODO: Implement HashAggregateNode - returning nullptr for now
    // auto hash_agg = std::make_shared<HashAggregateNode>();
    // hash_agg->group_by_exprs = logical_node->group_by_exprs;
    // hash_agg->aggregate_exprs = logical_node->aggregate_exprs;
    // hash_agg->aggregate_functions = logical_node->aggregate_functions;
    (void)logical_node; // Suppress unused parameter warning
    return nullptr;
}

PhysicalPlanNodePtr PhysicalPlanner::convert_sort(std::shared_ptr<SortNode> logical_node) {
    auto physical_sort = std::make_shared<PhysicalSortNode>();
    
    // Convert logical sort keys to physical sort keys
    for (const auto& logical_key : logical_node->sort_keys) {
        PhysicalSortNode::SortKey physical_key;
        physical_key.expression = logical_key.expression;
        physical_key.ascending = logical_key.ascending;
        physical_key.nulls_first = logical_key.nulls_first;
        physical_sort->sort_keys.push_back(physical_key);
    }
    
    return physical_sort;
}

PhysicalPlanNodePtr PhysicalPlanner::convert_limit(std::shared_ptr<LimitNode> logical_node) {
    auto physical_limit = std::make_shared<PhysicalLimitNode>();
    physical_limit->limit = logical_node->limit;
    physical_limit->offset = logical_node->offset;
    return physical_limit;
}

AccessMethod PhysicalPlanner::select_best_access_method(const std::string& table_name,
                                                       const std::vector<ExpressionPtr>& conditions) {
    auto available_methods = get_available_access_methods(table_name);
    
    if (available_methods.empty()) {
        AccessMethod heap_scan;
        heap_scan.type = AccessMethod::HEAP_SCAN;
        heap_scan.cost = estimate_physical_cost(nullptr); // Simplified
        return heap_scan;
    }
    
    // Select method with lowest cost
    AccessMethod best_method = available_methods[0];
    for (const auto& method : available_methods) {
        if (method.cost < best_method.cost) {
            best_method = method;
        }
    }
    
    return best_method;
}

std::vector<AccessMethod> PhysicalPlanner::get_available_access_methods(const std::string& table_name) {
    std::vector<AccessMethod> methods;
    
    // Always have heap scan available
    AccessMethod heap_scan;
    heap_scan.type = AccessMethod::HEAP_SCAN;
    heap_scan.cost = get_table_stats(table_name).row_count * 0.01; // Simplified cost
    methods.push_back(heap_scan);
    
    // Add configured access methods
    auto it = metadata_.access_methods.find(table_name);
    if (it != metadata_.access_methods.end()) {
        methods.insert(methods.end(), it->second.begin(), it->second.end());
    }
    
    return methods;
}

PhysicalPlanNodePtr PhysicalPlanner::select_join_algorithm(LogicalPlanNodePtr logical_join) {
    if (logical_join->children.size() != 2) return nullptr;
    
    auto left = logical_join->children[0];
    auto right = logical_join->children[1];
    
    JoinType join_type = JoinType::INNER; // Default
    std::vector<ExpressionPtr> join_conditions;
    
    // Extract join type and conditions based on logical node type
    if (auto nl_join = std::dynamic_pointer_cast<NestedLoopJoinNode>(logical_join)) {
        join_type = nl_join->join_type;
        join_conditions = nl_join->join_conditions;
    } else if (auto hash_join = std::dynamic_pointer_cast<HashJoinNode>(logical_join)) {
        join_type = hash_join->join_type;
        join_conditions = hash_join->join_conditions;
    }
    
    // Decide between hash join and nested loop join
    if (should_use_hash_join(left, right)) {
        // auto physical_hash_join = std::make_shared<PhysicalHashJoinNode>(join_type); // TODO: Implement PhysicalHashJoinNode
        auto physical_hash_join = std::make_shared<PhysicalNestedLoopJoinNode>(join_type); // Temporary fallback
        physical_hash_join->join_conditions = join_conditions;
        return physical_hash_join;
    } else {
        auto physical_nl_join = std::make_shared<PhysicalNestedLoopJoinNode>(join_type);
        physical_nl_join->join_conditions = join_conditions;
        return physical_nl_join;
    }
}

bool PhysicalPlanner::should_use_hash_join(LogicalPlanNodePtr left, LogicalPlanNodePtr right) {
    if (!config_.enable_parallel_execution) return false;
    
    // Use hash join if one side is significantly smaller or if total size exceeds threshold
    size_t left_rows = left->cost.estimated_rows;
    size_t right_rows = right->cost.estimated_rows;
    
    return (left_rows > config_.hash_join_threshold || right_rows > config_.hash_join_threshold) &&
           (left_rows != right_rows); // Avoid hash join for similar sized tables
}

bool PhysicalPlanner::should_use_merge_join(LogicalPlanNodePtr left, LogicalPlanNodePtr right) {
    // For now, don't use merge join (would need sort order analysis)
    return false;
}

bool PhysicalPlanner::should_parallelize(LogicalPlanNodePtr node) {
    if (!config_.enable_parallel_execution) return false;
    
    // Parallelize if estimated cost is above threshold
    double cost_threshold = 1000.0; // Simplified threshold
    return node->cost.total_cost > cost_threshold;
}

size_t PhysicalPlanner::calculate_parallel_degree(LogicalPlanNodePtr node) {
    size_t degree = std::min(config_.max_parallel_workers, 
                            static_cast<size_t>(node->cost.estimated_rows / 10000));
    return std::max(1UL, degree);
}

PhysicalPlanNodePtr PhysicalPlanner::add_parallelization(PhysicalPlanNodePtr physical_node) {
    if (!physical_node) return nullptr;
    
    if (physical_node->type == PhysicalOperatorType::SEQUENTIAL_SCAN) {
        auto seq_scan = std::static_pointer_cast<SequentialScanNode>(physical_node);
        size_t degree = std::min(config_.max_parallel_workers, 4UL);
        
        auto parallel_scan = std::make_shared<ParallelSequentialScanNode>(seq_scan->table_name, degree);
        parallel_scan->filter_conditions = seq_scan->filter_conditions;
        parallel_scan->estimated_cost = seq_scan->estimated_cost;
        parallel_scan->output_columns = seq_scan->output_columns;
        
        return parallel_scan;
    }
    
    return physical_node;
}

double PhysicalPlanner::estimate_physical_cost(PhysicalPlanNodePtr node) {
    if (!node) return 0.0;
    
    // Simplified cost estimation
    return node->estimated_cost.total_cost;
}

size_t PhysicalPlanner::estimate_memory_for_hash_join(PhysicalPlanNodePtr build_side) {
    if (!build_side) return 1024;
    
    // Estimate memory needed for hash table
    return build_side->estimated_cost.estimated_rows * 64; // 64 bytes per row estimate
}

size_t PhysicalPlanner::estimate_memory_for_sort(PhysicalPlanNodePtr input) {
    if (!input) return 1024;
    
    // Memory needed to sort all input data
    return input->estimated_cost.estimated_rows * 32; // 32 bytes per row for sorting
}

TableStats PhysicalPlanner::get_table_stats(const std::string& table_name) const {
    auto it = metadata_.table_stats.find(table_name);
    if (it != metadata_.table_stats.end()) {
        return it->second;
    }
    
    // Default stats
    TableStats default_stats;
    default_stats.row_count = 1000;
    default_stats.avg_row_size = 100.0;
    return default_stats;
}

bool PhysicalPlanner::table_has_index(const std::string& table_name, const std::vector<std::string>& columns) {
    auto it = metadata_.access_methods.find(table_name);
    if (it == metadata_.access_methods.end()) return false;
    
    for (const auto& method : it->second) {
        if (method.type == AccessMethod::INDEX_SCAN) {
            // Simple check - in real implementation would check column matching
            if (!method.key_columns.empty()) {
                return true;
            }
        }
    }
    
    return false;
}

double PhysicalPlanner::estimate_join_selectivity(const std::vector<ExpressionPtr>& conditions) {
    if (conditions.empty()) return 1.0;
    
    // Simplified selectivity estimation
    double selectivity = 1.0;
    for (const auto& condition : conditions) {
        selectivity *= 0.1; // Assume 10% selectivity per condition
    }
    
    return std::max(0.001, selectivity);
}

// PhysicalPlanOptimizer implementation
PhysicalPlanOptimizer::PhysicalPlanOptimizer(const PhysicalPlannerConfig& config) 
    : config_(config) {}

PhysicalPlan PhysicalPlanOptimizer::optimize(const PhysicalPlan& plan) {
    PhysicalPlan optimized = plan.copy();
    
    if (optimized.root) {
        optimized.root = optimize_memory_usage(optimized.root);
        optimized.root = optimize_parallelism(optimized.root);
        optimized.root = optimize_vectorization(optimized.root);
        optimized.root = add_buffer_nodes(optimized.root);
    }
    
    return optimized;
}

PhysicalPlanNodePtr PhysicalPlanOptimizer::optimize_memory_usage(PhysicalPlanNodePtr node) {
    // Add materialization nodes for memory-intensive operations
    if (is_memory_intensive(node)) {
        // In a real implementation, would add materialization or spill-to-disk logic
    }
    
    // Recursively optimize children
    for (auto& child : node->children) {
        child = optimize_memory_usage(child);
    }
    
    return node;
}

PhysicalPlanNodePtr PhysicalPlanOptimizer::optimize_parallelism(PhysicalPlanNodePtr node) {
    if (can_benefit_from_parallelism(node)) {
        // Add parallel execution wrappers
        // In a real implementation, would add gather/scatter nodes
    }
    
    for (auto& child : node->children) {
        child = optimize_parallelism(child);
    }
    
    return node;
}

PhysicalPlanNodePtr PhysicalPlanOptimizer::optimize_vectorization(PhysicalPlanNodePtr node) {
    // Add vectorized execution hints
    // In a real implementation, would optimize batch sizes and add SIMD operations
    
    for (auto& child : node->children) {
        child = optimize_vectorization(child);
    }
    
    return node;
}

PhysicalPlanNodePtr PhysicalPlanOptimizer::add_buffer_nodes(PhysicalPlanNodePtr node) {
    // Add buffering between operators where beneficial
    // In a real implementation, would add materialization nodes
    
    for (auto& child : node->children) {
        child = add_buffer_nodes(child);
    }
    
    return node;
}

bool PhysicalPlanOptimizer::is_memory_intensive(PhysicalPlanNodePtr node) {
    return node->type == PhysicalOperatorType::SORT || 
           node->type == PhysicalOperatorType::HASH_JOIN ||
           node->type == PhysicalOperatorType::HASH_AGGREGATE;
}

bool PhysicalPlanOptimizer::can_benefit_from_parallelism(PhysicalPlanNodePtr node) {
    return node->estimated_cost.estimated_rows > 10000 &&
           (node->type == PhysicalOperatorType::SEQUENTIAL_SCAN ||
            node->type == PhysicalOperatorType::SORT ||
            node->type == PhysicalOperatorType::HASH_AGGREGATE);
}

size_t PhysicalPlanOptimizer::estimate_parallelism_overhead(PhysicalPlanNodePtr node) {
    // Rough estimate of parallelization overhead
    return node->estimated_cost.estimated_rows / 1000;
}

// Mock data generator implementation
std::vector<Tuple> MockDataGenerator::generate_table_data(const std::string& table_name, 
                                                         size_t num_rows,
                                                         const std::vector<std::string>& columns) {
    std::vector<Tuple> data;
    data.reserve(num_rows);
    
    for (size_t i = 0; i < num_rows; ++i) {
        data.push_back(generate_random_tuple(columns));
    }
    
    return data;
}

Tuple MockDataGenerator::generate_random_tuple(const std::vector<std::string>& columns) {
    Tuple tuple;
    
    for (const auto& column : columns) {
        if (column == "id") {
            tuple.values.push_back(std::to_string(generate_random_int(1, 1000000)));
        } else if (column == "name" || column == "email") {
            tuple.values.push_back(generate_random_string());
        } else if (column == "price" || column == "amount") {
            tuple.values.push_back(std::to_string(generate_random_double()));
        } else {
            tuple.values.push_back(generate_random_string(5));
        }
    }
    
    return tuple;
}

std::string MockDataGenerator::generate_random_string(size_t length) {
    const std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::uniform_int_distribution<> dis(0, chars.size() - 1);
    
    std::string result;
    result.reserve(length);
    
    for (size_t i = 0; i < length; ++i) {
        result += chars[dis(gen_)];
    }
    
    return result;
}

int MockDataGenerator::generate_random_int(int min, int max) {
    std::uniform_int_distribution<> dis(min, max);
    return dis(gen_);
}

double MockDataGenerator::generate_random_double(double min, double max) {
    std::uniform_real_distribution<> dis(min, max);
    return dis(gen_);
}

}