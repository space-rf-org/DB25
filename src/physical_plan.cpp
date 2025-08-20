#include "physical_plan.hpp"
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <random>
#include <chrono>
#include <thread>

namespace pg {

// Helper function for indentation
std::string physical_indent_string(int indent) {
    return std::string(indent * 2, ' ');
}

// Helper function for cost formatting
std::string format_physical_cost(const PlanCost& cost) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    oss << "cost=" << cost.startup_cost << ".." << cost.total_cost 
        << " rows=" << cost.estimated_rows;
    return oss.str();
}

// SequentialScanNode implementation
void SequentialScanNode::initialize(ExecutionContext* ctx) {
    PhysicalPlanNode::initialize(ctx);
    current_position = 0;
    
    // Generate mock data if not already present
    if (mock_data.empty()) {
        size_t num_rows = estimated_cost.estimated_rows > 0 ? estimated_cost.estimated_rows : 1000;
        generate_mock_data(num_rows);
    }
}

TupleBatch SequentialScanNode::get_next_batch() {
    start_timing();
    
    TupleBatch batch;
    batch.column_names = output_columns;
    
    size_t batch_size = context ? context->work_mem_limit / 1000 : 1000;
    size_t end_pos = std::min(current_position + batch_size, mock_data.size());
    
    for (size_t i = current_position; i < end_pos; ++i) {
        // Apply filter conditions
        bool passes_filter = true;
        for (const auto& condition : filter_conditions) {
            // Simplified filter evaluation - in real implementation would parse expression
            if (condition->value.find("id = ") != std::string::npos) {
                std::string id_val = mock_data[i].get_value(0); // Assume first column is id
                if (condition->value.find(id_val) == std::string::npos) {
                    passes_filter = false;
                    break;
                }
            }
        }
        
        if (passes_filter) {
            batch.add_tuple(mock_data[i]);
            actual_stats.rows_returned++;
        }
        actual_stats.rows_processed++;
    }
    
    current_position = end_pos;
    has_more_data_ = current_position < mock_data.size();
    
    end_timing();
    return batch;
}

void SequentialScanNode::reset() {
    current_position = 0;
    has_more_data_ = true;
    actual_stats = ExecutionStats();
}

std::string SequentialScanNode::to_string(int indent) const {
    std::ostringstream oss;
    oss << physical_indent_string(indent) << "Seq Scan on " << table_name;
    if (!alias.empty() && alias != table_name) {
        oss << " " << alias;
    }
    oss << " (" << format_physical_cost(estimated_cost) << ")\n";
    
    if (!filter_conditions.empty()) {
        oss << physical_indent_string(indent + 1) << "Filter: ";
        for (size_t i = 0; i < filter_conditions.size(); ++i) {
            if (i > 0) oss << " AND ";
            oss << filter_conditions[i]->value;
        }
        oss << "\n";
    }
    
    return oss.str();
}

PhysicalPlanNodePtr SequentialScanNode::copy() const {
    auto node = std::make_shared<SequentialScanNode>(table_name);
    node->alias = alias;
    node->filter_conditions = filter_conditions;
    node->estimated_cost = estimated_cost;
    node->output_columns = output_columns;
    node->mock_data = mock_data;
    return node;
}

void SequentialScanNode::generate_mock_data(size_t num_rows) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> int_dist(1, 100000);
    std::uniform_real_distribution<> price_dist(10.0, 1000.0);
    
    mock_data.clear();
    mock_data.reserve(num_rows);
    
    for (size_t i = 0; i < num_rows; ++i) {
        Tuple tuple;
        if (table_name == "users") {
            tuple.values = {
                std::to_string(i + 1),
                "user" + std::to_string(i + 1) + "@example.com",
                "User " + std::to_string(i + 1)
            };
        } else if (table_name == "products") {
            tuple.values = {
                std::to_string(i + 1),
                "Product " + std::to_string(i + 1),
                std::to_string(price_dist(gen))
            };
        } else {
            // Generic data
            tuple.values = {
                std::to_string(i + 1),
                "value" + std::to_string(i + 1),
                std::to_string(int_dist(gen))
            };
        }
        mock_data.push_back(tuple);
    }
}

// PhysicalIndexScanNode implementation
void PhysicalIndexScanNode::initialize(ExecutionContext* ctx) {
    PhysicalPlanNode::initialize(ctx);
    current_position = 0;
    
    if (mock_data.empty()) {
        size_t num_rows = estimated_cost.estimated_rows > 0 ? estimated_cost.estimated_rows : 100;
        generate_mock_data(num_rows);
    }
}

TupleBatch PhysicalIndexScanNode::get_next_batch() {
    start_timing();
    
    TupleBatch batch;
    batch.column_names = output_columns;
    
    size_t batch_size = context ? context->work_mem_limit / 2000 : 500; // Smaller batches for index scan
    size_t end_pos = std::min(current_position + batch_size, mock_data.size());
    
    for (size_t i = current_position; i < end_pos; ++i) {
        batch.add_tuple(mock_data[i]);
        actual_stats.rows_returned++;
        actual_stats.rows_processed++;
    }
    
    current_position = end_pos;
    has_more_data_ = current_position < mock_data.size();
    
    actual_stats.disk_reads += (end_pos - current_position + 99) / 100; // Simulate index pages
    
    end_timing();
    return batch;
}

void PhysicalIndexScanNode::reset() {
    current_position = 0;
    has_more_data_ = true;
    actual_stats = ExecutionStats();
}

std::string PhysicalIndexScanNode::to_string(int indent) const {
    std::ostringstream oss;
    oss << physical_indent_string(indent) << "Index Scan using " << index_name 
        << " on " << table_name;
    if (!alias.empty() && alias != table_name) {
        oss << " " << alias;
    }
    oss << " (" << format_physical_cost(estimated_cost) << ")\n";
    
    if (!index_conditions.empty()) {
        oss << physical_indent_string(indent + 1) << "Index Cond: ";
        for (size_t i = 0; i < index_conditions.size(); ++i) {
            if (i > 0) oss << " AND ";
            oss << index_conditions[i]->value;
        }
        oss << "\n";
    }
    
    return oss.str();
}

PhysicalPlanNodePtr PhysicalIndexScanNode::copy() const {
    auto node = std::make_shared<PhysicalIndexScanNode>(table_name, index_name);
    node->alias = alias;
    node->index_conditions = index_conditions;
    node->filter_conditions = filter_conditions;
    node->estimated_cost = estimated_cost;
    node->output_columns = output_columns;
    node->mock_data = mock_data;
    return node;
}

void PhysicalIndexScanNode::generate_mock_data(size_t num_rows) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> int_dist(1, 1000);
    
    mock_data.clear();
    mock_data.reserve(num_rows);
    
    // Generate fewer rows for index scan (more selective)
    for (size_t i = 0; i < num_rows; ++i) {
        Tuple tuple;
        if (table_name == "users") {
            tuple.values = {
                std::to_string(int_dist(gen)),
                "indexed_user" + std::to_string(i) + "@example.com",
                "Indexed User " + std::to_string(i)
            };
        } else {
            tuple.values = {
                std::to_string(int_dist(gen)),
                "indexed_value" + std::to_string(i),
                std::to_string(int_dist(gen))
            };
        }
        mock_data.push_back(tuple);
    }
}

// PhysicalNestedLoopJoinNode implementation
void PhysicalNestedLoopJoinNode::initialize(ExecutionContext* ctx) {
    PhysicalPlanNode::initialize(ctx);
    outer_index = 0;
    inner_index = 0;
    outer_exhausted = false;
    inner_exhausted = false;
    
    // Initialize children
    for (auto& child : children) {
        child->initialize(ctx);
    }
}

TupleBatch PhysicalNestedLoopJoinNode::get_next_batch() {
    start_timing();
    
    TupleBatch result_batch;
    result_batch.column_names = output_columns;
    
    if (children.size() != 2) {
        end_timing();
        has_more_data_ = false;
        return result_batch;
    }
    
    auto outer_child = children[0];
    auto inner_child = children[1];
    
    while (result_batch.size() < result_batch.batch_size) {
        // Get outer batch if needed
        if (outer_batch.empty() || outer_index >= outer_batch.size()) {
            if (!outer_exhausted) {
                outer_batch = outer_child->get_next_batch();
                outer_index = 0;
                if (outer_batch.empty()) {
                    outer_exhausted = true;
                    break;
                }
            } else {
                break;
            }
        }
        
        // Get inner batch if needed
        if (inner_batch.empty() || inner_index >= inner_batch.size()) {
            inner_batch = inner_child->get_next_batch();
            inner_index = 0;
            if (inner_batch.empty()) {
                // Reset inner child for next outer tuple
                inner_child->reset();
                outer_index++;
                continue;
            }
        }
        
        // Join current outer tuple with all inner tuples
        const Tuple& outer_tuple = outer_batch.tuples[outer_index];
        
        while (inner_index < inner_batch.size() && result_batch.size() < result_batch.batch_size) {
            const Tuple& inner_tuple = inner_batch.tuples[inner_index];
            
            if (evaluate_join_condition(outer_tuple, inner_tuple)) {
                Tuple joined_tuple = merge_tuples(outer_tuple, inner_tuple);
                result_batch.add_tuple(joined_tuple);
                actual_stats.rows_returned++;
            }
            
            actual_stats.rows_processed++;
            inner_index++;
        }
        
        if (inner_index >= inner_batch.size()) {
            inner_index = 0;
            inner_batch.clear();
        }
    }
    
    has_more_data_ = !outer_exhausted || !inner_batch.empty();
    
    end_timing();
    return result_batch;
}

void PhysicalNestedLoopJoinNode::reset() {
    outer_index = 0;
    inner_index = 0;
    outer_exhausted = false;
    inner_exhausted = false;
    outer_batch.clear();
    inner_batch.clear();
    has_more_data_ = true;
    actual_stats = ExecutionStats();
    
    for (auto& child : children) {
        child->reset();
    }
}

std::string PhysicalNestedLoopJoinNode::to_string(int indent) const {
    std::ostringstream oss;
    std::string join_type_str;
    switch (join_type) {
        case JoinType::INNER: join_type_str = "Inner Join"; break;
        case JoinType::LEFT_OUTER: join_type_str = "Left Join"; break;
        case JoinType::RIGHT_OUTER: join_type_str = "Right Join"; break;
        case JoinType::FULL_OUTER: join_type_str = "Full Join"; break;
        default: join_type_str = "Join"; break;
    }
    
    oss << physical_indent_string(indent) << "Nested Loop " << join_type_str 
        << " (" << format_physical_cost(estimated_cost) << ")\n";
    
    if (!join_conditions.empty()) {
        oss << physical_indent_string(indent + 1) << "Join Filter: ";
        for (size_t i = 0; i < join_conditions.size(); ++i) {
            if (i > 0) oss << " AND ";
            oss << join_conditions[i]->value;
        }
        oss << "\n";
    }
    
    for (const auto& child : children) {
        oss << child->to_string(indent + 1);
    }
    
    return oss.str();
}

PhysicalPlanNodePtr PhysicalNestedLoopJoinNode::copy() const {
    auto node = std::make_shared<PhysicalNestedLoopJoinNode>(join_type);
    node->join_conditions = join_conditions;
    node->estimated_cost = estimated_cost;
    node->output_columns = output_columns;
    for (const auto& child : children) {
        node->children.push_back(child->copy());
    }
    return node;
}

bool PhysicalNestedLoopJoinNode::evaluate_join_condition(const Tuple& outer_tuple, const Tuple& inner_tuple) {
    // Simplified join condition evaluation
    // In real implementation, would parse and evaluate expressions properly
    
    if (join_conditions.empty()) {
        return true; // Cross join
    }
    
    for (const auto& condition : join_conditions) {
        std::string cond = condition->value;
        
        // Simple equality check: assumes format like "u.id = p.id"
        if (cond.find(" = ") != std::string::npos) {
            // For demo purposes, assume first columns are join keys
            if (outer_tuple.values.empty() || inner_tuple.values.empty()) {
                return false;
            }
            
            std::string outer_val = outer_tuple.values[0];
            std::string inner_val = inner_tuple.values[0];
            
            // Simple string comparison - in real implementation would handle types properly
            if (outer_val == inner_val) {
                continue;
            } else {
                return false;
            }
        }
    }
    
    return true;
}

Tuple PhysicalNestedLoopJoinNode::merge_tuples(const Tuple& outer_tuple, const Tuple& inner_tuple) {
    Tuple merged;
    
    // Add outer tuple values
    for (const auto& value : outer_tuple.values) {
        merged.values.push_back(value);
    }
    
    // Add inner tuple values  
    for (const auto& value : inner_tuple.values) {
        merged.values.push_back(value);
    }
    
    // Merge column maps
    for (const auto& pair : outer_tuple.column_map) {
        merged.column_map[pair.first] = pair.second;
    }
    for (const auto& pair : inner_tuple.column_map) {
        merged.column_map[pair.first] = pair.second;
    }
    
    return merged;
}

// PhysicalSortNode implementation  
void PhysicalSortNode::initialize(ExecutionContext* ctx) {
    PhysicalPlanNode::initialize(ctx);
    current_position = 0;
    sorting_complete = false;
    
    for (auto& child : children) {
        child->initialize(ctx);
    }
}

TupleBatch PhysicalSortNode::get_next_batch() {
    start_timing();
    
    if (!sorting_complete) {
        perform_sort();
        sorting_complete = true;
    }
    
    TupleBatch batch;
    batch.column_names = output_columns;
    
    size_t batch_size = context ? context->work_mem_limit / 1000 : 1000;
    size_t end_pos = std::min(current_position + batch_size, sorted_data.size());
    
    for (size_t i = current_position; i < end_pos; ++i) {
        batch.add_tuple(sorted_data[i]);
        actual_stats.rows_returned++;
    }
    
    current_position = end_pos;
    has_more_data_ = current_position < sorted_data.size();
    
    end_timing();
    return batch;
}

void PhysicalSortNode::reset() {
    current_position = 0;
    sorting_complete = false;
    sorted_data.clear();
    has_more_data_ = true;
    actual_stats = ExecutionStats();
    
    for (auto& child : children) {
        child->reset();
    }
}

void PhysicalSortNode::cleanup() {
    sorted_data.clear();
    sorted_data.shrink_to_fit();
}

std::string PhysicalSortNode::to_string(int indent) const {
    std::ostringstream oss;
    oss << physical_indent_string(indent) << "Sort (" << format_physical_cost(estimated_cost) << ")\n";
    
    if (!sort_keys.empty()) {
        oss << physical_indent_string(indent + 1) << "Sort Key: ";
        for (size_t i = 0; i < sort_keys.size(); ++i) {
            if (i > 0) oss << ", ";
            oss << sort_keys[i].expression->value;
            if (!sort_keys[i].ascending) oss << " DESC";
        }
        oss << "\n";
    }
    
    for (const auto& child : children) {
        oss << child->to_string(indent + 1);
    }
    
    return oss.str();
}

PhysicalPlanNodePtr PhysicalSortNode::copy() const {
    auto node = std::make_shared<PhysicalSortNode>();
    node->sort_keys = sort_keys;
    node->estimated_cost = estimated_cost;
    node->output_columns = output_columns;
    for (const auto& child : children) {
        node->children.push_back(child->copy());
    }
    return node;
}

void PhysicalSortNode::perform_sort() {
    if (children.empty()) return;
    
    auto child = children[0];
    
    // Collect all input data
    TupleBatch batch;
    while (child->has_more_data()) {
        batch = child->get_next_batch();
        for (const auto& tuple : batch.tuples) {
            sorted_data.push_back(tuple);
            actual_stats.rows_processed++;
        }
    }
    
    // Sort the data
    std::sort(sorted_data.begin(), sorted_data.end(),
              [this](const Tuple& a, const Tuple& b) {
                  return compare_tuples(a, b);
              });
    
    // Update memory usage statistics
    actual_stats.memory_used_bytes = sorted_data.size() * 100; // Rough estimate
    
    if (actual_stats.memory_used_bytes > context->work_mem_limit) {
        actual_stats.used_temp_files = true;
        actual_stats.disk_writes += (actual_stats.memory_used_bytes / (64 * 1024)) + 1;
    }
}

bool PhysicalSortNode::compare_tuples(const Tuple& a, const Tuple& b) {
    for (const auto& key : sort_keys) {
        std::string val_a = extract_sort_value(a, key.expression);
        std::string val_b = extract_sort_value(b, key.expression);
        
        int cmp = val_a.compare(val_b);
        if (cmp != 0) {
            return key.ascending ? (cmp < 0) : (cmp > 0);
        }
    }
    return false; // Equal
}

std::string PhysicalSortNode::extract_sort_value(const Tuple& tuple, const ExpressionPtr& expr) {
    // Simplified value extraction - in real implementation would evaluate expression
    if (expr->value == "name" && tuple.values.size() > 1) {
        return tuple.values[1]; // Assume name is second column
    } else if (expr->value == "id" && !tuple.values.empty()) {
        return tuple.values[0]; // Assume id is first column
    } else if (!tuple.values.empty()) {
        return tuple.values[0]; // Default to first column
    }
    return "";
}

// PhysicalPlan implementation
void PhysicalPlan::initialize() {
    if (root) {
        root->initialize(&context);
    }
}

std::vector<Tuple> PhysicalPlan::execute() {
    std::vector<Tuple> results;
    
    if (!root) return results;
    
    initialize();

    const auto start_time = std::chrono::high_resolution_clock::now();
    
    while (root->has_more_data()) {
        TupleBatch batch = root->get_next_batch();
        for (const auto& tuple : batch.tuples) {
            results.push_back(tuple);
        }
    }

    const auto end_time = std::chrono::high_resolution_clock::now();
    const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    total_stats.execution_time_ms = duration.count() / 1000.0;
    total_stats.rows_returned = results.size();
    
    // Collect stats from all nodes
    // collect_stats(root); // TODO: Implement utility function
    
    return results;
}

TupleBatch PhysicalPlan::execute_batch() {
    if (!root) return TupleBatch();
    
    initialize();
    return root->get_next_batch();
}

void PhysicalPlan::reset() {
    if (root) {
        root->reset();
    }
    total_stats = ExecutionStats();
}

void PhysicalPlan::cleanup() const {
    if (root) {
        root->cleanup();
    }
}

std::string PhysicalPlan::to_string() const {
    if (!root) {
        return "Empty Physical Plan\n";
    }
    
    std::ostringstream oss;
    oss << "PHYSICAL QUERY PLAN\n";
    oss << std::string(60, '-') << "\n";
    oss << root->to_string();
    oss << std::string(60, '-') << "\n";
    
    return oss.str();
}

std::string PhysicalPlan::explain_analyze() const {
    std::ostringstream oss;
    oss << "QUERY PLAN (ANALYZE)\n";
    oss << std::string(60, '=') << "\n";
    
    if (root) {
        // oss << format_node_stats(root, 0); // TODO: Implement utility function
    }
    
    oss << std::string(60, '=') << "\n";
    oss << "Planning time: N/A ms\n";
    oss << "Execution time: " << std::fixed << std::setprecision(3) 
        << total_stats.execution_time_ms << " ms\n";
    oss << "Total rows: " << total_stats.rows_returned << "\n";
    oss << "Peak memory: " << total_stats.memory_used_bytes << " bytes\n";
    
    return oss.str();
}

ExecutionStats PhysicalPlan::get_execution_stats() const {
    return total_stats;
}

PhysicalPlan PhysicalPlan::copy() const {
    PhysicalPlan copied;
    if (root) {
        copied.root = root->copy();
    }
    copied.context = context;
    copied.total_stats = total_stats;
    return copied;
}

// PhysicalLimitNode implementation
void PhysicalLimitNode::initialize(ExecutionContext* ctx) {
    PhysicalPlanNode::initialize(ctx);
    rows_returned = 0;
    rows_skipped = 0;
    
    for (auto& child : children) {
        child->initialize(ctx);
    }
}

TupleBatch PhysicalLimitNode::get_next_batch() {
    start_timing();
    
    TupleBatch result_batch;
    result_batch.column_names = output_columns;
    
    if (children.empty() || (limit && rows_returned >= *limit)) {
        has_more_data_ = false;
        end_timing();
        return result_batch;
    }
    
    auto child = children[0];
    
    while (child->has_more_data() && result_batch.size() < result_batch.batch_size) {
        if (limit && rows_returned >= *limit) {
            break;
        }
        
        TupleBatch child_batch = child->get_next_batch();
        
        for (const auto& tuple : child_batch.tuples) {
            // Handle OFFSET
            if (offset && rows_skipped < *offset) {
                rows_skipped++;
                actual_stats.rows_processed++;
                continue;
            }
            
            // Handle LIMIT
            if (limit && rows_returned >= *limit) {
                break;
            }
            
            result_batch.add_tuple(tuple);
            rows_returned++;
            actual_stats.rows_returned++;
            actual_stats.rows_processed++;
        }
    }
    
    has_more_data_ = child->has_more_data() && (!limit || rows_returned < *limit);
    
    end_timing();
    return result_batch;
}

void PhysicalLimitNode::reset() {
    rows_returned = 0;
    rows_skipped = 0;
    has_more_data_ = true;
    actual_stats = ExecutionStats();
    
    for (auto& child : children) {
        child->reset();
    }
}

std::string PhysicalLimitNode::to_string(const int indent) const {
    std::ostringstream oss;
    oss << physical_indent_string(indent) << "Limit (" << format_physical_cost(estimated_cost) << ")\n";
    
    oss << physical_indent_string(indent + 1);
    if (offset && *offset > 0) {
        oss << "Offset: " << *offset << " ";
    }
    if (limit) {
        oss << "Limit: " << *limit;
    } else {
        oss << "Limit: ALL";
    }
    oss << "\n";
    
    for (const auto& child : children) {
        oss << child->to_string(indent + 1);
    }
    
    return oss.str();
}

PhysicalPlanNodePtr PhysicalLimitNode::copy() const {
    auto node = std::make_shared<PhysicalLimitNode>();
    node->limit = limit;
    node->offset = offset;
    node->estimated_cost = estimated_cost;
    node->output_columns = output_columns;
    for (const auto& child : children) {
        node->children.push_back(child->copy());
    }
    return node;
}

// Private helper methods - TODO: Add to header if needed
/*
void PhysicalPlan::collect_stats(PhysicalPlanNodePtr node) {
    if (!node) return;
    
    total_stats.merge(node->get_stats());
    
    for (const auto& child : node->children) {
        collect_stats(child);
    }
}

std::string PhysicalPlan::format_node_stats(PhysicalPlanNodePtr node, int depth) const {
    if (!node) return "";
    
    std::ostringstream oss;
    std::string indent(depth * 2, ' ');
    
    oss << node->to_string(depth);
    
    const auto& stats = node->get_stats();
    if (stats.execution_time_ms > 0) {
        oss << indent << "  (actual time=" << std::fixed << std::setprecision(3)
            << stats.execution_time_ms << " ms rows=" << stats.rows_returned << ")\n";
    }
    
    for (const auto& child : node->children) {
        oss << format_node_stats(child, depth + 1);
    }
    
    return oss.str();
}

std::string PhysicalPlan::format_memory_size(size_t bytes) const {
    if (bytes < 1024) return std::to_string(bytes) + " B";
    else if (bytes < 1024 * 1024) return std::to_string(bytes / 1024) + " KB";
    else return std::to_string(bytes / (1024 * 1024)) + " MB";
}
*/

// ParallelSequentialScanNode implementation  
void ParallelSequentialScanNode::initialize(ExecutionContext* ctx) {
    PhysicalPlanNode::initialize(ctx);
    
    if (mock_data.empty()) {
        size_t num_rows = estimated_cost.estimated_rows > 0 ? estimated_cost.estimated_rows : 10000;
        generate_mock_data(num_rows);
    }
    
    parallel_ctx = std::make_shared<ParallelContext>();
    
    // Start worker threads
    size_t rows_per_worker = mock_data.size() / parallel_degree;
    for (size_t i = 0; i < parallel_degree; ++i) {
        size_t start_row = i * rows_per_worker;
        size_t end_row = (i == parallel_degree - 1) ? mock_data.size() : (i + 1) * rows_per_worker;
        
        worker_threads.emplace_back([this, i, start_row, end_row]() {
            worker_scan(i, start_row, end_row);
        });
    }
}

TupleBatch ParallelSequentialScanNode::get_next_batch() {
    start_timing();
    
    TupleBatch batch = parallel_ctx->get_result_batch();
    
    if (batch.empty() && parallel_ctx->execution_complete) {
        has_more_data_ = false;
    }
    
    actual_stats.rows_returned += batch.size();
    
    end_timing();
    return batch;
}

void ParallelSequentialScanNode::reset() {
    cleanup();
    has_more_data_ = true;
    actual_stats = ExecutionStats();
}

void ParallelSequentialScanNode::cleanup() {
    if (parallel_ctx) {
        parallel_ctx->signal_completion();
    }
    
    for (auto& thread : worker_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    
    worker_threads.clear();
    parallel_ctx.reset();
}

std::string ParallelSequentialScanNode::to_string(const int indent) const {
    std::ostringstream oss;
    oss << physical_indent_string(indent) << "Parallel Seq Scan on " << table_name
        << " (workers=" << parallel_degree << ") (" << format_physical_cost(estimated_cost) << ")\n";
    
    if (!filter_conditions.empty()) {
        oss << physical_indent_string(indent + 1) << "Filter: ";
        for (size_t i = 0; i < filter_conditions.size(); ++i) {
            if (i > 0) oss << " AND ";
            oss << filter_conditions[i]->value;
        }
        oss << "\n";
    }
    
    return oss.str();
}

PhysicalPlanNodePtr ParallelSequentialScanNode::copy() const {
    auto node = std::make_shared<ParallelSequentialScanNode>(table_name, parallel_degree);
    node->filter_conditions = filter_conditions;
    node->estimated_cost = estimated_cost;
    node->output_columns = output_columns;
    node->mock_data = mock_data;
    return node;
}

void ParallelSequentialScanNode::worker_scan(size_t worker_id, size_t start_row, const size_t end_row) const {
    ++parallel_ctx->active_workers;
    
    TupleBatch batch;
    batch.column_names = output_columns;
    
    for (size_t i = start_row; i < end_row; ++i) {
        // Apply filters
        bool passes_filter = true;
        for (const auto& condition : filter_conditions) {
            // Simplified filter evaluation
            if (condition->value.find("id = ") != std::string::npos) {
                std::string id_val = mock_data[i].get_value(0);
                if (condition->value.find(id_val) == std::string::npos) {
                    passes_filter = false;
                    break;
                }
            }
        }
        
        if (passes_filter) {
            batch.add_tuple(mock_data[i]);
        }
        
        // Send batch when full
        if (batch.is_full()) {
            parallel_ctx->add_result_batch(batch);
            batch.clear();
        }
    }
    
    // Send remaining tuples
    if (!batch.empty()) {
        parallel_ctx->add_result_batch(batch);
    }
    
    parallel_ctx->active_workers--;
    if (parallel_ctx->active_workers == 0) {
        parallel_ctx->signal_completion();
    }
}

void ParallelSequentialScanNode::generate_mock_data(size_t num_rows) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> int_dist(1, 100000);
    
    mock_data.clear();
    mock_data.reserve(num_rows);
    
    for (size_t i = 0; i < num_rows; ++i) {
        Tuple tuple;
        if (table_name == "users") {
            tuple.values = {
                std::to_string(i + 1),
                "parallel_user" + std::to_string(i + 1) + "@example.com",
                "Parallel User " + std::to_string(i + 1)
            };
        } else {
            tuple.values = {
                std::to_string(i + 1),
                "parallel_value" + std::to_string(i + 1),
                std::to_string(int_dist(gen))
            };
        }
        mock_data.push_back(tuple);
    }
}

// ParallelContext implementation
void ParallelContext::add_result_batch(const TupleBatch& batch) {
    std::lock_guard<std::mutex> lock(result_mutex);
    result_queue.push(batch);
    worker_cv.notify_one();
}

TupleBatch ParallelContext::get_result_batch() {
    std::unique_lock<std::mutex> lock(result_mutex);
    
    worker_cv.wait(lock, [this] { 
        return !result_queue.empty() || execution_complete; 
    });
    
    if (!result_queue.empty()) {
        TupleBatch batch = result_queue.front();
        result_queue.pop();
        return batch;
    }
    
    return TupleBatch(); // Empty batch
}

bool ParallelContext::has_results() const {
    std::lock_guard<std::mutex> lock(result_mutex);
    return !result_queue.empty();
}

void ParallelContext::signal_completion() {
    {
        std::lock_guard<std::mutex> lock(result_mutex);
        execution_complete = true;
    }
    worker_cv.notify_all();
}

void ParallelContext::wait_for_completion() {
    std::unique_lock<std::mutex> lock(result_mutex);
    worker_cv.wait(lock, [this] { return execution_complete.load(); });
}

}