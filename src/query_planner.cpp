#include "query_planner.hpp"
#include <regex>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <sstream>

namespace pg {

QueryPlanner::QueryPlanner(std::shared_ptr<DatabaseSchema> schema) 
    : schema_(schema) {
    // Initialize default table statistics
    for (const auto& table_name : schema_->get_table_names()) {
        TableStats stats;
        stats.row_count = 1000; // Default estimate
        stats.avg_row_size = 100.0;
        table_stats_[table_name] = stats;
    }
}

LogicalPlan QueryPlanner::create_plan(const std::string& query) {
    auto parse_result = parser_.parse(query);
    return create_plan(parse_result);
}

LogicalPlan QueryPlanner::create_plan(const QueryResult& parsed_query) {
    if (!parsed_query.is_valid) {
        return LogicalPlan(); // Empty plan for invalid queries
    }

    std::string query = parsed_query.query;
    std::string upper_query = query;
    std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);
    
    LogicalPlanNodePtr root;
    
    if (upper_query.find("SELECT") == 0) {
        root = build_plan_from_select(query);
    } else if (upper_query.find("INSERT") == 0) {
        root = build_plan_from_insert(query);
    } else if (upper_query.find("UPDATE") == 0) {
        root = build_plan_from_update(query);
    } else if (upper_query.find("DELETE") == 0) {
        root = build_plan_from_delete(query);
    } else {
        return LogicalPlan(); // Unsupported query type
    }
    
    LogicalPlan plan(root);
    
    // Calculate costs
    if (root) {
        estimate_costs(root);
        plan.calculate_costs();
    }
    
    return plan;
}

void QueryPlanner::set_table_stats(const std::string& table_name, const TableStats& stats) {
    table_stats_[table_name] = stats;
}

TableStats QueryPlanner::get_table_stats(const std::string& table_name) const {
    auto it = table_stats_.find(table_name);
    if (it != table_stats_.end()) {
        return it->second;
    }
    
    // Return default stats
    TableStats default_stats;
    default_stats.row_count = 1000;
    default_stats.avg_row_size = 100.0;
    return default_stats;
}

LogicalPlan QueryPlanner::optimize_plan(const LogicalPlan& plan) {
    if (!plan.root) {
        return plan;
    }
    
    LogicalPlan optimized = plan.copy();
    
    // Apply optimization transformations
    PredicatePushdownTransformer predicate_pushdown;
    optimized.root = predicate_pushdown.transform(optimized.root);
    
    ProjectionPushdownTransformer projection_pushdown;
    optimized.root = projection_pushdown.transform(optimized.root);
    
    JoinReorderingTransformer join_reordering(*this);
    optimized.root = join_reordering.transform(optimized.root);
    
    // Recalculate costs after optimization
    estimate_costs(optimized.root);
    optimized.calculate_costs();
    
    return optimized;
}

std::vector<LogicalPlan> QueryPlanner::generate_alternative_plans(const std::string& query) {
    std::vector<LogicalPlan> plans;
    
    // Generate base plan
    LogicalPlan base_plan = create_plan(query);
    if (base_plan.root) {
        plans.push_back(base_plan);
        
        // Generate optimized variants
        LogicalPlan optimized = optimize_plan(base_plan);
        if (optimized.root != base_plan.root) { // Different plan
            plans.push_back(optimized);
        }
        
        // TODO: Generate alternative join orders, index choices, etc.
    }
    
    return plans;
}

void QueryPlanner::estimate_costs(LogicalPlanNodePtr node) {
    if (!node) return;
    
    // First estimate costs for children
    for (auto& child : node->children) {
        estimate_costs(child);
    }
    
    // Then estimate cost for this node
    switch (node->type) {
        case PlanNodeType::TABLE_SCAN: {
            auto scan_node = std::static_pointer_cast<TableScanNode>(node);
            double base_cost = estimate_table_scan_cost(scan_node->table_name);
            double selectivity = estimate_selectivity(scan_node->filter_conditions, scan_node->table_name);
            
            node->cost.startup_cost = 0.0;
            node->cost.total_cost = base_cost;
            node->cost.estimated_rows = static_cast<size_t>(get_table_stats(scan_node->table_name).row_count * selectivity);
            node->cost.selectivity = selectivity;
            break;
        }
        
        case PlanNodeType::INDEX_SCAN: {
            auto index_node = std::static_pointer_cast<IndexScanNode>(node);
            double selectivity = estimate_selectivity(index_node->index_conditions, index_node->table_name);
            double base_cost = estimate_index_scan_cost(index_node->table_name, index_node->index_name, selectivity);
            
            node->cost.startup_cost = 0.0;
            node->cost.total_cost = base_cost;
            node->cost.estimated_rows = static_cast<size_t>(get_table_stats(index_node->table_name).row_count * selectivity);
            node->cost.selectivity = selectivity;
            break;
        }
        
        case PlanNodeType::NESTED_LOOP_JOIN: {
            auto join_node = std::static_pointer_cast<NestedLoopJoinNode>(node);
            if (join_node->children.size() == 2) {
                auto left = join_node->children[0];
                auto right = join_node->children[1];
                
                double selectivity = estimate_selectivity(join_node->join_conditions, "");
                double join_cost = estimate_join_cost_internal(join_node->join_type, 
                                                             left->cost.estimated_rows,
                                                             right->cost.estimated_rows,
                                                             selectivity);
                
                node->cost.startup_cost = left->cost.startup_cost + right->cost.startup_cost;
                node->cost.total_cost = left->cost.total_cost + right->cost.total_cost + join_cost;
                node->cost.estimated_rows = static_cast<size_t>(left->cost.estimated_rows * right->cost.estimated_rows * selectivity);
                node->cost.selectivity = selectivity;
            }
            break;
        }
        
        case PlanNodeType::HASH_JOIN: {
            auto join_node = std::static_pointer_cast<HashJoinNode>(node);
            if (join_node->children.size() == 2) {
                auto left = join_node->children[0];
                auto right = join_node->children[1];
                
                double selectivity = estimate_selectivity(join_node->join_conditions, "");
                // Hash join has higher startup cost but lower per-tuple cost
                double hash_build_cost = right->cost.estimated_rows * config_.cpu_tuple_cost;
                double hash_probe_cost = left->cost.estimated_rows * config_.cpu_tuple_cost * 0.5;
                
                node->cost.startup_cost = left->cost.startup_cost + right->cost.total_cost + hash_build_cost;
                node->cost.total_cost = left->cost.total_cost + right->cost.total_cost + hash_build_cost + hash_probe_cost;
                node->cost.estimated_rows = static_cast<size_t>(left->cost.estimated_rows * right->cost.estimated_rows * selectivity);
                node->cost.selectivity = selectivity;
            }
            break;
        }
        
        case PlanNodeType::PROJECTION: {
            if (!node->children.empty()) {
                auto child = node->children[0];
                node->cost.startup_cost = child->cost.startup_cost;
                node->cost.total_cost = child->cost.total_cost + child->cost.estimated_rows * config_.cpu_tuple_cost;
                node->cost.estimated_rows = child->cost.estimated_rows;
                node->cost.selectivity = child->cost.selectivity;
            }
            break;
        }
        
        case PlanNodeType::SELECTION: {
            auto selection_node = std::static_pointer_cast<SelectionNode>(node);
            if (!node->children.empty()) {
                auto child = node->children[0];
                double selectivity = estimate_selectivity(selection_node->conditions, "");
                
                node->cost.startup_cost = child->cost.startup_cost;
                node->cost.total_cost = child->cost.total_cost + child->cost.estimated_rows * config_.cpu_operator_cost;
                node->cost.estimated_rows = static_cast<size_t>(child->cost.estimated_rows * selectivity);
                node->cost.selectivity = selectivity;
            }
            break;
        }
        
        case PlanNodeType::SORT: {
            if (!node->children.empty()) {
                auto child = node->children[0];
                double sort_cost = child->cost.estimated_rows * std::log2(child->cost.estimated_rows) * config_.cpu_operator_cost;
                
                node->cost.startup_cost = child->cost.total_cost + sort_cost;
                node->cost.total_cost = node->cost.startup_cost;
                node->cost.estimated_rows = child->cost.estimated_rows;
                node->cost.selectivity = child->cost.selectivity;
            }
            break;
        }
        
        case PlanNodeType::LIMIT: {
            auto limit_node = std::static_pointer_cast<LimitNode>(node);
            if (!node->children.empty()) {
                auto child = node->children[0];
                size_t output_rows = child->cost.estimated_rows;
                
                if (limit_node->limit) {
                    output_rows = std::min(output_rows, *limit_node->limit);
                }
                
                double fraction = static_cast<double>(output_rows) / child->cost.estimated_rows;
                
                node->cost.startup_cost = child->cost.startup_cost;
                node->cost.total_cost = child->cost.startup_cost + child->cost.total_cost * fraction;
                node->cost.estimated_rows = output_rows;
                node->cost.selectivity = child->cost.selectivity;
            }
            break;
        }
        
        default:
            // For other node types, just copy from first child
            if (!node->children.empty()) {
                auto child = node->children[0];
                node->cost = child->cost;
            }
            break;
    }
}

double QueryPlanner::estimate_selectivity(const std::vector<ExpressionPtr>& conditions, 
                                        const std::string& table_name) const {
    if (conditions.empty()) {
        return 1.0;
    }
    
    // Simple selectivity estimation - in a real system this would be much more sophisticated
    double selectivity = 1.0;
    
    for (const auto& condition : conditions) {
        // Default selectivity for various condition types
        if (condition->value.find("=") != std::string::npos) {
            selectivity *= 0.1; // Equality conditions are fairly selective
        } else if (condition->value.find("<") != std::string::npos || 
                  condition->value.find(">") != std::string::npos) {
            selectivity *= 0.3; // Range conditions
        } else if (condition->value.find("LIKE") != std::string::npos) {
            selectivity *= 0.2; // Pattern matching
        } else {
            selectivity *= 0.5; // Default
        }
    }
    
    return std::max(0.001, std::min(1.0, selectivity)); // Clamp between 0.1% and 100%
}

LogicalPlanNodePtr QueryPlanner::build_plan_from_select(const std::string& query) {
    // Simple regex-based parsing for demonstration
    // In a real system, this would use the actual parse tree from libpg_query
    
    std::vector<std::string> table_names = extract_table_names_from_query(query);
    if (table_names.empty()) {
        return nullptr;
    }
    
    // Build scan nodes for each table
    std::vector<LogicalPlanNodePtr> scan_nodes;
    for (const auto& table_name : table_names) {
        scan_nodes.push_back(build_scan_node(table_name));
    }
    
    LogicalPlanNodePtr plan_root = scan_nodes[0];
    
    // If multiple tables, create joins
    if (scan_nodes.size() > 1) {
        auto join_conditions = extract_join_conditions(query);
        plan_root = optimize_join_order(scan_nodes, join_conditions);
    }
    
    // Add WHERE conditions as selection
    std::regex where_regex(R"(WHERE\s+(.+?)(?:\s+GROUP|\s+ORDER|\s+LIMIT|$))", 
                          std::regex_constants::icase);
    std::smatch where_match;
    if (std::regex_search(query, where_match, where_regex)) {
        std::string where_clause = where_match[1];
        auto conditions = parse_condition_list(where_clause);
        if (!conditions.empty()) {
            plan_root = build_selection_node(plan_root, conditions);
        }
    }
    
    // Add projection for SELECT list
    std::regex select_regex(R"(SELECT\s+(.+?)\s+FROM)", std::regex_constants::icase);
    std::smatch select_match;
    if (std::regex_search(query, select_match, select_regex)) {
        std::string select_list = select_match[1];
        if (select_list != "*") {
            // Parse projection list - simplified
            std::vector<ExpressionPtr> projections;
            std::stringstream ss(select_list);
            std::string item;
            while (std::getline(ss, item, ',')) {
                item = std::regex_replace(item, std::regex(R"(^\s+|\s+$)"), "");
                projections.push_back(std::make_shared<Expression>(ExpressionType::COLUMN_REF, item));
            }
            plan_root = build_projection_node(plan_root, projections);
        }
    }
    
    // Add ORDER BY
    std::regex order_regex(R"(ORDER\s+BY\s+(.+?)(?:\s+LIMIT|$))", std::regex_constants::icase);
    std::smatch order_match;
    if (std::regex_search(query, order_match, order_regex)) {
        auto sort_node = std::make_shared<SortNode>();
        sort_node->children.push_back(plan_root);
        
        // Simple parsing of sort keys
        std::string order_clause = order_match[1];
        SortNode::SortKey key;
        key.expression = std::make_shared<Expression>(ExpressionType::COLUMN_REF, order_clause);
        key.ascending = order_clause.find("DESC") == std::string::npos;
        sort_node->sort_keys.push_back(key);
        
        plan_root = sort_node;
    }
    
    // Add LIMIT
    std::regex limit_regex(R"(LIMIT\s+(\d+))", std::regex_constants::icase);
    std::smatch limit_match;
    if (std::regex_search(query, limit_match, limit_regex)) {
        auto limit_node = std::make_shared<LimitNode>();
        limit_node->children.push_back(plan_root);
        limit_node->limit = std::stoull(limit_match[1]);
        plan_root = limit_node;
    }
    
    return plan_root;
}

LogicalPlanNodePtr QueryPlanner::build_plan_from_insert(const std::string& query) {
    // Extract table name
    std::regex table_regex(R"(INSERT\s+INTO\s+(\w+))", std::regex_constants::icase);
    std::smatch table_match;
    if (!std::regex_search(query, table_match, table_regex)) {
        return nullptr;
    }
    
    std::string table_name = table_match[1];
    auto insert_node = std::make_shared<InsertNode>(table_name);
    
    // Parse column list if present
    std::regex columns_regex(R"(\(\s*([^)]+)\s*\)\s+VALUES)", std::regex_constants::icase);
    std::smatch columns_match;
    if (std::regex_search(query, columns_match, columns_regex)) {
        std::string columns_str = columns_match[1];
        std::stringstream ss(columns_str);
        std::string column;
        while (std::getline(ss, column, ',')) {
            column = std::regex_replace(column, std::regex(R"(^\s+|\s+$)"), "");
            insert_node->target_columns.push_back(column);
        }
    }
    
    return insert_node;
}

LogicalPlanNodePtr QueryPlanner::build_plan_from_update(const std::string& query) {
    // Extract table name
    std::regex table_regex(R"(UPDATE\s+(\w+))", std::regex_constants::icase);
    std::smatch table_match;
    if (!std::regex_search(query, table_match, table_regex)) {
        return nullptr;
    }
    
    std::string table_name = table_match[1];
    auto update_node = std::make_shared<UpdateNode>(table_name);
    
    // Add table scan as child for WHERE conditions
    auto scan_node = build_scan_node(table_name);
    update_node->children.push_back(scan_node);
    
    return update_node;
}

LogicalPlanNodePtr QueryPlanner::build_plan_from_delete(const std::string& query) {
    // Extract table name
    std::regex table_regex(R"(DELETE\s+FROM\s+(\w+))", std::regex_constants::icase);
    std::smatch table_match;
    if (!std::regex_search(query, table_match, table_regex)) {
        return nullptr;
    }
    
    std::string table_name = table_match[1];
    auto delete_node = std::make_shared<DeleteNode>(table_name);
    
    // Add table scan as child for WHERE conditions
    auto scan_node = build_scan_node(table_name);
    delete_node->children.push_back(scan_node);
    
    return delete_node;
}

LogicalPlanNodePtr QueryPlanner::build_scan_node(const std::string& table_name, const std::string& alias) {
    // Check if we can use an index scan
    // For now, always use table scan
    auto scan_node = std::make_shared<TableScanNode>(table_name);
    scan_node->alias = alias.empty() ? table_name : alias;
    return scan_node;
}

LogicalPlanNodePtr QueryPlanner::build_join_node(LogicalPlanNodePtr left, 
                                                LogicalPlanNodePtr right,
                                                JoinType join_type,
                                                const std::vector<ExpressionPtr>& conditions) {
    // Choose join algorithm based on configuration and estimated costs
    LogicalPlanNodePtr join_node;
    
    if (config_.enable_hash_joins && left->cost.estimated_rows < right->cost.estimated_rows) {
        // Use hash join with smaller relation as build side
        auto hash_join = std::make_shared<HashJoinNode>(join_type);
        hash_join->children = {right, left}; // Build on right (smaller)
        join_node = hash_join;
    } else {
        // Use nested loop join
        auto nl_join = std::make_shared<NestedLoopJoinNode>(join_type);
        nl_join->children = {left, right};
        join_node = nl_join;
    }
    
    // Set join conditions
    if (auto hash_join = std::dynamic_pointer_cast<HashJoinNode>(join_node)) {
        hash_join->join_conditions = conditions;
    } else if (auto nl_join = std::dynamic_pointer_cast<NestedLoopJoinNode>(join_node)) {
        nl_join->join_conditions = conditions;
    }
    
    return join_node;
}

LogicalPlanNodePtr QueryPlanner::build_projection_node(LogicalPlanNodePtr child,
                                                      const std::vector<ExpressionPtr>& projections) {
    auto proj_node = std::make_shared<ProjectionNode>();
    proj_node->children.push_back(child);
    proj_node->projections = projections;
    return proj_node;
}

LogicalPlanNodePtr QueryPlanner::build_selection_node(LogicalPlanNodePtr child,
                                                     const std::vector<ExpressionPtr>& conditions) {
    auto sel_node = std::make_shared<SelectionNode>();
    sel_node->children.push_back(child);
    sel_node->conditions = conditions;
    return sel_node;
}

ExpressionPtr QueryPlanner::parse_expression(const std::string& expr_str) {
    // Simple expression parsing - in a real system would use parse tree
    if (expr_str.find('=') != std::string::npos || 
        expr_str.find('<') != std::string::npos || 
        expr_str.find('>') != std::string::npos) {
        return std::make_shared<Expression>(ExpressionType::BINARY_OP, expr_str);
    } else if (expr_str.find('.') != std::string::npos) {
        return std::make_shared<Expression>(ExpressionType::COLUMN_REF, expr_str);
    } else {
        return std::make_shared<Expression>(ExpressionType::COLUMN_REF, expr_str);
    }
}

std::vector<ExpressionPtr> QueryPlanner::parse_condition_list(const std::string& where_clause) {
    std::vector<ExpressionPtr> conditions;
    
    // Split by AND (simple approach)
    std::regex and_regex(R"(\s+AND\s+)", std::regex_constants::icase);
    std::sregex_token_iterator iter(where_clause.begin(), where_clause.end(), and_regex, -1);
    std::sregex_token_iterator end;
    
    for (; iter != end; ++iter) {
        std::string condition = iter->str();
        condition = std::regex_replace(condition, std::regex(R"(^\s+|\s+$)"), "");
        if (!condition.empty()) {
            conditions.push_back(parse_expression(condition));
        }
    }
    
    return conditions;
}

LogicalPlanNodePtr QueryPlanner::optimize_join_order(const std::vector<LogicalPlanNodePtr>& tables,
                                                    const std::vector<ExpressionPtr>& join_conditions) {
    if (tables.size() == 1) {
        return tables[0];
    }
    
    if (tables.size() == 2) {
        return build_join_node(tables[0], tables[1], JoinType::INNER, join_conditions);
    }
    
    // For more than 2 tables, use simple left-deep join tree
    LogicalPlanNodePtr result = tables[0];
    for (size_t i = 1; i < tables.size(); ++i) {
        result = build_join_node(result, tables[i], JoinType::INNER, join_conditions);
    }
    
    return result;
}

std::vector<std::string> QueryPlanner::extract_table_names_from_query(const std::string& query) const {
    std::vector<std::string> table_names;
    
    std::regex from_regex(R"(FROM\s+(\w+)(?:\s+(\w+))?)", std::regex_constants::icase);
    std::regex join_regex(R"(JOIN\s+(\w+)(?:\s+(\w+))?)", std::regex_constants::icase);
    
    std::smatch match;
    std::string::const_iterator search_start = query.cbegin();
    
    // Find tables in FROM clause
    if (std::regex_search(search_start, query.cend(), match, from_regex)) {
        table_names.push_back(match[1]);
    }
    
    // Find tables in JOIN clauses
    search_start = query.cbegin();
    while (std::regex_search(search_start, query.cend(), match, join_regex)) {
        table_names.push_back(match[1]);
        search_start = match.suffix().first;
    }
    
    return table_names;
}

std::vector<ExpressionPtr> QueryPlanner::extract_join_conditions(const std::string& query) const {
    std::vector<ExpressionPtr> conditions;
    
    std::regex on_regex(R"(ON\s+(.+?)(?:\s+JOIN|\s+WHERE|\s+GROUP|\s+ORDER|$))", 
                       std::regex_constants::icase);
    std::smatch match;
    std::string::const_iterator search_start = query.cbegin();
    
    while (std::regex_search(search_start, query.cend(), match, on_regex)) {
        std::string condition = match[1];
        conditions.push_back(std::make_shared<Expression>(ExpressionType::BINARY_OP, condition));
        search_start = match.suffix().first;
    }
    
    return conditions;
}

double QueryPlanner::estimate_table_scan_cost(const std::string& table_name) const {
    auto stats = get_table_stats(table_name);
    double pages = (stats.row_count * stats.avg_row_size) / 8192.0; // 8KB pages
    return pages * config_.seq_page_cost + stats.row_count * config_.cpu_tuple_cost;
}

double QueryPlanner::estimate_index_scan_cost(const std::string& table_name, 
                                            const std::string& index_name,
                                            double selectivity) const {
    auto stats = get_table_stats(table_name);
    double index_pages = std::log2(stats.row_count) * selectivity; // Simplified B-tree cost
    double data_pages = (stats.row_count * selectivity * stats.avg_row_size) / 8192.0;
    
    return index_pages * config_.random_page_cost + 
           data_pages * config_.random_page_cost +
           stats.row_count * selectivity * config_.cpu_index_tuple_cost;
}

double QueryPlanner::estimate_join_cost_internal(JoinType join_type, 
                                                size_t left_rows, 
                                                size_t right_rows,
                                                double selectivity) const {
    // Simplified nested loop join cost
    return left_rows * right_rows * config_.cpu_tuple_cost * selectivity;
}

// Visitor implementations
void PlanVisitor::traverse(LogicalPlanNodePtr node) {
    if (!node) return;
    
    switch (node->type) {
        case PlanNodeType::TABLE_SCAN:
            visit(static_cast<TableScanNode*>(node.get()));
            break;
        case PlanNodeType::INDEX_SCAN:
            visit(static_cast<IndexScanNode*>(node.get()));
            break;
        case PlanNodeType::NESTED_LOOP_JOIN:
            visit(static_cast<NestedLoopJoinNode*>(node.get()));
            break;
        case PlanNodeType::HASH_JOIN:
            visit(static_cast<HashJoinNode*>(node.get()));
            break;
        case PlanNodeType::PROJECTION:
            visit(static_cast<ProjectionNode*>(node.get()));
            break;
        case PlanNodeType::SELECTION:
            visit(static_cast<SelectionNode*>(node.get()));
            break;
        case PlanNodeType::AGGREGATION:
            visit(static_cast<AggregationNode*>(node.get()));
            break;
        case PlanNodeType::SORT:
            visit(static_cast<SortNode*>(node.get()));
            break;
        case PlanNodeType::LIMIT:
            visit(static_cast<LimitNode*>(node.get()));
            break;
        case PlanNodeType::INSERT:
            visit(static_cast<InsertNode*>(node.get()));
            break;
        case PlanNodeType::UPDATE:
            visit(static_cast<UpdateNode*>(node.get()));
            break;
        case PlanNodeType::DELETE:
            visit(static_cast<DeleteNode*>(node.get()));
            break;
        default:
            break;
    }
    
    for (const auto& child : node->children) {
        traverse(child);
    }
}

LogicalPlanNodePtr PlanTransformer::transform(LogicalPlanNodePtr node) {
    if (!node) return nullptr;
    
    // Transform children first
    for (auto& child : node->children) {
        child = transform(child);
    }
    
    // Then transform this node
    return transform_node(node);
}

LogicalPlanNodePtr PredicatePushdownTransformer::transform_node(LogicalPlanNodePtr node) {
    // Simplified predicate pushdown - move selection below projection
    if (node->type == PlanNodeType::PROJECTION && !node->children.empty()) {
        auto child = node->children[0];
        if (child->type == PlanNodeType::SELECTION) {
            // Swap projection and selection
            auto proj_node = std::static_pointer_cast<ProjectionNode>(node);
            auto sel_node = std::static_pointer_cast<SelectionNode>(child);
            
            if (!sel_node->children.empty()) {
                proj_node->children[0] = sel_node->children[0];
                sel_node->children[0] = proj_node;
                return sel_node;
            }
        }
    }
    
    return node;
}

LogicalPlanNodePtr ProjectionPushdownTransformer::transform_node(LogicalPlanNodePtr node) {
    // Simplified projection pushdown
    return node;
}

LogicalPlanNodePtr JoinReorderingTransformer::transform_node(LogicalPlanNodePtr node) {
    // Simplified join reordering based on cost
    if (node->type == PlanNodeType::NESTED_LOOP_JOIN && node->children.size() == 2) {
        auto left = node->children[0];
        auto right = node->children[1];
        
        // If right child has fewer estimated rows, swap to make it the outer relation
        if (right->cost.estimated_rows < left->cost.estimated_rows) {
            std::swap(node->children[0], node->children[1]);
        }
    }
    
    return node;
}

}