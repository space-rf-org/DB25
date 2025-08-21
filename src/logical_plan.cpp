#include "logical_plan.hpp"
#include <sstream>
#include <iomanip>

namespace db25 {

// Helper function for indentation
std::string indent_string(const int indent) {
    return std::string(indent * 2, ' ');
}

// Helper function for cost formatting
std::string format_cost(const PlanCost& cost) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    oss << "cost=" << cost.startup_cost << ".." << cost.total_cost 
        << " rows=" << cost.estimated_rows;
    return oss.str();
}

// TableScanNode implementation
std::string TableScanNode::to_string(const int indent) const {
    std::ostringstream oss;
    oss << indent_string(indent) << "Seq Scan on " << table_name;
    if (!alias.empty() && alias != table_name) {
        oss << " " << alias;
    }
    oss << " (" << format_cost(cost) << ")\n";
    
    if (!filter_conditions.empty()) {
        oss << indent_string(indent + 1) << "Filter: ";
        for (size_t i = 0; i < filter_conditions.size(); ++i) {
            if (i > 0) oss << " AND ";
            oss << filter_conditions[i]->value;
        }
        oss << "\n";
    }
    
    return oss.str();
}

LogicalPlanNodePtr TableScanNode::copy() const {
    auto node = std::make_shared<TableScanNode>(table_name);
    node->alias = alias;
    node->filter_conditions = filter_conditions;
    node->cost = cost;
    node->output_columns = output_columns;
    return node;
}

// IndexScanNode implementation
std::string IndexScanNode::to_string(const int indent) const {
    std::ostringstream oss;
    oss << indent_string(indent) << "Index Scan using " << index_name 
        << " on " << table_name;
    if (!alias.empty() && alias != table_name) {
        oss << " " << alias;
    }
    oss << " (" << format_cost(cost) << ")\n";
    
    if (!index_conditions.empty()) {
        oss << indent_string(indent + 1) << "Index Cond: ";
        for (size_t i = 0; i < index_conditions.size(); ++i) {
            if (i > 0) oss << " AND ";
            oss << index_conditions[i]->value;
        }
        oss << "\n";
    }
    
    if (!filter_conditions.empty()) {
        oss << indent_string(indent + 1) << "Filter: ";
        for (size_t i = 0; i < filter_conditions.size(); ++i) {
            if (i > 0) oss << " AND ";
            oss << filter_conditions[i]->value;
        }
        oss << "\n";
    }
    
    return oss.str();
}

LogicalPlanNodePtr IndexScanNode::copy() const {
    auto node = std::make_shared<IndexScanNode>(table_name, index_name);
    node->alias = alias;
    node->index_conditions = index_conditions;
    node->filter_conditions = filter_conditions;
    node->cost = cost;
    node->output_columns = output_columns;
    return node;
}

// JoinNode helper
std::string JoinNode::join_type_to_string() const {
    switch (join_type) {
        case JoinType::INNER: return "Inner Join";
        case JoinType::LEFT_OUTER: return "Left Join";
        case JoinType::RIGHT_OUTER: return "Right Join";
        case JoinType::FULL_OUTER: return "Full Join";
        case JoinType::CROSS: return "Cross Join";
        case JoinType::SEMI: return "Semi Join";
        case JoinType::ANTI: return "Anti Join";
        default: return "Unknown Join";
    }
}

// NestedLoopJoinNode implementation
std::string NestedLoopJoinNode::to_string(const int indent) const {
    std::ostringstream oss;
    oss << indent_string(indent) << "Nested Loop " << join_type_to_string() 
        << " (" << format_cost(cost) << ")\n";
    
    if (!join_conditions.empty()) {
        oss << indent_string(indent + 1) << "Join Filter: ";
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

LogicalPlanNodePtr NestedLoopJoinNode::copy() const {
    auto node = std::make_shared<NestedLoopJoinNode>(join_type);
    node->join_conditions = join_conditions;
    node->cost = cost;
    node->output_columns = output_columns;
    for (const auto& child : children) {
        node->children.push_back(child->copy());
    }
    return node;
}

// HashJoinNode implementation
std::string HashJoinNode::to_string(const int indent) const {
    std::ostringstream oss;
    oss << indent_string(indent) << "Hash " << join_type_to_string() 
        << " (" << format_cost(cost) << ")\n";
    
    if (!join_conditions.empty()) {
        oss << indent_string(indent + 1) << "Hash Cond: ";
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

LogicalPlanNodePtr HashJoinNode::copy() const {
    auto node = std::make_shared<HashJoinNode>(join_type);
    node->join_conditions = join_conditions;
    node->hash_keys_left = hash_keys_left;
    node->hash_keys_right = hash_keys_right;
    node->cost = cost;
    node->output_columns = output_columns;
    for (const auto& child : children) {
        node->children.push_back(child->copy());
    }
    return node;
}

// ProjectionNode implementation
std::string ProjectionNode::to_string(int indent) const {
    std::ostringstream oss;
    oss << indent_string(indent) << "Projection (" << format_cost(cost) << ")\n";
    
    if (!projections.empty()) {
        oss << indent_string(indent + 1) << "Output: ";
        for (size_t i = 0; i < projections.size(); ++i) {
            if (i > 0) oss << ", ";
            oss << projections[i]->value;
            if (i < aliases.size() && !aliases[i].empty()) {
                oss << " AS " << aliases[i];
            }
        }
        oss << "\n";
    }
    
    for (const auto& child : children) {
        oss << child->to_string(indent + 1);
    }
    
    return oss.str();
}

LogicalPlanNodePtr ProjectionNode::copy() const {
    auto node = std::make_shared<ProjectionNode>();
    node->projections = projections;
    node->aliases = aliases;
    node->cost = cost;
    node->output_columns = output_columns;
    for (const auto& child : children) {
        node->children.push_back(child->copy());
    }
    return node;
}

// SelectionNode implementation
std::string SelectionNode::to_string(int indent) const {
    std::ostringstream oss;
    oss << indent_string(indent) << "Filter (" << format_cost(cost) << ")\n";
    
    if (!conditions.empty()) {
        oss << indent_string(indent + 1) << "Filter: ";
        for (size_t i = 0; i < conditions.size(); ++i) {
            if (i > 0) oss << " AND ";
            oss << conditions[i]->value;
        }
        oss << "\n";
    }
    
    for (const auto& child : children) {
        oss << child->to_string(indent + 1);
    }
    
    return oss.str();
}

LogicalPlanNodePtr SelectionNode::copy() const {
    auto node = std::make_shared<SelectionNode>();
    node->conditions = conditions;
    node->cost = cost;
    node->output_columns = output_columns;
    for (const auto& child : children) {
        node->children.push_back(child->copy());
    }
    return node;
}

// AggregationNode implementation
std::string AggregationNode::to_string(int indent) const {
    std::ostringstream oss;
    oss << indent_string(indent) << "Aggregate (" << format_cost(cost) << ")\n";
    
    if (!group_by_exprs.empty()) {
        oss << indent_string(indent + 1) << "Group Key: ";
        for (size_t i = 0; i < group_by_exprs.size(); ++i) {
            if (i > 0) oss << ", ";
            oss << group_by_exprs[i]->value;
        }
        oss << "\n";
    }
    
    if (!aggregate_exprs.empty()) {
        oss << indent_string(indent + 1) << "Aggregates: ";
        for (size_t i = 0; i < aggregate_exprs.size(); ++i) {
            if (i > 0) oss << ", ";
            if (i < aggregate_functions.size()) {
                oss << aggregate_functions[i] << "(";
            }
            oss << aggregate_exprs[i]->value;
            if (i < aggregate_functions.size()) {
                oss << ")";
            }
        }
        oss << "\n";
    }
    
    if (!having_conditions.empty()) {
        oss << indent_string(indent + 1) << "Filter: ";
        for (size_t i = 0; i < having_conditions.size(); ++i) {
            if (i > 0) oss << " AND ";
            oss << having_conditions[i]->value;
        }
        oss << "\n";
    }
    
    for (const auto& child : children) {
        oss << child->to_string(indent + 1);
    }
    
    return oss.str();
}

LogicalPlanNodePtr AggregationNode::copy() const {
    auto node = std::make_shared<AggregationNode>();
    node->group_by_exprs = group_by_exprs;
    node->aggregate_exprs = aggregate_exprs;
    node->aggregate_functions = aggregate_functions;
    node->having_conditions = having_conditions;
    node->cost = cost;
    node->output_columns = output_columns;
    for (const auto& child : children) {
        node->children.push_back(child->copy());
    }
    return node;
}

// SortNode implementation
std::string SortNode::to_string(int indent) const {
    std::ostringstream oss;
    oss << indent_string(indent) << "Sort (" << format_cost(cost) << ")\n";
    
    if (!sort_keys.empty()) {
        oss << indent_string(indent + 1) << "Sort Key: ";
        for (size_t i = 0; i < sort_keys.size(); ++i) {
            if (i > 0) oss << ", ";
            oss << sort_keys[i].expression->value;
            if (!sort_keys[i].ascending) oss << " DESC";
            if (sort_keys[i].nulls_first) oss << " NULLS FIRST";
            else oss << " NULLS LAST";
        }
        oss << "\n";
    }
    
    for (const auto& child : children) {
        oss << child->to_string(indent + 1);
    }
    
    return oss.str();
}

LogicalPlanNodePtr SortNode::copy() const {
    auto node = std::make_shared<SortNode>();
    node->sort_keys = sort_keys;
    node->cost = cost;
    node->output_columns = output_columns;
    for (const auto& child : children) {
        node->children.push_back(child->copy());
    }
    return node;
}

// LimitNode implementation
std::string LimitNode::to_string(int indent) const {
    std::ostringstream oss;
    oss << indent_string(indent) << "Limit (" << format_cost(cost) << ")\n";
    
    oss << indent_string(indent + 1);
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

LogicalPlanNodePtr LimitNode::copy() const {
    auto node = std::make_shared<LimitNode>();
    node->limit = limit;
    node->offset = offset;
    node->cost = cost;
    node->output_columns = output_columns;
    for (const auto& child : children) {
        node->children.push_back(child->copy());
    }
    return node;
}

// InsertNode implementation
std::string InsertNode::to_string(int indent) const {
    std::ostringstream oss;
    oss << indent_string(indent) << "Insert on " << table_name 
        << " (" << format_cost(cost) << ")\n";
    
    if (!target_columns.empty()) {
        oss << indent_string(indent + 1) << "Columns: ";
        for (size_t i = 0; i < target_columns.size(); ++i) {
            if (i > 0) oss << ", ";
            oss << target_columns[i];
        }
        oss << "\n";
    }
    
    if (select_plan) {
        oss << select_plan->to_string(indent + 1);
    } else if (!value_lists.empty()) {
        oss << indent_string(indent + 1) << "Values: " << value_lists.size() << " row(s)\n";
    }
    
    return oss.str();
}

LogicalPlanNodePtr InsertNode::copy() const {
    auto node = std::make_shared<InsertNode>(table_name);
    node->target_columns = target_columns;
    node->value_lists = value_lists;
    if (select_plan) {
        node->select_plan = select_plan->copy();
    }
    node->cost = cost;
    node->output_columns = output_columns;
    return node;
}

// UpdateNode implementation
std::string UpdateNode::to_string(int indent) const {
    std::ostringstream oss;
    oss << indent_string(indent) << "Update on " << table_name 
        << " (" << format_cost(cost) << ")\n";
    
    if (!target_columns.empty()) {
        oss << indent_string(indent + 1) << "Set: ";
        for (size_t i = 0; i < target_columns.size() && i < new_values.size(); ++i) {
            if (i > 0) oss << ", ";
            oss << target_columns[i] << " = " << new_values[i]->value;
        }
        oss << "\n";
    }
    
    if (!where_conditions.empty()) {
        oss << indent_string(indent + 1) << "Filter: ";
        for (size_t i = 0; i < where_conditions.size(); ++i) {
            if (i > 0) oss << " AND ";
            oss << where_conditions[i]->value;
        }
        oss << "\n";
    }
    
    for (const auto& child : children) {
        oss << child->to_string(indent + 1);
    }
    
    return oss.str();
}

LogicalPlanNodePtr UpdateNode::copy() const {
    auto node = std::make_shared<UpdateNode>(table_name);
    node->target_columns = target_columns;
    node->new_values = new_values;
    node->where_conditions = where_conditions;
    node->cost = cost;
    node->output_columns = output_columns;
    for (const auto& child : children) {
        node->children.push_back(child->copy());
    }
    return node;
}

// DeleteNode implementation
std::string DeleteNode::to_string(int indent) const {
    std::ostringstream oss;
    oss << indent_string(indent) << "Delete on " << table_name 
        << " (" << format_cost(cost) << ")\n";
    
    if (!where_conditions.empty()) {
        oss << indent_string(indent + 1) << "Filter: ";
        for (size_t i = 0; i < where_conditions.size(); ++i) {
            if (i > 0) oss << " AND ";
            oss << where_conditions[i]->value;
        }
        oss << "\n";
    }
    
    for (const auto& child : children) {
        oss << child->to_string(indent + 1);
    }
    
    return oss.str();
}

LogicalPlanNodePtr DeleteNode::copy() const {
    auto node = std::make_shared<DeleteNode>(table_name);
    node->where_conditions = where_conditions;
    node->cost = cost;
    node->output_columns = output_columns;
    for (const auto& child : children) {
        node->children.push_back(child->copy());
    }
    return node;
}

// LogicalPlan implementation
std::string LogicalPlan::to_string() const {
    if (!root) {
        return "Empty Plan\n";
    }
    
    std::ostringstream oss;
    oss << "QUERY PLAN\n";
    oss << std::string(50, '-') << "\n";
    oss << root->to_string(0);
    oss << std::string(50, '-') << "\n";
    oss << "Planning time: N/A ms\n";
    oss << "Execution time: N/A ms\n";
    
    return oss.str();
}

void LogicalPlan::calculate_costs() {
    if (root) {
        // This would be implemented to traverse and calculate costs
        total_cost = root->cost;
    }
}

LogicalPlan LogicalPlan::copy() const {
    LogicalPlan copied_plan;
    if (root) {
        copied_plan.root = root->copy();
    }
    copied_plan.table_aliases = table_aliases;
    copied_plan.total_cost = total_cost;
    return copied_plan;
}

}