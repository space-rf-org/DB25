#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <optional>

namespace pg {

// Forward declarations
struct LogicalPlanNode;
using LogicalPlanNodePtr = std::shared_ptr<LogicalPlanNode>;

// Cost estimation structure
struct PlanCost {
    double startup_cost = 0.0;
    double total_cost = 0.0;
    size_t estimated_rows = 0;
    double selectivity = 1.0;
};

// Column reference
struct ColumnRef {
    std::string table_alias;
    std::string column_name;
    std::string full_name() const {
        return table_alias.empty() ? column_name : table_alias + "." + column_name;
    }
};

// Expression types
enum class ExpressionType {
    COLUMN_REF,
    CONSTANT,
    FUNCTION_CALL,
    BINARY_OP,
    UNARY_OP,
    CASE_EXPR,
    SUBQUERY
};

struct Expression {
    ExpressionType type;
    std::string value;
    std::vector<std::shared_ptr<Expression>> children;
    std::optional<ColumnRef> column_ref;
    
    Expression(ExpressionType t, const std::string& v = "") : type(t), value(v) {}
};

using ExpressionPtr = std::shared_ptr<Expression>;

// Join types
enum class JoinType {
    INNER,
    LEFT_OUTER,
    RIGHT_OUTER,
    FULL_OUTER,
    CROSS,
    SEMI,
    ANTI
};

// Logical plan node types
enum class PlanNodeType {
    TABLE_SCAN,
    INDEX_SCAN,
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    MERGE_JOIN,
    PROJECTION,
    SELECTION,
    AGGREGATION,
    SORT,
    LIMIT,
    UNION,
    INTERSECTION,
    EXCEPT,
    INSERT,
    UPDATE,
    DELETE
};

// Base logical plan node
struct LogicalPlanNode {
    PlanNodeType type;
    PlanCost cost;
    std::vector<LogicalPlanNodePtr> children;
    std::vector<std::string> output_columns;
    
    LogicalPlanNode(PlanNodeType t) : type(t) {}
    virtual ~LogicalPlanNode() = default;
    
    virtual std::string to_string(int indent = 0) const = 0;
    virtual LogicalPlanNodePtr copy() const = 0;
};

// Table scan node
struct TableScanNode : LogicalPlanNode {
    std::string table_name;
    std::string alias;
    std::vector<ExpressionPtr> filter_conditions;
    
    TableScanNode(const std::string& table) 
        : LogicalPlanNode(PlanNodeType::TABLE_SCAN), table_name(table) {}
    
    std::string to_string(int indent = 0) const override;
    LogicalPlanNodePtr copy() const override;
};

// Index scan node
struct IndexScanNode : LogicalPlanNode {
    std::string table_name;
    std::string index_name;
    std::string alias;
    std::vector<ExpressionPtr> index_conditions;
    std::vector<ExpressionPtr> filter_conditions;
    
    IndexScanNode(const std::string& table, const std::string& index) 
        : LogicalPlanNode(PlanNodeType::INDEX_SCAN), table_name(table), index_name(index) {}
    
    std::string to_string(int indent = 0) const override;
    LogicalPlanNodePtr copy() const override;
};

// Join node base
struct JoinNode : LogicalPlanNode {
    JoinType join_type;
    std::vector<ExpressionPtr> join_conditions;
    
    JoinNode(PlanNodeType type, JoinType jt) 
        : LogicalPlanNode(type), join_type(jt) {}
    
    std::string join_type_to_string() const;
};

// Nested loop join node
struct NestedLoopJoinNode : JoinNode {
    NestedLoopJoinNode(JoinType jt) 
        : JoinNode(PlanNodeType::NESTED_LOOP_JOIN, jt) {}
    
    std::string to_string(int indent = 0) const override;
    LogicalPlanNodePtr copy() const override;
};

// Hash join node
struct HashJoinNode : JoinNode {
    std::vector<ExpressionPtr> hash_keys_left;
    std::vector<ExpressionPtr> hash_keys_right;
    
    HashJoinNode(JoinType jt) 
        : JoinNode(PlanNodeType::HASH_JOIN, jt) {}
    
    std::string to_string(int indent = 0) const override;
    LogicalPlanNodePtr copy() const override;
};

// Projection node
struct ProjectionNode : LogicalPlanNode {
    std::vector<ExpressionPtr> projections;
    std::vector<std::string> aliases;
    
    ProjectionNode() : LogicalPlanNode(PlanNodeType::PROJECTION) {}
    
    std::string to_string(int indent = 0) const override;
    LogicalPlanNodePtr copy() const override;
};

// Selection (filter) node
struct SelectionNode : LogicalPlanNode {
    std::vector<ExpressionPtr> conditions;
    
    SelectionNode() : LogicalPlanNode(PlanNodeType::SELECTION) {}
    
    std::string to_string(int indent = 0) const override;
    LogicalPlanNodePtr copy() const override;
};

// Aggregation node
struct AggregationNode : LogicalPlanNode {
    std::vector<ExpressionPtr> group_by_exprs;
    std::vector<ExpressionPtr> aggregate_exprs;
    std::vector<std::string> aggregate_functions;
    std::vector<ExpressionPtr> having_conditions;
    
    AggregationNode() : LogicalPlanNode(PlanNodeType::AGGREGATION) {}
    
    std::string to_string(int indent = 0) const override;
    LogicalPlanNodePtr copy() const override;
};

// Sort node
struct SortNode : LogicalPlanNode {
    struct SortKey {
        ExpressionPtr expression;
        bool ascending = true;
        bool nulls_first = false;
    };
    
    std::vector<SortKey> sort_keys;
    
    SortNode() : LogicalPlanNode(PlanNodeType::SORT) {}
    
    std::string to_string(int indent = 0) const override;
    LogicalPlanNodePtr copy() const override;
};

// Limit node
struct LimitNode : LogicalPlanNode {
    std::optional<size_t> limit;
    std::optional<size_t> offset;
    
    LimitNode() : LogicalPlanNode(PlanNodeType::LIMIT) {}
    
    std::string to_string(int indent = 0) const override;
    LogicalPlanNodePtr copy() const override;
};

// Insert node
struct InsertNode : LogicalPlanNode {
    std::string table_name;
    std::vector<std::string> target_columns;
    std::vector<std::vector<ExpressionPtr>> value_lists;
    LogicalPlanNodePtr select_plan; // For INSERT ... SELECT
    
    InsertNode(const std::string& table) 
        : LogicalPlanNode(PlanNodeType::INSERT), table_name(table) {}
    
    std::string to_string(int indent = 0) const override;
    LogicalPlanNodePtr copy() const override;
};

// Update node
struct UpdateNode : LogicalPlanNode {
    std::string table_name;
    std::vector<std::string> target_columns;
    std::vector<ExpressionPtr> new_values;
    std::vector<ExpressionPtr> where_conditions;
    
    UpdateNode(const std::string& table) 
        : LogicalPlanNode(PlanNodeType::UPDATE), table_name(table) {}
    
    std::string to_string(int indent = 0) const override;
    LogicalPlanNodePtr copy() const override;
};

// Delete node
struct DeleteNode : LogicalPlanNode {
    std::string table_name;
    std::vector<ExpressionPtr> where_conditions;
    
    DeleteNode(const std::string& table) 
        : LogicalPlanNode(PlanNodeType::DELETE), table_name(table) {}
    
    std::string to_string(int indent = 0) const override;
    LogicalPlanNodePtr copy() const override;
};

// Logical plan container
struct LogicalPlan {
    LogicalPlanNodePtr root;
    std::unordered_map<std::string, std::string> table_aliases;
    PlanCost total_cost;
    
    LogicalPlan() = default;
    LogicalPlan(LogicalPlanNodePtr r) : root(r) {}
    
    std::string to_string() const;
    void calculate_costs();
    LogicalPlan copy() const;
};

}