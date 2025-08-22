#pragma once

#include <string>
#include <utility>
#include <vector>
#include <memory>
#include <unordered_map>
#include <optional>

namespace db25 {
    // Forward declarations for schema integration
    using TableId = size_t;
    using ColumnId = size_t;
    using IndexId = size_t;
}

namespace db25 {
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

        [[nodiscard]] std::string full_name() const {
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
        std::vector<std::shared_ptr<Expression> > children;
        std::optional<ColumnRef> column_ref;

        explicit Expression(const ExpressionType t, std::string v = "") : type(t), value(std::move(v)) {
        }
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
        ANTI, LEFT, FULL, RIGHT
    };

    // In logical_plan.hpp or appropriate header
    enum class SubqueryType {
        EXISTS, // EXISTS (SELECT ...)
        IN, // column IN (SELECT ...)
        NOT_IN, // column NOT IN (SELECT ...)
        ANY, // column = ANY (SELECT ...)
        ALL, // column = ALL (SELECT ...)
        SCALAR // (SELECT single_value ...)
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

        explicit LogicalPlanNode(const PlanNodeType t) : type(t) {
        }

        virtual ~LogicalPlanNode() = default;

        [[nodiscard]] virtual std::string to_string(int indent) const = 0;

        [[nodiscard]] virtual LogicalPlanNodePtr copy() const = 0;
    };

    // Table scan node
    struct TableScanNode final : LogicalPlanNode {
        std::string table_name;                    // Keep for backward compatibility
        std::string alias;
        std::vector<ExpressionPtr> filter_conditions;
        
        // Schema-enhanced fields
        std::optional<TableId> table_id;           // Schema-resolved table ID
        std::vector<ColumnId> projected_columns;   // Schema-resolved columns
        std::unordered_map<std::string, ColumnId> column_name_to_id; // Fast column lookup

        explicit TableScanNode(std::string table)
            : LogicalPlanNode(PlanNodeType::TABLE_SCAN), table_name(std::move(table)) {
        }
        
        // Enhanced constructor with schema information
        TableScanNode(std::string table, TableId tid, std::vector<ColumnId> columns)
            : LogicalPlanNode(PlanNodeType::TABLE_SCAN), table_name(std::move(table)), 
              table_id(tid), projected_columns(std::move(columns)) {
        }

        [[nodiscard]] std::string to_string(int indent) const override;

        [[nodiscard]] LogicalPlanNodePtr copy() const override;
    };

    // Index scan node
    struct IndexScanNode final : LogicalPlanNode {
        std::string table_name;                    // Keep for backward compatibility
        std::string index_name;                    // Keep for backward compatibility
        std::string alias;
        std::vector<ExpressionPtr> index_conditions;
        std::vector<ExpressionPtr> filter_conditions;
        
        // Schema-enhanced fields
        std::optional<TableId> table_id;           // Schema-resolved table ID
        std::optional<IndexId> index_id;           // Schema-resolved index ID
        std::vector<ColumnId> index_columns;       // Schema-resolved index columns
        std::vector<ColumnId> projected_columns;   // Schema-resolved projected columns

        IndexScanNode(std::string table, std::string index)
            : LogicalPlanNode(PlanNodeType::INDEX_SCAN), table_name(std::move(table)), index_name(std::move(index)) {
        }
        
        // Enhanced constructor with schema information
        IndexScanNode(std::string table, std::string index, TableId tid, IndexId iid, 
                     std::vector<ColumnId> idx_cols, std::vector<ColumnId> proj_cols)
            : LogicalPlanNode(PlanNodeType::INDEX_SCAN), table_name(std::move(table)), 
              index_name(std::move(index)), table_id(tid), index_id(iid), 
              index_columns(std::move(idx_cols)), projected_columns(std::move(proj_cols)) {
        }

        [[nodiscard]] std::string to_string(int indent) const override;

        [[nodiscard]] LogicalPlanNodePtr copy() const override;
    };

    // Join node base
    struct JoinNode : LogicalPlanNode {
        JoinType join_type;
        std::vector<ExpressionPtr> join_conditions;

        JoinNode(PlanNodeType type, JoinType jt)
            : LogicalPlanNode(type), join_type(jt) {
        }

        [[nodiscard]] std::string join_type_to_string() const;
    };

    // Nested loop join node
    struct NestedLoopJoinNode : JoinNode {
        explicit NestedLoopJoinNode(JoinType jt)
            : JoinNode(PlanNodeType::NESTED_LOOP_JOIN, jt) {
        }

        [[nodiscard]] std::string to_string(int indent) const override;

        [[nodiscard]] LogicalPlanNodePtr copy() const override;
    };

    // Hash join node
    struct HashJoinNode : JoinNode {
        std::vector<ExpressionPtr> hash_keys_left;
        std::vector<ExpressionPtr> hash_keys_right;

        explicit HashJoinNode(JoinType jt)
            : JoinNode(PlanNodeType::HASH_JOIN, jt) {
        }

        [[nodiscard]] std::string to_string(int indent) const override;

        [[nodiscard]] LogicalPlanNodePtr copy() const override;
    };

    // Projection node
    struct ProjectionNode : LogicalPlanNode {
        std::vector<ExpressionPtr> projections;
        std::vector<std::string> aliases;

        ProjectionNode() : LogicalPlanNode(PlanNodeType::PROJECTION) {
        }

        [[nodiscard]] std::string to_string(int indent) const override;

        [[nodiscard]] LogicalPlanNodePtr copy() const override;
    };

    // Selection (filter) node
    struct SelectionNode final : LogicalPlanNode {
        std::vector<ExpressionPtr> conditions;

        SelectionNode() : LogicalPlanNode(PlanNodeType::SELECTION) {
        }

        [[nodiscard]] std::string to_string(int indent) const override;

        [[nodiscard]] LogicalPlanNodePtr copy() const override;
    };

    // Aggregation node
    struct AggregationNode final : LogicalPlanNode {
        std::vector<ExpressionPtr> group_by_exprs;
        std::vector<ExpressionPtr> aggregate_exprs;
        std::vector<std::string> aggregate_functions;
        std::vector<ExpressionPtr> having_conditions;

        AggregationNode() : LogicalPlanNode(PlanNodeType::AGGREGATION) {
        }

        [[nodiscard]] std::string to_string(int indent) const override;

        [[nodiscard]] LogicalPlanNodePtr copy() const override;
    };

    // Sort node
    struct SortNode : LogicalPlanNode {
        struct SortKey {
            ExpressionPtr expression;
            bool ascending = true;
            bool nulls_first = false;
        };

        std::vector<SortKey> sort_keys;

        SortNode() : LogicalPlanNode(PlanNodeType::SORT) {
        }

        [[nodiscard]] std::string to_string(int indent) const override;

        [[nodiscard]] LogicalPlanNodePtr copy() const override;
    };

    // Limit node
    struct LimitNode final : LogicalPlanNode {
        std::optional<size_t> limit;
        std::optional<size_t> offset;

        LimitNode() : LogicalPlanNode(PlanNodeType::LIMIT) {
        }

        [[nodiscard]] std::string to_string(int indent) const override;

        [[nodiscard]] LogicalPlanNodePtr copy() const override;
    };

    // Insert node
    struct InsertNode final : LogicalPlanNode {
        std::string table_name;
        std::vector<std::string> target_columns;
        std::vector<std::vector<ExpressionPtr> > value_lists;
        LogicalPlanNodePtr select_plan; // For INSERT ... SELECT

        explicit InsertNode(std::string table)
            : LogicalPlanNode(PlanNodeType::INSERT), table_name(std::move(table)) {
        }

        [[nodiscard]] std::string to_string(int indent) const override;

        [[nodiscard]] LogicalPlanNodePtr copy() const override;
    };

    // Update node
    struct UpdateNode : LogicalPlanNode {
        std::string table_name;
        std::vector<std::string> target_columns;
        std::vector<ExpressionPtr> new_values;
        std::vector<ExpressionPtr> where_conditions;

        explicit UpdateNode(std::string table)
            : LogicalPlanNode(PlanNodeType::UPDATE), table_name(std::move(table)) {
        }

        [[nodiscard]] std::string to_string(int indent) const override;

        [[nodiscard]] LogicalPlanNodePtr copy() const override;
    };

    // Delete node
    struct DeleteNode : LogicalPlanNode {
        std::string table_name;
        std::vector<ExpressionPtr> where_conditions;

        explicit DeleteNode(std::string table)
            : LogicalPlanNode(PlanNodeType::DELETE), table_name(std::move(table)) {
        }

        [[nodiscard]] std::string to_string(int indent) const override;

        [[nodiscard]] LogicalPlanNodePtr copy() const override;
    };

    // Logical plan container
    struct LogicalPlan {
        LogicalPlanNodePtr root;
        std::unordered_map<std::string, std::string> table_aliases;
        PlanCost total_cost;

        LogicalPlan() = default;

        explicit LogicalPlan(LogicalPlanNodePtr r) : root(std::move(r)) {
        }

        [[nodiscard]] std::string to_string() const;

        void calculate_costs();

        [[nodiscard]] LogicalPlan copy() const;
    };
}
