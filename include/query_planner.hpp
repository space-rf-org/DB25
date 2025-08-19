#pragma once

#include "logical_plan.hpp"
#include "database.hpp"
#include "pg_query_wrapper.hpp"
#include <unordered_set>

namespace pg {

// Statistics for cost estimation
struct TableStats {
    size_t row_count = 1000;
    double avg_row_size = 100.0;
    std::unordered_map<std::string, double> column_selectivity;
    std::unordered_map<std::string, size_t> distinct_values;
};

// Planner configuration
struct PlannerConfig {
    bool enable_hash_joins = true;
    bool enable_merge_joins = true;
    bool enable_index_scans = true;
    double random_page_cost = 4.0;
    double seq_page_cost = 1.0;
    double cpu_tuple_cost = 0.01;
    double cpu_index_tuple_cost = 0.005;
    double cpu_operator_cost = 0.0025;
    size_t work_mem = 1024 * 1024; // 1MB in bytes
};

// Query planner class
class QueryPlanner {
public:
    explicit QueryPlanner(std::shared_ptr<DatabaseSchema> schema);
    
    // Main planning interface
    LogicalPlan create_plan(const std::string& query);
    LogicalPlan create_plan(const QueryResult& parsed_query);
    
    // Configuration
    void set_config(const PlannerConfig& config) { config_ = config; }
    const PlannerConfig& get_config() const { return config_; }
    
    // Statistics management
    void set_table_stats(const std::string& table_name, const TableStats& stats);
    TableStats get_table_stats(const std::string& table_name) const;
    
    // Plan optimization
    LogicalPlan optimize_plan(const LogicalPlan& plan);
    std::vector<LogicalPlan> generate_alternative_plans(const std::string& query);
    
    // Cost estimation
    void estimate_costs(LogicalPlanNodePtr node);
    double estimate_selectivity(const std::vector<ExpressionPtr>& conditions, 
                               const std::string& table_name) const;

private:
    std::shared_ptr<DatabaseSchema> schema_;
    QueryParser parser_;
    PlannerConfig config_;
    std::unordered_map<std::string, TableStats> table_stats_;
    
    // Plan generation from parse tree
    LogicalPlanNodePtr build_plan_from_select(const std::string& query);
    LogicalPlanNodePtr build_plan_from_insert(const std::string& query);
    LogicalPlanNodePtr build_plan_from_update(const std::string& query);
    LogicalPlanNodePtr build_plan_from_delete(const std::string& query);
    
    // Plan building helpers
    LogicalPlanNodePtr build_scan_node(const std::string& table_name, 
                                      const std::string& alias = "");
    LogicalPlanNodePtr build_join_node(LogicalPlanNodePtr left, 
                                      LogicalPlanNodePtr right,
                                      JoinType join_type,
                                      const std::vector<ExpressionPtr>& conditions);
    LogicalPlanNodePtr build_projection_node(LogicalPlanNodePtr child,
                                            const std::vector<ExpressionPtr>& projections);
    LogicalPlanNodePtr build_selection_node(LogicalPlanNodePtr child,
                                           const std::vector<ExpressionPtr>& conditions);
    
    // Expression parsing
    ExpressionPtr parse_expression(const std::string& expr_str);
    ExpressionPtr parse_column_reference(const std::string& col_ref);
    std::vector<ExpressionPtr> parse_condition_list(const std::string& where_clause);
    
    // Join order optimization
    LogicalPlanNodePtr optimize_join_order(const std::vector<LogicalPlanNodePtr>& tables,
                                          const std::vector<ExpressionPtr>& join_conditions);
    double estimate_join_cost(LogicalPlanNodePtr left, LogicalPlanNodePtr right,
                             const std::vector<ExpressionPtr>& conditions) const;
    
    // Index selection
    std::optional<std::string> select_best_index(const std::string& table_name,
                                                 const std::vector<ExpressionPtr>& conditions) const;
    
    // Plan transformation rules
    LogicalPlanNodePtr apply_predicate_pushdown(LogicalPlanNodePtr node);
    LogicalPlanNodePtr apply_projection_pushdown(LogicalPlanNodePtr node);
    LogicalPlanNodePtr apply_join_reordering(LogicalPlanNodePtr node);
    
    // Utility functions
    std::vector<std::string> extract_table_names_from_query(const std::string& query) const;
    std::vector<ExpressionPtr> extract_join_conditions(const std::string& query) const;
    bool can_use_index_for_condition(const ExpressionPtr& condition, 
                                    const std::string& table_name,
                                    const std::string& index_name) const;
    
    // Cost estimation helpers
    double estimate_table_scan_cost(const std::string& table_name) const;
    double estimate_index_scan_cost(const std::string& table_name, 
                                   const std::string& index_name,
                                   double selectivity) const;
    double estimate_join_cost_internal(JoinType join_type, 
                                     size_t left_rows, 
                                     size_t right_rows,
                                     double selectivity) const;
};

// Plan visitor pattern for traversing and transforming plans
class PlanVisitor {
public:
    virtual ~PlanVisitor() = default;
    
    virtual void visit(TableScanNode* node) {}
    virtual void visit(IndexScanNode* node) {}
    virtual void visit(NestedLoopJoinNode* node) {}
    virtual void visit(HashJoinNode* node) {}
    virtual void visit(ProjectionNode* node) {}
    virtual void visit(SelectionNode* node) {}
    virtual void visit(AggregationNode* node) {}
    virtual void visit(SortNode* node) {}
    virtual void visit(LimitNode* node) {}
    virtual void visit(InsertNode* node) {}
    virtual void visit(UpdateNode* node) {}
    virtual void visit(DeleteNode* node) {}
    
    void traverse(LogicalPlanNodePtr node);
};

// Plan transformer for applying optimization rules
class PlanTransformer : public PlanVisitor {
public:
    LogicalPlanNodePtr transform(LogicalPlanNodePtr node);
    
protected:
    virtual LogicalPlanNodePtr transform_node(LogicalPlanNodePtr node) { return node; }
    
private:
    LogicalPlanNodePtr current_result_;
};

// Specific optimization transformers
class PredicatePushdownTransformer : public PlanTransformer {
protected:
    LogicalPlanNodePtr transform_node(LogicalPlanNodePtr node) override;
};

class ProjectionPushdownTransformer : public PlanTransformer {
protected:
    LogicalPlanNodePtr transform_node(LogicalPlanNodePtr node) override;
};

class JoinReorderingTransformer : public PlanTransformer {
public:
    JoinReorderingTransformer(const QueryPlanner& planner) : planner_(planner) {}
    
protected:
    LogicalPlanNodePtr transform_node(LogicalPlanNodePtr node) override;
    
private:
    const QueryPlanner& planner_;
};

}