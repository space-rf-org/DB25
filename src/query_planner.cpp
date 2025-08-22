#include "query_planner.hpp"
#include <regex>
#include <algorithm>
#include <cmath>
#include <iostream>
#include <sstream>
#include <memory>
#include <nlohmann/json.hpp>

namespace db25 {
    QueryPlanner::QueryPlanner(const std::shared_ptr<DatabaseSchema> &schema)
        : schema_(schema) {
        // Initialize default table statistics
        for (const auto &table_name: schema_->get_table_names()) {
            TableStats stats;
            stats.row_count = 1000; // Default estimate
            stats.avg_row_size = 100.0;
            table_stats_[table_name] = stats;
        }
    }

    LogicalPlan QueryPlanner::create_plan(const std::string &query) {
        const auto parse_result = parser_.parse(query);
        return create_plan(parse_result);
    }

    LogicalPlan QueryPlanner::create_plan(const QueryResult &parsed_query) {
        if (!parsed_query.is_valid) {
            return {}; // Empty plan for invalid queries
        }

        const std::string query = parsed_query.query;
        std::string upper_query = query;
        std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);

        LogicalPlanNodePtr root;

        if (upper_query.find("SELECT") == 0) {
            root = build_plan_from_select(query);
        } else if (upper_query.find("INSERT") == 0) {
            root = build_plan_from_insert(query);
        } else if (upper_query.find("WITH") == 0) {
            // Check if it's a CTE with INSERT, UPDATE, DELETE, or SELECT
            if (upper_query.find("INSERT") != std::string::npos) {
                root = build_plan_from_insert(query);
            } else if (upper_query.find("UPDATE") != std::string::npos) {
                root = build_plan_from_update(query);
            } else if (upper_query.find("DELETE") != std::string::npos) {
                root = build_plan_from_delete(query);
            } else {
                // Default to SELECT for WITH ... SELECT
                root = build_plan_from_select(query);
            }
        } else if (upper_query.find("UPDATE") == 0) {
            root = build_plan_from_update(query);
        } else if (upper_query.find("DELETE") == 0) {
            root = build_plan_from_delete(query);
        } else {
            return {}; // Unsupported query type
        }

        LogicalPlan plan(root);

        // Calculate costs
        if (root) {
            estimate_costs(root);
            plan.calculate_costs();
        }

        return plan;
    }

    void QueryPlanner::set_table_stats(const std::string &table_name, const TableStats &stats) {
        table_stats_[table_name] = stats;
    }

    TableStats QueryPlanner::get_table_stats(const std::string &table_name) const {
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

    LogicalPlan QueryPlanner::optimize_plan(const LogicalPlan &plan) {
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

    std::vector<LogicalPlan> QueryPlanner::generate_alternative_plans(const std::string &query) {
        std::vector<LogicalPlan> plans;

        // Generate base plan
        LogicalPlan base_plan = create_plan(query);
        if (base_plan.root) {
            plans.push_back(base_plan);

            // Generate optimized variants
            LogicalPlan optimized = optimize_plan(base_plan);
            if (optimized.root != base_plan.root) {
                // Different plan
                plans.push_back(optimized);
            }

            // TODO: Generate alternative join orders, index choices, etc.
        }

        return plans;
    }

    std::vector<ExpressionPtr> QueryPlanner::extract_join_conditions_from_ast(const std::string &query) const {
        std::vector<ExpressionPtr> conditions;

        try {
            // Parse query to get AST
            PgQueryParseResult parse_result = pg_query_parse(query.c_str());

            if (parse_result.error || !parse_result.parse_tree) {
                pg_query_free_parse_result(parse_result);
                // Fallback to regex method
                return extract_join_conditions(query);
            }

            // Parse JSON AST
            nlohmann::json ast = nlohmann::json::parse(parse_result.parse_tree);

            // Extract join conditions recursively
            extract_join_conditions_recursive(ast, conditions);

            pg_query_free_parse_result(parse_result);
        } catch (const std::exception &e) {
            // Fallback to regex method on any parsing error
            return extract_join_conditions(query);
        }

        return conditions;
    }

    void QueryPlanner::extract_join_conditions_recursive(const nlohmann::json &node,
                                                         std::vector<ExpressionPtr> &conditions) const {
        if (!node.is_object() && !node.is_array()) {
            return;
        }

        if (node.is_array()) {
            for (const auto &child: node) {
                extract_join_conditions_recursive(child, conditions);
            }
            return;
        }

        if (node.is_object()) {
            // Look for JoinExpr nodes which contain join conditions
            if (node.contains("JoinExpr")) {
                const auto &join_expr = node["JoinExpr"];

                // Extract join type
                JoinType join_type = determine_join_type_from_ast(join_expr);

                // Extract join conditions from quals field
                if (join_expr.contains("quals") && !join_expr["quals"].is_null()) {
                    auto condition = parse_expression_from_ast(join_expr["quals"]);
                    if (condition) {
                        conditions.push_back(condition);
                    }
                }

                // Recursively process left and right join operands
                if (join_expr.contains("larg")) {
                    extract_join_conditions_recursive(join_expr["larg"], conditions);
                }
                if (join_expr.contains("rarg")) {
                    extract_join_conditions_recursive(join_expr["rarg"], conditions);
                }
            }

            // Look for RangeSubselect (subquery joins)
            if (node.contains("RangeSubselect")) {
                const auto &subselect = node["RangeSubselect"];
                if (subselect.contains("subquery")) {
                    extract_join_conditions_recursive(subselect["subquery"], conditions);
                }
            }

            // Recursively traverse all other nodes
            for (const auto &[key, value]: node.items()) {
                extract_join_conditions_recursive(value, conditions);
            }
        }
    }

    // Implementation of enhanced methods:
    std::vector<ExpressionPtr> QueryPlanner::extract_where_conditions_from_ast(const std::string& query) const {
        try {
            // Parse query to get AST
            // TODO: Can we avoid repeated parsing and pass PgQueryParseResult instead?
            PgQueryParseResult parse_result = pg_query_parse(query.c_str());

            if (parse_result.error || !parse_result.parse_tree) {
                pg_query_free_parse_result(parse_result);
                // TODO: throw exception? cost?
                return {};
            }

            // Parse JSON AST
            const nlohmann::json ast = nlohmann::json::parse(parse_result.parse_tree);

            // Extract WHERE conditions from AST
            auto conditions = parse_where_clause_ast(ast);

            pg_query_free_parse_result(parse_result);
            return conditions;

        } catch (const std::exception& e) {
            // TODO: log exception
            return {};
        }
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    ExpressionPtr QueryPlanner::parse_expression_from_ast(const nlohmann::json &expr_node) const {
        if (expr_node.is_null() || !expr_node.is_object()) {
            return nullptr;
        }

        // Handle different expression types
        if (expr_node.contains("A_Expr")) {
            return build_binary_operation(expr_node["A_Expr"]);
        }

        if (expr_node.contains("ColumnRef")) {
            return build_column_reference(expr_node["ColumnRef"]);
        }

        if (expr_node.contains("A_Const")) {
            return build_constant_value(expr_node["A_Const"]);
        }

        if (expr_node.contains("FuncCall")) {
            return build_function_call(expr_node["FuncCall"]);
        }

        if (expr_node.contains("BoolExpr")) {
            return build_boolean_expression(expr_node["BoolExpr"]);
        }

        if (expr_node.contains("SubLink")) {
            return build_subquery_expression(expr_node["SubLink"]);
        }

        return nullptr;
    }

    ExpressionPtr QueryPlanner::build_subquery_expression(const nlohmann::json &sublink_node) const {
        if (!sublink_node.contains("subLinkType") || !sublink_node.contains("subselect")) {
            return nullptr;
        }

        // Determine subquery type
        int sublink_type = sublink_node["subLinkType"].get<int>();
        SubqueryType sq_type = determine_subquery_type(sublink_type);

        // Create subquery expression
        auto expr = std::make_shared<Expression>(ExpressionType::SUBQUERY);

        // Set subquery type as value for identification
        switch (sq_type) {
            case SubqueryType::EXISTS:
                expr->value = "EXISTS";
                break;
            case SubqueryType::IN:
                expr->value = "IN";
                break;
            case SubqueryType::NOT_IN:
                expr->value = "NOT IN";
                break;
            case SubqueryType::ANY:
                expr->value = "ANY";
                break;
            case SubqueryType::ALL:
                expr->value = "ALL";
                break;
            case SubqueryType::SCALAR:
                expr->value = "SCALAR";
                break;
            default:
                expr->value = "UNKNOWN";
        }

        // Parse the subselect query (recursive parsing)
        const auto &subselect = sublink_node["subselect"];
        if (subselect.contains("SelectStmt")) {
            // For join conditions, we mainly need to know this is a subquery
            // The actual subquery parsing would be handled by the main query planner

            // Store subquery text if available for later processing
            // In a full implementation, you'd recursively parse the SelectStmt
            expr->value += "_SUBQUERY";
        }

        // Handle test expression (the expression being compared to the subquery result)
        if (sublink_node.contains("testexpr") && !sublink_node["testexpr"].is_null()) {
            auto test_expr = parse_expression_from_ast(sublink_node["testexpr"]);
            if (test_expr) {
                expr->children.push_back(test_expr);
            }
        }

        // Handle operator name for IN/NOT IN/ANY/ALL subqueries
        if (sublink_node.contains("operName")) {
            const auto &oper_name = sublink_node["operName"];
            if (oper_name.is_array() && !oper_name.empty()) {
                if (oper_name[0].contains("String") && oper_name[0]["String"].contains("sval")) {
                    std::string op = oper_name[0]["String"]["sval"];
                    expr->value += "_" + op;
                }
            }
        }

        return expr;
    }

    SubqueryType QueryPlanner::determine_subquery_type(int sublink_type) const {
        // PostgreSQL SubLinkType constants from parsenodes.h
        switch (sublink_type) {
            case 0: return SubqueryType::EXISTS; // EXISTS_SUBLINK
            case 1: return SubqueryType::ALL; // ALL_SUBLINK
            case 2: return SubqueryType::ANY; // ANY_SUBLINK
            case 3: return SubqueryType::SCALAR; // ROWCOMPARE_SUBLINK
            case 4: return SubqueryType::SCALAR; // EXPR_SUBLINK (scalar subquery)
            case 5: return SubqueryType::SCALAR; // MULTIEXPR_SUBLINK
            case 6: return SubqueryType::IN; // ARRAY_SUBLINK (IN clause)
            case 7: return SubqueryType::SCALAR; // CTE_SUBLINK
            default: return SubqueryType::SCALAR;
        }
    }

    ExpressionPtr QueryPlanner::build_binary_operation(const nlohmann::json &a_expr_node) const {
        if (!a_expr_node.contains("name") || !a_expr_node.contains("lexpr") || !a_expr_node.contains("rexpr")) {
            return nullptr;
        }

        // Extract operator
        std::string op_name;
        const auto &name_array = a_expr_node["name"];
        if (name_array.is_array() && !name_array.empty()) {
            if (name_array[0].contains("String") && name_array[0]["String"].contains("sval")) {
                op_name = name_array[0]["String"]["sval"];
            }
        }

        // Create binary operation expression
        auto expr = std::make_shared<Expression>(ExpressionType::BINARY_OP, op_name);

        // Parse left and right expressions
        auto left_expr = parse_expression_from_ast(a_expr_node["lexpr"]);
        auto right_expr = parse_expression_from_ast(a_expr_node["rexpr"]);

        if (left_expr) expr->children.push_back(left_expr);
        if (right_expr) expr->children.push_back(right_expr);

        return expr;
    }

    ExpressionPtr QueryPlanner::build_column_reference(const nlohmann::json &column_ref_node) const {
        if (!column_ref_node.contains("fields")) {
            return nullptr;
        }

        std::string column_name;
        std::vector<std::string> parts;
        const auto &fields = column_ref_node["fields"];

        if (fields.is_array()) {
            for (const auto &field: fields) {
                if (field.contains("String") && field["String"].contains("sval")) {
                    parts.push_back(field["String"]["sval"]);
                }
            }

            // Build qualified column name (table.column)
            column_name = std::accumulate(parts.begin(), parts.end(), std::string(),
                                          [](const std::string &a, const std::string &b) {
                                              return a.empty() ? b : a + "." + b;
                                          });
        }

        auto expr = std::make_shared<Expression>(ExpressionType::COLUMN_REF, column_name);

        // Set column reference info
        if (parts.size() >= 2) {
            expr->column_ref = ColumnRef{parts[0], parts[1]}; // table, column
        } else if (parts.size() == 1) {
            expr->column_ref = ColumnRef{"", parts[0]}; // no table qualification
        }

        return expr;
    }

    std::vector<ExpressionPtr> QueryPlanner::parse_where_clause_ast(const nlohmann::json& ast) const {
        std::vector<ExpressionPtr> conditions;

        // Navigate to SelectStmt and find whereClause
        if (ast.contains("stmts") && ast["stmts"].is_array() && !ast["stmts"].empty()) {
            const auto& stmt = ast["stmts"][0];

            if (stmt.contains("stmt") && stmt["stmt"].contains("SelectStmt")) {
                const auto& select_stmt = stmt["stmt"]["SelectStmt"];

                if (select_stmt.contains("whereClause") && !select_stmt["whereClause"].is_null()) {
                    auto where_expr = parse_where_expression_ast(select_stmt["whereClause"]);
                    if (where_expr) {
                        // If it's a single condition, add it directly
                        // If it's a compound condition (AND/OR), break it down
                        flatten_condition_tree(where_expr, conditions);
                    }
                }
            }
        }

        return conditions;
    }

    ExpressionPtr QueryPlanner::parse_where_expression_ast(const nlohmann::json& where_node) const {
        if (where_node.is_null() || !where_node.is_object()) {
            return nullptr;
        }

        // Handle different WHERE expression types
        if (where_node.contains("A_Expr")) {
            return build_binary_operation(where_node["A_Expr"]);
        }

        if (where_node.contains("BoolExpr")) {
            return build_boolean_expression(where_node["BoolExpr"]);
        }

        if (where_node.contains("SubLink")) {
            return build_subquery_expression(where_node["SubLink"]);
        }

        if (where_node.contains("FuncCall")) {
            return build_function_call(where_node["FuncCall"]);
        }

        if (where_node.contains("CaseExpr")) {
            return build_case_expression(where_node["CaseExpr"]);
        }

        if (where_node.contains("ColumnRef")) {
            return build_column_reference(where_node["ColumnRef"]);
        }

        if (where_node.contains("A_Const")) {
            return build_constant_value(where_node["A_Const"]);
        }

        return nullptr;
    }

    // Enhanced condition list parsing with proper operator precedence
    // NOLINTNEXTLINE(misc-no-recursion, readability-convert-member-functions-to-static)
    void QueryPlanner::flatten_condition_tree(const ExpressionPtr& expr,
                                             std::vector<ExpressionPtr>& conditions) const {
        if (!expr) return;

        // If it's an AND expression, flatten its children
        if (expr->type == ExpressionType::BINARY_OP && expr->value == "AND") {
            for (const auto& child : expr->children) {
                flatten_condition_tree(child, conditions);
            }
        } else {
            // It's a leaf condition or OR/complex expression
            conditions.push_back(expr);
        }
    }

    // Enhanced CASE expression support
    // NOLINTNEXTLINE (misc-no-recursion)
    ExpressionPtr QueryPlanner::build_case_expression(const nlohmann::json& case_node) const {
        if (!case_node.contains("args")) {
            return nullptr;
        }

        auto expr = std::make_shared<Expression>(ExpressionType::CASE_EXPR);

        // Parse CASE WHEN conditions and results
        const auto& args = case_node["args"];
        if (args.is_array()) {
            for (const auto& when_clause : args) {
                if (when_clause.contains("CaseWhen")) {
                    const auto& case_when = when_clause["CaseWhen"];

                    // Parse WHEN condition
                    if (case_when.contains("expr")) {
                        auto when_expr = parse_where_expression_ast(case_when["expr"]);
                        if (when_expr) {
                            expr->children.push_back(when_expr);
                        }
                    }

                    // Parse THEN result
                    if (case_when.contains("result")) {
                        auto then_expr = parse_where_expression_ast(case_when["result"]);
                        if (then_expr) {
                            expr->children.push_back(then_expr);
                        }
                    }
                }
            }
        }

        // Parse ELSE clause
        if (case_node.contains("defresult")) {
            auto else_expr = parse_where_expression_ast(case_node["defresult"]);
            if (else_expr) {
                expr->children.push_back(else_expr);
            }
        }

        return expr;
    }

    ExpressionPtr QueryPlanner::build_constant_value(const nlohmann::json &const_node) const {
        std::string value;

        // Handle different constant types
        if (const_node.contains("ival")) {
            // Handle nested ival structure: {"ival": {"ival": 123}}
            const auto& ival_node = const_node["ival"];
            if (ival_node.contains("ival")) {
                value = std::to_string(ival_node["ival"].get<int>());
            } else {
                value = std::to_string(ival_node.get<int>());
            }
        } else if (const_node.contains("fval")) {
            value = const_node["fval"].get<std::string>();
        } else if (const_node.contains("sval")) {
            // Handle nested sval structure: {"sval": {"sval": "active"}}
            const auto& sval_node = const_node["sval"];
            if (sval_node.contains("sval")) {
                value = sval_node["sval"].get<std::string>();
            } else {
                value = sval_node.get<std::string>();
            }
        } else if (const_node.contains("boolval")) {
            value = const_node["boolval"].get<bool>() ? "true" : "false";
        }

        return std::make_shared<Expression>(ExpressionType::CONSTANT, value);
    }

    ExpressionPtr QueryPlanner::build_function_call(const nlohmann::json &func_call_node) const {
        if (!func_call_node.contains("funcname")) {
            return nullptr;
        }

        // Extract function name
        std::string func_name;
        const auto &funcname = func_call_node["funcname"];
        if (funcname.is_array() && !funcname.empty()) {
            if (funcname[0].contains("String") && funcname[0]["String"].contains("sval")) {
                func_name = funcname[0]["String"]["sval"];
            }
        }

        auto expr = std::make_shared<Expression>(ExpressionType::FUNCTION_CALL, func_name);

        // Parse function arguments
        if (func_call_node.contains("args")) {
            const auto &args = func_call_node["args"];
            if (args.is_array()) {
                for (const auto &arg: args) {
                    auto arg_expr = parse_expression_from_ast(arg);
                    if (arg_expr) {
                        expr->children.push_back(arg_expr);
                    }
                }
            }
        }

        return expr;
    }

    JoinType QueryPlanner::determine_join_type_from_ast(const nlohmann::json &join_node) const {
        if (!join_node.contains("jointype")) {
            return JoinType::INNER;
        }

        int join_type_int = join_node["jointype"].get<int>();

        // PostgreSQL join type constants
        switch (join_type_int) {
            case 0: return JoinType::INNER; // JOIN_INNER
            case 1: return JoinType::LEFT; // JOIN_LEFT
            case 2: return JoinType::FULL; // JOIN_FULL
            case 3: return JoinType::RIGHT; // JOIN_RIGHT
            case 4: return JoinType::SEMI; // JOIN_SEMI
            case 5: return JoinType::ANTI; // JOIN_ANTI
            default: return JoinType::INNER;
        }
    }

    // Enhanced boolean expression handling
    ExpressionPtr QueryPlanner::build_boolean_expression(const nlohmann::json &bool_expr_node) const {
        if (!bool_expr_node.contains("boolop") || !bool_expr_node.contains("args")) {
            return nullptr;
        }

        const std::string bool_op = bool_expr_node["boolop"].get<std::string>();
        std::string op_name;

        // PostgreSQL boolean operators
        if (bool_op == "AND_EXPR") {
            op_name = "AND";
        } else if (bool_op == "OR_EXPR") {
            op_name = "OR";
        } else if (bool_op == "NOT_EXPR") {
            op_name = "NOT";
        }

        if (op_name.empty()) { // DEFAULT
            op_name = "AND"; // default fallback
        }


        auto expr = std::make_shared<Expression>(ExpressionType::BINARY_OP, op_name);

        // Parse arguments
        const auto &args = bool_expr_node["args"];
        if (args.is_array()) {
            for (const auto &arg: args) {
                auto arg_expr = parse_expression_from_ast(arg);
                if (arg_expr) {
                    expr->children.push_back(arg_expr);
                }
            }
        }

        return expr;
    }

    void QueryPlanner::estimate_costs(const LogicalPlanNodePtr &node) { // NOLINT (recursively defined)
        if (!node) return;

        // First estimate costs for children
        for (auto &child: node->children) {
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
                node->cost.estimated_rows = static_cast<size_t>(
                    get_table_stats(scan_node->table_name).row_count * selectivity); // NOLINT(readability-magic-numbers
                node->cost.selectivity = selectivity;
                break;
            }

            case PlanNodeType::INDEX_SCAN: {
                const auto index_node = std::static_pointer_cast<IndexScanNode>(node);
                const double selectivity = estimate_selectivity(index_node->index_conditions, index_node->table_name);
                const double base_cost =
                        estimate_index_scan_cost(index_node->table_name, index_node->index_name, selectivity);

                node->cost.startup_cost = 0.0;
                node->cost.total_cost = base_cost;
                node->cost.estimated_rows = static_cast<size_t>(
                    get_table_stats(index_node->table_name).row_count * selectivity);
                node->cost.selectivity = selectivity;
                break;
            }

            case PlanNodeType::NESTED_LOOP_JOIN: {
                auto join_node = std::static_pointer_cast<NestedLoopJoinNode>(node);
                if (join_node->children.size() == 2) {
                    const auto left = join_node->children[0];
                    const auto right = join_node->children[1];

                    const double selectivity = estimate_selectivity(join_node->join_conditions, "");
                    const double join_cost = estimate_join_cost_internal(join_node->join_type,
                                                                   left->cost.estimated_rows,
                                                                   right->cost.estimated_rows,
                                                                   selectivity);

                    node->cost.startup_cost = left->cost.startup_cost + right->cost.startup_cost;
                    node->cost.total_cost = left->cost.total_cost + right->cost.total_cost + join_cost;
                    node->cost.estimated_rows = static_cast<size_t>(
                        left->cost.estimated_rows * right->cost.estimated_rows * selectivity); // NOLINT(readability-magic-numbers
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
                    node->cost.total_cost = left->cost.total_cost + right->cost.total_cost + hash_build_cost +
                                            hash_probe_cost;
                    node->cost.estimated_rows = static_cast<size_t>(
                        left->cost.estimated_rows * right->cost.estimated_rows * selectivity);
                    node->cost.selectivity = selectivity;
                }
                break;
            }

            case PlanNodeType::PROJECTION: {
                if (!node->children.empty()) {
                    auto child = node->children[0];
                    node->cost.startup_cost = child->cost.startup_cost;
                    node->cost.total_cost = child->cost.total_cost + child->cost.estimated_rows * config_.
                                            cpu_tuple_cost;
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
                    node->cost.total_cost = child->cost.total_cost + child->cost.estimated_rows * config_.
                                            cpu_operator_cost;
                    node->cost.estimated_rows = static_cast<size_t>(child->cost.estimated_rows * selectivity);
                    node->cost.selectivity = selectivity;
                }
                break;
            }

            case PlanNodeType::SORT: {
                if (!node->children.empty()) {
                    const auto child = node->children[0];
                    const double sort_cost = child->cost.estimated_rows * std::log2(child->cost.estimated_rows) * config_.
                                       cpu_operator_cost;

                    node->cost.startup_cost = child->cost.total_cost + sort_cost;
                    node->cost.total_cost = node->cost.startup_cost;
                    node->cost.estimated_rows = child->cost.estimated_rows;
                    node->cost.selectivity = child->cost.selectivity;
                }
                break;
            }

            case PlanNodeType::LIMIT: {
                const auto limit_node = std::static_pointer_cast<LimitNode>(node);
                if (!node->children.empty()) {
                    const auto child = node->children[0];
                    size_t output_rows = child->cost.estimated_rows;

                    if (limit_node->limit) {
                        output_rows = std::min(output_rows, *limit_node->limit);
                    }

                    const double fraction = static_cast<double>(output_rows) / child->cost.estimated_rows;

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

    //NOLINTNEXTLINE: xyz
    double QueryPlanner::estimate_selectivity(const std::vector<ExpressionPtr> &conditions,
                                              const std::string &table_name) const {
        if (conditions.empty()) {
            return 1.0;
        }

        // Simple selectivity estimation - in a real system this would be much more sophisticated
        double selectivity = 1.0;

        for (const auto &condition: conditions) {
            // Default selectivity for various condition types
            if (condition->value.find('=') != std::string::npos) {
                selectivity *= 0.1; // Equality conditions are fairly selective
            } else if (condition->value.find('<') != std::string::npos ||
                       condition->value.find('>') != std::string::npos) {
                selectivity *= 0.3; // Range conditions
            } else if (condition->value.find("LIKE") != std::string::npos) {
                selectivity *= 0.2; // Pattern matching
            } else {
                selectivity *= 0.5; // Default
            }
        }

        return std::max(0.001, std::min(1.0, selectivity)); // Clamp between 0.1% and 100%
    }

    LogicalPlanNodePtr QueryPlanner::build_plan_from_select(const std::string &query) {
        const auto result = new EnhancedQueryResult();
        result->extract_references(query);
        const std::vector<std::string> table_names = result->tables;

        if (table_names.empty()) {
            return nullptr;
        }

        // Build scan nodes for each table
        std::vector<LogicalPlanNodePtr> scan_nodes;
        scan_nodes.reserve(table_names.size());
        for (const auto &table_name: table_names) {
            scan_nodes.push_back(build_scan_node(table_name));
        }

        LogicalPlanNodePtr plan_root = scan_nodes[0];

        // If multiple tables, create joins
        if (scan_nodes.size() > 1) {
            auto join_conditions = extract_join_conditions_from_ast(query);
            plan_root = optimize_join_order(scan_nodes, join_conditions);
        }

        // Add WHERE conditions as selection - ENHANCED AST VERSION
        if (auto where_conditions = extract_where_conditions_from_ast(query); !where_conditions.empty()) {
            plan_root = build_selection_node(plan_root, where_conditions);
        }

        // Add projection for SELECT list - ENHANCED AST VERSION
        if (auto projections = extract_projections_from_ast(query); !projections.empty() && !is_star_projection(projections)) {
            plan_root = build_projection_node(plan_root, projections);
        }
        // Note: For SELECT *, no projection node is needed as all columns are included

        // Add ORDER BY - ENHANCED AST VERSION
        if (auto sort_keys = extract_order_by_from_ast(query); !sort_keys.empty()) {
            const auto sort_node = std::make_shared<SortNode>();
            sort_node->children.push_back(plan_root);
            sort_node->sort_keys = std::move(sort_keys);
            plan_root = sort_node;
        }

        // Add LIMIT - ENHANCED AST VERSION
        if (auto limit_value = extract_limit_from_ast(query); limit_value.has_value()) {
            const auto limit_node = std::make_shared<LimitNode>();
            limit_node->children.push_back(plan_root);
            limit_node->limit = limit_value.value();
            plan_root = limit_node;
        }

        return plan_root;
    }

    bool QueryPlanner::is_star_projection(const std::vector<ExpressionPtr>& projections) const {
        return projections.size() == 1 &&
               projections[0]->type == ExpressionType::COLUMN_REF &&
               projections[0]->value == "*";
    }

    std::vector<ExpressionPtr> QueryPlanner::extract_projections_from_ast(const std::string& query) const {
        std::vector<ExpressionPtr> projections;

        try {
            PgQueryParseResult parse_result = pg_query_parse(query.c_str());

            // RAII-style cleanup to prevent memory leaks
            auto cleanup = [&parse_result](void*) { pg_query_free_parse_result(parse_result); };
            std::unique_ptr<void, decltype(cleanup)> cleanup_guard(nullptr, cleanup);

            if (parse_result.error || !parse_result.parse_tree) {
                return projections;
            }

            const nlohmann::json ast = nlohmann::json::parse(parse_result.parse_tree);

            if (ast.contains("stmts") && ast["stmts"].is_array() && !ast["stmts"].empty()) {
                const auto& stmt = ast["stmts"][0];

                if (stmt.contains("stmt") && stmt["stmt"].contains("SelectStmt")) {
                    const auto& select_stmt = stmt["stmt"]["SelectStmt"];

                    if (select_stmt.contains("targetList") && select_stmt["targetList"].is_array()) {
                        for (const auto& target_item : select_stmt["targetList"]) {
                            if (target_item.contains("ResTarget")) {
                                auto projection = parse_projection_target(target_item["ResTarget"]);
                                if (projection) {
                                    projections.push_back(projection);
                                }
                            }
                        }
                    }
                }
            }

            return projections;

        } catch (const std::exception& e) {
            // cleanup_guard will automatically free resources
            return projections;
        }
    }

    ExpressionPtr QueryPlanner::parse_projection_target(const nlohmann::json& res_target) const {
        if (!res_target.contains("val")) {
            return nullptr;
        }

        // Parse the main expression using existing parse_expression_from_ast
        auto expr = parse_expression_from_ast(res_target["val"]);
        if (!expr) {
            return nullptr;
        }

        // Handle SELECT * case
        if (res_target["val"].contains("ColumnRef")) {
            const auto& column_ref = res_target["val"]["ColumnRef"];
            if (column_ref.contains("fields") && column_ref["fields"].is_array() &&
                !column_ref["fields"].empty() && column_ref["fields"][0].contains("A_Star")) {
                expr->value = "*";
                expr->type = ExpressionType::COLUMN_REF;
                return expr;
                }
        }

        // Handle alias (AS clause)
        if (res_target.contains("name") && !res_target["name"].is_null()) {
            std::string alias = res_target["name"].get<std::string>();
            expr->value += " AS " + alias;
        }

        return expr;
    }

    std::vector<SortNode::SortKey> QueryPlanner::extract_order_by_from_ast(const std::string& query) const {
        std::vector<SortNode::SortKey> sort_keys;

        try {
            PgQueryParseResult parse_result = pg_query_parse(query.c_str());

            // RAII-style cleanup to prevent memory leaks
            auto cleanup = [&parse_result](void*) { pg_query_free_parse_result(parse_result); };
            std::unique_ptr<void, decltype(cleanup)> cleanup_guard(nullptr, cleanup);

            if (parse_result.error || !parse_result.parse_tree) {
                return sort_keys;
            }

            const nlohmann::json ast = nlohmann::json::parse(parse_result.parse_tree);

            if (ast.contains("stmts") && ast["stmts"].is_array() && !ast["stmts"].empty()) {
                const auto& stmt = ast["stmts"][0];

                if (stmt.contains("stmt") && stmt["stmt"].contains("SelectStmt")) {
                    const auto& select_stmt = stmt["stmt"]["SelectStmt"];

                    if (select_stmt.contains("sortClause") && select_stmt["sortClause"].is_array()) {
                        for (const auto& sort_item : select_stmt["sortClause"]) {
                            if (sort_item.contains("SortBy")) {
                                const auto& sort_by = sort_item["SortBy"];
                                
                                SortNode::SortKey key;
                                
                                // Parse the sort expression
                                if (sort_by.contains("node")) {
                                    key.expression = parse_expression_from_ast(sort_by["node"]);
                                }
                                
                                // Parse sort direction (default is ascending)
                                key.ascending = true;
                                if (sort_by.contains("sortby_dir")) {
                                    // Handle both string and integer representations
                                    if (sort_by["sortby_dir"].is_string()) {
                                        const std::string direction = sort_by["sortby_dir"].get<std::string>();
                                        key.ascending = (direction != "SORTBY_DESC");
                                    } else if (sort_by["sortby_dir"].is_number()) {
                                        const int direction = sort_by["sortby_dir"].get<int>();
                                        // SORTBY_DESC = 2, SORTBY_ASC = 1, SORTBY_DEFAULT = 0
                                        key.ascending = (direction != 2);  // Everything except DESC is ascending
                                    }
                                }
                                
                                // Parse NULLS ordering
                                if (sort_by.contains("sortby_nulls")) {
                                    // Handle both string and integer representations
                                    // SORTBY_NULLS_FIRST = 1, SORTBY_NULLS_LAST = 2, SORTBY_NULLS_DEFAULT = 0
                                    // For now, we'll store this information in a comment or ignore it
                                    // since SortKey doesn't have a nulls_first field
                                    if (sort_by["sortby_nulls"].is_string()) {
                                        const std::string nulls_order = sort_by["sortby_nulls"].get<std::string>();
                                        // Could store or process NULLS FIRST/LAST information here
                                    } else if (sort_by["sortby_nulls"].is_number()) {
                                        const int nulls_order = sort_by["sortby_nulls"].get<int>();
                                        // Could store or process NULLS FIRST/LAST information here
                                    }
                                }
                                
                                if (key.expression) {
                                    sort_keys.push_back(key);
                                }
                            }
                        }
                    }
                }
            }

            return sort_keys;

        } catch (const std::exception& e) {
            // cleanup_guard will automatically free resources
            return sort_keys;
        }
    }

    std::optional<size_t> QueryPlanner::extract_limit_from_ast(const std::string& query) const {
        try {
            PgQueryParseResult parse_result = pg_query_parse(query.c_str());

            // RAII-style cleanup to prevent memory leaks
            auto cleanup = [&parse_result](void*) { pg_query_free_parse_result(parse_result); };
            std::unique_ptr<void, decltype(cleanup)> cleanup_guard(nullptr, cleanup);

            if (parse_result.error || !parse_result.parse_tree) {
                return std::nullopt;
            }

            const nlohmann::json ast = nlohmann::json::parse(parse_result.parse_tree);

            if (ast.contains("stmts") && ast["stmts"].is_array() && !ast["stmts"].empty()) {
                const auto& stmt = ast["stmts"][0];

                if (stmt.contains("stmt") && stmt["stmt"].contains("SelectStmt")) {
                    const auto& select_stmt = stmt["stmt"]["SelectStmt"];

                    if (select_stmt.contains("limitCount") && !select_stmt["limitCount"].is_null()) {
                        const auto& limit_node = select_stmt["limitCount"];
                        
                        // LIMIT value should be a constant (A_Const)
                        if (limit_node.contains("A_Const")) {
                            const auto& const_node = limit_node["A_Const"];
                            
                            // Handle integer values
                            if (const_node.contains("ival") && const_node["ival"].contains("ival")) {
                                const int limit_value = const_node["ival"]["ival"].get<int>();
                                if (limit_value >= 0) {
                                    return static_cast<size_t>(limit_value);
                                }
                            }
                            
                            // Handle string representation of numbers (less common)
                            if (const_node.contains("sval") && const_node["sval"].contains("sval")) {
                                const std::string limit_str = const_node["sval"]["sval"].get<std::string>();
                                try {
                                    const size_t limit_value = std::stoull(limit_str);
                                    return limit_value;
                                } catch (const std::exception&) {
                                    // Invalid number format
                                }
                            }
                        }
                        
                        // Handle parameter references ($1, $2, etc.) for prepared statements
                        if (limit_node.contains("ParamRef")) {
                            // For prepared statements, we can't determine the actual value at parse time
                            // Return a special value or handle this case appropriately
                            // For now, we'll return nullopt to indicate unknown limit
                        }
                    }
                }
            }

            return std::nullopt;

        } catch (const std::exception& e) {
            // cleanup_guard will automatically free resources
            return std::nullopt;
        }
    }

    std::optional<QueryPlanner::InsertInfo> QueryPlanner::extract_insert_info_from_ast(const std::string &query) const {
        try {
            PgQueryParseResult parse_result = pg_query_parse(query.c_str());
            
            // RAII cleanup using unique_ptr with custom deleter
            auto cleanup = [&parse_result](void*) { pg_query_free_parse_result(parse_result); };
            std::unique_ptr<void, decltype(cleanup)> cleanup_guard(nullptr, cleanup);
            
            if (parse_result.error) {
                return std::nullopt;
            }

            nlohmann::json ast = nlohmann::json::parse(parse_result.parse_tree);
            
            if (!ast.contains("stmts") || !ast["stmts"].is_array() || ast["stmts"].empty()) {
                return std::nullopt;
            }

            const auto& first_stmt = ast["stmts"][0];
            if (first_stmt.contains("stmt") && first_stmt["stmt"].contains("InsertStmt")) {
                const auto& insert_stmt = first_stmt["stmt"]["InsertStmt"];
                QueryPlanner::InsertInfo info;
                
                // Extract table name from relation
                if (insert_stmt.contains("relation") && insert_stmt["relation"].contains("relname")) {
                    info.table_name = insert_stmt["relation"]["relname"].get<std::string>();
                }
                
                // Extract target column list if present
                if (insert_stmt.contains("cols") && insert_stmt["cols"].is_array()) {
                    for (const auto& res_target : insert_stmt["cols"]) {
                        if (res_target.contains("ResTarget")) {
                            const auto& target = res_target["ResTarget"];
                            if (target.contains("name") && target["name"].is_string()) {
                                info.target_columns.push_back(target["name"].get<std::string>());
                            }
                        }
                    }
                }
                
                // Check for WITH clause (CTE)
                if (insert_stmt.contains("withClause")) {
                    info.has_cte = true;
                    const auto& with_clause = insert_stmt["withClause"];
                    
                    // Extract CTE names
                    if (with_clause.contains("ctes") && with_clause["ctes"].is_array()) {
                        for (const auto& cte : with_clause["ctes"]) {
                            if (cte.contains("CommonTableExpr")) {
                                const auto& cte_expr = cte["CommonTableExpr"];
                                if (cte_expr.contains("ctename") && cte_expr["ctename"].is_string()) {
                                    info.cte_names.push_back(cte_expr["ctename"].get<std::string>());
                                }
                            }
                        }
                    }
                }
                
                // Check if INSERT has VALUES clause or subquery
                if (insert_stmt.contains("selectStmt")) {
                    const auto& select_stmt = insert_stmt["selectStmt"];
                    if (select_stmt.contains("SelectStmt")) {
                        const auto& stmt_details = select_stmt["SelectStmt"];
                        if (stmt_details.contains("valuesLists") && stmt_details["valuesLists"].is_array()) {
                            info.has_values = true;
                        } else {
                            info.has_subquery = true;
                        }
                    }
                }
                
                return info;
            }
            
            return std::nullopt;
            
        } catch (const std::exception& e) {
            return std::nullopt;
        }
    }

    LogicalPlanNodePtr QueryPlanner::build_plan_from_insert(const std::string &query) { //NOLINT:static
        // Use AST-based parsing instead of regex
        auto insert_info = extract_insert_info_from_ast(query);
        if (!insert_info.has_value()) {
            return nullptr;
        }

        auto insert_node = std::make_shared<InsertNode>(insert_info->table_name);
        insert_node->target_columns = insert_info->target_columns;

        return insert_node;
    }

    LogicalPlanNodePtr QueryPlanner::build_plan_from_update(const std::string &query) {
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

    LogicalPlanNodePtr QueryPlanner::build_plan_from_delete(const std::string &query) {
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

    LogicalPlanNodePtr QueryPlanner::build_scan_node(const std::string &table_name, const std::string &alias) { //NOLINT:static
        // TODO: Check if we can use an index scan
        // For now, always use table scan
        auto scan_node = std::make_shared<TableScanNode>(table_name);
        scan_node->alias = alias.empty() ? table_name : alias;
        return scan_node;
    }

    // NOLINTNEXTLINE(b)
    LogicalPlanNodePtr QueryPlanner::build_join_node(LogicalPlanNodePtr left, LogicalPlanNodePtr right,
                                                     JoinType join_type,
                                                     const std::vector<ExpressionPtr> &conditions) const {
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
        if (const auto hash_join = std::dynamic_pointer_cast<HashJoinNode>(join_node)) {
            hash_join->join_conditions = conditions;
        } else if (auto nl_join = std::dynamic_pointer_cast<NestedLoopJoinNode>(join_node)) {
            nl_join->join_conditions = conditions;
        }

        return join_node;
    }

    // Simple projection node - in a real system would use projection list from parse tree
    // NOLINTNEXTLINE: xyz
    LogicalPlanNodePtr QueryPlanner::build_projection_node(const LogicalPlanNodePtr &child,
                                                           const std::vector<ExpressionPtr> &projections) {
        auto proj_node = std::make_shared<ProjectionNode>();
        proj_node->children.push_back(child);
        proj_node->projections = projections;
        return proj_node;
    }

    // Simple selection node - in a real system would use selection list from parse tree
    //NOLINTNEXTLINE: xyz
    LogicalPlanNodePtr QueryPlanner::build_selection_node(const LogicalPlanNodePtr &child,
                                                          const std::vector<ExpressionPtr> &conditions) {
        auto sel_node = std::make_shared<SelectionNode>();
        sel_node->children.push_back(child);
        sel_node->conditions = conditions;
        return sel_node;
    }

    // Simple aggregation node - in a real system would use aggregation list from parse tree
    //NOLINTNEXTLINE: xyz
    LogicalPlanNodePtr QueryPlanner::build_aggregation_node(const LogicalPlanNodePtr &child,
                                                            const std::vector<ExpressionPtr> &group_by,
                                                            const std::vector<ExpressionPtr> &aggregations) {
        auto agg_node = std::make_shared<AggregationNode>();
        agg_node->children.push_back(child);
        agg_node->group_by_exprs = group_by;
        agg_node->aggregate_exprs = aggregations;
        return agg_node;
    }

    // Simple sort node - in a real system would use sort keys from parse tree
    //NOLINTNEXTLINE: xyz
    ExpressionPtr QueryPlanner::parse_expression(const std::string &expr_str) {
        // Simple expression parsing - in a real system would use parse tree
        if (expr_str.find('=') != std::string::npos ||
            expr_str.find('<') != std::string::npos ||
            expr_str.find('>') != std::string::npos) {
            return std::make_shared<Expression>(ExpressionType::BINARY_OP, expr_str);
        }
        if (expr_str.find('.') != std::string::npos) {
            return std::make_shared<Expression>(ExpressionType::COLUMN_REF, expr_str);
        }
        return std::make_shared<Expression>(ExpressionType::COLUMN_REF, expr_str);
    }

    std::vector<ExpressionPtr> QueryPlanner::parse_condition_list(const std::string &where_clause) {
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

    LogicalPlanNodePtr QueryPlanner::optimize_join_order(const std::vector<LogicalPlanNodePtr> &tables,
                                                         const std::vector<ExpressionPtr> &join_conditions) {
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

    // NOLINTNEXTLINE: xyz
    std::vector<ExpressionPtr> QueryPlanner::extract_join_conditions(const std::string &query) const {
        std::vector<ExpressionPtr> conditions;

        const std::regex on_regex(R"(ON\s+(.+?)(?:\s+JOIN|\s+WHERE|\s+GROUP|\s+ORDER|$))",
                                  std::regex_constants::icase);
        std::smatch match;
        auto search_start = query.cbegin();

        while (std::regex_search(search_start, query.cend(), match, on_regex)) {
            std::string condition = match[1];
            conditions.push_back(std::make_shared<Expression>(ExpressionType::BINARY_OP, condition));
            search_start = match.suffix().first;
        }

        return conditions;
    }

    double QueryPlanner::estimate_table_scan_cost(const std::string &table_name) const {
        const auto stats = get_table_stats(table_name);
        const double pages = (stats.row_count * stats.avg_row_size) / 8192.0; // 8KB pages
        return pages * config_.seq_page_cost + stats.row_count * config_.cpu_tuple_cost;
    }

    double QueryPlanner::estimate_index_scan_cost(const std::string &table_name,
                                                  const std::string &index_name,
                                                  const double selectivity) const {
        const auto stats = get_table_stats(table_name);
        const double index_pages = std::log2(stats.row_count) * selectivity; // Simplified B-tree cost
        const double data_pages = (stats.row_count * selectivity * stats.avg_row_size) / 8192.0;

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
    void PlanVisitor::traverse(const LogicalPlanNodePtr &node) { // NOLINT(misc-no-recursion)
        if (!node) return;

        switch (node->type) {
            case PlanNodeType::TABLE_SCAN:
                visit(dynamic_cast<TableScanNode *>(node.get()));
                break;
            case PlanNodeType::INDEX_SCAN:
                visit(dynamic_cast<IndexScanNode *>(node.get()));
                break;
            case PlanNodeType::NESTED_LOOP_JOIN:
                visit(dynamic_cast<NestedLoopJoinNode *>(node.get()));
                break;
            case PlanNodeType::HASH_JOIN:
                visit(dynamic_cast<HashJoinNode *>(node.get()));
                break;
            case PlanNodeType::PROJECTION:
                visit(dynamic_cast<ProjectionNode *>(node.get()));
                break;
            case PlanNodeType::SELECTION:
                visit(dynamic_cast<SelectionNode *>(node.get()));
                break;
            case PlanNodeType::AGGREGATION:
                visit(dynamic_cast<AggregationNode *>(node.get()));
                break;
            case PlanNodeType::SORT:
                visit(dynamic_cast<SortNode *>(node.get()));
                break;
            case PlanNodeType::LIMIT:
                visit(dynamic_cast<LimitNode *>(node.get()));
                break;
            case PlanNodeType::INSERT:
                visit(dynamic_cast<InsertNode *>(node.get()));
                break;
            case PlanNodeType::UPDATE:
                visit(dynamic_cast<UpdateNode *>(node.get()));
                break;
            case PlanNodeType::DELETE:
                visit(dynamic_cast<DeleteNode *>(node.get()));
                break;
            default:
                break;
        }

        for (const auto &child: node->children) {
            traverse(child);
        }
    }

    LogicalPlanNodePtr PlanTransformer::transform(const LogicalPlanNodePtr &node) { // NOLINT(misc-no-recursion)
        if (!node) return nullptr;

        // Transform children first
        for (auto &child: node->children) {
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
