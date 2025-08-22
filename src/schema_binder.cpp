#include "bound_statement.hpp"
#include "schema_registry.hpp"
#include "pg_query_wrapper.hpp"
#include <sstream>
#include <algorithm>

namespace db25 {

    SchemaBinder::SchemaBinder(const std::shared_ptr<DatabaseSchema>& schema) 
        : schema_(schema), registry_(std::make_unique<SchemaRegistry>(schema)) {
        initialize_schema_mappings();
    }

    void SchemaBinder::initialize_schema_mappings() {
        if (!schema_ || !registry_) return;
        
        table_name_to_id_.clear();
        column_name_to_id_.clear();
        
        const auto table_names = schema_->get_table_names();
        for (const auto& table_name : table_names) {
            const auto table_id = registry_->resolve_table(table_name);
            if (table_id) {
                table_name_to_id_[table_name] = *table_id;
                
                const auto column_ids = registry_->get_table_column_ids(*table_id);
                std::unordered_map<std::string, ColumnId> column_map;
                for (const auto& column_id : column_ids) {
                    const auto column_name = registry_->get_column_name(*table_id, column_id);
                    column_map[column_name] = column_id;
                }
                column_name_to_id_[*table_id] = std::move(column_map);
            }
        }
    }

    BoundStatementPtr SchemaBinder::bind(const std::string& query) {
        clear_errors();
        
        try {
            const auto parse_result = pg_query_parse(query.c_str());
            if (parse_result.error) {
                add_error("Parse error: " + std::string(parse_result.error->message));
                pg_query_free_parse_result(parse_result);
                return nullptr;
            }
            
            const auto ast = nlohmann::json::parse(parse_result.parse_tree);
            pg_query_free_parse_result(parse_result);
            return bind_ast(ast);
        } catch (const std::exception& e) {
            add_error("Parse error: " + std::string(e.what()));
            return nullptr;
        }
    }

    BoundStatementPtr SchemaBinder::bind_ast(const nlohmann::json& ast) {
        clear_errors();
        clear_ctes();  // Clear any previous CTE state
        
        if (!ast.contains("stmts") || !ast["stmts"].is_array() || ast["stmts"].empty()) {
            add_error("Invalid AST: no statements found");
            return nullptr;
        }
        
        const auto& stmt = ast["stmts"][0];
        if (!stmt.contains("stmt")) {
            add_error("Invalid AST: statement missing");
            return nullptr;
        }
        
        const auto& stmt_node = stmt["stmt"];
        
        // Check for WITH clause at the statement level first
        if (stmt_node.contains("SelectStmt") && stmt_node["SelectStmt"].contains("withClause")) {
            parse_with_clause(stmt_node["SelectStmt"]["withClause"]);
        } else if (stmt_node.contains("InsertStmt") && stmt_node["InsertStmt"].contains("withClause")) {
            parse_with_clause(stmt_node["InsertStmt"]["withClause"]);
        } else if (stmt_node.contains("UpdateStmt") && stmt_node["UpdateStmt"].contains("withClause")) {
            parse_with_clause(stmt_node["UpdateStmt"]["withClause"]);
        } else if (stmt_node.contains("DeleteStmt") && stmt_node["DeleteStmt"].contains("withClause")) {
            parse_with_clause(stmt_node["DeleteStmt"]["withClause"]);
        }
        
        // Determine statement type and delegate to appropriate binder
        if (stmt_node.contains("SelectStmt")) {
            return bind_select(stmt_node["SelectStmt"]);
        } else if (stmt_node.contains("InsertStmt")) {
            return bind_insert(stmt_node["InsertStmt"]);
        } else if (stmt_node.contains("UpdateStmt")) {
            return bind_update(stmt_node["UpdateStmt"]);
        } else if (stmt_node.contains("DeleteStmt")) {
            return bind_delete(stmt_node["DeleteStmt"]);
        } else {
            add_error("Unsupported statement type");
            return nullptr;
        }
    }

    BoundStatementPtr SchemaBinder::bind_select(const nlohmann::json& select_stmt) {
        auto bound_select = std::make_shared<BoundSelect>();
        
        // Clear previous query context
        current_table_context_.clear();
        
        try {
            // 1. Bind FROM clause first (establishes table context)
            if (select_stmt.contains("fromClause") && select_stmt["fromClause"].is_array() && !select_stmt["fromClause"].empty()) {
                const auto& from_node = select_stmt["fromClause"][0];
                bound_select->from_table = bind_table_ref(from_node);
                if (!bound_select->from_table) {
                    add_error("Failed to bind FROM table");
                    return nullptr;
                }
                
                // Add to table references and current context
                bound_select->table_refs[bound_select->from_table->table_name] = bound_select->from_table;
                current_table_context_[bound_select->from_table->table_name] = bound_select->from_table;
                
                // Also add by alias if different from table name
                if (!bound_select->from_table->alias.empty() && bound_select->from_table->alias != bound_select->from_table->table_name) {
                    bound_select->table_refs[bound_select->from_table->alias] = bound_select->from_table;
                    current_table_context_[bound_select->from_table->alias] = bound_select->from_table;
                }
            }
            
            // 2. Bind JOIN clauses (extends table context)
            if (select_stmt.contains("fromClause") && select_stmt["fromClause"].is_array()) {
                for (size_t i = 1; i < select_stmt["fromClause"].size(); ++i) {
                    const auto& join_node = select_stmt["fromClause"][i];
                    
                    if (join_node.contains("JoinExpr")) {
                        const auto& join_expr = join_node["JoinExpr"];
                        
                        // Bind the right table in the JOIN
                        if (join_expr.contains("rarg")) {
                            auto right_table = bind_table_ref(join_expr["rarg"]);
                            if (!right_table) {
                                add_error("Failed to bind JOIN table");
                                return nullptr;
                            }
                            
                            // Add to bound select and table context
                            bound_select->join_tables.push_back(right_table);
                            bound_select->table_refs[right_table->table_name] = right_table;
                            current_table_context_[right_table->table_name] = right_table;
                            
                            // Also add by alias if different from table name
                            if (!right_table->alias.empty() && right_table->alias != right_table->table_name) {
                                bound_select->table_refs[right_table->alias] = right_table;
                                current_table_context_[right_table->alias] = right_table;
                            }
                            
                            // Bind JOIN condition if present
                            if (join_expr.contains("quals") && !join_expr["quals"].is_null()) {
                                if (auto join_condition = bind_expression(join_expr["quals"])) {
                                    bound_select->join_conditions.push_back(join_condition);
                                }
                            }
                        }
                    }
                }
            }
            
            // 3. Bind SELECT list (with full table context)
            if (select_stmt.contains("targetList") && select_stmt["targetList"].is_array()) {
                for (const auto& target : select_stmt["targetList"]) {
                    if (target.contains("ResTarget")) {
                        const auto& res_target = target["ResTarget"];
                        if (res_target.contains("val")) {
                            // Check for SELECT * special case
                            const auto& val = res_target["val"];
                            if (val.contains("ColumnRef") && val["ColumnRef"].contains("fields") && 
                                val["ColumnRef"]["fields"].is_array() && !val["ColumnRef"]["fields"].empty() &&
                                val["ColumnRef"]["fields"][0].contains("A_Star")) {
                                
                                // Handle SELECT * - expand to all columns from current table context
                                for (const auto& [table_name, table_ref] : current_table_context_) {
                                    for (const auto& column_id : table_ref->available_columns) {
                                        auto col_expr = std::make_shared<BoundExpression>();
                                        col_expr->type = BoundExpressionType::COLUMN_REF;
                                        col_expr->data = column_id;
                                        col_expr->source_table_id = table_ref->table_id;
                                        
                                        // Get column definition - handle CTEs differently
                                        Column column_def;
                                        bool is_cte = false;
                                        for (const auto& cte : current_ctes_) {
                                            if (cte->temp_table_id == table_ref->table_id) {
                                                is_cte = true;
                                                // For CTEs, use the column definition from our BoundTableRef
                                                if (column_id < table_ref->column_definitions.size()) {
                                                    column_def = table_ref->column_definitions[column_id];
                                                }
                                                break;
                                            }
                                        }
                                        
                                        if (!is_cte) {
                                            // Regular table - use schema registry
                                            column_def = registry_->get_column_definition(table_ref->table_id, column_id);
                                        }
                                        
                                        col_expr->result_type = column_def.type;
                                        col_expr->nullable = column_def.nullable;
                                        col_expr->original_text = column_def.name;
                                        
                                        bound_select->select_list.push_back(col_expr);
                                    }
                                }
                            } else {
                                // Normal expression binding
                                auto bound_expr = bind_expression(res_target["val"]);
                                if (bound_expr) {
                                    bound_select->select_list.push_back(bound_expr);
                                } else {
                                    // Expression binding failed - this should cause the entire SELECT to fail
                                    add_error("Failed to bind SELECT expression");
                                    return nullptr;
                                }
                            }
                        }
                    }
                }
            }
            
            // 4. Bind WHERE clause
            if (select_stmt.contains("whereClause")) {
                bound_select->where_clause = bind_expression(select_stmt["whereClause"]);
            }
            
            // 5. Bind GROUP BY, HAVING, ORDER BY
            // TODO: Implement aggregation clause binding
            
            // 6. Bind LIMIT
            if (select_stmt.contains("limitCount")) {
                // TODO: Extract limit value from AST
            }
            
            return bound_select;
            
        } catch (const std::exception& e) {
            // DEBUG: Exception in bind_select: " << e.what() << std::endl;
            add_error("Error binding SELECT statement: " + std::string(e.what()));
            return nullptr;
        }
    }

    BoundStatementPtr SchemaBinder::bind_insert(const nlohmann::json& insert_stmt) {
        auto bound_insert = std::make_shared<BoundInsert>();
        
        try {
            // Bind target table
            if (insert_stmt.contains("relation")) {
                const auto& relation = insert_stmt["relation"];
                bound_insert->target_table = bind_table_ref(relation);
                if (!bound_insert->target_table) {
                    add_error("Failed to bind INSERT target table");
                    return nullptr;
                }
            }
            
            // Bind target columns
            if (insert_stmt.contains("cols") && insert_stmt["cols"].is_array()) {
                for (const auto& col : insert_stmt["cols"]) {
                    if (col.contains("ResTarget")) {
                        const auto& res_target = col["ResTarget"];
                        if (res_target.contains("name")) {
                            const std::string column_name = res_target["name"];
                            const auto column_id = registry_->resolve_column(bound_insert->target_table->table_id, column_name);
                            if (column_id) {
                                bound_insert->target_columns.push_back(*column_id);
                            } else {
                                add_column_not_found_error(column_name, bound_insert->target_table->table_name);
                            }
                        }
                    }
                }
            }
            
            // Bind VALUES or SELECT source
            if (insert_stmt.contains("selectStmt")) {
                // INSERT ... SELECT
                auto select_bound = bind_select(insert_stmt["selectStmt"]);
                if (select_bound) {
                    bound_insert->source = select_bound;
                }
            } else if (insert_stmt.contains("VALUES")) {
                // INSERT ... VALUES
                std::vector<std::vector<BoundExpressionPtr>> values_rows;
                // TODO: Parse VALUES clause
                bound_insert->source = values_rows;
            }
            
            return bound_insert;
            
        } catch (const std::exception& e) {
            add_error("Error binding INSERT statement: " + std::string(e.what()));
            return nullptr;
        }
    }

    BoundStatementPtr SchemaBinder::bind_update(const nlohmann::json& update_stmt) {
        auto bound_update = std::make_shared<BoundUpdate>();
        
        try {
            // Bind target table
            if (update_stmt.contains("relation")) {
                bound_update->target_table = bind_table_ref(update_stmt["relation"]);
                if (!bound_update->target_table) {
                    add_error("Failed to bind UPDATE target table");
                    return nullptr;
                }
            }
            
            // Bind SET clauses
            if (update_stmt.contains("targetList") && update_stmt["targetList"].is_array()) {
                for (const auto& target : update_stmt["targetList"]) {
                    if (target.contains("ResTarget")) {
                        const auto& res_target = target["ResTarget"];
                        if (res_target.contains("name") && res_target.contains("val")) {
                            const std::string column_name = res_target["name"];
                            const auto column_id = registry_->resolve_column(bound_update->target_table->table_id, column_name);
                            if (column_id) {
                                auto value_expr = bind_expression(res_target["val"]);
                                if (value_expr) {
                                    bound_update->assignments.emplace_back(*column_id, value_expr);
                                }
                            } else {
                                add_column_not_found_error(column_name, bound_update->target_table->table_name);
                            }
                        }
                    }
                }
            }
            
            // Bind WHERE clause
            if (update_stmt.contains("whereClause")) {
                bound_update->where_clause = bind_expression(update_stmt["whereClause"]);
            }
            
            return bound_update;
            
        } catch (const std::exception& e) {
            add_error("Error binding UPDATE statement: " + std::string(e.what()));
            return nullptr;
        }
    }

    BoundStatementPtr SchemaBinder::bind_delete(const nlohmann::json& delete_stmt) {
        auto bound_delete = std::make_shared<BoundDelete>();
        
        try {
            // Bind target table
            if (delete_stmt.contains("relation")) {
                bound_delete->target_table = bind_table_ref(delete_stmt["relation"]);
                if (!bound_delete->target_table) {
                    add_error("Failed to bind DELETE target table");
                    return nullptr;
                }
            }
            
            // Bind WHERE clause
            if (delete_stmt.contains("whereClause")) {
                bound_delete->where_clause = bind_expression(delete_stmt["whereClause"]);
            }
            
            return bound_delete;
            
        } catch (const std::exception& e) {
            add_error("Error binding DELETE statement: " + std::string(e.what()));
            return nullptr;
        }
    }

    BoundExpressionPtr SchemaBinder::bind_expression(const nlohmann::json& expr_node) {
        if (expr_node.is_null()) {
            return nullptr;
        }
        
        auto bound_expr = std::make_shared<BoundExpression>();
        
        // Handle different expression types
        if (expr_node.contains("ColumnRef")) {
            return bind_column_ref(expr_node["ColumnRef"]);
        } else if (expr_node.contains("A_Const")) {
            return bind_constant(expr_node["A_Const"]);
        } else if (expr_node.contains("ParamRef")) {
            return bind_parameter(expr_node["ParamRef"]);
        } else if (expr_node.contains("FuncCall")) {
            return bind_function_call(expr_node["FuncCall"]);
        } else if (expr_node.contains("A_Expr")) {
            return bind_binary_op(expr_node["A_Expr"]);
        } else if (expr_node.contains("BoolExpr")) {
            return bind_boolean_expression(expr_node["BoolExpr"]);
        } else if (expr_node.contains("SubLink")) {
            return bind_subquery(expr_node["SubLink"]);
        }
        
        // Fallback: create a generic expression
        bound_expr->type = BoundExpressionType::CONSTANT;
        bound_expr->result_type = ColumnType::TEXT;
        bound_expr->data = std::string("UNKNOWN");
        bound_expr->original_text = "UNKNOWN_EXPRESSION";
        
        return bound_expr;
    }

    BoundExpressionPtr SchemaBinder::bind_column_ref(const nlohmann::json& column_ref) {
        auto bound_expr = std::make_shared<BoundExpression>();
        bound_expr->type = BoundExpressionType::COLUMN_REF;
        
        if (!column_ref.contains("fields") || !column_ref["fields"].is_array()) {
            add_error("Invalid column reference");
            return nullptr;
        }
        
        const auto& fields = column_ref["fields"];
        
        if (fields.size() == 1) {
            // Unqualified column reference - search only in current table context
            const std::string column_name = extract_string_value(fields[0]);
            
            std::vector<std::pair<TableId, ColumnId>> context_resolutions;
            std::vector<std::string> matching_tables;
            
            // Search only tables in the current query context
            for (const auto& [table_name, table_ref] : current_table_context_) {
                const auto column_id = resolve_column(table_ref->table_id, column_name);  // Use CTE-aware resolve_column
                if (column_id) {
                    context_resolutions.emplace_back(table_ref->table_id, *column_id);
                    matching_tables.push_back(table_name);
                }
            }
            
            if (context_resolutions.empty()) {
                add_column_not_found_error(column_name, "");
                return nullptr;
            } else if (context_resolutions.size() > 1) {
                std::ostringstream oss;
                oss << "Ambiguous column reference '" << column_name << "'. Could be: ";
                for (size_t i = 0; i < matching_tables.size(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << matching_tables[i] << "." << column_name;
                }
                add_error(oss.str());
                return nullptr;
            }
            
            const auto& [table_id, column_id] = context_resolutions[0];
            bound_expr->data = column_id;
            bound_expr->source_table_id = table_id;
            
            // Get column definition - handle CTEs differently
            Column column_def;
            bool found_cte = false;
            for (const auto& cte : current_ctes_) {
                if (cte->temp_table_id == table_id) {
                    found_cte = true;
                    if (column_id < cte->column_types.size()) {
                        column_def.name = column_name;
                        column_def.type = cte->column_types[column_id];
                        column_def.nullable = true;  // CTE columns can be NULL
                    }
                    break;
                }
            }
            
            if (!found_cte) {
                column_def = registry_->get_column_definition(table_id, column_id);
            }
            
            bound_expr->result_type = column_def.type;
            bound_expr->nullable = column_def.nullable;
            bound_expr->original_text = column_name;
            
        } else if (fields.size() == 2) {
            // Qualified column reference (table.column)
            const std::string table_name = extract_string_value(fields[0]);
            const std::string column_name = extract_string_value(fields[1]);
            
            const auto table_id = resolve_table(table_name);  // Use CTE-aware resolve_table
            if (!table_id) {
                add_table_not_found_error(table_name);
                return nullptr;
            }
            
            const auto column_id = resolve_column(*table_id, column_name);  // Use CTE-aware resolve_column
            if (!column_id) {
                add_column_not_found_error(column_name, table_name);
                return nullptr;
            }
            
            bound_expr->data = *column_id;
            bound_expr->source_table_id = *table_id;
            
            // Get column definition - handle CTEs differently
            Column column_def;
            bool found_cte = false;
            for (const auto& cte : current_ctes_) {
                if (cte->temp_table_id == *table_id) {
                    found_cte = true;
                    if (*column_id < cte->column_types.size()) {
                        column_def.name = column_name;
                        column_def.type = cte->column_types[*column_id];
                        column_def.nullable = true;  // CTE columns can be NULL
                    }
                    break;
                }
            }
            
            if (!found_cte) {
                column_def = registry_->get_column_definition(*table_id, *column_id);
            }
            
            bound_expr->result_type = column_def.type;
            bound_expr->nullable = column_def.nullable;
            bound_expr->original_text = table_name + "." + column_name;
        } else {
            add_error("Invalid column reference format");
            return nullptr;
        }
        
        return bound_expr;
    }

    BoundExpressionPtr SchemaBinder::bind_constant(const nlohmann::json& const_node) {
        auto bound_expr = std::make_shared<BoundExpression>();
        bound_expr->type = BoundExpressionType::CONSTANT;
        bound_expr->nullable = false; // Constants are never null
        
        if (const_node.contains("val")) {
            const auto& val = const_node["val"];
            if (val.contains("Integer")) {
                bound_expr->result_type = ColumnType::INTEGER;
                const std::string int_str = std::to_string(val["Integer"]["ival"].get<int>());
                bound_expr->data = int_str;
                bound_expr->original_text = int_str;
            } else if (val.contains("String")) {
                bound_expr->result_type = ColumnType::TEXT;
                const std::string str_val = val["String"]["str"].get<std::string>();
                bound_expr->data = str_val;
                bound_expr->original_text = "'" + str_val + "'";
            } else if (val.contains("Float")) {
                bound_expr->result_type = ColumnType::DECIMAL;
                const std::string float_str = val["Float"]["str"].get<std::string>();
                bound_expr->data = float_str;
                bound_expr->original_text = float_str;
            } else {
                bound_expr->result_type = ColumnType::TEXT;
                bound_expr->data = std::string("UNKNOWN_CONSTANT");
                bound_expr->original_text = "UNKNOWN_CONSTANT";
            }
        }
        
        return bound_expr;
    }

    BoundExpressionPtr SchemaBinder::bind_parameter(const nlohmann::json& param_node) {
        auto bound_expr = std::make_shared<BoundExpression>();
        bound_expr->type = BoundExpressionType::PARAMETER;
        bound_expr->result_type = ColumnType::TEXT; // Default, will be inferred later
        bound_expr->nullable = true; // Parameters can be null
        
        if (param_node.contains("number")) {
            const size_t param_index = param_node["number"].get<size_t>();
            
            BoundParameter param;
            param.parameter_index = param_index;
            param.type = ParameterType::UNKNOWN; // Will be inferred
            param.description = "$" + std::to_string(param_index);
            
            bound_expr->data = param;
            bound_expr->original_text = "$" + std::to_string(param_index);
            
            // Add to parameters collection
            // TODO: Implement parameter type inference
        }
        
        return bound_expr;
    }

    BoundExpressionPtr SchemaBinder::bind_function_call(const nlohmann::json& func_node) {
        auto bound_expr = std::make_shared<BoundExpression>();
        bound_expr->type = BoundExpressionType::FUNCTION_CALL;
        
        if (func_node.contains("funcname") && func_node["funcname"].is_array() && !func_node["funcname"].empty()) {
            const std::string func_name = extract_string_value(func_node["funcname"][0]);
            bound_expr->data = func_name;
            bound_expr->original_text = func_name + "(...)";
            
            // Bind function arguments
            if (func_node.contains("args") && func_node["args"].is_array()) {
                for (const auto& arg : func_node["args"]) {
                    auto bound_arg = bind_expression(arg);
                    if (bound_arg) {
                        bound_expr->children.push_back(bound_arg);
                    }
                }
            }
            
            // Infer return type based on function name
            // TODO: Implement proper function signature resolution
            if (func_name == "count" || func_name == "sum") {
                bound_expr->result_type = ColumnType::INTEGER;
            } else if (func_name == "max" || func_name == "min") {
                // Return type depends on argument type
                if (!bound_expr->children.empty()) {
                    bound_expr->result_type = bound_expr->children[0]->result_type;
                } else {
                    bound_expr->result_type = ColumnType::TEXT;
                }
            } else {
                bound_expr->result_type = ColumnType::TEXT; // Default
            }
        }
        
        return bound_expr;
    }

    BoundExpressionPtr SchemaBinder::bind_binary_op(const nlohmann::json& binary_node) {
        auto bound_expr = std::make_shared<BoundExpression>();
        bound_expr->type = BoundExpressionType::BINARY_OP;
        
        if (binary_node.contains("name") && binary_node["name"].is_array() && !binary_node["name"].empty()) {
            const std::string op_name = extract_string_value(binary_node["name"][0]);
            bound_expr->data = op_name;
            
            // Bind operands
            if (binary_node.contains("lexpr")) {
                auto left_expr = bind_expression(binary_node["lexpr"]);
                if (left_expr) {
                    bound_expr->children.push_back(left_expr);
                }
            }
            
            if (binary_node.contains("rexpr")) {
                auto right_expr = bind_expression(binary_node["rexpr"]);
                if (right_expr) {
                    bound_expr->children.push_back(right_expr);
                }
            }
            
            // Infer result type based on operator and operands
            if (bound_expr->children.size() == 2) {
                const auto& left_type = bound_expr->children[0]->result_type;
                const auto& right_type = bound_expr->children[1]->result_type;
                
                if (op_name == "=" || op_name == "<>" || op_name == "<" || op_name == ">" || 
                    op_name == "<=" || op_name == ">=" || op_name == "AND" || op_name == "OR") {
                    bound_expr->result_type = ColumnType::BOOLEAN;
                } else if (op_name == "+" || op_name == "-" || op_name == "*" || op_name == "/") {
                    bound_expr->result_type = registry_->get_common_type(left_type, right_type);
                } else {
                    bound_expr->result_type = ColumnType::TEXT; // Default
                }
            }
            
            bound_expr->original_text = "BINARY_OP(" + op_name + ")";
        }
        
        return bound_expr;
    }

    BoundExpressionPtr SchemaBinder::bind_boolean_expression(const nlohmann::json& bool_node) {
        auto bound_expr = std::make_shared<BoundExpression>();
        bound_expr->type = BoundExpressionType::BINARY_OP;
        bound_expr->result_type = ColumnType::BOOLEAN;
        
        if (bool_node.contains("boolop") && bool_node.contains("args")) {
            const std::string boolop = bool_node["boolop"].get<std::string>();
            const auto& args = bool_node["args"];
            
            // Map PostgreSQL boolean operators to our operators
            std::string op_name;
            if (boolop == "AND_EXPR") {
                op_name = "AND";
            } else if (boolop == "OR_EXPR") {
                op_name = "OR";
            } else if (boolop == "NOT_EXPR") {
                op_name = "NOT";
            } else {
                op_name = boolop; // Fallback
            }
            
            bound_expr->data = op_name;
            
            // Bind the arguments
            if (args.is_array()) {
                for (const auto& arg : args) {
                    auto bound_arg = bind_expression(arg);
                    if (bound_arg) {
                        bound_expr->children.push_back(bound_arg);
                    }
                }
            }
            
            bound_expr->original_text = "BINARY_OP(" + op_name + ")";
        }
        
        return bound_expr;
    }

    BoundExpressionPtr SchemaBinder::bind_subquery(const nlohmann::json& subquery_node) {
        auto bound_expr = std::make_shared<BoundExpression>();
        bound_expr->type = BoundExpressionType::SUBQUERY;
        
        if (subquery_node.contains("subselect")) {
            auto subquery_stmt = bind_select(subquery_node["subselect"]);
            if (subquery_stmt) {
                bound_expr->data = subquery_stmt;
                bound_expr->result_type = ColumnType::TEXT; // TODO: Infer from subquery select list
                bound_expr->original_text = "(SUBQUERY)";
            }
        }
        
        return bound_expr;
    }

    BoundTableRefPtr SchemaBinder::bind_table_ref(const nlohmann::json& table_node) {
        std::string table_name;
        std::string alias;
        
        if (table_node.contains("RangeVar")) {
            const auto& range_var = table_node["RangeVar"];
            if (range_var.contains("relname")) {
                table_name = range_var["relname"].get<std::string>();
            }
            if (range_var.contains("alias") && range_var["alias"].contains("aliasname")) {
                alias = range_var["alias"]["aliasname"].get<std::string>();
            }
        } else if (table_node.contains("relname")) {
            table_name = table_node["relname"].get<std::string>();
        }
        
        if (table_name.empty()) {
            add_error("Empty table name in table reference");
            return nullptr;
        }
        
        const auto table_id = resolve_table(table_name);  // Use our CTE-aware resolve_table method
        if (!table_id) {
            add_table_not_found_error(table_name);
            return nullptr;
        }
        
        auto bound_table = std::make_shared<BoundTableRef>();
        bound_table->table_id = *table_id;
        bound_table->table_name = table_name;
        bound_table->alias = alias.empty() ? table_name : alias;
        
        // Check if this is a CTE table
        bool is_cte_table = false;
        CTEDefinitionPtr cte_def = nullptr;
        for (const auto& cte : current_ctes_) {
            if (cte->temp_table_id == *table_id) {
                is_cte_table = true;
                cte_def = cte;
                break;
            }
        }
        
        if (is_cte_table && cte_def) {
            // Populate CTE column information
            for (size_t i = 0; i < cte_def->column_names.size(); ++i) {
                const auto& column_name = cte_def->column_names[i];
                const ColumnId column_id = static_cast<ColumnId>(i);  // Use index as column ID
                
                bound_table->available_columns.push_back(column_id);
                bound_table->column_name_to_id[column_name] = column_id;
                
                // Create a Column definition for the CTE column
                Column col_def;
                col_def.name = column_name;
                col_def.type = (i < cte_def->column_types.size()) ? cte_def->column_types[i] : ColumnType::TEXT;
                col_def.nullable = true;  // CTE columns can be NULL
                col_def.primary_key = false;
                col_def.unique = false;
                col_def.max_length = 0;  // No limit for CTE columns
                
                bound_table->column_definitions.push_back(col_def);
            }
            
            // CTEs don't have indexes
            bound_table->available_indexes.clear();
        } else {
            // Populate regular table column information from schema registry
            const auto column_ids = registry_->get_table_column_ids(*table_id);
            for (const auto& column_id : column_ids) {
                const auto column_name = registry_->get_column_name(*table_id, column_id);
                const auto& column_def = registry_->get_column_definition(*table_id, column_id);
                
                bound_table->available_columns.push_back(column_id);
                bound_table->column_name_to_id[column_name] = column_id;
                bound_table->column_definitions.push_back(column_def);
            }
            
            // Populate index information
            bound_table->available_indexes = registry_->get_table_indexes(*table_id);
        }
        
        return bound_table;
    }

    std::optional<TableId> SchemaBinder::resolve_table(const std::string& table_name) {
        // DEBUG: resolve_table called for: '" << table_name << "'" << std::endl;
        // DEBUG: CTE registry has " << cte_registry_.size() << " entries" << std::endl;
        
        // First check CTEs (Common Table Expressions)
        auto cte_table_id = resolve_cte_table(table_name);
        // DEBUG: resolve_cte_table returned: " << (cte_table_id ? "FOUND" : "NOT_FOUND") << std::endl;
        if (cte_table_id) {
            // DEBUG: Found CTE table ID: " << *cte_table_id << std::endl;
            return cte_table_id;
        }
        
        // Then check real tables in schema
        auto real_table_id = registry_->resolve_table(table_name);
        // DEBUG: registry resolve_table returned: " << (real_table_id ? "FOUND" : "NOT_FOUND") << std::endl;
        return real_table_id;
    }

    std::optional<ColumnId> SchemaBinder::resolve_column(TableId table_id, const std::string& column_name) {
        // DEBUG: resolve_column called: table_id=" << table_id << ", column='" << column_name << "'" << std::endl;
        
        // Check if this is a CTE table
        for (const auto& cte : current_ctes_) {
            // DEBUG: Checking CTE '" << cte->name << "' with table_id=" << cte->temp_table_id << std::endl;
            if (cte->temp_table_id == table_id) {
                // DEBUG: Found matching CTE table. Columns: ";
                // CTE column names extracted successfully
                
                // Find column index in CTE
                for (size_t i = 0; i < cte->column_names.size(); ++i) {
                    // DEBUG: Comparing '" << cte->column_names[i] << "' with '" << column_name << "'" << std::endl;
                    if (cte->column_names[i] == column_name) {
                        // DEBUG: Found CTE column match at index " << i << std::endl;
                        return static_cast<ColumnId>(i);  // Use index as column ID for CTEs
                    }
                }
                // DEBUG: Column not found in CTE" << std::endl;
                return std::nullopt;  // Column not found in CTE
            }
        }
        
        // DEBUG: Not a CTE table, falling back to registry" << std::endl;
        // Fall back to regular schema registry for real tables
        return registry_->resolve_column(table_id, column_name);
    }

    ColumnType SchemaBinder::infer_expression_type(const BoundExpressionPtr& expr) {
        if (!expr) return ColumnType::TEXT;
        return expr->result_type;
    }

    void SchemaBinder::collect_parameters(const BoundExpressionPtr& expr) {
        if (!expr) return;
        
        if (expr->type == BoundExpressionType::PARAMETER) {
            // TODO: Add to parameters collection
        }
        
        for (const auto& child : expr->children) {
            collect_parameters(child);
        }
    }

    ParameterType SchemaBinder::infer_parameter_type(const BoundExpressionPtr& context_expr, size_t param_index) {
        // TODO: Implement parameter type inference based on context
        return ParameterType::UNKNOWN;
    }

    void SchemaBinder::add_error(const std::string& message, const std::string& location) {
        BindingError error;
        error.message = message;
        error.query_location = location;
        errors_.push_back(error);
    }

    void SchemaBinder::add_table_not_found_error(const std::string& table_name) {
        BindingError error;
        error.message = "Table '" + table_name + "' not found";
        error.suggestions = registry_->suggest_table_names(table_name);
        errors_.push_back(error);
    }

    void SchemaBinder::add_column_not_found_error(const std::string& column_name, const std::string& table_name) {
        BindingError error;
        if (table_name.empty()) {
            error.message = "Column '" + column_name + "' not found";
            // TODO: Add suggestions from all tables
        } else {
            error.message = "Column '" + column_name + "' not found in table '" + table_name + "'";
            const auto table_id = registry_->resolve_table(table_name);
            if (table_id) {
                error.suggestions = registry_->suggest_column_names(column_name, *table_id);
            }
        }
        errors_.push_back(error);
    }

    std::string SchemaBinder::format_type(ColumnType type) const {
        return TypeSystem::type_to_string(type);
    }

    std::string SchemaBinder::extract_string_value(const nlohmann::json& node) {
        if (node.contains("String")) {
            const auto& string_node = node["String"];
            // PostgreSQL AST uses "sval" for string values, not "str"
            if (string_node.contains("sval")) {
                return string_node["sval"].get<std::string>();
            } else if (string_node.contains("str")) {
                return string_node["str"].get<std::string>();
            }
        } else if (node.is_string()) {
            return node.get<std::string>();
        }
        return "";
    }

    // =====================================================================================
    // CTE (Common Table Expression) Implementation
    // =====================================================================================

    void SchemaBinder::parse_with_clause(const nlohmann::json& with_clause) {
        // Parse WITH clause for CTE definitions
        if (!with_clause.contains("ctes") || !with_clause["ctes"].is_array()) {
            // DEBUG: Invalid WITH clause: missing CTE definitions" << std::endl;
            add_error("Invalid WITH clause: missing CTE definitions");
            return;
        }
        
        // DEBUG: Found " << with_clause["ctes"].size() << " CTE definitions" << std::endl;
        
        // Extract all CTE definitions
        auto cte_definitions = extract_cte_definitions(with_clause["ctes"]);
        
        // DEBUG: Extracted " << cte_definitions.size() << " CTE definitions" << std::endl;
        
        // Process each CTE in order (important for dependencies)
        for (const auto& cte : cte_definitions) {
            // DEBUG: Registering CTE: " << (cte ? cte->name : "NULL") << std::endl;
            register_cte(cte);
        }
        
        // DEBUG: CTE registry now has " << cte_registry_.size() << " entries" << std::endl;
    }

    std::vector<CTEDefinitionPtr> SchemaBinder::extract_cte_definitions(const nlohmann::json& ctes_array) {
        std::vector<CTEDefinitionPtr> definitions;
        
        for (const auto& cte_item : ctes_array) {
            if (!cte_item.contains("CommonTableExpr")) {
                add_error("Invalid CTE definition: missing CommonTableExpr");
                continue;
            }
            
            auto cte_def = bind_cte_definition(cte_item["CommonTableExpr"]);
            if (cte_def) {
                definitions.push_back(cte_def);
            }
        }
        
        return definitions;
    }

    CTEDefinitionPtr SchemaBinder::bind_cte_definition(const nlohmann::json& cte_node) {
        auto cte = std::make_shared<CTEDefinition>();
        
        // Extract CTE name
        if (cte_node.contains("ctename") && cte_node["ctename"].is_string()) {
            cte->name = cte_node["ctename"].get<std::string>();
        } else {
            add_error("CTE definition missing name");
            return nullptr;
        }
        
        // Extract explicit column names if specified
        if (cte_node.contains("aliascolnames") && cte_node["aliascolnames"].is_array()) {
            for (const auto& col_item : cte_node["aliascolnames"]) {
                if (col_item.contains("String")) {
                    cte->column_names.push_back(extract_string_value(col_item));
                }
            }
        } else {
            // No explicit column names - we'll infer them from the SELECT statement
            // Extract column names from targetList in the CTE query
            if (cte_node.contains("ctequery") && 
                cte_node["ctequery"].contains("SelectStmt") &&
                cte_node["ctequery"]["SelectStmt"].contains("targetList")) {
                
                auto target_list = cte_node["ctequery"]["SelectStmt"]["targetList"];
                if (target_list.is_array()) {
                    for (const auto& target_entry : target_list) {
                        if (target_entry.contains("ResTarget")) {
                            std::string col_name;
                            
                            // Check if there's an explicit alias
                            if (target_entry["ResTarget"].contains("name") && 
                                target_entry["ResTarget"]["name"].is_string()) {
                                col_name = target_entry["ResTarget"]["name"].get<std::string>();
                            } else {
                                // No alias - use a default name
                                col_name = "column_" + std::to_string(cte->column_names.size() + 1);
                            }
                            
                            cte->column_names.push_back(col_name);
                        }
                    }
                }
            }
        }
        
        // Check for recursive CTE
        // In PostgreSQL AST, recursive flag might be at the WITH level
        // For now, detect by analyzing the query content
        cte->is_recursive = false;  // TODO: Implement proper recursive detection
        
        // Bind the CTE query definition
        if (cte_node.contains("ctequery")) {
            // Save current CTE context to avoid infinite recursion
            auto saved_ctes = cte_registry_;
            
            // Temporarily register this CTE for self-references (recursive)
            cte->temp_table_id = next_temp_table_id_++;
            
            // Bind the CTE's SELECT statement
            if (cte_node["ctequery"].contains("SelectStmt")) {
                cte->definition = bind_select(cte_node["ctequery"]["SelectStmt"]);
            } else {
                add_error("CTE definition must be a SELECT statement", cte->name);
                return nullptr;
            }
            
            // Restore CTE context
            cte_registry_ = saved_ctes;
            
            if (!cte->definition) {
                add_error("Failed to bind CTE definition", cte->name);
                return nullptr;
            }
            
            // Infer CTE schema from the bound SELECT
            infer_cte_schema(cte);
            
            // CTE column names extracted successfully
        } else {
            add_error("CTE definition missing query", cte->name);
            return nullptr;
        }
        
        return cte;
    }

    void SchemaBinder::register_cte(CTEDefinitionPtr cte) {
        if (!cte) return;
        
        // Check for duplicate CTE names
        if (cte_registry_.find(cte->name) != cte_registry_.end()) {
            add_error("Duplicate CTE name: " + cte->name);
            return;
        }
        
        // Assign unique temporary table ID
        cte->temp_table_id = next_temp_table_id_++;
        
        // Register in both collections
        current_ctes_.push_back(cte);
        cte_registry_[cte->name] = cte;
    }

    std::optional<TableId> SchemaBinder::resolve_cte_table(const std::string& table_name) {
        auto it = cte_registry_.find(table_name);
        if (it != cte_registry_.end()) {
            return it->second->temp_table_id;
        }
        return std::nullopt;
    }

    void SchemaBinder::infer_cte_schema(CTEDefinitionPtr cte) {
        if (!cte || !cte->definition) return;
        
        // Cast to BoundSelect to extract schema information
        if (auto bound_select = std::dynamic_pointer_cast<BoundSelect>(cte->definition)) {
            // If explicit column names were provided, use them
            if (!cte->column_names.empty()) {
                // Verify count matches
                if (cte->column_names.size() != bound_select->select_list.size()) {
                    add_error("CTE column count mismatch: specified " + 
                             std::to_string(cte->column_names.size()) + 
                             " but query returns " + std::to_string(bound_select->select_list.size()),
                             cte->name);
                    return;
                }
            } else {
                // Infer column names from SELECT list
                for (const auto& expr : bound_select->select_list) {
                    // Try to extract column name from expression
                    std::string col_name = "column_" + std::to_string(cte->column_names.size() + 1);
                    
                    // TODO: Better column name inference from expressions
                    if (expr->type == BoundExpressionType::COLUMN_REF) {
                        // For COLUMN_REF, the column name would need to be extracted
                        // from the schema registry using the ColumnId in expr->data
                        // This is a simplified implementation
                        col_name = "col_" + std::to_string(cte->column_names.size() + 1);
                    } else if (expr->type == BoundExpressionType::CONSTANT) {
                        // For constants, use a generic name
                        col_name = "const_" + std::to_string(cte->column_names.size() + 1);
                    }
                    
                    cte->column_names.push_back(col_name);
                }
            }
            
            // Infer column types from SELECT list
            for (const auto& expr : bound_select->select_list) {
                ColumnType col_type = infer_expression_type(expr);
                cte->column_types.push_back(col_type);
            }
        }
    }

    void SchemaBinder::clear_ctes() {
        current_ctes_.clear();
        cte_registry_.clear();
        next_temp_table_id_ = 10000;  // Reset temporary table ID counter
    }

} // namespace db25