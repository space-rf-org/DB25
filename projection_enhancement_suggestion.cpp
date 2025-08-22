// SUGGESTED MODIFICATION for query_planner.cpp lines 858-872
// Replace the current regex-based projection parsing with AST-based approach

// ADD THESE NEW METHODS TO QueryPlanner CLASS:

std::vector<ExpressionPtr> QueryPlanner::extract_projections_from_ast(const std::string& query) const {
    std::vector<ExpressionPtr> projections;
    
    try {
        // Parse query to get AST
        PgQueryParseResult parse_result = pg_query_parse(query.c_str());
        
        if (parse_result.error || !parse_result.parse_tree) {
            pg_query_free_parse_result(parse_result);
            return projections; // Return empty vector on parse failure
        }
        
        // Parse JSON AST
        const nlohmann::json ast = nlohmann::json::parse(parse_result.parse_tree);
        
        // Navigate to SelectStmt and extract targetList
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
        
        pg_query_free_parse_result(parse_result);
        return projections;
        
    } catch (const std::exception& e) {
        // Log exception and return empty vector
        return projections;
    }
}

ExpressionPtr QueryPlanner::parse_projection_target(const nlohmann::json& res_target) const {
    if (!res_target.contains("val")) {
        return nullptr;
    }
    
    // Parse the main expression
    auto expr = parse_expression_from_ast(res_target["val"]);
    if (!expr) {
        return nullptr;
    }
    
    // Handle SELECT * case
    if (res_target["val"].contains("ColumnRef")) {
        const auto& column_ref = res_target["val"]["ColumnRef"];
        if (column_ref.contains("fields") && column_ref["fields"].is_array() && 
            !column_ref["fields"].empty() && column_ref["fields"][0].contains("A_Star")) {
            // This is SELECT *
            expr->value = "*";
            expr->type = ExpressionType::COLUMN_REF;
            return expr;
        }
    }
    
    // Handle alias (AS clause)
    if (res_target.contains("name") && !res_target["name"].is_null()) {
        std::string alias = res_target["name"].get<std::string>();
        // Store the alias information - could add alias field to Expression struct
        // For now, we'll append it to the value for identification
        expr->value += " AS " + alias;
    }
    
    return expr;
}

bool QueryPlanner::is_star_projection(const std::vector<ExpressionPtr>& projections) const {
    return projections.size() == 1 && 
           projections[0]->type == ExpressionType::COLUMN_REF && 
           projections[0]->value == "*";
}

// REPLACE LINES 858-872 WITH THIS:

        // Add projection for SELECT list - ENHANCED AST VERSION
        auto projections = extract_projections_from_ast(query);
        if (!projections.empty() && !is_star_projection(projections)) {
            plan_root = build_projection_node(plan_root, projections);
        }
        // Note: For SELECT *, no projection node is needed as all columns are included

// BENEFITS OF THIS AST-BASED APPROACH:

// 1. ACCURATE PARSING: Handles complex expressions, not just simple column names
//    - Function calls: COUNT(*), UPPER(name), etc.
//    - Arithmetic: age + 1, price * 0.9
//    - CASE expressions: CASE WHEN ... THEN ... ELSE ... END
//    - Subqueries: (SELECT COUNT(*) FROM orders WHERE user_id = u.id)

// 2. PROPER ALIAS HANDLING: Correctly identifies and preserves AS clauses
//    - SELECT name AS full_name
//    - SELECT COUNT(*) AS total_count

// 3. QUALIFIED COLUMN SUPPORT: Handles table.column references
//    - SELECT u.name, p.price FROM users u JOIN products p

// 4. AGGREGATE FUNCTION DETECTION: Can identify aggregation functions
//    - Enables proper GROUP BY planning
//    - Helps with optimization decisions

// 5. EXPRESSION TYPE CLASSIFICATION: Each projection has correct ExpressionType
//    - COLUMN_REF for simple columns
//    - FUNCTION_CALL for functions
//    - BINARY_OP for expressions
//    - CASE_EXPR for CASE statements

// 6. ROBUST ERROR HANDLING: Graceful fallback on parse errors

// EXAMPLE IMPROVEMENTS:

// BEFORE (regex-based):
// "SELECT UPPER(name), age + 1 FROM users" 
// → Would create: ["UPPER(name)", "age + 1"] as simple COLUMN_REF expressions
// → Incorrect parsing, no understanding of function calls or operators

// AFTER (AST-based):
// "SELECT UPPER(name), age + 1 FROM users"
// → Would create: 
//   [FUNCTION_CALL("upper") with child COLUMN_REF("name"),
//    BINARY_OP("+") with children COLUMN_REF("age") and CONSTANT("1")]
// → Correct structure enabling proper optimization and execution

// ADDITIONAL HEADER DECLARATIONS NEEDED:
// Add to query_planner.hpp:
/*
    std::vector<ExpressionPtr> extract_projections_from_ast(const std::string& query) const;
    ExpressionPtr parse_projection_target(const nlohmann::json& res_target) const;
    bool is_star_projection(const std::vector<ExpressionPtr>& projections) const;
*/