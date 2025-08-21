#include "pg_query_wrapper.hpp"
#include <stdexcept>

namespace db25 {

    // FEAT: Enhanced AST-Based Table Extraction #1
    void EnhancedQueryResult::extract_references(const std::string &query) { // NOLINT(readability-convert-member-functions-to-static)
        tables.clear();
        columns.clear();
        indexes.clear();
        comment = "";
        if (query.empty()) {
            throw std::runtime_error("Empty query");
            return;
        }
        const PgQueryParseResult parse_result = pg_query_parse(query.c_str());
        if (parse_result.error || !parse_result.parse_tree) {
            throw std::runtime_error(parse_result.error->message);
        }
        try {
            // Parse JSON AST
            const nlohmann::json ast = nlohmann::json::parse(parse_result.parse_tree);
            // extract tables from ast
            extract_tables_from_ast(ast);
            // extract indexes from ast
            extract_indexes_from_ast(ast);
            // extract columns from ast
            extract_columns_from_ast(ast);
            // extract comment from ast
            // extract_comment_from_ast(ast);
        } catch (const std::exception &e) {
            // TODO: exception handling and logging
        }
        pg_query_free_parse_result(parse_result);
    }

    void EnhancedQueryResult::extract_tables_from_ast(nlohmann::json::const_reference ast) { // NOLINT(readability-convert-member-functions-to-static)
        std::set<std::string> unique_tables;

        // Traverse AST recursively to find all RangeVar nodes
        traverse_ast_for_tables(ast, unique_tables);

        // Convert set to vector
        tables.assign(unique_tables.begin(), unique_tables.end());
    }

    // NOLINTNEXTLINE
    void EnhancedQueryResult::traverse_ast_for_tables(const nlohmann::json &ast_node, std::set<std::string> &table_set) { // NOLINT(readability-convert-member-functions-to-static)
        if (!ast_node.is_object() && !ast_node.is_array()) {
            return;
        }
        if (ast_node.is_array()) {
            for (const auto &child : ast_node) {
                traverse_ast_for_tables(child, table_set);
            }
        }
        if (ast_node.is_object()) {
            // Check for RangeVar (table reference)
            if (ast_node.contains("RangeVar")) {
                if (auto range_var = ast_node["RangeVar"]; range_var.contains("relname")) {
                    std::string table_name = range_var["relname"];

                    // Handle schema-qualified names
                    if (range_var.contains("schemaname")) {
                        std::string schema = range_var["schemaname"];
                        table_name = schema + "." + table_name;
                    }
                    table_set.insert(table_name);
                }
            }
            // Check for JoinExpr (JOIN operations)
            if (ast_node.contains("JoinExpr")) {
                auto join_expr = ast_node["JoinExpr"];
                if (join_expr.contains("larg")) {
                    traverse_ast_for_tables(join_expr["larg"], table_set);
                }
                if (join_expr.contains("rarg")) {
                    traverse_ast_for_tables(join_expr["rarg"], table_set);
                }
            }
            // Check for SubLink (subqueries)
            if (ast_node.contains("SubLink")) {
                if (auto sublink = ast_node["SubLink"]; sublink.contains("subselect")) {
                    traverse_ast_for_tables(sublink["subselect"], table_set);
                }
            }
            // Check for CommonTableExpr (CTEs)
            if (ast_node.contains("CommonTableExpr")) {
                if (auto cte = ast_node["CommonTableExpr"]; cte.contains("ctename")) {
                    table_set.insert(cte["ctename"]);
                }
            }
            // Recursively traverse all object values
            for (auto& [key, value] : ast_node.items()) {
                traverse_ast_for_tables(value, table_set);
            }
        }
    }

    void EnhancedQueryResult::extract_indexes_from_ast(nlohmann::json::const_reference ast) {
        std::set<std::string> unique_indexes;

        // Look for index-related operations
        traverse_ast_for_indexes(ast, unique_indexes);

        indexes.assign(unique_indexes.begin(), unique_indexes.end());
    }

    // NOLINTNEXTLINE
    void EnhancedQueryResult::traverse_ast_for_indexes(const nlohmann::json &ast_node, std::set<std::string> &index_set) {
        if (!ast_node.is_object() && !ast_node.is_array()) {
            return;
        }
        if (ast_node.is_array()) {
            for (const auto &child : ast_node) {
                traverse_ast_for_indexes(child, index_set);
            }
        }
        if (ast_node.is_object()) {
            // Check for IndexStmt (CREATE INDEX)
            if (ast_node.contains("IndexStmt")) {
                if (auto index_stmt = ast_node["IndexStmt"]; index_stmt.contains("idxname")) {
                    index_set.insert(index_stmt["idxname"]);
                }
            }
            // Check for DropStmt for indexes
            if (ast_node.contains("DropStmt")) {
                if (auto drop_stmt = ast_node["DropStmt"]; drop_stmt.contains("removeType") &&
                                                           drop_stmt["removeType"] == "OBJECT_INDEX") {
                    if (drop_stmt.contains("objects")) {
                        for (const auto& obj : drop_stmt["objects"]) {
                            if (obj.is_array() && !obj.empty()) {
                                for (const auto& name_part : obj) {
                                    if (name_part.contains("String")) {
                                        index_set.insert(name_part["String"]["sval"]);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Recursively traverse all object values
            for (auto& [key, value] : ast_node.items()) {
                traverse_ast_for_indexes(value, index_set);
            }
        }
    }

    void EnhancedQueryResult::extract_columns_from_ast(nlohmann::json::const_reference ast_node) {
        std::set<std::string> unique_columns;

        traverse_ast_for_columns(ast_node, unique_columns);

        columns.assign(unique_columns.begin(), unique_columns.end());
    }

    // NOLINTNEXTLINE (misc-no-recursion)
    void EnhancedQueryResult::traverse_ast_for_columns(nlohmann::json::const_reference ast_node, std::set<std::string> &column_set) { // NOLINT(readability-convert-member-functions-to-static)
        if (!ast_node.is_object() && !ast_node.is_array()) {
            return;
        }
        if (ast_node.is_array()) {
            for (const auto &child : ast_node) {
                traverse_ast_for_columns(child, column_set);
            }
        }
        if (ast_node.is_object()) {
            // Check for ColumnRef (column references)
            if (ast_node.contains("ColumnRef")) {
                if (auto col_ref = ast_node["ColumnRef"]; col_ref.contains("fields")) {
                    if (auto fields = col_ref["fields"]; fields.is_array() && !fields.empty()) {
                        // Handle qualified column names (table.column)
                        std::string column_name;
                        for (const auto& field : fields) {
                            if (field.contains("String")) {
                                if (!column_name.empty()) {
                                    column_name += ".";
                                }
                                column_name += field["String"]["sval"];
                            }
                        }
                        if (!column_name.empty() && column_name != "*") {
                            column_set.insert(column_name);
                        }
                    }
                }
            }
            // Check for ResTarget (SELECT target list)
            if (ast_node.contains("ResTarget")) {
                auto res_target = ast_node["ResTarget"];
                if (res_target.contains("name")) {
                    column_set.insert(res_target["name"]);
                }
                if (res_target.contains("val")) {
                    traverse_ast_for_columns(res_target["val"], column_set);
                }
            }
            // Recursively traverse all object values
            for (auto& [key, value] : ast_node.items()) {
                traverse_ast_for_columns(value, column_set);
            }
        }
    }

    QueryParser::QueryParser() = default;

    QueryParser::~QueryParser() = default;

    QueryResult QueryParser::parse(const std::string &query) {
        QueryResult result;
        result.query = query;
        result.is_valid = false;

        PgQueryParseResult parse_result = pg_query_parse(query.c_str());

        if (parse_result.error) {
            result.errors.emplace_back(parse_result.error->message);
        } else {
            result.is_valid = true;
            if (parse_result.parse_tree) {
                result.parse_tree.emplace_back(parse_result.parse_tree);
            }
        }

        cleanup_result(parse_result);
        return result;
    }

    NormalizedQuery QueryParser::normalize(const std::string &query) {
        NormalizedQuery result;
        result.is_valid = false;

        const PgQueryNormalizeResult normalize_result = pg_query_normalize(query.c_str());

        if (normalize_result.error) {
            result.errors.emplace_back(normalize_result.error->message);
        } else {
            result.is_valid = true;
            if (normalize_result.normalized_query) {
                result.normalized_query = normalize_result.normalized_query;
            }
        }

        cleanup_normalize_result(normalize_result);
        return result;
    }

    std::optional<std::string> QueryParser::get_query_fingerprint(const std::string &query) {
        PgQueryFingerprintResult fingerprint_result = pg_query_fingerprint(query.c_str());

        std::optional<std::string> result;
        if (!fingerprint_result.error && fingerprint_result.fingerprint_str) {
            result = std::string(fingerprint_result.fingerprint_str);
        }

        cleanup_fingerprint_result(fingerprint_result);
        return result;
    }

    bool QueryParser::is_valid_sql(const std::string &query) {
        PgQueryParseResult parse_result = pg_query_parse(query.c_str());
        const bool valid = !parse_result.error;
        cleanup_result(parse_result);
        return valid;
    }

    void QueryParser::cleanup_result(const PgQueryParseResult &result) {
        pg_query_free_parse_result(result);
    }

    void QueryParser::cleanup_fingerprint_result(const PgQueryFingerprintResult &result) {
        pg_query_free_fingerprint_result(result);
    }

    void QueryParser::cleanup_normalize_result(const PgQueryNormalizeResult &result) {
        pg_query_free_normalize_result(result);
    }
}
