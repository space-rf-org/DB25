#pragma once

#include <string>
#include <vector>
#include <optional>
#include <pg_query.h>
#include <nlohmann/json.hpp>
#include <set>

namespace db25 {
    struct QueryResult {
        std::string query;
        std::vector<std::string> parse_tree;
        std::vector<std::string> errors;
        bool is_valid;
    };

    struct NormalizedQuery {
        std::string normalized_query;
        std::string fingerprint;
        std::vector<std::string> errors;
        bool is_valid;
    };

    struct EnhancedQueryResult : QueryResult {
        std::vector<std::string> tables;
        std::vector<std::string> columns;
        std::vector<std::string> indexes;
        std::string comment;

        void extract_references(const std::string &query);
    private:
        void extract_tables_from_ast(nlohmann::json::const_reference ast);
        void traverse_ast_for_tables(const nlohmann::json &ast_node, std::set<std::string> &table_set);

        void extract_indexes_from_ast(nlohmann::json::const_reference ast);
        void traverse_ast_for_indexes(const nlohmann::json &ast_node, std::set<std::string> &index_set);

        void extract_columns_from_ast(nlohmann::json::const_reference ast_node);
        void traverse_ast_for_columns(nlohmann::json::const_reference ast_node, std::set<std::string> &column_set);
        void extract_comment_from_ast(nlohmann::json::const_reference ast);
    };

    class QueryParser {
    public:
        QueryParser();

        ~QueryParser();

        QueryResult parse(const std::string &query);

        NormalizedQuery normalize(const std::string &query);

        std::optional<std::string> get_query_fingerprint(const std::string &query);

        bool is_valid_sql(const std::string &query);

    private:
        void cleanup_result(const PgQueryParseResult &result);

        void cleanup_fingerprint_result(const PgQueryFingerprintResult &result);

        void cleanup_normalize_result(const PgQueryNormalizeResult &result);
    };
}
