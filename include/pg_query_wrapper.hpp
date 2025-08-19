#pragma once

#include <string>
#include <vector>
#include <optional>
#include <pg_query.h>

namespace pg {

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

class QueryParser {
public:
    QueryParser();
    ~QueryParser();

    QueryResult parse(const std::string& query);
    NormalizedQuery normalize(const std::string& query);
    std::optional<std::string> get_query_fingerprint(const std::string& query);
    bool is_valid_sql(const std::string& query);

private:
    void cleanup_result(PgQueryParseResult& result);
    void cleanup_fingerprint_result(PgQueryFingerprintResult& result);
    void cleanup_normalize_result(PgQueryNormalizeResult& result);
};

}