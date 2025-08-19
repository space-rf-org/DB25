#include "pg_query_wrapper.hpp"
#include <stdexcept>

namespace pg {

QueryParser::QueryParser() {
    // No initialization needed for this version
}

QueryParser::~QueryParser() {
    // No cleanup needed for this version
}

QueryResult QueryParser::parse(const std::string& query) {
    QueryResult result;
    result.query = query;
    result.is_valid = false;

    PgQueryParseResult parse_result = pg_query_parse(query.c_str());
    
    if (parse_result.error) {
        result.errors.push_back(parse_result.error->message);
    } else {
        result.is_valid = true;
        if (parse_result.parse_tree) {
            result.parse_tree.push_back(parse_result.parse_tree);
        }
    }
    
    cleanup_result(parse_result);
    return result;
}

NormalizedQuery QueryParser::normalize(const std::string& query) {
    NormalizedQuery result;
    result.is_valid = false;

    PgQueryNormalizeResult normalize_result = pg_query_normalize(query.c_str());
    
    if (normalize_result.error) {
        result.errors.push_back(normalize_result.error->message);
    } else {
        result.is_valid = true;
        if (normalize_result.normalized_query) {
            result.normalized_query = normalize_result.normalized_query;
        }
    }
    
    cleanup_normalize_result(normalize_result);
    return result;
}

std::optional<std::string> QueryParser::get_query_fingerprint(const std::string& query) {
    PgQueryFingerprintResult fingerprint_result = pg_query_fingerprint(query.c_str());
    
    std::optional<std::string> result;
    if (!fingerprint_result.error && fingerprint_result.fingerprint_str) {
        result = std::string(fingerprint_result.fingerprint_str);
    }
    
    cleanup_fingerprint_result(fingerprint_result);
    return result;
}

bool QueryParser::is_valid_sql(const std::string& query) {
    PgQueryParseResult parse_result = pg_query_parse(query.c_str());
    bool valid = !parse_result.error;
    cleanup_result(parse_result);
    return valid;
}

void QueryParser::cleanup_result(PgQueryParseResult& result) {
    pg_query_free_parse_result(result);
}

void QueryParser::cleanup_fingerprint_result(PgQueryFingerprintResult& result) {
    pg_query_free_fingerprint_result(result);
}

void QueryParser::cleanup_normalize_result(PgQueryNormalizeResult& result) {
    pg_query_free_normalize_result(result);
}

}