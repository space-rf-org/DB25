#pragma once

#include "pg_query_wrapper.hpp"
#include "database.hpp"
#include <string>
#include <vector>
#include <memory>
#include <functional>

namespace db25 {

struct QueryValidationResult {
    bool is_valid;
    std::vector<std::string> errors;
    std::vector<std::string> warnings;
    std::string normalized_query;
    std::string fingerprint;
};

struct QueryAnalysis {
    std::vector<std::string> referenced_tables;
    std::vector<std::string> referenced_columns;
    std::string query_type; // SELECT, INSERT, UPDATE, DELETE, etc.
    bool modifies_data;
    bool requires_transaction;
};

class QueryExecutor {
public:
    explicit QueryExecutor(std::shared_ptr<DatabaseSchema> schema);
    
    QueryValidationResult validate_query(const std::string& query);
    QueryAnalysis analyze_query(const std::string& query);
    
    std::string optimize_query(const std::string& query);
    std::vector<std::string> suggest_indexes(const std::string& query);
    
    [[nodiscard]] bool check_table_exists(const std::string& table_name) const;
    [[nodiscard]] bool check_column_exists(const std::string& table_name, const std::string& column_name) const;
    
    std::string format_query(const std::string& query, bool pretty_print = true);
    
private:
    std::shared_ptr<DatabaseSchema> schema_;
    QueryParser parser_;
    
    std::vector<std::string> extract_table_names(const std::string& query);
    std::vector<std::string> extract_column_names(const std::string& query);
    std::string determine_query_type(const std::string& query);
    
    QueryValidationResult validate_against_schema(const std::string& query, 
                                                  const QueryResult& parse_result);
};

}