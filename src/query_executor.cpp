#include "query_executor.hpp"
#include <algorithm>
#include <regex>
#include <sstream>
#include <utility>

namespace db25 {

QueryExecutor::QueryExecutor(std::shared_ptr<DatabaseSchema> schema) 
    : schema_(std::move(schema)) {}

QueryValidationResult QueryExecutor::validate_query(const std::string& query) {
    QueryValidationResult result;
    result.is_valid = false;
    
    auto parse_result = parser_.parse(query);
    if (!parse_result.is_valid) {
        result.errors = parse_result.errors;
        return result;
    }
    
    auto normalized = parser_.normalize(query);
    if (normalized.is_valid) {
        result.normalized_query = normalized.normalized_query;
    }

    if (auto fingerprint = parser_.get_query_fingerprint(query)) {
        result.fingerprint = *fingerprint;
    }
    
    auto schema_validation = validate_against_schema(query, parse_result);
    result.errors.insert(result.errors.end(), 
                        schema_validation.errors.begin(), 
                        schema_validation.errors.end());
    result.warnings.insert(result.warnings.end(), 
                          schema_validation.warnings.begin(), 
                          schema_validation.warnings.end());
    
    result.is_valid = result.errors.empty();
    return result;
}

QueryAnalysis QueryExecutor::analyze_query(const std::string& query) {
    QueryAnalysis analysis;
    
    analysis.referenced_tables = extract_table_names(query);
    analysis.referenced_columns = extract_column_names(query);
    analysis.query_type = determine_query_type(query);
    
    std::string lower_query = query;
    std::transform(lower_query.begin(), lower_query.end(), lower_query.begin(), ::tolower);
    
    analysis.modifies_data = (lower_query.find("insert") != std::string::npos ||
                             lower_query.find("update") != std::string::npos ||
                             lower_query.find("delete") != std::string::npos ||
                             lower_query.find("create") != std::string::npos ||
                             lower_query.find("drop") != std::string::npos ||
                             lower_query.find("alter") != std::string::npos);
    
    analysis.requires_transaction = analysis.modifies_data && 
                                   analysis.referenced_tables.size() > 1;
    
    return analysis;
}

std::string QueryExecutor::optimize_query(const std::string& query) {
    auto normalized = parser_.normalize(query);
    if (normalized.is_valid && !normalized.normalized_query.empty()) {
        return normalized.normalized_query;
    }
    return query;
}

std::vector<std::string> QueryExecutor::suggest_indexes(const std::string& query) {
    std::vector<std::string> suggestions;
    
    auto analysis = analyze_query(query);
    
    if (analysis.query_type == "SELECT") {
        std::regex where_pattern(R"(WHERE\s+(\w+\.\w+|\w+)\s*[=<>])");
        std::smatch matches;
        std::string search_query = query;
        
        while (std::regex_search(search_query, matches, where_pattern)) {
            std::string column = matches[1];
            suggestions.push_back("CREATE INDEX ON table_name (" + column + ");");
            search_query = matches.suffix();
        }
        
        std::regex join_pattern(R"(JOIN\s+(\w+)\s+ON\s+(\w+\.\w+|\w+)\s*=\s*(\w+\.\w+|\w+))");
        search_query = query;
        while (std::regex_search(search_query, matches, join_pattern)) {
            std::string join_column = matches[2];
            suggestions.push_back("CREATE INDEX ON table_name (" + join_column + ");");
            search_query = matches.suffix();
        }
    }
    
    return suggestions;
}

bool QueryExecutor::check_table_exists(const std::string& table_name) const {
    auto table_names = schema_->get_table_names();
    return std::find(table_names.begin(), table_names.end(), table_name) != table_names.end();
}

bool QueryExecutor::check_column_exists(const std::string& table_name, 
                                       const std::string& column_name) const {
    auto table = schema_->get_table(table_name);
    if (!table) return false;
    
    for (const auto& column : table->columns) {
        if (column.name == column_name) return true;
    }
    return false;
}

std::string QueryExecutor::format_query(const std::string& query, bool pretty_print) {
    if (!pretty_print) return query;
    
    std::string formatted = query;
    std::regex keyword_pattern(R"(\b(SELECT|FROM|WHERE|JOIN|INNER|LEFT|RIGHT|FULL|ON|GROUP|BY|HAVING|ORDER|LIMIT|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|TABLE|DROP|ALTER|INDEX)\b)", 
                              std::regex_constants::icase);
    
    formatted = std::regex_replace(formatted, keyword_pattern, "\n$1");
    
    std::istringstream iss(formatted);
    std::ostringstream oss;
    std::string line;
    int indent = 0;
    
    while (std::getline(iss, line)) {
        if (!line.empty() && line[0] != '\n') {
            for (int i = 0; i < indent; ++i) {
                oss << "  ";
            }
            oss << line << "\n";
        }
    }
    
    return oss.str();
}

std::vector<std::string> QueryExecutor::extract_table_names(const std::string& query) {
    std::vector<std::string> table_names;
    
    std::regex from_pattern(R"(FROM\s+(\w+))", std::regex_constants::icase);
    std::regex join_pattern(R"(JOIN\s+(\w+))", std::regex_constants::icase);
    std::regex into_pattern(R"(INTO\s+(\w+))", std::regex_constants::icase);
    std::regex update_pattern(R"(UPDATE\s+(\w+))", std::regex_constants::icase);
    
    std::smatch matches;
    std::string search_query = query;
    
    std::vector<std::regex> patterns = {from_pattern, join_pattern, into_pattern, update_pattern};
    
    for (const auto& pattern : patterns) {
        search_query = query;
        while (std::regex_search(search_query, matches, pattern)) {
            table_names.push_back(matches[1]);
            search_query = matches.suffix();
        }
    }
    
    std::sort(table_names.begin(), table_names.end());
    table_names.erase(std::unique(table_names.begin(), table_names.end()), table_names.end());
    
    return table_names;
}

std::vector<std::string> QueryExecutor::extract_column_names(const std::string& query) {
    std::vector<std::string> column_names;
    
    std::regex select_pattern(R"(SELECT\s+(.*?)\s+FROM)", std::regex_constants::icase);
    std::regex where_pattern(R"(WHERE\s+(.*?)(?:\s+GROUP|\s+ORDER|\s+HAVING|$))", std::regex_constants::icase);
    
    std::smatch matches;
    
    if (std::regex_search(query, matches, select_pattern)) {
        std::string select_clause = matches[1];
        if (select_clause != "*") {
            std::regex column_pattern(R"((\w+(?:\.\w+)?))");
            std::sregex_iterator iter(select_clause.begin(), select_clause.end(), column_pattern);
            std::sregex_iterator end;
            
            for (; iter != end; ++iter) {
                column_names.push_back(iter->str());
            }
        }
    }
    
    return column_names;
}

std::string QueryExecutor::determine_query_type(const std::string& query) {
    std::string upper_query = query;
    std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);
    
    if (upper_query.find("SELECT") == 0) return "SELECT";
    if (upper_query.find("INSERT") == 0) return "INSERT";
    if (upper_query.find("UPDATE") == 0) return "UPDATE";
    if (upper_query.find("DELETE") == 0) return "DELETE";
    if (upper_query.find("CREATE") == 0) return "CREATE";
    if (upper_query.find("DROP") == 0) return "DROP";
    if (upper_query.find("ALTER") == 0) return "ALTER";
    
    return "UNKNOWN";
}

QueryValidationResult QueryExecutor::validate_against_schema(const std::string& query, 
                                                           const QueryResult& parse_result) {
    QueryValidationResult result;
    result.is_valid = true;
    
    auto table_names = extract_table_names(query);
    for (const auto& table_name : table_names) {
        if (!check_table_exists(table_name)) {
            result.errors.push_back("Table '" + table_name + "' does not exist in schema");
            result.is_valid = false;
        }
    }
    
    auto column_names = extract_column_names(query);
    for (const auto& column_name : column_names) {
        bool found = false;
        for (const auto& table_name : table_names) {
            if (check_column_exists(table_name, column_name)) {
                found = true;
                break;
            }
        }
        if (!found && column_name != "*") {
            result.warnings.push_back("Column '" + column_name + "' may not exist in referenced tables");
        }
    }
    
    return result;
}

}