#include "schema_registry.hpp"
#include <algorithm>
#include <iostream>
#include <iomanip>
#include <cmath>

namespace db25 {

    SchemaRegistry::SchemaRegistry(const std::shared_ptr<DatabaseSchema>& schema) 
        : schema_(schema) {
        if (schema_) {
            initialize_schema_mappings();
        }
    }

    void SchemaRegistry::register_schema(const DatabaseSchema& schema) {
        // Clear existing mappings
        table_name_to_id_.clear();
        table_id_to_name_.clear();
        table_definitions_.clear();
        column_mappings_.clear();
        column_id_to_name_.clear();
        column_definitions_.clear();
        global_column_index_.clear();
        table_indexes_.clear();
        next_table_id_ = 1;
        next_column_id_.clear();

        // Create new schema reference
        schema_ = std::make_shared<DatabaseSchema>(schema);
        initialize_schema_mappings();
    }

    void SchemaRegistry::refresh_mappings() {
        if (schema_) {
            register_schema(*schema_);
        }
    }

    void SchemaRegistry::initialize_schema_mappings() {
        if (!schema_) return;

        const auto table_names = schema_->get_table_names();
        
        for (const auto& table_name : table_names) {
            if (const auto table_opt = schema_->get_table(table_name)) {
                register_table(*table_opt);
            }
        }
        
        build_global_column_index();
    }

    void SchemaRegistry::register_table(const Table& table) {
        const TableId table_id = next_table_id_++;
        
        // Register table
        table_name_to_id_[table.name] = table_id;
        table_id_to_name_[table_id] = table.name;
        table_definitions_[table_id] = table;
        
        // Register columns
        std::unordered_map<std::string, ColumnId> column_map;
        std::unordered_map<ColumnId, std::string> column_id_map;
        std::vector<Column> columns;
        
        ColumnId column_id = 1;
        for (const auto& column : table.columns) {
            column_map[column.name] = column_id;
            column_id_map[column_id] = column.name;
            columns.push_back(column);
            ++column_id;
        }
        
        column_mappings_[table_id] = std::move(column_map);
        column_id_to_name_[table_id] = std::move(column_id_map);
        column_definitions_[table_id] = std::move(columns);
        next_column_id_[table_id] = column_id;
        
        // Register indexes
        table_indexes_[table_id] = table.indexes;
    }

    void SchemaRegistry::build_global_column_index() {
        global_column_index_.clear();
        
        for (const auto& [table_id, column_map] : column_mappings_) {
            for (const auto& [column_name, column_id] : column_map) {
                global_column_index_[column_name].emplace_back(table_id, column_id);
            }
        }
    }

    std::optional<TableId> SchemaRegistry::resolve_table(const std::string& name) const {
        const auto it = table_name_to_id_.find(name);
        return (it != table_name_to_id_.end()) ? std::optional<TableId>(it->second) : std::nullopt;
    }

    const Table& SchemaRegistry::get_table_definition(TableId table_id) const {
        const auto it = table_definitions_.find(table_id);
        if (it == table_definitions_.end()) {
            throw std::runtime_error("Table ID " + std::to_string(table_id) + " not found");
        }
        return it->second;
    }

    std::string SchemaRegistry::get_table_name(TableId table_id) const {
        const auto it = table_id_to_name_.find(table_id);
        if (it == table_id_to_name_.end()) {
            throw std::runtime_error("Table ID " + std::to_string(table_id) + " not found");
        }
        return it->second;
    }

    std::vector<TableId> SchemaRegistry::get_all_table_ids() const {
        std::vector<TableId> ids;
        ids.reserve(table_definitions_.size());
        for (const auto& [table_id, _] : table_definitions_) {
            ids.push_back(table_id);
        }
        return ids;
    }

    std::optional<ColumnId> SchemaRegistry::resolve_column(TableId table_id, const std::string& name) const {
        const auto table_it = column_mappings_.find(table_id);
        if (table_it == column_mappings_.end()) {
            return std::nullopt;
        }
        
        const auto column_it = table_it->second.find(name);
        return (column_it != table_it->second.end()) ? std::optional<ColumnId>(column_it->second) : std::nullopt;
    }

    const Column& SchemaRegistry::get_column_definition(TableId table_id, ColumnId column_id) const {
        const auto table_it = column_definitions_.find(table_id);
        if (table_it == column_definitions_.end()) {
            throw std::runtime_error("Table ID " + std::to_string(table_id) + " not found");
        }
        
        if (column_id == 0 || column_id > table_it->second.size()) {
            throw std::runtime_error("Column ID " + std::to_string(column_id) + " not found in table " + std::to_string(table_id));
        }
        
        return table_it->second[column_id - 1]; // Column IDs start at 1
    }

    std::string SchemaRegistry::get_column_name(TableId table_id, ColumnId column_id) const {
        const auto table_it = column_id_to_name_.find(table_id);
        if (table_it == column_id_to_name_.end()) {
            throw std::runtime_error("Table ID " + std::to_string(table_id) + " not found");
        }
        
        const auto column_it = table_it->second.find(column_id);
        if (column_it == table_it->second.end()) {
            throw std::runtime_error("Column ID " + std::to_string(column_id) + " not found in table " + std::to_string(table_id));
        }
        
        return column_it->second;
    }

    std::vector<ColumnId> SchemaRegistry::get_table_column_ids(TableId table_id) const {
        const auto table_it = column_mappings_.find(table_id);
        if (table_it == column_mappings_.end()) {
            return {};
        }
        
        std::vector<ColumnId> column_ids;
        column_ids.reserve(table_it->second.size());
        for (const auto& [_, column_id] : table_it->second) {
            column_ids.push_back(column_id);
        }
        
        std::sort(column_ids.begin(), column_ids.end());
        return column_ids;
    }

    std::vector<SchemaRegistry::ColumnResolution> SchemaRegistry::resolve_unqualified_column(const std::string& column_name) const {
        std::vector<ColumnResolution> resolutions;
        
        const auto it = global_column_index_.find(column_name);
        if (it == global_column_index_.end()) {
            return resolutions;
        }
        
        resolutions.reserve(it->second.size());
        for (const auto& [table_id, column_id] : it->second) {
            resolutions.push_back({
                table_id,
                column_id,
                get_table_name(table_id),
                column_name
            });
        }
        
        return resolutions;
    }

    bool SchemaRegistry::is_column_ambiguous(const std::string& column_name) const {
        const auto it = global_column_index_.find(column_name);
        return it != global_column_index_.end() && it->second.size() > 1;
    }

    std::vector<Index> SchemaRegistry::get_table_indexes(TableId table_id) const {
        const auto it = table_indexes_.find(table_id);
        return (it != table_indexes_.end()) ? it->second : std::vector<Index>{};
    }

    bool SchemaRegistry::has_index_on_column(TableId table_id, ColumnId column_id) const {
        const auto column_name = get_column_name(table_id, column_id);
        const auto indexes = get_table_indexes(table_id);
        
        return std::any_of(indexes.begin(), indexes.end(), [&column_name](const Index& index) {
            return std::find(index.columns.begin(), index.columns.end(), column_name) != index.columns.end();
        });
    }

    bool SchemaRegistry::table_exists(const std::string& name) const {
        return table_name_to_id_.find(name) != table_name_to_id_.end();
    }

    bool SchemaRegistry::column_exists(TableId table_id, const std::string& column_name) const {
        const auto table_it = column_mappings_.find(table_id);
        if (table_it == column_mappings_.end()) {
            return false;
        }
        return table_it->second.find(column_name) != table_it->second.end();
    }

    bool SchemaRegistry::validate_foreign_key(TableId table_id, ColumnId column_id, 
                                               TableId ref_table_id, ColumnId ref_column_id) const {
        try {
            const auto& column = get_column_definition(table_id, column_id);
            const auto& ref_column = get_column_definition(ref_table_id, ref_column_id);
            
            // Check type compatibility
            if (!are_types_compatible(column.type, ref_column.type)) {
                return false;
            }
            
            // Referenced column should be unique or primary key
            return ref_column.primary_key || ref_column.unique;
        } catch (const std::exception&) {
            return false;
        }
    }

    bool SchemaRegistry::are_types_compatible(ColumnType left, ColumnType right) const {
        if (left == right) return true;
        
        // Numeric type compatibility
        if (TypeSystem::is_numeric_type(left) && TypeSystem::is_numeric_type(right)) {
            return true;
        }
        
        // String type compatibility
        if (TypeSystem::is_string_type(left) && TypeSystem::is_string_type(right)) {
            return true;
        }
        
        return false;
    }

    bool SchemaRegistry::can_cast_implicitly(ColumnType from, ColumnType to) const {
        if (from == to) return true;
        
        // Integer can be cast to BIGINT
        if (from == ColumnType::INTEGER && to == ColumnType::BIGINT) return true;
        
        // VARCHAR can be cast to TEXT
        if (from == ColumnType::VARCHAR && to == ColumnType::TEXT) return true;
        
        // DATE can be cast to TIMESTAMP
        if (from == ColumnType::DATE && to == ColumnType::TIMESTAMP) return true;
        
        return false;
    }

    ColumnType SchemaRegistry::get_common_type(ColumnType left, ColumnType right) const {
        if (left == right) return left;
        
        // Numeric type promotion
        if (TypeSystem::is_numeric_type(left) && TypeSystem::is_numeric_type(right)) {
            if (left == ColumnType::BIGINT || right == ColumnType::BIGINT) {
                return ColumnType::BIGINT;
            }
            if (left == ColumnType::DECIMAL || right == ColumnType::DECIMAL) {
                return ColumnType::DECIMAL;
            }
            return ColumnType::INTEGER;
        }
        
        // String type promotion
        if (TypeSystem::is_string_type(left) && TypeSystem::is_string_type(right)) {
            return ColumnType::TEXT; // TEXT is the most general string type
        }
        
        // Date type promotion
        if (TypeSystem::is_date_type(left) && TypeSystem::is_date_type(right)) {
            return ColumnType::TIMESTAMP; // TIMESTAMP is more general than DATE
        }
        
        // Default to TEXT for mixed types
        return ColumnType::TEXT;
    }

    std::vector<std::string> SchemaRegistry::suggest_table_names(const std::string& input) const {
        std::vector<std::string> all_names;
        all_names.reserve(table_name_to_id_.size());
        
        for (const auto& [name, _] : table_name_to_id_) {
            all_names.push_back(name);
        }
        
        const auto ranked = rank_suggestions(input, all_names);
        
        std::vector<std::string> suggestions;
        suggestions.reserve(std::min(static_cast<size_t>(3), ranked.size()));
        
        for (size_t i = 0; i < std::min(static_cast<size_t>(3), ranked.size()); ++i) {
            if (ranked[i].second > 0.3) { // Only suggest if similarity > 30%
                suggestions.push_back(ranked[i].first);
            }
        }
        
        return suggestions;
    }

    std::vector<std::string> SchemaRegistry::suggest_column_names(const std::string& input, TableId table_id) const {
        const auto table_it = column_mappings_.find(table_id);
        if (table_it == column_mappings_.end()) {
            return {};
        }
        
        std::vector<std::string> all_names;
        all_names.reserve(table_it->second.size());
        
        for (const auto& [name, _] : table_it->second) {
            all_names.push_back(name);
        }
        
        const auto ranked = rank_suggestions(input, all_names);
        
        std::vector<std::string> suggestions;
        suggestions.reserve(std::min(static_cast<size_t>(3), ranked.size()));
        
        for (size_t i = 0; i < std::min(static_cast<size_t>(3), ranked.size()); ++i) {
            if (ranked[i].second > 0.3) { // Only suggest if similarity > 30%
                suggestions.push_back(ranked[i].first);
            }
        }
        
        return suggestions;
    }

    size_t SchemaRegistry::get_total_column_count() const {
        size_t total = 0;
        for (const auto& [_, column_map] : column_mappings_) {
            total += column_map.size();
        }
        return total;
    }

    void SchemaRegistry::dump_mappings() const {
        std::cout << "=== Schema Registry Mappings ===" << std::endl;
        std::cout << "Tables: " << table_name_to_id_.size() << std::endl;
        std::cout << "Total Columns: " << get_total_column_count() << std::endl;
        std::cout << std::endl;
        
        for (const auto& [table_name, table_id] : table_name_to_id_) {
            std::cout << "Table: " << table_name << " (ID: " << table_id << ")" << std::endl;
            
            const auto table_it = column_mappings_.find(table_id);
            if (table_it != column_mappings_.end()) {
                for (const auto& [column_name, column_id] : table_it->second) {
                    const auto& column_def = get_column_definition(table_id, column_id);
                    std::cout << "  - " << column_name << " (ID: " << column_id 
                              << ", Type: " << TypeSystem::type_to_string(column_def.type) << ")" << std::endl;
                }
            }
            std::cout << std::endl;
        }
    }

    double SchemaRegistry::calculate_string_similarity(const std::string& a, const std::string& b) const {
        if (a.empty() && b.empty()) return 1.0;
        if (a.empty() || b.empty()) return 0.0;
        
        // Simple Levenshtein distance-based similarity
        const size_t len_a = a.length();
        const size_t len_b = b.length();
        
        std::vector<std::vector<size_t>> dp(len_a + 1, std::vector<size_t>(len_b + 1));
        
        for (size_t i = 0; i <= len_a; ++i) dp[i][0] = i;
        for (size_t j = 0; j <= len_b; ++j) dp[0][j] = j;
        
        for (size_t i = 1; i <= len_a; ++i) {
            for (size_t j = 1; j <= len_b; ++j) {
                const size_t cost = (std::tolower(a[i-1]) == std::tolower(b[j-1])) ? 0 : 1;
                dp[i][j] = std::min({
                    dp[i-1][j] + 1,      // deletion
                    dp[i][j-1] + 1,      // insertion
                    dp[i-1][j-1] + cost  // substitution
                });
            }
        }
        
        const size_t max_len = std::max(len_a, len_b);
        return 1.0 - static_cast<double>(dp[len_a][len_b]) / max_len;
    }

    std::vector<std::pair<std::string, double>> SchemaRegistry::rank_suggestions(
        const std::string& input, const std::vector<std::string>& candidates) const {
        
        std::vector<std::pair<std::string, double>> scored;
        scored.reserve(candidates.size());
        
        for (const auto& candidate : candidates) {
            const double similarity = calculate_string_similarity(input, candidate);
            scored.emplace_back(candidate, similarity);
        }
        
        std::sort(scored.begin(), scored.end(), 
                  [](const auto& a, const auto& b) { return a.second > b.second; });
        
        return scored;
    }

    // TypeSystem implementation
    bool TypeSystem::is_numeric_type(ColumnType type) {
        return type == ColumnType::INTEGER || 
               type == ColumnType::BIGINT || 
               type == ColumnType::DECIMAL;
    }

    bool TypeSystem::is_string_type(ColumnType type) {
        return type == ColumnType::VARCHAR || 
               type == ColumnType::TEXT;
    }

    bool TypeSystem::is_date_type(ColumnType type) {
        return type == ColumnType::DATE || 
               type == ColumnType::TIMESTAMP;
    }

    bool TypeSystem::is_compatible_for_comparison(ColumnType left, ColumnType right) {
        if (left == right) return true;
        if (is_numeric_type(left) && is_numeric_type(right)) return true;
        if (is_string_type(left) && is_string_type(right)) return true;
        if (is_date_type(left) && is_date_type(right)) return true;
        return false;
    }

    bool TypeSystem::is_compatible_for_arithmetic(ColumnType left, ColumnType right) {
        return is_numeric_type(left) && is_numeric_type(right);
    }

    ColumnType TypeSystem::resolve_arithmetic_result_type(ColumnType left, ColumnType right) {
        if (!is_compatible_for_arithmetic(left, right)) {
            throw std::runtime_error("Types not compatible for arithmetic");
        }
        
        if (left == ColumnType::DECIMAL || right == ColumnType::DECIMAL) {
            return ColumnType::DECIMAL;
        }
        if (left == ColumnType::BIGINT || right == ColumnType::BIGINT) {
            return ColumnType::BIGINT;
        }
        return ColumnType::INTEGER;
    }

    std::string TypeSystem::type_to_string(ColumnType type) {
        switch (type) {
            case ColumnType::INTEGER: return "INTEGER";
            case ColumnType::BIGINT: return "BIGINT";
            case ColumnType::VARCHAR: return "VARCHAR";
            case ColumnType::TEXT: return "TEXT";
            case ColumnType::BOOLEAN: return "BOOLEAN";
            case ColumnType::TIMESTAMP: return "TIMESTAMP";
            case ColumnType::DATE: return "DATE";
            case ColumnType::DECIMAL: return "DECIMAL";
            case ColumnType::JSON: return "JSON";
            case ColumnType::UUID: return "UUID";
            default: return "UNKNOWN";
        }
    }

    std::optional<ColumnType> TypeSystem::string_to_type(const std::string& type_str) {
        const std::string upper_str = [&type_str]() {
            std::string result = type_str;
            std::transform(result.begin(), result.end(), result.begin(), ::toupper);
            return result;
        }();
        
        if (upper_str == "INTEGER" || upper_str == "INT") return ColumnType::INTEGER;
        if (upper_str == "BIGINT") return ColumnType::BIGINT;
        if (upper_str == "VARCHAR") return ColumnType::VARCHAR;
        if (upper_str == "TEXT") return ColumnType::TEXT;
        if (upper_str == "BOOLEAN" || upper_str == "BOOL") return ColumnType::BOOLEAN;
        if (upper_str == "TIMESTAMP") return ColumnType::TIMESTAMP;
        if (upper_str == "DATE") return ColumnType::DATE;
        if (upper_str == "DECIMAL" || upper_str == "NUMERIC") return ColumnType::DECIMAL;
        if (upper_str == "JSON") return ColumnType::JSON;
        if (upper_str == "UUID") return ColumnType::UUID;
        
        return std::nullopt;
    }

} // namespace db25