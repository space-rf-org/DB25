#pragma once

#include "database.hpp"
#include <unordered_map>
#include <optional>
#include <memory>
#include <vector>
#include <string>

namespace db25 {

    // Schema object IDs for fast lookups
    using TableId = size_t;
    using ColumnId = size_t;

    // Schema registry provides ID-based access to database schema
    class SchemaRegistry {
    public:
        explicit SchemaRegistry(const std::shared_ptr<DatabaseSchema>& schema);

        // Schema registration
        void register_schema(const DatabaseSchema& schema);
        void refresh_mappings();

        // Table resolution
        [[nodiscard]] std::optional<TableId> resolve_table(const std::string& name) const;
        [[nodiscard]] const Table& get_table_definition(TableId table_id) const;
        [[nodiscard]] std::string get_table_name(TableId table_id) const;
        [[nodiscard]] std::vector<TableId> get_all_table_ids() const;

        // Column resolution  
        [[nodiscard]] std::optional<ColumnId> resolve_column(TableId table_id, const std::string& name) const;
        [[nodiscard]] const Column& get_column_definition(TableId table_id, ColumnId column_id) const;
        [[nodiscard]] std::string get_column_name(TableId table_id, ColumnId column_id) const;
        [[nodiscard]] std::vector<ColumnId> get_table_column_ids(TableId table_id) const;
        
        // Ambiguous column resolution (for unqualified column references)
        struct ColumnResolution {
            TableId table_id;
            ColumnId column_id;
            std::string table_name;  // For error messages
            std::string column_name; // For error messages
        };
        
        [[nodiscard]] std::vector<ColumnResolution> resolve_unqualified_column(const std::string& column_name) const;
        [[nodiscard]] bool is_column_ambiguous(const std::string& column_name) const;

        // Index information
        [[nodiscard]] std::vector<Index> get_table_indexes(TableId table_id) const;
        [[nodiscard]] bool has_index_on_column(TableId table_id, ColumnId column_id) const;

        // Schema validation
        [[nodiscard]] bool table_exists(const std::string& name) const;
        [[nodiscard]] bool column_exists(TableId table_id, const std::string& column_name) const;
        [[nodiscard]] bool validate_foreign_key(TableId table_id, ColumnId column_id, 
                                               TableId ref_table_id, ColumnId ref_column_id) const;

        // Type compatibility
        [[nodiscard]] bool are_types_compatible(ColumnType left, ColumnType right) const;
        [[nodiscard]] bool can_cast_implicitly(ColumnType from, ColumnType to) const;
        [[nodiscard]] ColumnType get_common_type(ColumnType left, ColumnType right) const;

        // Error suggestions  
        [[nodiscard]] std::vector<std::string> suggest_table_names(const std::string& input) const;
        [[nodiscard]] std::vector<std::string> suggest_column_names(const std::string& input, TableId table_id) const;
        
        // Debug and introspection
        [[nodiscard]] size_t get_table_count() const { return table_name_to_id_.size(); }
        [[nodiscard]] size_t get_total_column_count() const;
        void dump_mappings() const; // For debugging

    private:
        std::shared_ptr<DatabaseSchema> schema_;
        
        // Core mapping tables
        std::unordered_map<std::string, TableId> table_name_to_id_;
        std::unordered_map<TableId, std::string> table_id_to_name_;
        std::unordered_map<TableId, Table> table_definitions_;
        
        // Column mappings per table
        std::unordered_map<TableId, std::unordered_map<std::string, ColumnId>> column_mappings_;
        std::unordered_map<TableId, std::unordered_map<ColumnId, std::string>> column_id_to_name_;
        std::unordered_map<TableId, std::vector<Column>> column_definitions_;
        
        // Global column lookup (for ambiguity detection)
        std::unordered_map<std::string, std::vector<std::pair<TableId, ColumnId>>> global_column_index_;
        
        // Index mappings
        std::unordered_map<TableId, std::vector<Index>> table_indexes_;
        
        // ID generators
        TableId next_table_id_ = 1;
        std::unordered_map<TableId, ColumnId> next_column_id_; // Per table
        
        // Helper methods
        void initialize_schema_mappings();
        void build_global_column_index();
        void register_table(const Table& table);
        [[nodiscard]] double calculate_string_similarity(const std::string& a, const std::string& b) const;
        [[nodiscard]] std::vector<std::pair<std::string, double>> rank_suggestions(
            const std::string& input, const std::vector<std::string>& candidates) const;
    };

    // Type system utilities
    class TypeSystem {
    public:
        static bool is_numeric_type(ColumnType type);
        static bool is_string_type(ColumnType type);
        static bool is_date_type(ColumnType type);
        static bool is_compatible_for_comparison(ColumnType left, ColumnType right);
        static bool is_compatible_for_arithmetic(ColumnType left, ColumnType right);
        static ColumnType resolve_arithmetic_result_type(ColumnType left, ColumnType right);
        static std::string type_to_string(ColumnType type);
        static std::optional<ColumnType> string_to_type(const std::string& type_str);
    };

} // namespace db25