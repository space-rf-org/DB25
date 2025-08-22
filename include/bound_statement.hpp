#pragma once

#include "database.hpp"
#include "logical_plan.hpp"
#include "schema_registry.hpp"  // Include full SchemaRegistry definition
#include <nlohmann/json.hpp>
#include <memory>
#include <unordered_map>
#include <variant>
#include <optional>

namespace db25 {

    // Forward declarations
    struct BoundStatement;
    struct BoundExpression;
    struct BoundTableRef;
    struct CTEDefinition;
    
    using BoundStatementPtr = std::shared_ptr<BoundStatement>;
    using BoundExpressionPtr = std::shared_ptr<BoundExpression>;
    using BoundTableRefPtr = std::shared_ptr<BoundTableRef>;
    using CTEDefinitionPtr = std::shared_ptr<CTEDefinition>;

    // Schema object IDs (instead of string names)
    using TableId = size_t;
    using ColumnId = size_t;
    using IndexId = size_t;

    // Parameter types for bound statements
    enum class ParameterType {
        INTEGER,
        BIGINT, 
        VARCHAR,
        TEXT,
        BOOLEAN,
        TIMESTAMP,
        DATE,
        DECIMAL,
        JSON,
        UUID,
        UNKNOWN  // For inference during binding
    };

    // Bound parameter information
    struct BoundParameter {
        size_t parameter_index;     // $1, $2, etc.
        ParameterType type;         // Inferred or explicitly provided
        bool nullable = true;       // Can this parameter be NULL?
        std::optional<size_t> max_length;  // For VARCHAR types
        std::string description;    // For debugging/error messages
    };

    // CTE (Common Table Expression) definition
    struct CTEDefinition {
        std::string name;           // CTE name (e.g., "user_stats")
        std::vector<std::string> column_names;  // Explicit column names if specified
        std::vector<ColumnType> column_types;   // Inferred column types
        BoundStatementPtr definition;           // The bound SELECT statement
        bool is_recursive = false;              // true for WITH RECURSIVE
        TableId temp_table_id;                  // Assigned temporary table ID
        
        // For recursive CTEs
        BoundStatementPtr anchor_query;         // Non-recursive part
        BoundStatementPtr recursive_query;      // Recursive part
    };

    // Bound table reference with resolved schema information  
    struct BoundTableRef {
        TableId table_id;
        std::string table_name;     // Keep name for debugging
        std::string alias;          // Optional table alias
        std::vector<ColumnId> available_columns;
        std::unordered_map<std::string, ColumnId> column_name_to_id;
        
        // Schema information resolved at bind time
        std::vector<Column> column_definitions;
        std::vector<Index> available_indexes;
    };

    // Bound expression types
    enum class BoundExpressionType {
        COLUMN_REF,     // Resolved column reference
        CONSTANT,       // Literal value
        PARAMETER,      // Parameter placeholder ($1, $2, etc.)
        FUNCTION_CALL,  // Function with resolved signature
        BINARY_OP,      // Binary operation with type checking
        UNARY_OP,       // Unary operation
        CASE,           // CASE expression
        SUBQUERY,       // Bound subquery
        CAST            // Type cast operation
    };

    // Bound expression with resolved types and schema references
    struct BoundExpression {
        BoundExpressionType type;
        ColumnType result_type;     // Resolved result type
        bool nullable = true;       // Can this expression be NULL?
        
        // Expression-specific data
        std::variant<
            ColumnId,                           // COLUMN_REF
            std::string,                        // CONSTANT (serialized value) or FUNCTION_CALL (function name) or BINARY_OP/UNARY_OP (operator)
            BoundParameter,                     // PARAMETER
            BoundStatementPtr                   // SUBQUERY
        > data;
        
        std::vector<BoundExpressionPtr> children;  // Operands/arguments
        
        // Additional metadata
        std::string original_text;  // For debugging/error messages
        TableId source_table_id = 0;  // For column references
    };

    // Base bound statement
    struct BoundStatement {
        enum class Type {
            SELECT,
            INSERT, 
            UPDATE,
            DELETE
        };
        
        Type statement_type;
        std::vector<BoundParameter> parameters;  // All parameters in this statement
        std::unordered_map<std::string, BoundTableRefPtr> table_refs;  // Tables referenced
        
        virtual ~BoundStatement() = default;
    };

    // Bound SELECT statement
    struct BoundSelect : public BoundStatement {
        std::vector<BoundExpressionPtr> select_list;
        BoundTableRefPtr from_table;
        std::vector<BoundTableRefPtr> join_tables;
        std::vector<BoundExpressionPtr> join_conditions;
        BoundExpressionPtr where_clause;
        std::vector<BoundExpressionPtr> group_by;
        BoundExpressionPtr having_clause;
        std::vector<std::pair<BoundExpressionPtr, bool>> order_by;  // expression, ascending
        std::optional<size_t> limit_count;
        std::optional<size_t> offset_count;
        
        // CTE support
        std::vector<std::pair<std::string, BoundStatementPtr>> ctes;
        
        BoundSelect() { statement_type = Type::SELECT; }
    };

    // Bound INSERT statement
    struct BoundInsert : public BoundStatement {
        BoundTableRefPtr target_table;
        std::vector<ColumnId> target_columns;
        
        // Either VALUES or SELECT subquery
        std::variant<
            std::vector<std::vector<BoundExpressionPtr>>,  // VALUES rows
            BoundStatementPtr                              // SELECT subquery
        > source;
        
        // PostgreSQL extensions
        std::vector<ColumnId> conflict_columns;    // ON CONFLICT
        std::vector<ColumnId> returning_columns;   // RETURNING
        
        BoundInsert() { statement_type = Type::INSERT; }
    };

    // Bound UPDATE statement  
    struct BoundUpdate : public BoundStatement {
        BoundTableRefPtr target_table;
        std::vector<std::pair<ColumnId, BoundExpressionPtr>> assignments;  // SET clauses
        BoundExpressionPtr where_clause;
        std::vector<ColumnId> returning_columns;   // RETURNING
        
        BoundUpdate() { statement_type = Type::UPDATE; }
    };

    // Bound DELETE statement
    struct BoundDelete : public BoundStatement {
        BoundTableRefPtr target_table;
        BoundExpressionPtr where_clause;
        std::vector<ColumnId> returning_columns;   // RETURNING
        
        BoundDelete() { statement_type = Type::DELETE; }
    };

    // Schema binder - converts AST to BoundStatement
    class SchemaBinder {

    public:
        explicit SchemaBinder(const std::shared_ptr<DatabaseSchema>& schema);
        // Main binding interface
        [[nodiscard]] BoundStatementPtr bind(const std::string& query);
        [[nodiscard]] BoundStatementPtr bind_ast(const nlohmann::json& ast);
        
        // Error handling
        struct BindingError {
            std::string message;
            std::string query_location;
            std::vector<std::string> suggestions;
        };
        
        [[nodiscard]] const std::vector<BindingError>& get_errors() const { return errors_; }
        void clear_errors() { errors_.clear(); }
        
    private:
        std::shared_ptr<DatabaseSchema> schema_;
        std::unique_ptr<SchemaRegistry> registry_;
        std::unordered_map<std::string, TableId> table_name_to_id_;
        std::unordered_map<TableId, std::unordered_map<std::string, ColumnId>> column_name_to_id_;
        std::vector<BindingError> errors_;
        
        // Current query context for scoped column resolution
        std::unordered_map<std::string, BoundTableRefPtr> current_table_context_;
        
        // CTE (Common Table Expression) support
        std::vector<CTEDefinitionPtr> current_ctes_;         // CTEs in current query
        std::unordered_map<std::string, CTEDefinitionPtr> cte_registry_;  // Name -> CTE mapping
        TableId next_temp_table_id_ = 10000;                // Starting ID for CTE tables
        
        // Binding methods for different statement types
        BoundStatementPtr bind_select(const nlohmann::json& select_stmt);
        BoundStatementPtr bind_insert(const nlohmann::json& insert_stmt);
        BoundStatementPtr bind_update(const nlohmann::json& update_stmt);
        BoundStatementPtr bind_delete(const nlohmann::json& delete_stmt);
        
        // Expression binding
        BoundExpressionPtr bind_expression(const nlohmann::json& expr_node);
        BoundExpressionPtr bind_column_ref(const nlohmann::json& column_ref);
        BoundExpressionPtr bind_constant(const nlohmann::json& const_node);
        BoundExpressionPtr bind_parameter(const nlohmann::json& param_node);
        BoundExpressionPtr bind_function_call(const nlohmann::json& func_node);
        BoundExpressionPtr bind_binary_op(const nlohmann::json& binary_node);
        BoundExpressionPtr bind_boolean_expression(const nlohmann::json& bool_node);
        BoundExpressionPtr bind_subquery(const nlohmann::json& subquery_node);
        BoundTableRefPtr bind_table_ref(const nlohmann::json& table_node);
        
        // Schema resolution
        [[nodiscard]] std::optional<TableId> resolve_table(const std::string& table_name);
        [[nodiscard]] std::optional<ColumnId> resolve_column(TableId table_id, const std::string& column_name);
        [[nodiscard]] ColumnType infer_expression_type(const BoundExpressionPtr& expr);
        
        // CTE support methods
        void parse_with_clause(const nlohmann::json& with_clause);
        std::vector<CTEDefinitionPtr> extract_cte_definitions(const nlohmann::json& ctes_array);
        CTEDefinitionPtr bind_cte_definition(const nlohmann::json& cte_node);
        void register_cte(CTEDefinitionPtr cte);
        [[nodiscard]] std::optional<TableId> resolve_cte_table(const std::string& table_name);
        void infer_cte_schema(CTEDefinitionPtr cte);
        void clear_ctes();  // Clear CTEs after query processing
        
        // Parameter handling
        void collect_parameters(const BoundExpressionPtr& expr);
        ParameterType infer_parameter_type(const BoundExpressionPtr& context_expr, size_t param_index);
        
        // Error reporting
        void add_error(const std::string& message, const std::string& location = "");
        void add_table_not_found_error(const std::string& table_name);
        void add_column_not_found_error(const std::string& column_name, const std::string& table_name);
        
        // Utility methods
        void initialize_schema_mappings();
        [[nodiscard]] std::string format_type(ColumnType type) const;
        std::string extract_string_value(const nlohmann::json& node);
        
        // Friend class for accessing private members
        friend class QueryPlanner;
    };

    // Utility functions
    [[nodiscard]] std::string bound_statement_to_string(const BoundStatementPtr& stmt);
    [[nodiscard]] std::string bound_expression_to_string(const BoundExpressionPtr& expr);
    [[nodiscard]] bool validate_bound_statement(const BoundStatementPtr& stmt);

} // namespace db25