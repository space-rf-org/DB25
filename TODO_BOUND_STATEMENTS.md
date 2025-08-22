# 🔗 BoundStatement Implementation TODO

**Created:** January 2025  
**Status:** Planning Phase  
**Priority:** High - Core Infrastructure Enhancement  
**DuckDB Comparison:** Analysis completed - our design aligns with DuckDB's proven architecture

---

## 📋 **OVERVIEW**

Implement a BoundStatement layer to bridge AST parsing and logical plan creation. This provides schema validation, type resolution, and parameter binding support.

### **🔄 DuckDB Architecture Alignment**
Our design matches DuckDB's proven approach:
- ✅ **Pipeline**: `AST → BoundStatement → LogicalPlan` (same as DuckDB)
- ✅ **Schema Resolution**: String names → ID-based lookups (same as DuckDB)  
- ✅ **Type Inference**: Early binding with validation (same as DuckDB)
- ✅ **Error Handling**: Schema-aware error messages (same as DuckDB)

**Grade: A- (Excellent Foundation)** - Architecturally sound, room for advanced features

### **Current Architecture**
```
AST → LogicalPlan (direct conversion, string-based references)
```

### **Target Architecture**
```
AST → BoundStatement → LogicalPlan (schema-resolved, type-safe, ID-based)
```

### **Benefits**
- ✅ Early schema validation (table/column existence)
- ✅ Type safety and inference
- ✅ Performance improvement (IDs vs string lookups)  
- ✅ Foundation for PreparedStatement support
- ✅ Better error messages with schema context
- ✅ Parameter placeholder support ($1, $2, etc.)

---

## 🏗️ **PHASE 1: CORE INFRASTRUCTURE**

### **TASK 1.1: Schema ID Mapping System**
**Priority:** High | **Estimated Time:** 2-3 days

**Implementation:**
```cpp
class SchemaRegistry {
private:
    std::unordered_map<std::string, TableId> table_name_to_id_;
    std::unordered_map<TableId, Table> table_definitions_;
    std::unordered_map<TableId, std::unordered_map<std::string, ColumnId>> column_mappings_;
    
public:
    void register_schema(const DatabaseSchema& schema);
    std::optional<TableId> resolve_table(const std::string& name);
    std::optional<ColumnId> resolve_column(TableId table_id, const std::string& name);
    const Table& get_table_definition(TableId table_id);
    const Column& get_column_definition(TableId table_id, ColumnId column_id);
};
```

**Files to Create:**
- `include/schema_registry.hpp` - Schema ID mapping system
- `src/schema_registry.cpp` - Implementation

**Test Cases:**
- Table name resolution
- Column name resolution with table context
- Ambiguous column resolution
- Case sensitivity handling

---

### **TASK 1.2: BoundExpression Implementation**
**Priority:** High | **Estimated Time:** 3-4 days

**Target Implementation:**
```cpp
class ExpressionBinder {
private:
    const SchemaRegistry& registry_;
    std::vector<BoundParameter>& parameters_;
    
public:
    BoundExpressionPtr bind_expression(const nlohmann::json& ast_node);
    BoundExpressionPtr bind_column_ref(const nlohmann::json& column_ref);
    BoundExpressionPtr bind_function_call(const nlohmann::json& func_call);
    BoundExpressionPtr bind_binary_op(const nlohmann::json& binary_op);
    BoundExpressionPtr bind_parameter(const nlohmann::json& param_ref);
    
private:
    ColumnType infer_expression_type(const BoundExpressionPtr& expr);
    void validate_type_compatibility(ColumnType left, ColumnType right, const std::string& op);
    ParameterType infer_parameter_type(const BoundExpressionPtr& context);
};
```

**Key Features:**
- Column reference resolution with table context
- Function signature validation
- Type inference and checking
- Parameter type inference
- Operator compatibility validation

**Files to Modify/Create:**
- `src/expression_binder.cpp` - Expression binding logic
- `include/type_system.hpp` - Type compatibility rules

---

### **TASK 1.3: Basic Statement Binding**
**Priority:** High | **Estimated Time:** 4-5 days

**Implementation Order:**
1. **SELECT binding** - Most complex, foundation for others
2. **INSERT binding** - Simpler, builds on expression binding
3. **UPDATE binding** - Assignment validation
4. **DELETE binding** - Simplest case

**SELECT Binding Logic:**
```cpp
BoundStatementPtr SchemaBinder::bind_select(const nlohmann::json& select_stmt) {
    auto bound_select = std::make_shared<BoundSelect>();
    
    // 1. Bind FROM clause first (establishes table context)
    bind_from_clause(select_stmt, bound_select);
    
    // 2. Bind JOIN clauses (extends table context)
    bind_join_clauses(select_stmt, bound_select);
    
    // 3. Bind SELECT list (with full table context)
    bind_select_list(select_stmt, bound_select);
    
    // 4. Bind WHERE clause
    bind_where_clause(select_stmt, bound_select);
    
    // 5. Bind GROUP BY, HAVING, ORDER BY
    bind_aggregation_clauses(select_stmt, bound_select);
    
    return bound_select;
}
```

**Files to Create:**
- `src/statement_binder.cpp` - Main statement binding logic

---

## 🔍 **PHASE 2: ADVANCED BINDING FEATURES**

### **TASK 2.1: Table Reference Binding**
**Priority:** High | **Estimated Time:** 2-3 days

**Features to Implement:**
- Table alias support (`users u`)
- Qualified column references (`u.name`)
- Ambiguous column resolution
- Subquery table references
- CTE table references

**Implementation:**
```cpp
struct TableContext {
    std::unordered_map<std::string, BoundTableRefPtr> available_tables;
    std::unordered_map<std::string, std::vector<BoundTableRefPtr>> ambiguous_columns;
    
    BoundTableRefPtr resolve_table(const std::string& name_or_alias);
    std::optional<std::pair<BoundTableRefPtr, ColumnId>> resolve_column(const std::string& name);
    void add_table(const BoundTableRefPtr& table_ref);
};
```

---

### **TASK 2.2: Parameter Binding System**
**Priority:** Medium | **Estimated Time:** 3-4 days

**PostgreSQL Parameter Support:**
```sql
-- Target support
SELECT * FROM users WHERE id = $1 AND status = $2;
INSERT INTO users (name, email) VALUES ($1, $2);
UPDATE users SET name = $1 WHERE id = $2;
```

**Implementation:**
```cpp
class ParameterBinder {
private:
    std::vector<BoundParameter> parameters_;
    std::unordered_map<size_t, ParameterType> inferred_types_;
    
public:
    BoundExpressionPtr bind_parameter_ref(size_t param_index);
    void infer_parameter_type(size_t param_index, const BoundExpressionPtr& context);
    ParameterType get_parameter_type(size_t param_index);
    std::vector<BoundParameter> get_all_parameters();
};
```

**Type Inference Rules:**
- `WHERE id = $1` → Infer $1 type from `id` column type
- `INSERT VALUES ($1, $2)` → Infer from target column types
- `$1 + $2` → Require explicit context or default to TEXT

---

### **TASK 2.3: Type System & Validation**
**Priority:** Medium | **Estimated Time:** 2-3 days

**Type Compatibility Matrix:**
```cpp
class TypeSystem {
public:
    static bool is_compatible(ColumnType from, ColumnType to);
    static bool can_cast_implicitly(ColumnType from, ColumnType to);
    static ColumnType resolve_binary_op_type(ColumnType left, ColumnType right, const std::string& op);
    static ColumnType resolve_function_return_type(const std::string& func_name, const std::vector<ColumnType>& args);
};
```

**Validation Rules:**
- Arithmetic operations: INTEGER + INTEGER → INTEGER
- Comparison operations: compatible types only
- String operations: TEXT/VARCHAR compatible
- Function signatures: validate argument types and count

---

## 🧩 **PHASE 3: INTEGRATION & COMPLEX FEATURES**

### **TASK 3.1: CTE Binding Support**
**Priority:** Medium | **Estimated Time:** 3-4 days

**CTE Binding Logic:**
```cpp
struct CTEContext {
    std::unordered_map<std::string, BoundStatementPtr> cte_definitions;
    std::unordered_map<std::string, std::vector<ColumnType>> cte_column_types;
    
    void register_cte(const std::string& name, const BoundStatementPtr& definition);
    BoundTableRefPtr resolve_cte_reference(const std::string& name);
};
```

**Support Matrix:**
- ✅ Basic CTEs: `WITH cte AS (...) SELECT ...`
- ✅ Multiple CTEs: `WITH cte1 AS (...), cte2 AS (...) SELECT ...`
- ✅ Recursive CTEs: `WITH RECURSIVE ...`
- ✅ CTE column type inference

---

### **TASK 3.2: Subquery Binding**
**Priority:** Medium | **Estimated Time:** 2-3 days

**Subquery Types:**
- Scalar subqueries: `(SELECT COUNT(*) FROM orders)`
- EXISTS subqueries: `EXISTS (SELECT 1 FROM orders WHERE ...)`
- IN subqueries: `id IN (SELECT user_id FROM orders)`
- Correlated subqueries: References to outer query

**Implementation:**
```cpp
BoundExpressionPtr bind_subquery(const nlohmann::json& subquery_node, SubqueryType type) {
    auto bound_subquery = bind_select(subquery_node);
    validate_subquery_type_compatibility(bound_subquery, type);
    return create_subquery_expression(bound_subquery, type);
}
```

---

### **TASK 3.3: JOIN Binding & Validation**
**Priority:** Medium | **Estimated Time:** 2-3 days

**JOIN Types & Validation:**
- INNER JOIN: Validate join conditions
- LEFT/RIGHT/FULL OUTER JOIN: Nullability tracking
- CROSS JOIN: No conditions required
- Natural joins: Column name matching

**Implementation:**
```cpp
void bind_join_clause(const nlohmann::json& join_node, BoundSelect* bound_select) {
    auto right_table = bind_table_ref(join_node["right_table"]);
    auto join_conditions = bind_join_conditions(join_node["conditions"], bound_select->table_refs);
    
    validate_join_conditions(join_conditions);
    bound_select->join_tables.push_back(right_table);
    bound_select->join_conditions.insert(bound_select->join_conditions.end(), 
                                       join_conditions.begin(), join_conditions.end());
}
```

---

## 📊 **PHASE 4: ERROR HANDLING & DIAGNOSTICS**

### **TASK 4.1: Comprehensive Error System**
**Priority:** High | **Estimated Time:** 2-3 days

**Error Categories:**
```cpp
enum class BindingErrorType {
    TABLE_NOT_FOUND,
    COLUMN_NOT_FOUND,
    AMBIGUOUS_COLUMN,
    TYPE_MISMATCH,
    PARAMETER_TYPE_UNKNOWN,
    FUNCTION_NOT_FOUND,
    INVALID_ARGUMENT_COUNT,
    CIRCULAR_DEPENDENCY
};

struct BindingError {
    BindingErrorType type;
    std::string message;
    std::string query_location;  // Line/column info
    std::vector<std::string> suggestions;
    std::optional<std::string> fix_hint;
};
```

**Smart Error Messages:**
- `Table 'user' not found. Did you mean 'users'?`
- `Column 'nam' not found in table 'users'. Did you mean 'name'?`
- `Ambiguous column 'id'. Specify table: 'users.id' or 'orders.id'`

---

### **TASK 4.2: Schema Suggestion System**
**Priority:** Low | **Estimated Time:** 1-2 days

**Features:**
- Fuzzy string matching for table/column names
- Context-aware suggestions
- Common typo detection

**Implementation:**
```cpp
class SuggestionEngine {
public:
    std::vector<std::string> suggest_table_names(const std::string& input);
    std::vector<std::string> suggest_column_names(const std::string& input, TableId table_id);
    std::vector<std::string> suggest_function_names(const std::string& input);
    
private:
    double calculate_similarity(const std::string& a, const std::string& b);
    std::vector<std::pair<std::string, double>> rank_suggestions(const std::string& input, const std::vector<std::string>& candidates);
};
```

---

## 🔌 **PHASE 5: INTEGRATION WITH EXISTING SYSTEM**

### **TASK 5.1: QueryPlanner Integration**
**Priority:** High | **Estimated Time:** 2-3 days

**New QueryPlanner Interface:**
```cpp
class QueryPlanner {
public:
    // Existing interface (preserved for backward compatibility)
    LogicalPlan create_plan(const std::string& query);
    
    // New BoundStatement-based interface
    LogicalPlan create_plan_from_bound_statement(const BoundStatementPtr& bound_stmt);
    BoundStatementPtr bind_and_validate(const std::string& query);
    
private:
    std::unique_ptr<SchemaBinder> binder_;
    
    // Convert BoundStatement → LogicalPlan
    LogicalPlanNodePtr convert_bound_select(const std::shared_ptr<BoundSelect>& bound_select);
    LogicalPlanNodePtr convert_bound_insert(const std::shared_ptr<BoundInsert>& bound_insert);
    LogicalPlanNodePtr convert_bound_update(const std::shared_ptr<BoundUpdate>& bound_update);
    LogicalPlanNodePtr convert_bound_delete(const std::shared_ptr<BoundDelete>& bound_delete);
};
```

**Migration Strategy:**
1. Keep existing `create_plan(string)` method working
2. Add new `create_plan_from_bound_statement()` method
3. Gradually migrate internal logic to use bound statements
4. Eventually make string interface a wrapper around bound statement interface

---

### **TASK 5.2: LogicalPlan Enhancement**
**Priority:** Medium | **Estimated Time:** 1-2 days

**Enhanced LogicalPlan Nodes:**
```cpp
// Add schema metadata to plan nodes
struct TableScanNode : public LogicalPlanNode {
    TableId table_id;           // NEW: Schema-resolved table ID
    std::string table_name;     // Keep for backward compatibility
    std::vector<ColumnId> projected_columns;  // NEW: Schema-resolved columns
    // ... existing fields
};
```

**Benefits:**
- Faster optimization (use IDs instead of string lookups)
- Better cost estimation (access to full schema metadata)
- Type-aware optimization decisions

---

## 🧪 **PHASE 6: COMPREHENSIVE TESTING**

### **TASK 6.1: BoundStatement Test Suite**
**Priority:** High | **Estimated Time:** 3-4 days

**Test Categories:**
```cpp
class BoundStatementTestSuite {
private:
    struct BindingTestCase {
        std::string name;
        std::string sql;
        bool should_succeed;
        std::vector<std::string> expected_tables;
        std::vector<std::string> expected_columns;
        std::vector<ParameterType> expected_parameter_types;
        std::string expected_error;
    };
    
public:
    void run_basic_binding_tests();
    void run_type_inference_tests();
    void run_parameter_binding_tests();
    void run_error_handling_tests();
    void run_performance_tests();
};
```

**Test Cases:**
1. **Basic Binding Tests (50 cases)**
   - Simple SELECT, INSERT, UPDATE, DELETE
   - Table and column resolution
   - Basic type inference

2. **Complex Binding Tests (30 cases)**
   - JOINs with multiple tables
   - Subqueries and CTEs
   - Complex expressions

3. **Parameter Tests (20 cases)**
   - Parameter type inference
   - Multiple parameters
   - Parameter in different contexts

4. **Error Handling Tests (40 cases)**
   - Table not found errors
   - Column not found errors
   - Type mismatch errors
   - Ambiguous column errors

5. **Performance Tests (10 cases)**
   - Large schema binding performance
   - Complex query binding time
   - Memory usage validation

---

### **TASK 6.2: Integration Testing**
**Priority:** Medium | **Estimated Time:** 2-3 days

**Integration Test Areas:**
- BoundStatement → LogicalPlan conversion
- End-to-end query processing with bound statements
- Backward compatibility with existing string-based interface
- Performance comparison: string-based vs bound statement approach

**Files to Create:**
- `tests/test_bound_statement_comprehensive.cpp` - Full test suite
- `tests/test_schema_binder.cpp` - Binder-specific tests
- `tests/test_bound_statement_integration.cpp` - Integration tests

---

## 📅 **IMPLEMENTATION TIMELINE**

### **Sprint 1 (Week 1-2): Foundation**
- [ ] **Task 1.1**: Schema ID mapping system
- [ ] **Task 1.2**: BoundExpression implementation
- [ ] **Task 4.1**: Basic error handling system
- [ ] **Task 6.1**: Initial test framework

### **Sprint 2 (Week 3-4): Core Binding**
- [ ] **Task 1.3**: Basic statement binding (SELECT, INSERT, UPDATE, DELETE)
- [ ] **Task 2.1**: Table reference binding
- [ ] **Task 5.1**: Initial QueryPlanner integration

### **Sprint 3 (Week 5-6): Advanced Features**
- [ ] **Task 2.2**: Parameter binding system
- [ ] **Task 2.3**: Type system & validation
- [ ] **Task 3.1**: CTE binding support

### **Sprint 4 (Week 7-8): Complex Features**
- [ ] **Task 3.2**: Subquery binding
- [ ] **Task 3.3**: JOIN binding & validation
- [ ] **Task 5.2**: LogicalPlan enhancement

### **Sprint 5 (Week 9): Testing & Polish**
- [ ] **Task 6.1**: Comprehensive test suite
- [ ] **Task 6.2**: Integration testing
- [ ] **Task 4.2**: Schema suggestion system
- [ ] Performance tuning and optimization

---

## 🎯 **SUCCESS METRICS**

### **Functional Requirements**
- [ ] **100% schema validation** - All table/column references validated at bind time
- [ ] **Complete type safety** - All expressions have resolved types
- [ ] **Parameter support** - Full PostgreSQL parameter syntax ($1, $2, etc.)
- [ ] **Error quality** - Specific, actionable error messages with suggestions
- [ ] **CTE support** - All CTE patterns from existing tests work with binding
- [ ] **Backward compatibility** - Existing query interface continues working

### **Performance Requirements**
- [ ] **Binding overhead** < 10% of total query processing time
- [ ] **Memory efficiency** - BoundStatement memory usage ≤ 2x AST size
- [ ] **Schema lookup performance** - O(1) table/column resolution
- [ ] **Large schema support** - Handle schemas with 1000+ tables/10000+ columns

### **Quality Requirements**
- [ ] **Test coverage** ≥ 95% for all binding logic
- [ ] **Error coverage** - Test all error conditions with good messages
- [ ] **Documentation** - Complete API documentation and examples
- [ ] **Integration** - Seamless integration with existing optimizer/executor

---

## 📚 **REFERENCE MATERIALS**

### **Database System References**
- PostgreSQL Source Code - Parser/Analyzer architecture
- DuckDB Source Code - Modern binding implementation
- Apache Calcite - SQL validation and type system
- SQLite - Lightweight binding approach

### **Academic Papers**
- "Efficient Compilation of SQL Queries" (Neumann, 2011)
- "Type Inference for SQL" (Benzaken et al., 2003)
- "Query Compilation in Relational Database Systems" (Kleppmann, 2017)

### **Implementation Patterns**
- Visitor Pattern - AST traversal for binding
- Strategy Pattern - Different binding strategies per statement type
- Factory Pattern - BoundStatement creation
- Observer Pattern - Error collection and reporting

---

## 📝 **NOTES & CONSIDERATIONS**

### **Design Decisions**
- **ID-based references**: Use numeric IDs for performance, keep names for debugging
- **Type safety first**: Resolve all types at bind time, fail fast on errors
- **Extensible design**: Easy to add new expression types and statement types
- **Error-friendly**: Collect multiple errors, provide suggestions

### **Edge Cases to Handle**
- Ambiguous column references (`SELECT id FROM users, orders`)
- Self-joins with aliases (`SELECT * FROM users u1 JOIN users u2 ON u1.manager_id = u2.id`)
- Correlated subqueries with complex scoping
- Recursive CTE type inference
- Parameter type conflicts (`$1` used as both INTEGER and TEXT)

### **Future Enhancements**
- **PreparedStatement**: Cache bound statements for reuse
- **Query hints**: Schema-aware optimization hints
- **Dynamic schemas**: Support for runtime schema changes
- **Advanced type system**: User-defined types, arrays, composite types

---

**END OF TODO LIST**

*This comprehensive plan establishes BoundStatement as a core infrastructure component, providing the foundation for advanced query processing features like prepared statements, better optimization, and robust error handling.*