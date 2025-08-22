# CTE Architecture Gap Analysis

## Problem Statement

The new Schema-Aware Query Binding system has a fundamental limitation with Common Table Expressions (CTEs) in SELECT statements:

- ✅ **INSERT/UPDATE/DELETE CTEs work** (use old string-based parsing)
- ❌ **SELECT CTEs fail** (new schema binder doesn't understand CTE temporary tables)

## Root Cause

```
Error: Table 'user_stats' not found
```

The SchemaBinder only knows about real schema tables, not CTE-defined temporary tables like `user_stats` in:

```sql
WITH user_stats AS (SELECT COUNT(*) as total FROM users) 
SELECT total FROM user_stats  -- ❌ 'user_stats' not found
```

## Current Architecture Limitation

```
┌─────────────────────┐    ┌──────────────────┐    ┌──────────────┐
│ WITH user_stats AS  │───▶│ Schema Binder    │───▶│ FAILURE      │
│ (SELECT COUNT(*))   │    │ • Knows: users   │    │ Table not    │
│ SELECT total FROM   │    │ • Missing:       │    │ found        │
│ user_stats          │    │   user_stats     │    │              │
└─────────────────────┘    └──────────────────┘    └──────────────┘
```

## Required Solution Architecture

### 1. CTE-Aware Schema Binder

```cpp
class CTEAwareSchemaBinder : public SchemaBinder {
    struct CTEDefinition {
        std::string name;
        std::vector<std::string> column_names;
        std::vector<DataType> column_types;
        std::shared_ptr<BoundStatement> definition;
    };
    
    std::vector<CTEDefinition> cte_definitions_;
    std::unordered_map<std::string, TableId> temp_table_registry_;
    
public:
    // New methods needed:
    void parse_with_clause(const nlohmann::json& ast);
    std::optional<TableId> resolve_cte_table(const std::string& table_name);
    bool bind_cte_definition(const CTEDefinition& cte);
};
```

### 2. Enhanced Binding Flow

```
┌─────────────────────┐    ┌──────────────────┐    ┌──────────────┐
│ WITH user_stats AS  │───▶│ CTE-Aware        │───▶│ SUCCESS      │
│ (SELECT COUNT(*))   │    │ Schema Binder    │    │ Both tables  │
│ SELECT total FROM   │    │ • Knows: users   │    │ resolved     │
│ user_stats          │    │ • Extracts:      │    │              │
└─────────────────────┘    │   user_stats     │    └──────────────┘
                           └──────────────────┘
```

### 3. Implementation Steps

1. **Extend SchemaBinder Constructor**
   ```cpp
   SchemaBinder(schema, parse_ctes = true)
   ```

2. **Add CTE Parsing Methods**
   ```cpp
   void parse_with_clause(const nlohmann::json& ast);
   std::vector<CTEDefinition> extract_cte_definitions(const nlohmann::json& with_clause);
   ```

3. **Enhance Table Resolution**
   ```cpp
   std::optional<TableId> resolve_table(const std::string& name) override {
       // First check real tables
       auto real_table = SchemaBinder::resolve_table(name);
       if (real_table) return real_table;
       
       // Then check CTE temporary tables
       return resolve_cte_table(name);
   }
   ```

4. **Implement CTE Type Inference**
   ```cpp
   void infer_cte_schema(CTEDefinition& cte) {
       auto bound_select = bind_select_statement(cte.ast);
       cte.column_names = extract_column_names(bound_select);
       cte.column_types = infer_column_types(bound_select);
   }
   ```

### 4. Recursive CTE Support

For `WITH RECURSIVE` CTEs, additional complexity is needed:

```cpp
struct RecursiveCTEDefinition : public CTEDefinition {
    std::shared_ptr<BoundStatement> anchor_query;    // Non-recursive part
    std::shared_ptr<BoundStatement> recursive_query; // Recursive part
    bool is_recursive = false;
};
```

## Testing Strategy

Once implemented, all these queries should work:

```sql
-- ✅ Should work
WITH user_stats AS (SELECT COUNT(*) as total FROM users) 
SELECT total FROM user_stats;

-- ✅ Should work  
WITH active_users AS (SELECT id, name FROM users WHERE active = true),
     user_orders AS (SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id)
SELECT au.name, uo.order_count 
FROM active_users au JOIN user_orders uo ON au.id = uo.user_id;

-- ✅ Should work (recursive)
WITH RECURSIVE category_tree AS (
    SELECT id, name, parent_id FROM categories WHERE parent_id IS NULL
    UNION ALL 
    SELECT c.id, c.name, c.parent_id 
    FROM categories c JOIN category_tree ct ON c.parent_id = ct.id
) 
SELECT * FROM category_tree;
```

## Priority Assessment

**High Priority**: This is a significant architectural gap that limits the Schema-Aware Query Binding system's completeness.

**Impact**: 
- SELECT CTEs are common in production SQL
- Current workaround requires reverting to old string-based system
- Breaks the unified architecture vision

**Effort**: Medium-High (2-3 days of focused development)
- Requires AST parsing enhancements
- Need temporary table registry 
- Complex type inference for CTE columns
- Recursive CTE support adds complexity

## Recommendation

**Phase 1**: Implement basic CTE support (non-recursive)
**Phase 2**: Add recursive CTE support  
**Phase 3**: Optimize CTE query planning with pushdown optimizations

This would complete the Schema-Aware Query Binding architecture and eliminate the mixed results in CTE testing.