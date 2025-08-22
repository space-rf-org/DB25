# BoundStatement → LogicalPlan Integration Plan

## Overview
Convert schema-resolved BoundStatement objects into LogicalPlan nodes for optimization and execution.

## Current QueryPlanner Interface
```cpp
class QueryPlanner {
    LogicalPlan create_plan(const std::string &query);  // Current
    LogicalPlan create_plan(const QueryResult &parsed_query);  // Current
};
```

## Target Enhanced Interface
```cpp
class QueryPlanner {
    // Existing interface (preserved for backward compatibility)
    LogicalPlan create_plan(const std::string &query);
    
    // NEW: BoundStatement-based interface
    LogicalPlan create_plan_from_bound_statement(const BoundStatementPtr& bound_stmt);
    BoundStatementPtr bind_and_validate(const std::string& query);
    
private:
    std::unique_ptr<SchemaBinder> binder_;
    
    // Conversion methods: BoundStatement → LogicalPlan
    LogicalPlanNodePtr convert_bound_select(const std::shared_ptr<BoundSelect>& bound_select);
    LogicalPlanNodePtr convert_bound_insert(const std::shared_ptr<BoundInsert>& bound_insert);
    LogicalPlanNodePtr convert_bound_update(const std::shared_ptr<BoundUpdate>& bound_update);
    LogicalPlanNodePtr convert_bound_delete(const std::shared_ptr<BoundDelete>& bound_delete);
};
```

## Conversion Examples

### 1. BoundSelect → LogicalPlan Conversion

**Input BoundSelect:**
```cpp
BoundSelect {
    from_table: BoundTableRef {
        table_id: 1,
        table_name: "users",
        available_columns: [1, 2, 3, 4]  // id, name, email, created_at
    },
    select_list: [
        BoundExpression { type: COLUMN_REF, data: ColumnId(1), result_type: INTEGER },
        BoundExpression { type: COLUMN_REF, data: ColumnId(2), result_type: VARCHAR }
    ],
    where_clause: BoundExpression {
        type: BINARY_OP,
        data: "=",
        children: [
            BoundExpression { type: COLUMN_REF, data: ColumnId(1) },
            BoundExpression { type: CONSTANT, data: "42" }
        ]
    }
}
```

**Output LogicalPlan:**
```cpp
ProjectionNode {
    children: [
        SelectionNode {
            filter_conditions: [
                Expression { type: BINARY_OP, value: "=", children: [...] }
            ],
            children: [
                TableScanNode {
                    table_name: "users",
                    table_id: 1,  // NEW: Schema-resolved ID
                    projected_columns: [1, 2],  // NEW: Column IDs
                    output_columns: ["id", "name"]
                }
            ]
        }
    ],
    projection_list: [
        Expression { type: COLUMN_REF, column_ref: ColumnRef("users", "id") },
        Expression { type: COLUMN_REF, column_ref: ColumnRef("users", "name") }
    ]
}
```

### 2. Enhanced LogicalPlan Nodes

**Current TableScanNode:**
```cpp
struct TableScanNode {
    std::string table_name;
    std::string alias;
    std::vector<ExpressionPtr> filter_conditions;
};
```

**Enhanced TableScanNode:**
```cpp
struct TableScanNode {
    std::string table_name;                    // Keep for backward compatibility
    TableId table_id;                          // NEW: Schema-resolved table ID
    std::string alias;
    std::vector<ExpressionPtr> filter_conditions;
    std::vector<ColumnId> projected_columns;   // NEW: Schema-resolved columns
    std::vector<Column> column_definitions;    // NEW: Full schema metadata
};
```

## Benefits of Integration

### 1. Performance Improvements
- **Faster optimization**: Use IDs instead of string lookups
- **Better cost estimation**: Access to full schema metadata
- **Efficient joins**: Compare TableId/ColumnId instead of strings

### 2. Type Safety
- **Compile-time type checking**: All expressions have resolved types
- **Better error messages**: "Column 'nam' in table 'users' has type VARCHAR, expected INTEGER"
- **Type-aware optimization**: Skip unnecessary casts, choose better algorithms

### 3. Schema-Aware Optimization
- **Index selection**: Direct access to table indexes via TableId
- **Join ordering**: Use foreign key relationships from schema
- **Predicate pushdown**: Know exactly which columns exist in which tables

## Implementation Steps

### Phase 1: Basic Conversion
1. Add `SchemaBinder` to `QueryPlanner`
2. Implement `create_plan_from_bound_statement()`
3. Add conversion methods for each statement type
4. Enhance LogicalPlan nodes with schema IDs

### Phase 2: Optimization Enhancement
1. Update cost estimation to use schema metadata
2. Enhance join selection with foreign key info
3. Improve predicate pushdown with type information
4. Add index-aware optimization

### Phase 3: Advanced Features
1. Parameter binding support
2. Prepared statement caching
3. Dynamic schema updates
4. Advanced type system (arrays, composites)

## Example Usage

```cpp
// Current workflow
QueryPlanner planner(schema);
LogicalPlan plan = planner.create_plan("SELECT id, name FROM users WHERE id = 42");

// Enhanced workflow
SchemaBinder binder(schema);
QueryPlanner planner(schema, &binder);

// Option 1: Direct bound statement creation
auto bound_stmt = binder.bind("SELECT id, name FROM users WHERE id = 42");
if (bound_stmt) {
    LogicalPlan plan = planner.create_plan_from_bound_statement(bound_stmt);
}

// Option 2: Integrated workflow
auto [bound_stmt, plan] = planner.bind_and_plan("SELECT id, name FROM users WHERE id = 42");
```

## Migration Strategy

1. **Backward Compatibility**: Keep existing `create_plan(string)` interface
2. **Gradual Migration**: Add new interface alongside existing one
3. **Internal Refactoring**: Eventually make string interface wrap bound statement interface
4. **Performance Testing**: Benchmark old vs new approach

This integration provides the foundation for advanced features like prepared statements, better optimization, and robust error handling while maintaining backward compatibility.