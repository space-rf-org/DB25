# 🎯 DB25 Query Optimizer Implementation TODO

**Created:** August 2025  
**Status:** Planning Phase  
**Priority:** High Impact Performance Enhancement

---

## 📋 **OVERVIEW**

Current optimizer implementations are no-ops that provide no actual optimization. This TODO outlines the complete plan to implement industry-standard query optimization algorithms.

### **Current State Analysis**
- ❌ `ProjectionPushdownTransformer`: Does nothing (`return node;`)
- ❌ `PredicatePushdownTransformer`: Minimal implementation (~5% effectiveness)
- ❌ `JoinReorderingTransformer`: Basic swap only (~15% effectiveness)
- ❌ `PlanTransformer` base: No-op implementation

### **Target State**
- ✅ Real predicate pushdown with cost-based analysis
- ✅ Intelligent projection elimination 
- ✅ Cost-based join reordering with multiple algorithms
- ✅ Comprehensive optimization framework

---

## 🏗️ **PHASE 1: FOUNDATION & ARCHITECTURE**

### **TASK 1.1: Enhanced Cost Model** 
**Priority:** High | **Estimated Time:** 2-3 days

**Current State:**
```cpp
struct PlanCost {
    double total_cost = 0.0;
    size_t estimated_rows = 0;
};
```

**Target Implementation:**
```cpp
struct OptimizationCost {
    double startup_cost = 0.0;
    double total_cost = 0.0;
    size_t estimated_rows = 0;
    double selectivity = 1.0;
    
    // Detailed cost breakdown
    double io_cost = 0.0;           // Disk I/O operations
    double cpu_cost = 0.0;          // CPU processing time
    double memory_cost = 0.0;       // Memory usage cost
    double network_cost = 0.0;      // Network transfer cost
    
    // Optimization metrics
    bool is_index_scan = false;
    bool requires_sorting = false;
    bool can_use_parallel = false;
    
    bool operator<(const OptimizationCost& other) const;
    OptimizationCost operator+(const OptimizationCost& other) const;
};
```

**Files to Modify:**
- `include/logical_plan.hpp` - Add OptimizationCost struct
- `src/query_planner.cpp` - Update cost calculation methods

---

### **TASK 1.2: Node Analysis Framework**
**Priority:** High | **Estimated Time:** 2-3 days

**Target Implementation:**
```cpp
struct NodeAnalysis {
    // Column analysis
    std::set<std::string> required_columns;    // Columns needed by parent nodes
    std::set<std::string> available_columns;   // Columns provided by this node  
    std::set<std::string> unused_columns;      // Columns that can be eliminated
    
    // Predicate analysis
    std::vector<ExpressionPtr> pushable_predicates;    // Can push to children
    std::vector<ExpressionPtr> local_predicates;       // Must evaluate here
    std::vector<ExpressionPtr> blocking_predicates;    // Prevent pushdown
    
    // Cost analysis
    std::map<std::string, double> table_selectivity;   // Per-table filtering
    std::map<std::string, double> column_selectivity;  // Per-column filtering
    bool is_expensive_operation = false;               // CPU/IO intensive?
    
    // Optimization opportunities
    bool can_use_index = false;
    bool benefits_from_projection = false;
    std::vector<std::string> recommended_indexes;
};

class NodeAnalyzer {
public:
    static NodeAnalysis analyze(const LogicalPlanNodePtr& node);
    static std::set<std::string> extract_referenced_tables(const ExpressionPtr& expr);
    static std::set<std::string> extract_referenced_columns(const ExpressionPtr& expr);
    static bool is_predicate_pushable(const ExpressionPtr& predicate, const std::string& target_table);
};
```

**Files to Create:**
- `include/optimization_analysis.hpp` - NodeAnalysis struct and NodeAnalyzer class
- `src/optimization_analysis.cpp` - Implementation

---

### **TASK 1.3: Statistics Collection Framework**
**Priority:** Medium | **Estimated Time:** 3-4 days

**Target Implementation:**
```cpp
struct TableStatistics {
    size_t row_count = 1000;
    double avg_row_size = 100.0;
    size_t total_pages = 0;
    double null_fraction = 0.0;
    
    // Index statistics
    std::map<std::string, IndexStatistics> index_stats;
    
    // Most common values for selectivity estimation
    std::map<std::string, std::vector<std::pair<std::string, double>>> mcv_lists;
};

struct ColumnStatistics {
    size_t distinct_values = 100;
    std::string min_value;
    std::string max_value;
    double null_fraction = 0.0;
    double avg_width = 10.0;
    
    // Histogram for range selectivity
    std::vector<std::pair<std::string, std::string>> histogram_bounds;
    std::vector<double> histogram_frequencies;
    
    // Most common values
    std::vector<std::pair<std::string, double>> most_common_values;
};

class StatisticsCollector {
private:
    std::map<std::string, TableStatistics> table_stats_;
    std::map<std::pair<std::string, std::string>, ColumnStatistics> column_stats_;
    
public:
    void collect_table_stats(const std::string& table_name);
    void collect_column_stats(const std::string& table_name, const std::string& column_name);
    
    double estimate_selectivity(const ExpressionPtr& condition) const;
    size_t estimate_result_size(const LogicalPlanNodePtr& node) const;
    double estimate_join_selectivity(const ExpressionPtr& join_condition) const;
    
    TableStatistics get_table_stats(const std::string& table_name) const;
    ColumnStatistics get_column_stats(const std::string& table_name, const std::string& column_name) const;
};
```

**Files to Create:**
- `include/statistics_collector.hpp` - Statistics framework
- `src/statistics_collector.cpp` - Implementation

---

## 🔍 **PHASE 2: PREDICATE PUSHDOWN OPTIMIZER**

### **TASK 2.1: Predicate Analysis Engine**
**Priority:** High | **Estimated Time:** 3-4 days

**Current Problem:**
```cpp
LogicalPlanNodePtr PredicatePushdownTransformer::transform_node(LogicalPlanNodePtr node) {
    // Only handles projection-over-selection case
    if (node->type == PlanNodeType::PROJECTION && !node->children.empty()) {
        auto child = node->children[0];
        if (child->type == PlanNodeType::SELECTION) {
            // Trivial swap - not real optimization
        }
    }
    return node;  // Does nothing in most cases
}
```

**Target Implementation:**
```cpp
class PredicatePushdownTransformer : public PlanTransformer {
private:
    struct PredicateClassification {
        std::vector<ExpressionPtr> single_table_predicates;   // Can push to table scan
        std::vector<ExpressionPtr> join_predicates;           // Evaluate at join level
        std::vector<ExpressionPtr> complex_predicates;        // Subqueries, aggregates
        std::map<std::string, std::vector<ExpressionPtr>> table_predicates; // Organized by table
    };
    
    PredicateClassification classify_predicates(const std::vector<ExpressionPtr>& predicates) const;
    bool can_push_to_table(const ExpressionPtr& predicate, const std::string& table_name) const;
    LogicalPlanNodePtr push_predicates_to_scans(LogicalPlanNodePtr node, 
                                               const PredicateClassification& classification);
    LogicalPlanNodePtr remove_redundant_selections(LogicalPlanNodePtr node);
    
public:
    LogicalPlanNodePtr transform_node(LogicalPlanNodePtr node) override;
};

// Predicate Analysis Utilities
class PredicateAnalyzer {
public:
    static std::set<std::string> extract_table_references(const ExpressionPtr& predicate);
    static bool is_deterministic(const ExpressionPtr& predicate);
    static bool is_index_friendly(const ExpressionPtr& predicate);
    static double estimate_predicate_cost(const ExpressionPtr& predicate);
    static bool blocks_other_optimizations(const ExpressionPtr& predicate);
};
```

**Algorithm Details:**
1. **Collect all selection conditions** from the plan tree
2. **Classify predicates** by table dependencies:
   - Single-table: `users.active = true` → Push to users scan
   - Join predicates: `users.id = orders.user_id` → Keep at join level
   - Complex predicates: Subqueries, volatile functions → Handle carefully
3. **Cost-benefit analysis**: Only push if reduces overall cost
4. **Safety checks**: Ensure semantic correctness

**Test Cases to Add:**
```sql
-- Should push user.active to users scan
SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true;

-- Should push both conditions to respective scans  
SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true AND o.status = 'completed';

-- Should NOT push join condition
SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.created_date > o.order_date;

-- Complex case with subquery
SELECT * FROM users u WHERE u.id IN (SELECT user_id FROM orders WHERE amount > 100) AND u.active = true;
```

**Files to Modify:**
- `src/query_planner.cpp` - Replace PredicatePushdownTransformer::transform_node
- `include/query_planner.hpp` - Add private methods to PredicatePushdownTransformer

---

### **TASK 2.2: Index-Aware Predicate Pushdown**
**Priority:** Medium | **Estimated Time:** 2-3 days

**Target Enhancement:**
```cpp
class IndexAwarePredicatePushdown {
private:
    struct IndexOpportunity {
        std::string table_name;
        std::string index_name;
        std::vector<ExpressionPtr> index_predicates;
        std::vector<ExpressionPtr> filter_predicates;
        double estimated_selectivity;
        double index_scan_cost;
    };
    
public:
    std::vector<IndexOpportunity> analyze_index_opportunities(
        const LogicalPlanNodePtr& scan_node,
        const std::vector<ExpressionPtr>& predicates) const;
        
    LogicalPlanNodePtr convert_to_index_scan(LogicalPlanNodePtr table_scan,
                                            const IndexOpportunity& opportunity);
};
```

**Algorithm:**
1. **Identify available indexes** for each table
2. **Match predicates to indexes**: `WHERE id = 123` → Use primary key index
3. **Cost comparison**: Index scan vs table scan + filter
4. **Convert beneficial cases** to IndexScanNode

---

## 📊 **PHASE 3: PROJECTION PUSHDOWN OPTIMIZER**

### **TASK 3.1: Column Usage Analysis**
**Priority:** High | **Estimated Time:** 3-4 days

**Current Problem:**
```cpp
LogicalPlanNodePtr ProjectionPushdownTransformer::transform_node(LogicalPlanNodePtr node) {
    // Simplified projection pushdown
    return node;  // DOES ABSOLUTELY NOTHING!
}
```

**Target Implementation:**
```cpp
class ProjectionPushdownTransformer : public PlanTransformer {
private:
    struct ColumnUsageAnalysis {
        std::set<std::string> required_columns;      // Must be available
        std::set<std::string> optional_columns;      // Nice to have
        std::set<std::string> never_used_columns;    // Can eliminate
        std::map<std::string, std::string> column_aliases; // Rename mapping
        
        bool benefits_from_early_projection = false;
        double estimated_memory_savings = 0.0;
        double estimated_io_savings = 0.0;
    };
    
    ColumnUsageAnalysis analyze_column_usage(const LogicalPlanNodePtr& node);
    LogicalPlanNodePtr add_beneficial_projections(LogicalPlanNodePtr node, 
                                                 const ColumnUsageAnalysis& analysis);
    LogicalPlanNodePtr eliminate_unused_columns(LogicalPlanNodePtr node,
                                               const ColumnUsageAnalysis& analysis);
    bool is_projection_beneficial(const LogicalPlanNodePtr& node, 
                                 const ColumnUsageAnalysis& analysis);
    
public:
    LogicalPlanNodePtr transform_node(LogicalPlanNodePtr node) override;
};

class ColumnAnalyzer {
public:
    static std::set<std::string> extract_required_columns(const LogicalPlanNodePtr& node);
    static std::set<std::string> extract_available_columns(const LogicalPlanNodePtr& node);
    static double estimate_column_width(const std::string& table, const std::string& column);
    static bool is_expensive_to_compute(const ExpressionPtr& projection);
};
```

**Algorithm Details:**
1. **Top-down analysis**: Determine which columns are actually needed
2. **Bottom-up projection**: Add projections where beneficial
3. **Cost-benefit analysis**: Only add projections that reduce total cost
4. **Memory optimization**: Prioritize wide tables and expensive operations

**Optimization Scenarios:**
```sql
-- Scenario 1: Large table, few columns needed
SELECT name FROM users;  -- Only project 'name' and 'id' (for joins)

-- Scenario 2: After expensive join, before sort
SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.name;
-- Add projection after join, before sort

-- Scenario 3: Wide table with aggregate
SELECT COUNT(*) FROM products WHERE category = 'electronics';
-- Project only 'category' column for filtering

-- Scenario 4: Multiple joins
SELECT u.name, p.title FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id;
-- Project minimal columns at each join level
```

**Files to Modify:**
- `src/query_planner.cpp` - Replace ProjectionPushdownTransformer::transform_node  
- `include/query_planner.hpp` - Add private methods to ProjectionPushdownTransformer

---

### **TASK 3.2: Memory-Aware Projection Optimization**
**Priority:** Medium | **Estimated Time:** 2 days

**Target Enhancement:**
```cpp
class MemoryAwareProjectionOptimizer {
private:
    struct MemoryProfile {
        size_t current_memory_usage;
        size_t peak_memory_usage;
        size_t memory_limit;
        double memory_pressure_factor;
    };
    
public:
    bool should_add_projection_for_memory(const LogicalPlanNodePtr& node,
                                        const ColumnUsageAnalysis& analysis,
                                        const MemoryProfile& profile);
                                        
    std::vector<std::string> prioritize_columns_for_elimination(
        const std::set<std::string>& unused_columns,
        const std::string& table_name);
};
```

---

## ⚡ **PHASE 4: JOIN REORDERING OPTIMIZER**

### **TASK 4.1: Cost-Based Join Enumeration**
**Priority:** High | **Estimated Time:** 4-5 days

**Current Problem:**
```cpp
LogicalPlanNodePtr JoinReorderingTransformer::transform_node(LogicalPlanNodePtr node) {
    // Only swaps left/right based on row count - very naive
    if (node->type == PlanNodeType::NESTED_LOOP_JOIN && node->children.size() == 2) {
        if (right->cost.estimated_rows < left->cost.estimated_rows) {
            std::swap(node->children[0], node->children[1]);  // Trivial swap
        }
    }
    return node;
}
```

**Target Implementation:**
```cpp
class JoinReorderingTransformer : public PlanTransformer {
private:
    struct JoinCandidate {
        std::vector<std::string> left_tables;
        std::vector<std::string> right_tables;
        std::vector<ExpressionPtr> join_conditions;
        std::vector<ExpressionPtr> filter_conditions;
        JoinType join_type;
        JoinAlgorithm recommended_algorithm;
        OptimizationCost estimated_cost;
        
        bool is_valid() const;
        bool is_cross_product() const;
    };
    
    struct JoinGraph {
        std::vector<std::string> tables;
        std::vector<ExpressionPtr> join_conditions;
        std::vector<ExpressionPtr> filter_conditions;
        std::map<std::pair<std::string, std::string>, double> join_selectivity;
        std::map<std::string, size_t> table_sizes;
    };
    
    JoinGraph build_join_graph(const LogicalPlanNodePtr& node);
    std::vector<JoinCandidate> enumerate_join_orders(const JoinGraph& graph);
    JoinCandidate find_optimal_join_order(const std::vector<JoinCandidate>& candidates);
    LogicalPlanNodePtr build_join_tree(const JoinCandidate& optimal_plan);
    
public:
    LogicalPlanNodePtr transform_node(LogicalPlanNodePtr node) override;
};

enum class JoinAlgorithm {
    NESTED_LOOP,     // Small outer table, any inner table
    HASH_JOIN,       // Large tables, equi-joins, enough memory
    MERGE_JOIN,      // Sorted inputs, equi-joins
    INDEX_JOIN       // One table has index on join key
};

class JoinAlgorithmSelector {
public:
    static JoinAlgorithm select_best_algorithm(const JoinCandidate& candidate,
                                             const StatisticsCollector& stats);
    static OptimizationCost estimate_join_cost(const JoinCandidate& candidate,
                                              JoinAlgorithm algorithm,
                                              const StatisticsCollector& stats);
};
```

**Algorithm Details:**
1. **Extract join graph**: Tables and join conditions
2. **Dynamic programming**: For 2-12 tables, find optimal order
3. **Heuristic approach**: For >12 tables, use greedy algorithm
4. **Algorithm selection**: Choose best join algorithm for each pair
5. **Cost estimation**: Accurate cost model for each join type

**Optimization Examples:**
```sql
-- Example 1: Star schema optimization
SELECT * FROM facts f 
JOIN dim_time t ON f.time_id = t.id 
JOIN dim_product p ON f.product_id = p.id 
JOIN dim_customer c ON f.customer_id = c.id
WHERE t.year = 2024 AND p.category = 'electronics';
-- Should start with most selective dimension (time or product)

-- Example 2: Chain join optimization  
SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id WHERE a.x = 1;
-- Should start with filtered table 'a'

-- Example 3: Complex join with multiple conditions
SELECT * FROM users u 
JOIN orders o ON u.id = o.user_id AND u.region = o.region
JOIN products p ON o.product_id = p.id
WHERE u.active = true AND p.price > 100;
-- Consider compound join conditions and filter selectivity
```

**Files to Modify:**
- `src/query_planner.cpp` - Replace JoinReorderingTransformer::transform_node
- `include/query_planner.hpp` - Add sophisticated join reordering methods

---

### **TASK 4.2: Join Algorithm Selection**
**Priority:** Medium | **Estimated Time:** 3 days

**Target Implementation:**
```cpp
struct JoinCostModel {
    // Nested Loop Join
    static OptimizationCost estimate_nested_loop_cost(size_t outer_rows, size_t inner_rows,
                                                     double join_selectivity, bool has_inner_index);
    
    // Hash Join  
    static OptimizationCost estimate_hash_join_cost(size_t build_rows, size_t probe_rows,
                                                   double join_selectivity, size_t available_memory);
    
    // Merge Join
    static OptimizationCost estimate_merge_join_cost(size_t left_rows, size_t right_rows,
                                                    double join_selectivity, bool inputs_sorted);
    
    // Index Join
    static OptimizationCost estimate_index_join_cost(size_t outer_rows, const IndexStatistics& index_stats,
                                                    double join_selectivity);
};
```

---

## 📈 **PHASE 5: ADVANCED OPTIMIZATION FRAMEWORK**

### **TASK 5.1: Multi-Pass Optimization**
**Priority:** Medium | **Estimated Time:** 3-4 days

**Target Implementation:**
```cpp
class MultiPassOptimizer {
private:
    std::vector<std::unique_ptr<PlanTransformer>> optimizers_;
    OptimizationConfig config_;
    
public:
    void add_optimizer(std::unique_ptr<PlanTransformer> optimizer);
    LogicalPlan optimize(const LogicalPlan& initial_plan);
    
private:
    bool has_plan_improved(const LogicalPlan& before, const LogicalPlan& after);
    LogicalPlan apply_optimization_pass(const LogicalPlan& plan);
};

struct OptimizationConfig {
    int max_passes = 5;
    double improvement_threshold = 0.01;  // 1% improvement required
    bool enable_aggressive_optimizations = false;
    size_t memory_limit = 1024 * 1024 * 1024;  // 1GB
};
```

**Algorithm:**
1. **Apply optimizations in order**: Predicate → Projection → Join reordering
2. **Iterate until convergence**: Stop when no significant improvement
3. **Cost validation**: Ensure each pass actually improves the plan
4. **Safety limits**: Prevent infinite optimization loops

---

### **TASK 5.2: Plan Quality Validation**
**Priority:** Medium | **Estimated Time:** 2 days

**Target Implementation:**
```cpp
class PlanValidator {
public:
    struct ValidationResult {
        bool is_valid = true;
        std::vector<std::string> warnings;
        std::vector<std::string> errors;
        double quality_score = 0.0;  // 0-100
    };
    
    static ValidationResult validate_plan(const LogicalPlan& plan);
    static ValidationResult compare_plans(const LogicalPlan& original, const LogicalPlan& optimized);
    
private:
    static bool check_semantic_correctness(const LogicalPlan& plan);
    static bool check_cost_estimates_reasonable(const LogicalPlan& plan);
    static double calculate_plan_quality_score(const LogicalPlan& plan);
};
```

---

## 🧪 **PHASE 6: COMPREHENSIVE TESTING**

### **TASK 6.1: Optimization Test Suite**
**Priority:** High | **Estimated Time:** 3-4 days

**Target Implementation:**
```cpp
class OptimizationTestSuite {
private:
    struct OptimizationTestCase {
        std::string name;
        std::string sql;
        std::string description;
        std::function<bool(const LogicalPlan&)> validator;
        double min_expected_improvement;
        std::vector<std::string> required_optimizations;
    };
    
    std::vector<OptimizationTestCase> predicate_tests_;
    std::vector<OptimizationTestCase> projection_tests_;
    std::vector<OptimizationTestCase> join_tests_;
    std::vector<OptimizationTestCase> integration_tests_;
    
public:
    void run_all_tests();
    void run_predicate_pushdown_tests();
    void run_projection_pushdown_tests();
    void run_join_reordering_tests();
    void run_performance_regression_tests();
    
    void generate_optimization_report();
};
```

**Test Categories:**

1. **Predicate Pushdown Tests:**
```cpp
// Test: Single table predicate pushdown
{"Single table filter", 
 "SELECT * FROM users WHERE active = true",
 "Should push active=true to users scan",
 [](const LogicalPlan& plan) { return has_scan_predicate(plan, "users", "active = true"); },
 1.5  // 50% improvement expected
},

// Test: Multi-table with pushable predicates
{"Multi-table pushdown",
 "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true AND o.status = 'completed'",
 "Should push both predicates to respective scans",
 [](const LogicalPlan& plan) { 
     return has_scan_predicate(plan, "users", "active = true") && 
            has_scan_predicate(plan, "orders", "status = 'completed'"); 
 },
 3.0  // 200% improvement expected
},
```

2. **Projection Pushdown Tests:**
```cpp
// Test: Column elimination
{"Column elimination",
 "SELECT name FROM users",  
 "Should project only name column (plus any join keys)",
 [](const LogicalPlan& plan) { return scan_projects_limited_columns(plan, "users", {"name"}); },
 2.0
},
```

3. **Join Reordering Tests:**
```cpp
// Test: Selective filter join reordering
{"Selective filter reordering",
 "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id WHERE a.rare_condition = true",
 "Should start with table 'a' due to selective filter",
 [](const LogicalPlan& plan) { return first_joined_table(plan) == "a"; },
 10.0  // 900% improvement expected
},
```

**Files to Create:**
- `tests/test_optimization_suite.cpp` - Comprehensive optimization tests
- `tests/optimization_test_helpers.hpp` - Test utility functions

---

### **TASK 6.2: Performance Benchmarking**
**Priority:** Medium | **Estimated Time:** 2-3 days

**Target Implementation:**
```cpp
class OptimizationBenchmark {
private:
    struct BenchmarkResult {
        std::string test_name;
        double original_cost;
        double optimized_cost;
        double improvement_factor;
        std::chrono::milliseconds optimization_time;
        bool optimization_successful;
    };
    
public:
    std::vector<BenchmarkResult> run_benchmarks();
    void generate_performance_report(const std::vector<BenchmarkResult>& results);
    
private:
    std::vector<std::string> load_benchmark_queries();
    BenchmarkResult benchmark_single_query(const std::string& sql);
};
```

---

## 📅 **IMPLEMENTATION TIMELINE**

### **Sprint 1 (Week 1-2): Foundation**
- [ ] **Task 1.1**: Enhanced cost model
- [ ] **Task 1.2**: Node analysis framework  
- [ ] **Task 1.3**: Statistics collection framework
- [ ] **Task 2.1**: Predicate pushdown implementation

### **Sprint 2 (Week 3-4): Core Optimizers**
- [ ] **Task 2.2**: Index-aware predicate pushdown
- [ ] **Task 3.1**: Projection pushdown implementation
- [ ] **Task 3.2**: Memory-aware projection optimization
- [ ] **Task 4.1**: Join reordering implementation

### **Sprint 3 (Week 5-6): Advanced Features**  
- [ ] **Task 4.2**: Join algorithm selection
- [ ] **Task 5.1**: Multi-pass optimization framework
- [ ] **Task 5.2**: Plan quality validation
- [ ] **Task 6.1**: Comprehensive test suite

### **Sprint 4 (Week 7): Testing & Polish**
- [ ] **Task 6.2**: Performance benchmarking
- [ ] Integration testing
- [ ] Documentation updates
- [ ] Performance tuning

---

## 🎯 **SUCCESS METRICS**

### **Performance Targets**
| Query Type | Current | Target | Improvement |
|------------|---------|---------|-------------|
| **Single table with WHERE** | Baseline | 2-5x faster | Predicate pushdown |
| **Multi-table JOINs** | Baseline | 10-100x faster | Join reordering |
| **Wide table SELECT** | Baseline | 2-10x faster | Projection pushdown |
| **Complex WHERE + JOIN** | Baseline | 20-500x faster | Combined optimizations |

### **Quality Metrics**
- [ ] **100% test coverage** for optimization transformers
- [ ] **Zero correctness regressions** in existing test suite
- [ ] **Optimization effectiveness** > 90% for standard TPC-H queries
- [ ] **Optimization time** < 1% of query execution time

---

## 📚 **REFERENCE MATERIALS**

### **Academic Papers**
- "Access Path Selection in a Relational Database Management System" (Selinger et al., 1979)
- "The Volcano Optimizer Generator: Extensibility and Efficient Search" (Graefe & McKenna, 1993)
- "Optimization of Queries with User-Defined Predicates" (Hellerstein & Stonebraker, 1993)

### **Industry References**
- PostgreSQL Query Planner Architecture
- Apache Calcite Optimization Framework
- SQL Server Query Optimizer Deep Dive

### **Implementation Patterns**
- Visitor Pattern for plan traversal
- Strategy Pattern for join algorithm selection  
- Chain of Responsibility for multi-pass optimization

---

## 📝 **NOTES & CONSIDERATIONS**

### **Implementation Notes**
- Use visitor pattern for consistent plan tree traversal
- Implement comprehensive cost estimation before optimization
- Add extensive logging for optimization decisions (debug mode)
- Consider thread safety for parallel optimization

### **Edge Cases to Handle**
- Circular join dependencies
- Very large join graphs (>20 tables)
- Queries with multiple optimization opportunities
- Memory-constrained environments
- Subqueries and CTEs

### **Future Enhancements**
- Adaptive optimization based on runtime statistics
- Machine learning-guided cost estimation
- Parallel query optimization
- Cross-query optimization opportunities

---

**END OF TODO LIST**

*This comprehensive plan transforms DB25's no-op optimizers into industry-standard query optimization components. Each task is designed to build upon previous work while delivering measurable performance improvements.*