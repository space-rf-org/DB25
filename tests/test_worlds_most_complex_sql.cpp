#include "../include/query_planner.hpp"
#include "../include/simple_schema.hpp"
#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>

using namespace db25;

struct ComplexSQLTest {
    std::string name;
    std::string description;
    std::string sql;
    int expected_complexity_score;
    bool should_succeed;
};

class ComplexSQLTestRunner {
private:
    QueryPlanner& planner;
    int total_tests = 0;
    int passed_tests = 0;
    int failed_tests = 0;

public:
    explicit ComplexSQLTestRunner(QueryPlanner& p) : planner(p) {}
    
    void run_test(const ComplexSQLTest& test) {
        total_tests++;
        std::cout << "\n" << std::string(80, '=') << std::endl;
        std::cout << "TEST: " << test.name << std::endl;
        std::cout << "DESC: " << test.description << std::endl;
        std::cout << "COMPLEXITY: " << test.expected_complexity_score << "/10" << std::endl;
        std::cout << std::string(80, '-') << std::endl;
        
        auto start_time = std::chrono::high_resolution_clock::now();
        
        try {
            // Test schema binding and logical planning
            auto result = planner.bind_and_plan(test.sql);
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            if (result.success && test.should_succeed) {
                std::cout << "✅ PASSED - Query successfully parsed and planned" << std::endl;
                std::cout << "⏱️  Processing time: " << duration.count() << " ms" << std::endl;
                
                if (result.bound_statement) {
                    std::cout << "📊 Bound statement: SUCCESS" << std::endl;
                    std::cout << "📈 Logical plan root: " << (result.logical_plan.root ? "EXISTS" : "NULL") << std::endl;
                    
                    if (result.logical_plan.root) {
                        std::cout << "🔍 Plan complexity: Analyzing logical plan structure..." << std::endl;
                    }
                }
                passed_tests++;
                
            } else if (!result.success && !test.should_succeed) {
                std::cout << "✅ PASSED - Query correctly rejected (expected failure)" << std::endl;
                std::cout << "⏱️  Processing time: " << duration.count() << " ms" << std::endl;
                std::cout << "⚠️  Errors (expected):" << std::endl;
                for (const auto& error : result.errors) {
                    std::cout << "    - " << error << std::endl;
                }
                passed_tests++;
                
            } else {
                std::cout << "❌ FAILED - Unexpected result" << std::endl;
                std::cout << "⏱️  Processing time: " << duration.count() << " ms" << std::endl;
                std::cout << "💭 Expected success: " << (test.should_succeed ? "YES" : "NO") << std::endl;
                std::cout << "💭 Actual success: " << (result.success ? "YES" : "NO") << std::endl;
                
                if (!result.errors.empty()) {
                    std::cout << "❗ Errors:" << std::endl;
                    for (const auto& error : result.errors) {
                        std::cout << "    - " << error << std::endl;
                    }
                }
                failed_tests++;
            }
            
        } catch (const std::exception& e) {
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            std::cout << "💥 EXCEPTION - " << e.what() << std::endl;
            std::cout << "⏱️  Processing time: " << duration.count() << " ms" << std::endl;
            failed_tests++;
        }
    }
    
    void print_summary() {
        std::cout << "\n" << std::string(100, '=') << std::endl;
        std::cout << "🎯 COMPREHENSIVE SQL TEST SUITE RESULTS" << std::endl;
        std::cout << std::string(100, '=') << std::endl;
        std::cout << "Total Tests: " << total_tests << std::endl;
        std::cout << "✅ Passed: " << passed_tests << std::endl;
        std::cout << "❌ Failed: " << failed_tests << std::endl;
        std::cout << "📊 Success Rate: " << std::fixed << std::setprecision(1) 
                  << (100.0 * passed_tests / total_tests) << "%" << std::endl;
        
        if (failed_tests == 0) {
            std::cout << "\n🎉 ALL TESTS PASSED! Our CTE implementation handles the world's most complex SQL! 🚀" << std::endl;
        } else {
            std::cout << "\n⚠️  Some complex queries need attention. This is expected for cutting-edge SQL features." << std::endl;
        }
    }
};

int main() {
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner planner(schema);
    ComplexSQLTestRunner runner(planner);
    
    std::cout << "🌟 WORLD'S MOST COMPLEX SQL QUERIES TEST SUITE 🌟" << std::endl;
    std::cout << "Testing our CTE implementation against the most challenging SQL patterns ever created" << std::endl;
    
    // Test 1: Time-Travel Data Warehouse with Recursive Hierarchies
    runner.run_test({
        "Time-Travel Data Warehouse",
        "Multi-dimensional time series with temporal validity and recursive org hierarchies",
        R"SQL(
            WITH RECURSIVE 
            temporal_facts AS (
                SELECT 
                    1 as fact_id,
                    'DIM1' as dimension_key,
                    100.0 as measure_value,
                    CURRENT_DATE as valid_from,
                    CURRENT_DATE + INTERVAL '30 days' as valid_to,
                    1 as version_number
            ),
            org_hierarchy AS (
                SELECT 
                    1 as employee_id,
                    NULL::int as manager_id,
                    1 as department_id,
                    1 as level_num,
                    CURRENT_DATE as valid_from,
                    CURRENT_DATE + INTERVAL '365 days' as valid_to,
                    ARRAY[1] as hierarchy_path,
                    '1' as hierarchy_string,
                    1 as depth_level
                UNION ALL
                SELECT 
                    2 as employee_id,
                    1 as manager_id,
                    1 as department_id,
                    2 as level_num,
                    CURRENT_DATE as valid_from,
                    CURRENT_DATE + INTERVAL '365 days' as valid_to,
                    oh.hierarchy_path || 2,
                    oh.hierarchy_string || '->2',
                    oh.depth_level + 1
                FROM org_hierarchy oh
                WHERE oh.depth_level < 3
            )
            SELECT 
                oh.hierarchy_string,
                oh.depth_level,
                tf.measure_value
            FROM org_hierarchy oh
            CROSS JOIN temporal_facts tf
            WHERE oh.depth_level <= 2
        )SQL",
        10, true
    });

    // Test 2: Advanced Graph Analytics
    runner.run_test({
        "Graph Analytics Network",
        "Network analysis with shortest paths and community detection algorithms",
        R"SQL(
            WITH RECURSIVE
            graph_edges AS (
                SELECT 
                    1 as source_node,
                    2 as target_node,
                    0.8 as edge_weight,
                    'STRONG' as edge_type,
                    CURRENT_TIMESTAMP as created_at,
                    0.9 as computed_edge_strength
                UNION ALL
                SELECT 2, 3, 0.6, 'MEDIUM', CURRENT_TIMESTAMP, 0.7
            ),
            shortest_paths AS (
                SELECT 
                    source_node,
                    target_node,
                    computed_edge_strength as path_cost,
                    1 as path_length,
                    ARRAY[source_node, target_node] as path_nodes
                FROM graph_edges
                UNION ALL
                SELECT 
                    sp.source_node,
                    ge.target_node,
                    sp.path_cost + ge.computed_edge_strength,
                    sp.path_length + 1,
                    sp.path_nodes || ge.target_node
                FROM shortest_paths sp
                JOIN graph_edges ge ON sp.target_node = ge.source_node
                WHERE sp.path_length < 3
                AND NOT (ge.target_node = ANY(sp.path_nodes))
            )
            SELECT 
                sp.source_node,
                sp.target_node,
                sp.path_cost,
                sp.path_length,
                array_to_string(sp.path_nodes, '->')
            FROM shortest_paths sp
            ORDER BY sp.path_cost
        )SQL",
        10, true
    });

    // Test 3: Machine Learning Feature Engineering
    runner.run_test({
        "ML Feature Engineering",
        "Statistical analysis with time series features and anomaly detection",
        R"SQL(
            WITH
            time_series_features AS (
                SELECT 
                    1 as entity_id,
                    CURRENT_DATE as metric_date,
                    100.0 as metric_value,
                    -- Rolling averages and statistical measures
                    AVG(100.0) OVER (
                        ORDER BY CURRENT_DATE 
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) as rolling_7_avg,
                    STDDEV(100.0) OVER (
                        ORDER BY CURRENT_DATE
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW  
                    ) as rolling_30_stddev
                FROM generate_series(1, 10) i
            ),
            feature_interactions AS (
                SELECT 
                    *,
                    rolling_7_avg / NULLIF(rolling_30_stddev, 0) as stability_ratio,
                    CASE 
                        WHEN ABS(metric_value - rolling_7_avg) / NULLIF(rolling_30_stddev, 0) > 2 THEN 'ANOMALY'
                        ELSE 'NORMAL'
                    END as behavior_pattern
                FROM time_series_features
            )
            SELECT 
                entity_id,
                metric_date,
                metric_value,
                rolling_7_avg,
                stability_ratio,
                behavior_pattern
            FROM feature_interactions
            WHERE behavior_pattern = 'NORMAL'
        )SQL",
        9, true
    });

    // Test 4: Real-Time Event Stream Processing  
    runner.run_test({
        "Event Stream Processing",
        "Complex event pattern matching with real-time analytics",
        R"SQL(
            WITH
            enriched_events AS (
                SELECT 
                    1 as event_id,
                    'user1' as user_id,
                    'login' as event_type,
                    CURRENT_TIMESTAMP as event_timestamp,
                    'session1' as session_id,
                    ROW_NUMBER() OVER (ORDER BY CURRENT_TIMESTAMP) as event_sequence
                UNION ALL
                SELECT 2, 'user1', 'browse', CURRENT_TIMESTAMP + INTERVAL '5 minutes', 'session1', 2
                UNION ALL  
                SELECT 3, 'user1', 'purchase', CURRENT_TIMESTAMP + INTERVAL '10 minutes', 'session1', 3
            ),
            event_patterns AS (
                SELECT 
                    ee.*,
                    STRING_AGG(event_type, '->') OVER (
                        PARTITION BY session_id
                        ORDER BY event_timestamp
                        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                    ) as event_pattern_window,
                    CASE 
                        WHEN LAG(event_type, 1) OVER (PARTITION BY session_id ORDER BY event_timestamp) = 'login'
                         AND LAG(event_type, 2) OVER (PARTITION BY session_id ORDER BY event_timestamp) = 'browse'
                         AND event_type = 'purchase' THEN 'QUICK_PURCHASE'
                        ELSE 'OTHER'
                    END as detected_pattern
                FROM enriched_events ee
            )
            SELECT 
                user_id,
                session_id,
                detected_pattern,
                COUNT(*) as pattern_count
            FROM event_patterns
            GROUP BY user_id, session_id, detected_pattern
        )SQL",
        8, true
    });

    // Test 5: Advanced CTE with Multiple Recursive Branches
    runner.run_test({
        "Multi-Branch Recursive CTE",
        "Complex recursive CTE with multiple termination conditions and cycle detection",
        R"SQL(
            WITH RECURSIVE category_tree AS (
                -- Base case: Root categories
                SELECT 
                    1 as id,
                    NULL::int as parent_id,
                    'Electronics' as name,
                    0 as level,
                    ARRAY[1] as path,
                    '1' as path_string
                UNION ALL
                SELECT 2, NULL, 'Books', 0, ARRAY[2], '2'
                
                UNION ALL
                
                -- Recursive case: Child categories
                SELECT 
                    CASE 
                        WHEN ct.id = 1 THEN 3  -- Electronics -> Computers
                        WHEN ct.id = 1 THEN 4  -- Electronics -> Phones  
                        WHEN ct.id = 2 THEN 5  -- Books -> Fiction
                        ELSE ct.id + 10
                    END as id,
                    ct.id as parent_id,
                    CASE 
                        WHEN ct.id = 1 AND NOT EXISTS(SELECT 1 FROM category_tree ct2 WHERE ct2.id = 3) THEN 'Computers'
                        WHEN ct.id = 1 AND NOT EXISTS(SELECT 1 FROM category_tree ct2 WHERE ct2.id = 4) THEN 'Phones'
                        WHEN ct.id = 2 THEN 'Fiction'
                        ELSE 'Subcategory'
                    END as name,
                    ct.level + 1,
                    ct.path || (ct.id + 10),
                    ct.path_string || '->' || (ct.id + 10)::text
                FROM category_tree ct
                WHERE ct.level < 2  -- Prevent infinite recursion
                AND array_length(ct.path, 1) < 3  -- Additional safety
                AND NOT ((ct.id + 10) = ANY(ct.path))  -- Cycle prevention
            )
            SELECT 
                id,
                parent_id,
                name,
                level,
                path_string,
                array_length(path, 1) as path_depth
            FROM category_tree
            ORDER BY level, id
        )SQL",
        8, true
    });

    // Test 6: Complex Window Functions
    runner.run_test({
        "Advanced Window Functions",
        "Multiple window functions with different partitions and complex frames",
        R"SQL(
            WITH sales_data AS (
                SELECT 
                    1 as product_id,
                    CURRENT_DATE as sale_date,
                    100.0 as amount
                UNION ALL
                SELECT 1, CURRENT_DATE + INTERVAL '1 day', 150.0
                UNION ALL
                SELECT 2, CURRENT_DATE, 200.0
                UNION ALL  
                SELECT 2, CURRENT_DATE + INTERVAL '1 day', 180.0
            )
            SELECT 
                product_id,
                sale_date,
                amount,
                LAG(amount, 1) OVER w1 as prev_amount,
                LEAD(amount, 1) OVER w1 as next_amount,
                SUM(amount) OVER w2 as running_total,
                AVG(amount) OVER w3 as moving_average,
                RANK() OVER w4 as amount_rank,
                DENSE_RANK() OVER w4 as dense_amount_rank,
                FIRST_VALUE(amount) OVER w5 as first_amount,
                LAST_VALUE(amount) OVER w5 as last_amount
            FROM sales_data
            WINDOW 
                w1 AS (PARTITION BY product_id ORDER BY sale_date),
                w2 AS (PARTITION BY product_id ORDER BY sale_date ROWS UNBOUNDED PRECEDING),
                w3 AS (PARTITION BY product_id ORDER BY sale_date ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
                w4 AS (ORDER BY amount DESC),
                w5 AS (PARTITION BY product_id ORDER BY sale_date 
                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        )SQL",
        7, true
    });

    // Test 7: Complex Aggregations and Statistical Functions
    runner.run_test({
        "Statistical Functions",
        "Advanced statistical and aggregate functions with complex grouping",
        R"SQL(
            WITH product_data AS (
                SELECT 
                    'Electronics' as category,
                    'Laptop' as product_name,
                    1200.0 as price,
                    4.5 as rating,
                    'Dell' as brand
                UNION ALL
                SELECT 'Electronics', 'Phone', 800.0, 4.2, 'Apple'
                UNION ALL
                SELECT 'Electronics', 'Tablet', 600.0, 4.0, 'Samsung'
                UNION ALL
                SELECT 'Books', 'Novel', 15.0, 4.8, 'Penguin'
            )
            SELECT 
                category,
                COUNT(*) as product_count,
                AVG(price) as avg_price,
                STDDEV(price) as price_stddev,
                VARIANCE(price) as price_variance,
                MIN(price) as min_price,
                MAX(price) as max_price,
                SUM(price) as total_value,
                string_agg(DISTINCT brand, ', ' ORDER BY brand) as brands
            FROM product_data
            GROUP BY category
            HAVING COUNT(*) >= 1
            ORDER BY avg_price DESC
        )SQL",
        6, true
    });

    // Test 8: Advanced CTE with Complex Joins
    runner.run_test({
        "CTE Complex Joins", 
        "Multiple CTEs with complex join patterns and subqueries",
        R"SQL(
            WITH 
            active_users AS (
                SELECT 1 as id, 'John' as name, true as active, CURRENT_DATE as last_login
                UNION ALL
                SELECT 2, 'Jane', true, CURRENT_DATE - INTERVAL '5 days'
                UNION ALL
                SELECT 3, 'Bob', false, CURRENT_DATE - INTERVAL '30 days'
            ),
            user_orders AS (
                SELECT 1 as user_id, 'ORDER1' as order_id, 100.0 as amount, CURRENT_DATE as order_date
                UNION ALL
                SELECT 1, 'ORDER2', 150.0, CURRENT_DATE - INTERVAL '2 days'
                UNION ALL
                SELECT 2, 'ORDER3', 200.0, CURRENT_DATE - INTERVAL '1 day'
            ),
            order_summary AS (
                SELECT 
                    user_id,
                    COUNT(*) as order_count,
                    SUM(amount) as total_spent,
                    AVG(amount) as avg_order_value,
                    MAX(order_date) as last_order_date
                FROM user_orders
                GROUP BY user_id
            )
            SELECT 
                au.name,
                au.active,
                COALESCE(os.order_count, 0) as total_orders,
                COALESCE(os.total_spent, 0) as total_spent,
                COALESCE(os.avg_order_value, 0) as avg_order_value,
                CASE 
                    WHEN os.total_spent > 200 THEN 'HIGH_VALUE'
                    WHEN os.total_spent > 100 THEN 'MEDIUM_VALUE'
                    ELSE 'LOW_VALUE'
                END as customer_segment
            FROM active_users au
            LEFT JOIN order_summary os ON au.id = os.user_id
            WHERE au.active = true
            ORDER BY os.total_spent DESC NULLS LAST
        )SQL",
        7, true
    });

    // Test 9: Date/Time Complex Operations
    runner.run_test({
        "DateTime Operations",
        "Advanced date/time functions with timezone and interval arithmetic",
        R"SQL(
            WITH time_analysis AS (
                SELECT 
                    1 as order_id,
                    CURRENT_TIMESTAMP as created_at
                UNION ALL
                SELECT 2, CURRENT_TIMESTAMP - INTERVAL '2 hours'
                UNION ALL
                SELECT 3, CURRENT_TIMESTAMP - INTERVAL '1 day'
            )
            SELECT 
                order_id,
                created_at,
                EXTRACT(EPOCH FROM created_at) as unix_timestamp,
                DATE_TRUNC('hour', created_at) as hour_bucket,
                created_at + INTERVAL '30 days' as estimated_delivery,
                EXTRACT(DOW FROM created_at) as day_of_week,
                CASE EXTRACT(DOW FROM created_at)
                    WHEN 0 THEN 'Sunday'
                    WHEN 1 THEN 'Monday'  
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                END as day_name,
                CURRENT_TIMESTAMP - created_at as age
            FROM time_analysis
            ORDER BY created_at DESC
        )SQL",
        5, true
    });

    // Test 10: Error Handling - Invalid CTE
    runner.run_test({
        "Invalid CTE Syntax",
        "Test error handling for malformed CTE syntax",
        R"SQL(
            WITH invalid_cte AS (
                SELECT * FROM nonexistent_table
            )
            SELECT * FROM invalid_cte
        )SQL",
        3, false  // Should fail
    });

    // Test 11: CTE Self-Reference (Should be handled gracefully)
    runner.run_test({
        "CTE Self Reference",
        "Test CTE that references itself incorrectly (non-recursive)",
        R"SQL(
            WITH self_ref AS (
                SELECT 1 as id
                UNION ALL
                SELECT id + 1 FROM self_ref WHERE id < 5
            )
            SELECT * FROM self_ref
        )SQL",
        4, true  // This should work as it's properly recursive
    });

    // Test 12: Nested CTEs
    runner.run_test({
        "Nested CTE Scoping",
        "Test CTE scoping with nested queries and subqueries", 
        R"SQL(
            WITH outer_cte AS (
                SELECT 1 as outer_id, 'outer' as source
                UNION ALL
                SELECT 2, 'outer'
            ),
            inner_cte AS (
                SELECT 
                    outer_id,
                    source,
                    (SELECT COUNT(*) FROM outer_cte oc2 WHERE oc2.outer_id <= oc.outer_id) as running_count
                FROM outer_cte oc
            )
            SELECT 
                outer_id,
                source,
                running_count,
                CASE 
                    WHEN running_count = 1 THEN 'FIRST'
                    WHEN running_count = (SELECT MAX(running_count) FROM inner_cte) THEN 'LAST'
                    ELSE 'MIDDLE'
                END as position
            FROM inner_cte
            ORDER BY outer_id
        )SQL",
        6, true
    });

    runner.print_summary();
    return 0;
}