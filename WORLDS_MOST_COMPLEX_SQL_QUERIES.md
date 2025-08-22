# The World's Most Complex SQL Queries - PostgreSQL Test Suite

## Overview

This test suite contains the most complex SQL queries in the world, designed to test every aspect of PostgreSQL functionality. These queries demonstrate advanced SQL patterns, complex business logic, and edge cases that push database engines to their limits.

**Note: Physical planning tests excluded as requested.**

---

## 🌟 THE TOP 10 MOST COMPLEX SQL QUERIES IN THE WORLD

### 1. **Time-Travel Data Warehouse with Recursive Hierarchies** ⭐⭐⭐⭐⭐
**Complexity: EXTREME | Lines: ~80 | Features: 15+**

```sql
WITH RECURSIVE 
-- Multi-dimensional time series with temporal validity
temporal_facts AS (
    SELECT 
        fact_id,
        dimension_key,
        measure_value,
        valid_from,
        valid_to,
        version_number,
        LAG(valid_to) OVER (
            PARTITION BY fact_id, dimension_key 
            ORDER BY valid_from
        ) AS prev_valid_to,
        LEAD(valid_from) OVER (
            PARTITION BY fact_id, dimension_key 
            ORDER BY valid_from  
        ) AS next_valid_from
    FROM fact_table 
    WHERE deleted_at IS NULL
),

-- Recursive organizational hierarchy with temporal changes
org_hierarchy AS (
    -- Base case: top-level executives
    SELECT 
        employee_id,
        manager_id,
        department_id,
        level_num,
        valid_from,
        valid_to,
        ARRAY[employee_id] AS hierarchy_path,
        employee_id::text AS hierarchy_string,
        1 as depth_level
    FROM employees 
    WHERE manager_id IS NULL 
    AND CURRENT_DATE BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31'::date)
    
    UNION ALL
    
    -- Recursive case: subordinates at each level  
    SELECT 
        e.employee_id,
        e.manager_id,
        e.department_id,
        e.level_num,
        e.valid_from,
        e.valid_to,
        oh.hierarchy_path || e.employee_id,
        oh.hierarchy_string || '->' || e.employee_id::text,
        oh.depth_level + 1
    FROM employees e
    INNER JOIN org_hierarchy oh ON e.manager_id = oh.employee_id
    WHERE e.valid_from <= oh.valid_to 
    AND COALESCE(e.valid_to, '9999-12-31'::date) >= oh.valid_from
    AND oh.depth_level < 10  -- Prevent infinite recursion
),

-- Complex windowing with multiple partitions and advanced analytics
revenue_analytics AS (
    SELECT 
        r.product_id,
        r.region_id,
        r.revenue_date,
        r.revenue_amount,
        -- Multiple window functions with different partitions
        SUM(r.revenue_amount) OVER (
            PARTITION BY r.product_id 
            ORDER BY r.revenue_date 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS rolling_12_month_revenue,
        
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r.revenue_amount) OVER (
            PARTITION BY r.region_id, EXTRACT(YEAR FROM r.revenue_date)
        ) AS regional_median_revenue,
        
        -- Advanced lag/lead with complex conditions
        FIRST_VALUE(r.revenue_amount) OVER (
            PARTITION BY r.product_id, r.region_id
            ORDER BY r.revenue_date DESC
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS most_recent_revenue,
        
        -- Custom aggregations with filtering
        COUNT(CASE WHEN r.revenue_amount > 
            AVG(r.revenue_amount) OVER (PARTITION BY r.region_id) 
            THEN 1 END
        ) OVER (
            PARTITION BY r.product_id 
            ORDER BY r.revenue_date
        ) AS above_avg_count,
        
        -- Complex ranking with ties handling
        DENSE_RANK() OVER (
            PARTITION BY r.region_id, DATE_TRUNC('quarter', r.revenue_date)
            ORDER BY r.revenue_amount DESC, r.product_id
        ) AS quarterly_product_rank
        
    FROM revenue r
    WHERE r.revenue_date >= CURRENT_DATE - INTERVAL '5 years'
),

-- Advanced set operations with temporal logic
market_segments AS (
    -- High-value customers with complex segmentation
    SELECT 
        customer_id,
        'HIGH_VALUE' as segment,
        segment_start_date,
        segment_end_date,
        RANK() OVER (ORDER BY total_lifetime_value DESC) as value_rank
    FROM (
        SELECT 
            c.customer_id,
            SUM(o.order_amount) as total_lifetime_value,
            MIN(o.order_date) as segment_start_date,
            MAX(o.order_date) as segment_end_date
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        WHERE o.order_date >= CURRENT_DATE - INTERVAL '2 years'
        GROUP BY c.customer_id
        HAVING SUM(o.order_amount) > 10000
    ) high_value
    
    UNION
    
    -- Churned customers with predictive indicators
    SELECT 
        customer_id,
        'CHURNED' as segment,
        last_order_date as segment_start_date,
        CURRENT_DATE as segment_end_date,
        NULL as value_rank
    FROM (
        SELECT 
            c.customer_id,
            MAX(o.order_date) as last_order_date,
            COUNT(o.order_id) as total_orders
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id
        HAVING MAX(o.order_date) < CURRENT_DATE - INTERVAL '6 months'
        OR MAX(o.order_date) IS NULL
    ) churned
    
    UNION
    
    -- New customers with acquisition metrics
    SELECT 
        customer_id,
        'NEW_ACQUISITION' as segment,
        first_order_date as segment_start_date,
        CURRENT_DATE as segment_end_date,
        NULL as value_rank
    FROM (
        SELECT 
            c.customer_id,
            MIN(o.order_date) as first_order_date
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id
        HAVING MIN(o.order_date) >= CURRENT_DATE - INTERVAL '3 months'
    ) new_customers
)

-- Final mega-query combining all CTEs with advanced joins and analytics
SELECT 
    oh.hierarchy_string,
    oh.depth_level,
    p.product_name,
    r.region_name,
    ra.revenue_date,
    
    -- Complex calculated fields
    CASE 
        WHEN ra.quarterly_product_rank <= 5 THEN 'TOP_PERFORMER'
        WHEN ra.quarterly_product_rank <= 20 THEN 'GOOD_PERFORMER'  
        ELSE 'NEEDS_IMPROVEMENT'
    END as performance_category,
    
    -- Advanced aggregations with conditions
    ra.rolling_12_month_revenue,
    ra.regional_median_revenue,
    
    -- Temporal calculations  
    EXTRACT(EPOCH FROM (tf.valid_to - tf.valid_from)) / 86400 as validity_days,
    
    -- Complex string operations
    REGEXP_REPLACE(
        UPPER(CONCAT(p.product_name, '-', r.region_name)),
        '[^A-Z0-9]', 
        '_', 
        'g'
    ) as normalized_identifier,
    
    -- Advanced conditional aggregations
    SUM(CASE 
        WHEN ms.segment = 'HIGH_VALUE' THEN ra.revenue_amount 
        ELSE 0 
    END) OVER (
        PARTITION BY oh.employee_id, DATE_TRUNC('year', ra.revenue_date)
    ) as high_value_customer_revenue,
    
    -- Statistical functions
    CORR(ra.revenue_amount, ra.above_avg_count) OVER (
        PARTITION BY ra.product_id
        ORDER BY ra.revenue_date
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW  
    ) as revenue_performance_correlation,
    
    -- JSON aggregations for complex reporting
    JSONB_AGG(
        JSONB_BUILD_OBJECT(
            'date', ra.revenue_date,
            'amount', ra.revenue_amount,
            'rank', ra.quarterly_product_rank,
            'segment', ms.segment
        ) ORDER BY ra.revenue_date
    ) OVER (
        PARTITION BY ra.product_id, ra.region_id
        ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
    ) as revenue_context_window

FROM org_hierarchy oh
CROSS JOIN revenue_analytics ra
JOIN temporal_facts tf ON tf.dimension_key = CONCAT(ra.product_id, '-', ra.region_id)
    AND ra.revenue_date BETWEEN tf.valid_from AND COALESCE(tf.valid_to, CURRENT_DATE)
LEFT JOIN products p ON ra.product_id = p.product_id
LEFT JOIN regions r ON ra.region_id = r.region_id  
LEFT JOIN market_segments ms ON ms.customer_id = ra.customer_id
    AND ra.revenue_date BETWEEN ms.segment_start_date AND ms.segment_end_date

WHERE oh.depth_level BETWEEN 2 AND 6
AND ra.revenue_amount IS NOT NULL
AND (
    ra.quarterly_product_rank <= 10 
    OR ms.segment IN ('HIGH_VALUE', 'NEW_ACQUISITION')
    OR ra.rolling_12_month_revenue > (
        SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY rolling_12_month_revenue)
        FROM revenue_analytics
    )
)

ORDER BY 
    oh.depth_level,
    oh.hierarchy_string,
    ra.quarterly_product_rank,
    ra.revenue_date DESC

LIMIT 10000;
```

### 2. **Advanced Graph Analytics with Temporal Networks** ⭐⭐⭐⭐⭐
**Complexity: EXTREME | Lines: ~70 | Features: Network Analysis, Graph Algorithms**

```sql
WITH RECURSIVE
-- Build dynamic graph structure with weighted edges
graph_edges AS (
    SELECT 
        source_node,
        target_node,
        edge_weight,
        edge_type,
        created_at,
        -- Calculate edge strength based on multiple factors
        (edge_weight * 
         EXP(-EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_at)) / 86400.0 / 30.0) *
         CASE edge_type 
            WHEN 'STRONG' THEN 2.0
            WHEN 'MEDIUM' THEN 1.0  
            WHEN 'WEAK' THEN 0.5
            ELSE 0.1 
         END
        ) AS computed_edge_strength,
        
        -- Advanced graph metrics
        DENSE_RANK() OVER (
            PARTITION BY source_node 
            ORDER BY edge_weight DESC, created_at DESC
        ) AS outbound_edge_rank,
        
        COUNT(*) OVER (PARTITION BY source_node) AS node_out_degree,
        COUNT(*) OVER (PARTITION BY target_node) AS node_in_degree
        
    FROM network_connections nc
    WHERE nc.active = true
    AND nc.created_at >= CURRENT_DATE - INTERVAL '1 year'
),

-- Shortest path algorithm implementation
shortest_paths AS (
    -- Base case: direct connections
    SELECT 
        source_node,
        target_node,
        computed_edge_strength as path_cost,
        1 as path_length,
        ARRAY[source_node, target_node] as path_nodes,
        source_node::text || '->' || target_node::text as path_string
    FROM graph_edges
    WHERE computed_edge_strength > 0.1  -- Filter weak connections
    
    UNION ALL
    
    -- Recursive case: extend paths
    SELECT 
        sp.source_node,
        ge.target_node,
        sp.path_cost + ge.computed_edge_strength,
        sp.path_length + 1,
        sp.path_nodes || ge.target_node,
        sp.path_string || '->' || ge.target_node::text
    FROM shortest_paths sp
    JOIN graph_edges ge ON sp.target_node = ge.source_node
    WHERE sp.path_length < 6  -- Limit path depth
    AND NOT (ge.target_node = ANY(sp.path_nodes))  -- Prevent cycles
    AND sp.path_cost + ge.computed_edge_strength < 10.0  -- Prune expensive paths
),

-- Community detection using modularity optimization
community_analysis AS (
    SELECT 
        node_id,
        -- Simulate community detection algorithm
        NTILE(LEAST(50, COUNT(*) OVER() / 10)) OVER (
            ORDER BY 
                total_edge_strength DESC,
                clustering_coefficient DESC,
                node_betweenness DESC
        ) as community_id,
        
        total_edge_strength,
        clustering_coefficient,
        node_betweenness,
        
        -- Page rank approximation
        (total_edge_strength / NULLIF(total_nodes, 0)) * 
        (1 - 0.85) + 0.85 * (weighted_in_strength / NULLIF(max_in_strength, 0)) as page_rank_score
        
    FROM (
        SELECT 
            COALESCE(ge1.source_node, ge2.target_node) as node_id,
            
            -- Total connection strength
            SUM(COALESCE(ge1.computed_edge_strength, 0) + 
                COALESCE(ge2.computed_edge_strength, 0)) as total_edge_strength,
                
            -- Clustering coefficient approximation
            COUNT(DISTINCT ge1.target_node) * COUNT(DISTINCT ge2.source_node) / 
            NULLIF(POWER(COUNT(DISTINCT COALESCE(ge1.target_node, ge2.source_node)), 2), 0) 
            as clustering_coefficient,
            
            -- Betweenness centrality approximation
            COUNT(DISTINCT sp.source_node) * COUNT(DISTINCT sp.target_node) /
            NULLIF(COUNT(*) OVER (), 0) as node_betweenness,
            
            COUNT(*) OVER () as total_nodes,
            SUM(ge2.computed_edge_strength) as weighted_in_strength,
            MAX(SUM(ge2.computed_edge_strength)) OVER () as max_in_strength
            
        FROM graph_edges ge1
        FULL OUTER JOIN graph_edges ge2 ON ge1.source_node = ge2.target_node
        LEFT JOIN shortest_paths sp ON sp.source_node = COALESCE(ge1.source_node, ge2.target_node)
            OR sp.target_node = COALESCE(ge1.source_node, ge2.target_node)
        GROUP BY COALESCE(ge1.source_node, ge2.target_node)
    ) node_metrics
),

-- Time-based network evolution analysis  
temporal_network_changes AS (
    SELECT 
        time_window,
        community_id,
        
        -- Network topology changes
        COUNT(DISTINCT node_id) as community_size,
        AVG(page_rank_score) as avg_page_rank,
        SUM(total_edge_strength) as community_strength,
        
        -- Temporal stability metrics
        LAG(COUNT(DISTINCT node_id)) OVER (
            PARTITION BY community_id ORDER BY time_window
        ) as prev_community_size,
        
        -- Growth and decay patterns
        CASE 
            WHEN LAG(COUNT(DISTINCT node_id)) OVER (
                PARTITION BY community_id ORDER BY time_window
            ) IS NULL THEN 'NEW_COMMUNITY'
            WHEN COUNT(DISTINCT node_id) > LAG(COUNT(DISTINCT node_id)) OVER (
                PARTITION BY community_id ORDER BY time_window
            ) THEN 'GROWING'
            WHEN COUNT(DISTINCT node_id) < LAG(COUNT(DISTINCT node_id)) OVER (
                PARTITION BY community_id ORDER BY time_window  
            ) THEN 'SHRINKING'
            ELSE 'STABLE'
        END as community_trend,
        
        -- Advanced statistical measures
        VARIANCE(page_rank_score) as page_rank_variance,
        CORR(total_edge_strength, clustering_coefficient) as strength_clustering_correlation
        
    FROM (
        SELECT 
            ca.*,
            DATE_TRUNC('month', ge.created_at) as time_window
        FROM community_analysis ca
        JOIN graph_edges ge ON ca.node_id IN (ge.source_node, ge.target_node)
    ) ca_temporal
    GROUP BY time_window, community_id
    HAVING COUNT(DISTINCT node_id) >= 3  -- Filter small communities
)

-- Final complex network analysis query
SELECT 
    tnc.time_window,
    tnc.community_id,
    tnc.community_size,
    tnc.community_trend,
    
    -- Advanced network metrics
    tnc.avg_page_rank,
    tnc.community_strength,
    tnc.strength_clustering_correlation,
    
    -- Cross-community analysis
    RANK() OVER (
        PARTITION BY tnc.time_window 
        ORDER BY tnc.community_strength DESC
    ) as community_strength_rank,
    
    -- Temporal evolution metrics
    (tnc.community_size - tnc.prev_community_size) / 
    NULLIF(tnc.prev_community_size::float, 0) as growth_rate,
    
    -- Network influence scoring
    (tnc.avg_page_rank * tnc.community_size * tnc.community_strength) /
    NULLIF(SUM(tnc.community_strength) OVER (PARTITION BY tnc.time_window), 0) 
    as network_influence_score,
    
    -- Most influential nodes in community
    STRING_AGG(
        DISTINCT ca.node_id::text || '(' || ROUND(ca.page_rank_score, 3) || ')',
        ', ' 
        ORDER BY ca.page_rank_score DESC
    ) FILTER (WHERE ca.page_rank_score > tnc.avg_page_rank) as top_influential_nodes,
    
    -- Network stability indicators
    CASE 
        WHEN tnc.page_rank_variance < 0.1 THEN 'HIGHLY_STABLE'
        WHEN tnc.page_rank_variance < 0.3 THEN 'STABLE'
        WHEN tnc.page_rank_variance < 0.6 THEN 'MODERATE_VOLATILITY'
        ELSE 'HIGH_VOLATILITY'
    END as stability_classification

FROM temporal_network_changes tnc
JOIN community_analysis ca ON tnc.community_id = ca.community_id
JOIN graph_edges ge ON ca.node_id IN (ge.source_node, ge.target_node)
    AND DATE_TRUNC('month', ge.created_at) = tnc.time_window

WHERE tnc.time_window >= CURRENT_DATE - INTERVAL '2 years'
AND tnc.community_size >= 5

GROUP BY 
    tnc.time_window,
    tnc.community_id,
    tnc.community_size,
    tnc.community_trend,
    tnc.avg_page_rank,
    tnc.community_strength,
    tnc.strength_clustering_correlation,
    tnc.prev_community_size,
    tnc.page_rank_variance

ORDER BY 
    tnc.time_window DESC,
    network_influence_score DESC,
    tnc.community_strength DESC

LIMIT 500;
```

### 3. **Machine Learning Feature Engineering Pipeline** ⭐⭐⭐⭐⭐
**Complexity: EXTREME | Lines: ~75 | Features: Statistical Analysis, Feature Engineering**

```sql
WITH
-- Feature extraction for time series forecasting
time_series_features AS (
    SELECT 
        entity_id,
        metric_date,
        metric_value,
        
        -- Trend analysis features
        metric_value - LAG(metric_value, 1) OVER w as daily_change,
        metric_value - LAG(metric_value, 7) OVER w as weekly_change,
        metric_value - LAG(metric_value, 30) OVER w as monthly_change,
        
        -- Rolling statistical features  
        AVG(metric_value) OVER (
            PARTITION BY entity_id 
            ORDER BY metric_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7_avg,
        
        STDDEV(metric_value) OVER (
            PARTITION BY entity_id
            ORDER BY metric_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW  
        ) as rolling_30_stddev,
        
        -- Advanced statistical measures
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY metric_value) OVER (
            PARTITION BY entity_id
            ORDER BY metric_date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as rolling_14_q1,
        
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) OVER (
            PARTITION BY entity_id  
            ORDER BY metric_date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as rolling_14_q3,
        
        -- Seasonality detection
        AVG(metric_value) OVER (
            PARTITION BY entity_id, EXTRACT(DOW FROM metric_date)
            ORDER BY metric_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as day_of_week_avg,
        
        AVG(metric_value) OVER (
            PARTITION BY entity_id, EXTRACT(MONTH FROM metric_date)
            ORDER BY metric_date  
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as month_avg,
        
        -- Volatility measures
        ABS(metric_value - AVG(metric_value) OVER (
            PARTITION BY entity_id
            ORDER BY metric_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )) / NULLIF(STDDEV(metric_value) OVER (
            PARTITION BY entity_id
            ORDER BY metric_date  
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 0) as z_score,
        
        -- Momentum indicators
        CASE 
            WHEN LAG(metric_value, 1) OVER w > LAG(metric_value, 2) OVER w 
                AND metric_value > LAG(metric_value, 1) OVER w THEN 1
            WHEN LAG(metric_value, 1) OVER w < LAG(metric_value, 2) OVER w
                AND metric_value < LAG(metric_value, 1) OVER w THEN -1  
            ELSE 0
        END as momentum_direction,
        
        -- Autocorrelation approximation
        CORR(metric_value, LAG(metric_value, 1) OVER w) OVER (
            PARTITION BY entity_id
            ORDER BY metric_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as lag1_autocorr
        
    FROM metrics_data
    WINDOW w AS (PARTITION BY entity_id ORDER BY metric_date)
),

-- Advanced feature engineering with interactions
feature_interactions AS (
    SELECT 
        *,
        
        -- Feature interactions and ratios
        rolling_7_avg / NULLIF(rolling_30_stddev, 0) as stability_ratio,
        
        (rolling_14_q3 - rolling_14_q1) / NULLIF(rolling_14_q1, 0) as iqr_ratio,
        
        ABS(metric_value - day_of_week_avg) / NULLIF(day_of_week_avg, 0) as dow_deviation,
        
        ABS(metric_value - month_avg) / NULLIF(month_avg, 0) as monthly_deviation,
        
        -- Complex composite features
        CASE 
            WHEN ABS(z_score) > 2 AND momentum_direction != 0 THEN 'ANOMALY_WITH_TREND'
            WHEN ABS(z_score) > 2 THEN 'ANOMALY_STATIONARY'  
            WHEN momentum_direction != 0 THEN 'TREND_NORMAL'
            ELSE 'NORMAL'
        END as behavior_pattern,
        
        -- Technical indicators
        EXP(-0.1 * EXTRACT(EPOCH FROM (CURRENT_DATE - metric_date)) / 86400.0) as time_decay_weight,
        
        TANH((metric_value - rolling_7_avg) / NULLIF(rolling_30_stddev, 0)) as normalized_deviation,
        
        -- Fourier transform approximation for cyclical patterns
        SIN(2 * PI() * EXTRACT(DOY FROM metric_date) / 365.25) as annual_sine,
        COS(2 * PI() * EXTRACT(DOY FROM metric_date) / 365.25) as annual_cosine,
        SIN(2 * PI() * EXTRACT(DOW FROM metric_date) / 7.0) as weekly_sine,
        COS(2 * PI() * EXTRACT(DOW FROM metric_date) / 7.0) as weekly_cosine
        
    FROM time_series_features
    WHERE rolling_30_stddev IS NOT NULL  -- Ensure enough history
),

-- Clustering features using statistical similarity
entity_clusters AS (
    SELECT 
        entity_id,
        
        -- Statistical profile of entity
        AVG(metric_value) as mean_value,
        STDDEV(metric_value) as stddev_value,  
        MIN(metric_value) as min_value,
        MAX(metric_value) as max_value,
        
        -- Behavioral characteristics
        AVG(ABS(daily_change)) as avg_daily_volatility,
        CORR(metric_value, EXTRACT(DOY FROM metric_date)) as seasonality_strength,
        AVG(ABS(z_score)) as avg_z_score,
        
        -- Trend characteristics
        REGR_SLOPE(metric_value, EXTRACT(EPOCH FROM metric_date)) as overall_trend,
        CORR(metric_value, EXTRACT(EPOCH FROM metric_date)) as trend_strength,
        
        -- Pattern classification
        MODE() WITHIN GROUP (ORDER BY behavior_pattern) as dominant_pattern,
        COUNT(DISTINCT behavior_pattern) as pattern_diversity,
        
        -- Advanced metrics
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ABS(normalized_deviation)) as extreme_deviation_threshold,
        AVG(lag1_autocorr) as avg_autocorrelation
        
    FROM feature_interactions
    WHERE metric_date >= CURRENT_DATE - INTERVAL '1 year'
    GROUP BY entity_id
    HAVING COUNT(*) >= 30  -- Ensure sufficient data
),

-- K-means style clustering using statistical distance
similarity_matrix AS (
    SELECT 
        ec1.entity_id as entity1,
        ec2.entity_id as entity2,
        
        -- Multi-dimensional distance calculation
        SQRT(
            POWER((ec1.mean_value - ec2.mean_value) / GREATEST(ec1.stddev_value, ec2.stddev_value, 0.001), 2) +
            POWER((ec1.avg_daily_volatility - ec2.avg_daily_volatility) / 
                   GREATEST(ec1.avg_daily_volatility, ec2.avg_daily_volatility, 0.001), 2) +
            POWER(ec1.seasonality_strength - ec2.seasonality_strength, 2) +
            POWER(ec1.trend_strength - ec2.trend_strength, 2) +
            POWER(ec1.avg_autocorrelation - ec2.avg_autocorrelation, 2)
        ) as statistical_distance,
        
        -- Cosine similarity for behavioral patterns
        (ec1.mean_value * ec2.mean_value + 
         ec1.avg_daily_volatility * ec2.avg_daily_volatility + 
         ec1.seasonality_strength * ec2.seasonality_strength) /
        NULLIF(SQRT(
            POWER(ec1.mean_value, 2) + POWER(ec1.avg_daily_volatility, 2) + POWER(ec1.seasonality_strength, 2)
        ) * SQRT(
            POWER(ec2.mean_value, 2) + POWER(ec2.avg_daily_volatility, 2) + POWER(ec2.seasonality_strength, 2)
        ), 0) as cosine_similarity
        
    FROM entity_clusters ec1
    CROSS JOIN entity_clusters ec2  
    WHERE ec1.entity_id < ec2.entity_id  -- Avoid duplicate pairs
    AND ec1.dominant_pattern = ec2.dominant_pattern  -- Same behavioral pattern
),

-- Anomaly detection using isolation forest approximation
anomaly_detection AS (
    SELECT 
        fi.*,
        ec.dominant_pattern,
        ec.avg_autocorrelation,
        
        -- Isolation score approximation
        (ABS(fi.z_score) + 
         ABS(fi.dow_deviation) * 2 + 
         ABS(fi.monthly_deviation) * 1.5 +
         CASE fi.behavior_pattern 
            WHEN 'ANOMALY_WITH_TREND' THEN 3.0
            WHEN 'ANOMALY_STATIONARY' THEN 2.5  
            ELSE 0.0
         END) as composite_anomaly_score,
         
        -- Context-aware anomaly scoring  
        NTILE(100) OVER (
            PARTITION BY fi.entity_id
            ORDER BY ABS(fi.z_score) + ABS(fi.normalized_deviation)
        ) as percentile_rank,
        
        -- Multi-variate outlier detection
        MAHALANOBIS_DISTANCE(
            ARRAY[fi.metric_value, fi.rolling_7_avg, fi.rolling_30_stddev, fi.stability_ratio],
            ARRAY[
                AVG(fi.metric_value) OVER (PARTITION BY fi.entity_id),
                AVG(fi.rolling_7_avg) OVER (PARTITION BY fi.entity_id),
                AVG(fi.rolling_30_stddev) OVER (PARTITION BY fi.entity_id),
                AVG(fi.stability_ratio) OVER (PARTITION BY fi.entity_id)
            ]
        ) as mahalanobis_distance
        
    FROM feature_interactions fi  
    JOIN entity_clusters ec ON fi.entity_id = ec.entity_id
)

-- Final ML feature dataset with comprehensive feature engineering
SELECT 
    ad.entity_id,
    ad.metric_date,
    ad.metric_value,
    
    -- Original engineered features
    ad.daily_change,
    ad.weekly_change, 
    ad.monthly_change,
    ad.rolling_7_avg,
    ad.rolling_30_stddev,
    ad.z_score,
    ad.momentum_direction,
    ad.lag1_autocorr,
    
    -- Advanced composite features
    ad.stability_ratio,
    ad.iqr_ratio,
    ad.dow_deviation,
    ad.monthly_deviation,
    ad.behavior_pattern,
    ad.normalized_deviation,
    
    -- Cyclical features  
    ad.annual_sine,
    ad.annual_cosine,
    ad.weekly_sine,
    ad.weekly_cosine,
    
    -- Entity cluster features
    ad.dominant_pattern,
    ad.avg_autocorrelation,
    
    -- Anomaly detection features
    ad.composite_anomaly_score,
    ad.percentile_rank,
    ad.mahalanobis_distance,
    
    -- Target variable engineering
    LEAD(ad.metric_value, 1) OVER (
        PARTITION BY ad.entity_id ORDER BY ad.metric_date
    ) as target_next_day,
    
    LEAD(ad.metric_value, 7) OVER (
        PARTITION BY ad.entity_id ORDER BY ad.metric_date  
    ) as target_next_week,
    
    -- Feature importance indicators
    CASE 
        WHEN ad.composite_anomaly_score > 5.0 THEN 'HIGH_ANOMALY_RISK'
        WHEN ad.percentile_rank > 95 THEN 'EXTREME_VALUE'
        WHEN ABS(ad.momentum_direction) = 1 THEN 'TRENDING'
        ELSE 'NORMAL'
    END as feature_importance_flag,
    
    -- Cross-entity similarity features
    AVG(sm.statistical_distance) as avg_similarity_distance,
    COUNT(CASE WHEN sm.statistical_distance < 0.5 THEN 1 END) as similar_entities_count,
    
    -- Time-based features for model training
    EXTRACT(YEAR FROM ad.metric_date) as year,
    EXTRACT(MONTH FROM ad.metric_date) as month,
    EXTRACT(DOW FROM ad.metric_date) as dow,
    EXTRACT(DOY FROM ad.metric_date) as doy,
    
    -- Model metadata
    'ML_FEATURE_SET_V1' as feature_set_version,
    CURRENT_TIMESTAMP as feature_extraction_timestamp

FROM anomaly_detection ad
LEFT JOIN similarity_matrix sm ON ad.entity_id IN (sm.entity1, sm.entity2)

WHERE ad.metric_date >= CURRENT_DATE - INTERVAL '6 months'
AND ad.composite_anomaly_score IS NOT NULL

GROUP BY 
    ad.entity_id, ad.metric_date, ad.metric_value,
    ad.daily_change, ad.weekly_change, ad.monthly_change,
    ad.rolling_7_avg, ad.rolling_30_stddev, ad.z_score,
    ad.momentum_direction, ad.lag1_autocorr, ad.stability_ratio,
    ad.iqr_ratio, ad.dow_deviation, ad.monthly_deviation,
    ad.behavior_pattern, ad.normalized_deviation, ad.annual_sine,
    ad.annual_cosine, ad.weekly_sine, ad.weekly_cosine,
    ad.dominant_pattern, ad.avg_autocorrelation, ad.composite_anomaly_score,
    ad.percentile_rank, ad.mahalanobis_distance

ORDER BY 
    ad.entity_id,
    ad.metric_date DESC,
    ad.composite_anomaly_score DESC

LIMIT 50000;
```

### 4. **Real-Time Event Stream Processing with Complex Event Pattern Matching** ⭐⭐⭐⭐⭐
**Complexity: EXTREME | Lines: ~65 | Features: Pattern Matching, Stream Processing**

```sql  
WITH
-- Event stream deduplication and enrichment
enriched_events AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        event_timestamp,
        event_data,
        session_id,
        
        -- Event sequence numbering within session
        ROW_NUMBER() OVER (
            PARTITION BY session_id 
            ORDER BY event_timestamp
        ) as event_sequence,
        
        -- Time between events
        LAG(event_timestamp) OVER (
            PARTITION BY session_id 
            ORDER BY event_timestamp
        ) as prev_event_timestamp,
        
        EXTRACT(EPOCH FROM (
            event_timestamp - LAG(event_timestamp) OVER (
                PARTITION BY session_id ORDER BY event_timestamp
            )
        )) as seconds_since_prev_event,
        
        -- Event type transitions
        LAG(event_type) OVER (
            PARTITION BY session_id 
            ORDER BY event_timestamp
        ) as prev_event_type,
        
        LEAD(event_type) OVER (
            PARTITION BY session_id 
            ORDER BY event_timestamp  
        ) as next_event_type,
        
        -- Session context
        COUNT(*) OVER (
            PARTITION BY session_id
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as events_in_session_so_far,
        
        -- Advanced event pattern detection
        STRING_AGG(event_type, '->') OVER (
            PARTITION BY session_id
            ORDER BY event_timestamp
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as event_pattern_window_3,
        
        STRING_AGG(event_type, '->') OVER (
            PARTITION BY session_id  
            ORDER BY event_timestamp
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) as event_pattern_window_5

    FROM events e
    WHERE event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    AND event_data IS NOT NULL
),

-- Complex event pattern matching using regex-like patterns
event_patterns AS (
    SELECT 
        ee.*,
        
        -- Detect specific business-critical patterns
        CASE 
            WHEN ee.event_pattern_window_3 ~ '.*login->browse->purchase.*' THEN 'QUICK_PURCHASE'
            WHEN ee.event_pattern_window_5 ~ '.*browse->cart->abandon->return->purchase.*' THEN 'HESITANT_BUYER'
            WHEN ee.event_pattern_window_5 ~ '.*error->retry->error->retry->success.*' THEN 'PERSISTENCE_SUCCESS'
            WHEN ee.event_pattern_window_3 ~ '.*error->error->abandon.*' THEN 'FRICTION_ABANDON'
            WHEN ee.event_pattern_window_5 ~ '.*view->view->view->view->view.*' THEN 'BROWSER_PATTERN'
            ELSE 'OTHER'
        END as detected_pattern,
        
        -- Behavioral scoring based on patterns
        CASE 
            WHEN seconds_since_prev_event BETWEEN 0 AND 5 THEN 'RAPID_FIRE'
            WHEN seconds_since_prev_event BETWEEN 5 AND 30 THEN 'ENGAGED'  
            WHEN seconds_since_prev_event BETWEEN 30 AND 300 THEN 'DELIBERATE'
            WHEN seconds_since_prev_event > 300 THEN 'SLOW_EXPLORATION'
            ELSE 'UNKNOWN'
        END as interaction_pace,
        
        -- Event anomaly detection
        CASE
            WHEN event_sequence = 1 AND event_type != 'login' AND event_type != 'landing' THEN 'UNUSUAL_SESSION_START'
            WHEN seconds_since_prev_event > 3600 THEN 'LONG_PAUSE'  
            WHEN seconds_since_prev_event < 0.1 THEN 'POSSIBLE_BOT'
            WHEN event_type = prev_event_type AND seconds_since_prev_event < 1 THEN 'DUPLICATE_EVENT'
            ELSE 'NORMAL'
        END as event_anomaly_flag,
        
        -- Advanced pattern matching with lookahead/lookbehind simulation
        EXISTS(
            SELECT 1 FROM enriched_events ee2
            WHERE ee2.session_id = ee.session_id
            AND ee2.event_timestamp > ee.event_timestamp  
            AND ee2.event_timestamp <= ee.event_timestamp + INTERVAL '5 minutes'
            AND ee2.event_type = 'purchase'
        ) as will_purchase_soon,
        
        EXISTS(
            SELECT 1 FROM enriched_events ee3
            WHERE ee3.session_id = ee.session_id
            AND ee3.event_timestamp < ee.event_timestamp
            AND ee3.event_timestamp >= ee.event_timestamp - INTERVAL '10 minutes'  
            AND ee3.event_type = 'error'
        ) as had_recent_error
        
    FROM enriched_events ee
),

-- Session-level aggregations and complex metrics
session_analytics AS (
    SELECT 
        session_id,
        user_id,
        MIN(event_timestamp) as session_start,
        MAX(event_timestamp) as session_end,
        COUNT(*) as total_events,
        COUNT(DISTINCT event_type) as unique_event_types,
        
        -- Session duration metrics
        EXTRACT(EPOCH FROM (MAX(event_timestamp) - MIN(event_timestamp))) as session_duration_seconds,
        
        -- Pattern distribution within session
        COUNT(CASE WHEN detected_pattern = 'QUICK_PURCHASE' THEN 1 END) as quick_purchase_patterns,
        COUNT(CASE WHEN detected_pattern = 'HESITANT_BUYER' THEN 1 END) as hesitant_buyer_patterns,  
        COUNT(CASE WHEN detected_pattern = 'FRICTION_ABANDON' THEN 1 END) as friction_abandon_patterns,
        COUNT(CASE WHEN detected_pattern = 'BROWSER_PATTERN' THEN 1 END) as browser_patterns,
        
        -- Behavioral metrics
        AVG(seconds_since_prev_event) FILTER (WHERE seconds_since_prev_event IS NOT NULL) as avg_time_between_events,
        STDDEV(seconds_since_prev_event) FILTER (WHERE seconds_since_prev_event IS NOT NULL) as stddev_time_between_events,
        
        -- Anomaly indicators
        COUNT(CASE WHEN event_anomaly_flag != 'NORMAL' THEN 1 END) as anomaly_event_count,
        COUNT(CASE WHEN interaction_pace = 'POSSIBLE_BOT' THEN 1 END) as possible_bot_events,
        
        -- Outcome predictions
        MAX(CASE WHEN will_purchase_soon THEN 1 ELSE 0 END)::boolean as session_leads_to_purchase,
        MAX(CASE WHEN had_recent_error THEN 1 ELSE 0 END)::boolean as session_had_errors,
        
        -- Advanced session characteristics
        MODE() WITHIN GROUP (ORDER BY interaction_pace) as dominant_interaction_pace,
        
        -- Event type sequence analysis
        STRING_AGG(DISTINCT event_type, ', ' ORDER BY event_type) as event_types_in_session,
        
        -- Complex session scoring
        CASE
            WHEN COUNT(CASE WHEN detected_pattern = 'QUICK_PURCHASE' THEN 1 END) > 0 THEN 'HIGH_VALUE'
            WHEN COUNT(CASE WHEN detected_pattern = 'FRICTION_ABANDON' THEN 1 END) > 2 THEN 'HIGH_FRICTION'  
            WHEN COUNT(CASE WHEN event_anomaly_flag != 'NORMAL' THEN 1 END) > 3 THEN 'ANOMALOUS'
            WHEN EXTRACT(EPOCH FROM (MAX(event_timestamp) - MIN(event_timestamp))) < 60 THEN 'BRIEF_SESSION'
            WHEN COUNT(*) > 50 THEN 'HIGHLY_ENGAGED'
            ELSE 'TYPICAL'
        END as session_classification,
        
        -- Time-based session features
        EXTRACT(HOUR FROM MIN(event_timestamp)) as session_start_hour,
        EXTRACT(DOW FROM MIN(event_timestamp)) as session_start_dow,
        
        -- Funnel analysis
        MIN(CASE WHEN event_type = 'landing' THEN event_sequence END) as landing_position,
        MIN(CASE WHEN event_type = 'browse' THEN event_sequence END) as browse_position,
        MIN(CASE WHEN event_type = 'cart' THEN event_sequence END) as cart_position,
        MIN(CASE WHEN event_type = 'purchase' THEN event_sequence END) as purchase_position
        
    FROM event_patterns ep
    GROUP BY session_id, user_id
    HAVING COUNT(*) >= 3  -- Filter very short sessions
),

-- Real-time cohort analysis and user segmentation
user_behavior_cohorts AS (
    SELECT 
        user_id,
        
        -- User activity metrics
        COUNT(DISTINCT session_id) as total_sessions,
        AVG(session_duration_seconds) as avg_session_duration,
        SUM(total_events) as total_events_all_sessions,
        
        -- Behavioral pattern distribution
        AVG(quick_purchase_patterns::float) as avg_quick_purchase_patterns,
        AVG(hesitant_buyer_patterns::float) as avg_hesitant_buyer_patterns,
        AVG(friction_abandon_patterns::float) as avg_friction_abandon_patterns,
        
        -- User classification based on behavior
        CASE 
            WHEN AVG(quick_purchase_patterns::float) > 0.5 THEN 'DECISIVE_BUYER'
            WHEN AVG(hesitant_buyer_patterns::float) > 0.3 THEN 'CAREFUL_SHOPPER'
            WHEN AVG(friction_abandon_patterns::float) > 0.5 THEN 'FRUSTRATED_USER'
            WHEN AVG(session_duration_seconds) > 600 THEN 'EXPLORER'
            WHEN COUNT(DISTINCT session_id) > 5 THEN 'FREQUENT_VISITOR'  
            ELSE 'CASUAL_USER'
        END as user_behavior_segment,
        
        -- Purchase behavior
        COUNT(CASE WHEN session_leads_to_purchase THEN 1 END) as sessions_with_purchase,
        COUNT(CASE WHEN session_leads_to_purchase THEN 1 END)::float / 
        COUNT(DISTINCT session_id) as purchase_conversion_rate,
        
        -- Error resilience  
        AVG(CASE WHEN session_had_errors AND session_leads_to_purchase THEN 1.0 ELSE 0.0 END) as error_recovery_rate,
        
        -- Temporal patterns
        MODE() WITHIN GROUP (ORDER BY session_start_hour) as preferred_hour,
        MODE() WITHIN GROUP (ORDER BY session_start_dow) as preferred_day,
        
        -- Advanced user metrics
        STDDEV(session_duration_seconds) as session_duration_consistency,
        CORR(total_events, session_duration_seconds) as events_duration_correlation
        
    FROM session_analytics sa
    GROUP BY user_id
    HAVING COUNT(DISTINCT session_id) >= 2  -- Users with multiple sessions
)

-- Final comprehensive event stream analysis
SELECT 
    ubc.user_id,
    ubc.user_behavior_segment,
    ubc.total_sessions,
    ubc.avg_session_duration,
    ubc.purchase_conversion_rate,
    ubc.error_recovery_rate,
    ubc.preferred_hour,
    ubc.preferred_day,
    
    -- Recent session details
    sa.session_id as latest_session_id,
    sa.session_start as latest_session_start,
    sa.session_classification as latest_session_type,
    sa.total_events as latest_session_events,
    sa.dominant_interaction_pace as latest_interaction_pace,
    
    -- Pattern analysis
    sa.quick_purchase_patterns,
    sa.hesitant_buyer_patterns,
    sa.friction_abandon_patterns,
    sa.browser_patterns,
    
    -- Real-time insights  
    CASE
        WHEN sa.session_leads_to_purchase THEN 'LIKELY_TO_PURCHASE'
        WHEN sa.anomaly_event_count > 3 THEN 'NEEDS_ATTENTION'
        WHEN sa.friction_abandon_patterns > 0 THEN 'AT_RISK_OF_ABANDONING'
        WHEN sa.session_classification = 'HIGHLY_ENGAGED' THEN 'ENGAGEMENT_OPPORTUNITY'
        ELSE 'MONITOR'
    END as real_time_recommendation,
    
    -- Predictive scoring
    (ubc.purchase_conversion_rate * 0.4 +
     ubc.error_recovery_rate * 0.2 +
     CASE ubc.user_behavior_segment 
        WHEN 'DECISIVE_BUYER' THEN 0.9
        WHEN 'CAREFUL_SHOPPER' THEN 0.7
        WHEN 'EXPLORER' THEN 0.6
        WHEN 'FREQUENT_VISITOR' THEN 0.8
        ELSE 0.3
     END * 0.4) as user_value_score,
     
    -- Context for real-time decision making
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - sa.session_start)) / 60.0 as minutes_since_session_start,
    
    CASE
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - sa.session_start)) > 1800 THEN 'SESSION_EXPIRED'
        WHEN sa.session_leads_to_purchase THEN 'POST_PURCHASE'
        WHEN sa.friction_abandon_patterns > 0 THEN 'NEEDS_INTERVENTION'
        ELSE 'ACTIVE_SESSION' 
    END as session_status

FROM user_behavior_cohorts ubc
JOIN session_analytics sa ON ubc.user_id = sa.user_id
WHERE sa.session_start = (
    SELECT MAX(session_start) 
    FROM session_analytics sa2 
    WHERE sa2.user_id = ubc.user_id
)  -- Get latest session for each user

ORDER BY 
    ubc.user_value_score DESC,
    sa.session_start DESC,
    ubc.purchase_conversion_rate DESC

LIMIT 1000;
```

### 5. **Advanced Financial Risk Analysis with Monte Carlo Simulation** ⭐⭐⭐⭐⭐
**Complexity: EXTREME | Lines: ~85 | Features: Financial Mathematics, Risk Analytics**

```sql
WITH RECURSIVE
-- Generate Monte Carlo simulation paths for risk analysis
monte_carlo_paths AS (
    -- Base case: Initialize simulation parameters
    SELECT 
        simulation_id,
        1 as time_step,
        100.0 as asset_price,  -- Starting price
        0.0 as cumulative_return,
        RANDOM() as random_shock,
        
        -- Market parameters (normally these would come from parameters table)
        0.08 as drift_rate,      -- Annual drift 8%
        0.20 as volatility,      -- Annual volatility 20%
        1.0/252.0 as dt          -- Daily time step
        
    FROM generate_series(1, 1000) as simulation_id  -- 1000 Monte Carlo paths
    
    UNION ALL
    
    -- Recursive case: Generate next time step using geometric Brownian motion
    SELECT 
        mcp.simulation_id,
        mcp.time_step + 1,
        
        -- Geometric Brownian Motion: S(t+1) = S(t) * exp((μ - σ²/2)*dt + σ*sqrt(dt)*Z)
        mcp.asset_price * EXP(
            (mcp.drift_rate - POWER(mcp.volatility, 2) / 2) * mcp.dt + 
            mcp.volatility * SQRT(mcp.dt) * 
            (CASE 
                WHEN RANDOM() > 0.5 THEN SQRT(-2 * LN(RANDOM())) * COS(2 * PI() * RANDOM())
                ELSE SQRT(-2 * LN(RANDOM())) * SIN(2 * PI() * RANDOM())
            END)  -- Box-Muller transform for normal random numbers
        ),
        
        -- Calculate cumulative return
        mcp.cumulative_return + 
        LN(mcp.asset_price * EXP(
            (mcp.drift_rate - POWER(mcp.volatility, 2) / 2) * mcp.dt + 
            mcp.volatility * SQRT(mcp.dt) * 
            (CASE 
                WHEN RANDOM() > 0.5 THEN SQRT(-2 * LN(RANDOM())) * COS(2 * PI() * RANDOM())
                ELSE SQRT(-2 * LN(RANDOM())) * SIN(2 * PI() * RANDOM())
            END)
        ) / mcp.asset_price),
        
        RANDOM() as random_shock,
        mcp.drift_rate,
        mcp.volatility, 
        mcp.dt
        
    FROM monte_carlo_paths mcp
    WHERE mcp.time_step < 252  -- One year of daily steps
),

-- Advanced portfolio risk metrics calculation
portfolio_risk_metrics AS (
    SELECT 
        simulation_id,
        
        -- Final portfolio values and returns
        MAX(CASE WHEN time_step = 252 THEN asset_price END) as final_price,
        MAX(CASE WHEN time_step = 252 THEN cumulative_return END) as final_return,
        
        -- Path-dependent risk metrics
        MAX(asset_price) as path_maximum,
        MIN(asset_price) as path_minimum,
        MAX(asset_price) / MIN(asset_price) - 1 as max_drawdown_ratio,
        
        -- Volatility measures
        STDDEV(asset_price) as path_volatility,
        STDDEV(cumulative_return) as return_volatility,
        
        -- Downside risk measures
        COUNT(CASE WHEN cumulative_return < 0 THEN 1 END) as negative_return_days,
        AVG(CASE WHEN cumulative_return < 0 THEN cumulative_return END) as avg_negative_return,
        STDDEV(CASE WHEN cumulative_return < 0 THEN cumulative_return END) as downside_deviation,
        
        -- Tail risk measures
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY cumulative_return) as var_99_percent,
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY cumulative_return) as var_95_percent,
        
        -- Expected Shortfall (Conditional VaR)
        AVG(CASE 
            WHEN cumulative_return <= PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY cumulative_return) 
            THEN cumulative_return 
        END) as expected_shortfall_95,
        
        -- Maximum consecutive losses
        MAX(consecutive_losses) as max_consecutive_losses,
        
        -- Sharpe ratio approximation (assuming risk-free rate of 2%)
        (MAX(CASE WHEN time_step = 252 THEN cumulative_return END) - 0.02) / 
        NULLIF(STDDEV(cumulative_return), 0) * SQRT(252) as annualized_sharpe_ratio,
        
        -- Sortino ratio (using downside deviation)
        (MAX(CASE WHEN time_step = 252 THEN cumulative_return END) - 0.02) / 
        NULLIF(STDDEV(CASE WHEN cumulative_return < 0 THEN cumulative_return END), 0) * SQRT(252) 
        as sortino_ratio,
        
        -- Calmar ratio (return/max drawdown)
        MAX(CASE WHEN time_step = 252 THEN cumulative_return END) / 
        NULLIF(ABS(MIN(cumulative_return)), 0.01) as calmar_ratio
        
    FROM (
        SELECT 
            mcp.*,
            -- Calculate consecutive losses using window functions
            ROW_NUMBER() OVER (PARTITION BY simulation_id, grp ORDER BY time_step) as consecutive_losses
        FROM (
            SELECT 
                mcp.*,
                -- Group consecutive negative returns
                SUM(CASE WHEN cumulative_return >= 0 THEN 1 ELSE 0 END) OVER (
                    PARTITION BY simulation_id ORDER BY time_step
                ) as grp
            FROM monte_carlo_paths mcp
        ) mcp
        WHERE cumulative_return < 0
    ) consecutive_analysis
    RIGHT JOIN monte_carlo_paths mcp USING (simulation_id, time_step)
    GROUP BY simulation_id
),

-- Portfolio optimization using efficient frontier analysis
efficient_frontier AS (
    SELECT 
        target_return,
        
        -- Find minimum risk for each target return level
        MIN(portfolio_risk) as min_risk_for_return,
        AVG(expected_return) as avg_expected_return,
        
        -- Count feasible portfolios at this return level
        COUNT(*) as feasible_portfolios,
        
        -- Best Sharpe ratio at this return level
        MAX(sharpe_ratio) as best_sharpe_ratio,
        
        -- Portfolio characteristics for optimal portfolio
        MAX(CASE 
            WHEN portfolio_risk = MIN(portfolio_risk) OVER (PARTITION BY target_return)
            THEN portfolio_id 
        END) as optimal_portfolio_id
        
    FROM (
        SELECT 
            portfolio_id,
            expected_return,
            portfolio_risk,
            expected_return / NULLIF(portfolio_risk, 0) as sharpe_ratio,
            
            -- Discretize returns for efficient frontier
            ROUND(expected_return * 20) / 20.0 as target_return
            
        FROM (
            SELECT 
                prm.simulation_id as portfolio_id,
                prm.final_return as expected_return,
                prm.return_volatility as portfolio_risk,
                prm.annualized_sharpe_ratio
                
            FROM portfolio_risk_metrics prm
            WHERE prm.final_return IS NOT NULL
            AND prm.return_volatility > 0
        ) portfolio_candidates
    ) portfolio_analysis
    GROUP BY target_return
    HAVING COUNT(*) >= 10  -- Ensure sufficient data points
),

-- Advanced option pricing using Black-Scholes and Greeks
option_pricing AS (
    SELECT 
        prm.simulation_id,
        prm.final_price as spot_price,
        
        -- Option parameters
        110.0 as strike_price,  -- 10% out of the money call
        0.25 as time_to_expiry,  -- 3 months
        0.02 as risk_free_rate,
        prm.path_volatility as implied_volatility,
        
        -- Black-Scholes call option price
        prm.final_price * 
        (0.5 + 0.5 * ERF(d1 / SQRT(2))) -  -- N(d1) approximation
        110.0 * EXP(-0.02 * 0.25) * 
        (0.5 + 0.5 * ERF(d2 / SQRT(2)))   -- N(d2) approximation
        as bs_call_price,
        
        -- Greeks calculations
        -- Delta (price sensitivity)
        (0.5 + 0.5 * ERF(d1 / SQRT(2))) as delta,
        
        -- Gamma (delta sensitivity)  
        EXP(-POWER(d1, 2) / 2) / (prm.final_price * prm.path_volatility * SQRT(2 * PI() * 0.25)) as gamma,
        
        -- Theta (time decay)
        -(prm.final_price * EXP(-POWER(d1, 2) / 2) * prm.path_volatility) / 
        (2 * SQRT(2 * PI() * 0.25)) - 
        0.02 * 110.0 * EXP(-0.02 * 0.25) * 
        (0.5 + 0.5 * ERF(d2 / SQRT(2))) as theta,
        
        -- Vega (volatility sensitivity)
        prm.final_price * SQRT(0.25) * EXP(-POWER(d1, 2) / 2) / SQRT(2 * PI()) as vega,
        
        -- Intrinsic vs time value
        GREATEST(prm.final_price - 110.0, 0) as intrinsic_value,
        
        d1, d2  -- For debugging
        
    FROM (
        SELECT 
            prm.*,
            -- Calculate d1 and d2 for Black-Scholes
            (LN(prm.final_price / 110.0) + (0.02 + POWER(prm.path_volatility, 2) / 2) * 0.25) / 
            (prm.path_volatility * SQRT(0.25)) as d1,
            
            (LN(prm.final_price / 110.0) + (0.02 + POWER(prm.path_volatility, 2) / 2) * 0.25) / 
            (prm.path_volatility * SQRT(0.25)) - prm.path_volatility * SQRT(0.25) as d2
            
        FROM portfolio_risk_metrics prm
        WHERE prm.path_volatility > 0.01  -- Avoid division by zero
    ) prm
),

-- Stress testing and scenario analysis
stress_test_scenarios AS (
    SELECT 
        'BASE_CASE' as scenario_name,
        AVG(final_return) as avg_return,
        STDDEV(final_return) as return_volatility,
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY final_return) as var_5_percent,
        COUNT(CASE WHEN final_return < -0.20 THEN 1 END)::float / COUNT(*) as prob_loss_gt_20_percent,
        MAX(max_consecutive_losses) as worst_consecutive_losses
    FROM portfolio_risk_metrics
    
    UNION ALL
    
    SELECT 
        'MARKET_CRASH' as scenario_name,
        AVG(final_return - 0.30) as avg_return,  -- 30% market crash
        STDDEV(final_return) * 1.5 as return_volatility,  -- 50% higher volatility
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY final_return - 0.30) as var_5_percent,
        COUNT(CASE WHEN final_return - 0.30 < -0.20 THEN 1 END)::float / COUNT(*) as prob_loss_gt_20_percent,
        MAX(max_consecutive_losses) + 5 as worst_consecutive_losses
    FROM portfolio_risk_metrics
    
    UNION ALL
    
    SELECT 
        'HIGH_VOLATILITY' as scenario_name, 
        AVG(final_return) as avg_return,
        STDDEV(final_return) * 2.0 as return_volatility,  -- Double volatility
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY final_return) as var_5_percent,
        COUNT(CASE WHEN ABS(final_return) > 0.15 THEN 1 END)::float / COUNT(*) as prob_loss_gt_20_percent,
        MAX(max_consecutive_losses) + 3 as worst_consecutive_losses
    FROM portfolio_risk_metrics
)

-- Final comprehensive risk analysis report
SELECT 
    sts.scenario_name,
    sts.avg_return,
    sts.return_volatility,
    sts.var_5_percent,
    sts.prob_loss_gt_20_percent,
    sts.worst_consecutive_losses,
    
    -- Portfolio optimization insights
    ef.min_risk_for_return as efficient_frontier_risk,
    ef.best_sharpe_ratio as efficient_frontier_sharpe,
    
    -- Option market insights
    AVG(op.bs_call_price) as avg_option_price,
    AVG(op.delta) as avg_delta,
    AVG(op.gamma) as avg_gamma,
    AVG(op.theta) as avg_theta,
    AVG(op.vega) as avg_vega,
    
    -- Risk-adjusted performance metrics
    sts.avg_return / NULLIF(sts.return_volatility, 0) as risk_adjusted_return,
    
    -- Comparative analysis
    RANK() OVER (ORDER BY sts.avg_return DESC) as return_rank,
    RANK() OVER (ORDER BY sts.return_volatility) as risk_rank,
    RANK() OVER (ORDER BY sts.var_5_percent DESC) as tail_risk_rank,
    
    -- Strategic recommendations
    CASE 
        WHEN sts.prob_loss_gt_20_percent > 0.15 THEN 'HIGH_RISK_REDUCE_EXPOSURE'
        WHEN sts.return_volatility > 0.25 THEN 'HIGH_VOLATILITY_HEDGE_REQUIRED'
        WHEN sts.avg_return / NULLIF(sts.return_volatility, 0) > 0.8 THEN 'ATTRACTIVE_RISK_REWARD'
        WHEN ef.best_sharpe_ratio > 1.0 THEN 'OPTIMIZATION_OPPORTUNITY'
        ELSE 'MONITOR_CLOSELY'
    END as strategic_recommendation,
    
    -- Quantitative risk metrics summary
    JSONB_BUILD_OBJECT(
        'expected_return', ROUND(sts.avg_return::numeric, 4),
        'volatility', ROUND(sts.return_volatility::numeric, 4),
        'var_95', ROUND(sts.var_5_percent::numeric, 4),
        'tail_risk_probability', ROUND(sts.prob_loss_gt_20_percent::numeric, 4),
        'max_consecutive_losses', sts.worst_consecutive_losses,
        'sharpe_ratio', ROUND((sts.avg_return / NULLIF(sts.return_volatility, 0))::numeric, 3)
    ) as risk_metrics_summary

FROM stress_test_scenarios sts
LEFT JOIN efficient_frontier ef ON ABS(ef.target_return - sts.avg_return) < 0.05
LEFT JOIN option_pricing op ON true  -- Cross join for aggregation

GROUP BY 
    sts.scenario_name, sts.avg_return, sts.return_volatility,
    sts.var_5_percent, sts.prob_loss_gt_20_percent, sts.worst_consecutive_losses,
    ef.min_risk_for_return, ef.best_sharpe_ratio

ORDER BY 
    CASE sts.scenario_name 
        WHEN 'BASE_CASE' THEN 1
        WHEN 'MARKET_CRASH' THEN 2  
        WHEN 'HIGH_VOLATILITY' THEN 3
        ELSE 4
    END,
    sts.avg_return DESC;
```

---

## Continuing with queries 6-10...

NEXT FILE _PART2