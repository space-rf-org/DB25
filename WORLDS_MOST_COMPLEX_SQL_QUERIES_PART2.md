# World's Most Complex SQL Queries - Part 2 (Queries 6-10)

## Continuing the World's Most Complex SQL Queries

### 6. **Multi-Dimensional Data Cube with OLAP Operations** ⭐⭐⭐⭐⭐
**Complexity: EXTREME | Lines: ~70 | Features: OLAP, Data Warehousing, Cube Operations**

```sql
WITH RECURSIVE
-- Build multi-dimensional data cube for OLAP analysis
dimension_hierarchy AS (
    -- Time dimension hierarchy
    SELECT 
        'TIME' as dimension_name,
        date_key,
        day_name,
        week_number,
        month_name,
        quarter_name,
        year_number,
        1 as hierarchy_level,
        date_key::text as dimension_path
    FROM time_dimension
    
    UNION ALL
    
    -- Product dimension hierarchy  
    SELECT 
        'PRODUCT' as dimension_name,
        product_key,
        product_name,
        subcategory_name,
        category_name,
        department_name,
        2 as hierarchy_level,
        product_key::text as dimension_path
    FROM product_dimension
    
    UNION ALL
    
    -- Geography dimension hierarchy
    SELECT 
        'GEOGRAPHY' as dimension_name,
        geography_key,
        city_name,
        state_name,
        country_name,
        region_name,
        3 as hierarchy_level,
        geography_key::text as dimension_path
    FROM geography_dimension
),

-- Advanced OLAP cube with all possible aggregation levels
olap_cube AS (
    SELECT 
        -- Dimension keys (null for rollup levels)
        GROUPING SETS (
            -- Grand total
            (),
            -- Single dimension rollups
            (td.year_number),
            (pd.department_name),
            (gd.region_name),
            -- Two dimension combinations
            (td.year_number, pd.department_name),
            (td.year_number, gd.region_name),
            (pd.department_name, gd.region_name),
            -- Three dimension combinations
            (td.year_number, pd.department_name, gd.region_name),
            (td.quarter_name, pd.category_name, gd.country_name),
            (td.month_name, pd.subcategory_name, gd.state_name),
            -- Detailed level
            (td.date_key, pd.product_key, gd.geography_key)
        ),
        
        -- Aggregated measures
        SUM(f.sales_amount) as total_sales,
        SUM(f.quantity_sold) as total_quantity,
        COUNT(DISTINCT f.transaction_id) as transaction_count,
        AVG(f.unit_price) as avg_unit_price,
        
        -- Advanced calculated measures
        SUM(f.sales_amount) / NULLIF(SUM(f.cost_amount), 0) - 1 as profit_margin,
        SUM(f.sales_amount) / NULLIF(COUNT(DISTINCT f.customer_id), 0) as revenue_per_customer,
        
        -- Statistical measures
        STDDEV(f.sales_amount) as sales_volatility,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.sales_amount) as median_sale,
        
        -- Time-based calculations
        SUM(f.sales_amount) - LAG(SUM(f.sales_amount)) OVER (
            PARTITION BY pd.department_name, gd.region_name 
            ORDER BY td.year_number
        ) as year_over_year_growth,
        
        -- Market share calculations
        SUM(f.sales_amount) / SUM(SUM(f.sales_amount)) OVER (
            PARTITION BY td.year_number
        ) as market_share_by_year,
        
        -- Ranking measures
        DENSE_RANK() OVER (
            PARTITION BY td.year_number 
            ORDER BY SUM(f.sales_amount) DESC
        ) as sales_rank_in_year,
        
        -- OLAP specific functions
        GROUPING(td.year_number) as time_grouping_flag,
        GROUPING(pd.department_name) as product_grouping_flag,  
        GROUPING(gd.region_name) as geography_grouping_flag,
        
        -- Cube identifiers for drill-down/roll-up
        CASE 
            WHEN GROUPING(td.year_number) = 0 AND GROUPING(pd.department_name) = 0 AND GROUPING(gd.region_name) = 0
            THEN 'DETAILED'
            WHEN GROUPING(td.year_number) = 1 AND GROUPING(pd.department_name) = 1 AND GROUPING(gd.region_name) = 1  
            THEN 'GRAND_TOTAL'
            WHEN GROUPING(td.year_number) = 0 AND GROUPING(pd.department_name) = 1 AND GROUPING(gd.region_name) = 1
            THEN 'TIME_ROLLUP'
            WHEN GROUPING(td.year_number) = 1 AND GROUPING(pd.department_name) = 0 AND GROUPING(gd.region_name) = 1
            THEN 'PRODUCT_ROLLUP'
            WHEN GROUPING(td.year_number) = 1 AND GROUPING(pd.department_name) = 1 AND GROUPING(gd.region_name) = 0
            THEN 'GEOGRAPHY_ROLLUP'
            ELSE 'PARTIAL_ROLLUP'
        END as cube_level
        
    FROM fact_sales f
    JOIN time_dimension td ON f.date_key = td.date_key
    JOIN product_dimension pd ON f.product_key = pd.product_key
    JOIN geography_dimension gd ON f.geography_key = gd.geography_key
    WHERE f.sales_date >= CURRENT_DATE - INTERVAL '3 years'
),

-- Advanced analytics on the OLAP cube
cube_analytics AS (
    SELECT 
        oc.*,
        
        -- Contribution analysis
        total_sales / NULLIF(SUM(total_sales) OVER (
            PARTITION BY cube_level
        ), 0) as contribution_to_level,
        
        -- Performance quartiles within each cube level
        NTILE(4) OVER (
            PARTITION BY cube_level 
            ORDER BY total_sales
        ) as performance_quartile,
        
        -- Variance analysis
        (total_sales - AVG(total_sales) OVER (PARTITION BY cube_level)) / 
        NULLIF(STDDEV(total_sales) OVER (PARTITION BY cube_level), 0) as performance_z_score,
        
        -- Pareto analysis (80/20 rule)
        SUM(total_sales) OVER (
            PARTITION BY cube_level 
            ORDER BY total_sales DESC 
            ROWS UNBOUNDED PRECEDING
        ) / SUM(total_sales) OVER (PARTITION BY cube_level) as cumulative_sales_pct,
        
        -- Advanced time series analysis
        CASE 
            WHEN year_over_year_growth > 0 THEN 'GROWING'
            WHEN year_over_year_growth < 0 THEN 'DECLINING'  
            WHEN year_over_year_growth IS NULL THEN 'NEW'
            ELSE 'FLAT'
        END as growth_trend,
        
        -- Seasonal analysis
        total_sales / NULLIF(AVG(total_sales) OVER (
            PARTITION BY EXTRACT(MONTH FROM CURRENT_DATE)
        ), 0) as seasonal_index,
        
        -- Market dynamics
        CASE
            WHEN market_share_by_year > 0.1 THEN 'MARKET_LEADER'
            WHEN market_share_by_year > 0.05 THEN 'MAJOR_PLAYER'
            WHEN market_share_by_year > 0.01 THEN 'NICHE_PLAYER'
            ELSE 'EMERGING'
        END as market_position
        
    FROM olap_cube oc
),

-- Drill-across analysis combining multiple fact tables
cross_fact_analysis AS (
    SELECT 
        ca.cube_level,
        ca.total_sales,
        ca.transaction_count,
        
        -- Customer behavior metrics from customer fact
        AVG(cf.customer_satisfaction_score) as avg_customer_satisfaction,
        COUNT(DISTINCT cf.customer_id) as unique_customers,
        SUM(cf.customer_lifetime_value) as total_customer_value,
        
        -- Inventory metrics from inventory fact  
        AVG(if.inventory_turnover_ratio) as avg_inventory_turnover,
        SUM(if.stockout_incidents) as total_stockouts,
        AVG(if.days_in_inventory) as avg_days_in_inventory,
        
        -- Marketing metrics from marketing fact
        SUM(mf.marketing_spend) as total_marketing_spend,
        SUM(mf.campaign_impressions) as total_impressions,
        SUM(mf.campaign_clicks) as total_clicks,
        
        -- Cross-fact calculated metrics
        ca.total_sales / NULLIF(SUM(mf.marketing_spend), 0) as marketing_roi,
        ca.total_sales / NULLIF(COUNT(DISTINCT cf.customer_id), 0) as revenue_per_customer,
        SUM(cf.customer_lifetime_value) / NULLIF(ca.total_sales, 0) as ltv_to_revenue_ratio,
        
        -- Advanced business metrics
        (ca.total_sales - SUM(mf.marketing_spend)) / NULLIF(ca.total_sales, 0) as net_profit_margin,
        
        SUM(mf.campaign_clicks) / NULLIF(SUM(mf.campaign_impressions), 0) as click_through_rate,
        
        ca.transaction_count / NULLIF(COUNT(DISTINCT cf.customer_id), 0) as transactions_per_customer
        
    FROM cube_analytics ca
    LEFT JOIN customer_fact cf ON ca.cube_level = 'DETAILED'  -- Only join at detailed level
    LEFT JOIN inventory_fact if ON ca.cube_level = 'DETAILED'
    LEFT JOIN marketing_fact mf ON ca.cube_level = 'DETAILED'
    
    GROUP BY 
        ca.cube_level, ca.total_sales, ca.transaction_count,
        ca.contribution_to_level, ca.performance_quartile, 
        ca.market_position, ca.growth_trend
)

-- Final comprehensive OLAP analysis
SELECT 
    cfa.cube_level,
    
    -- Core business metrics
    ROUND(cfa.total_sales::numeric, 2) as total_sales,
    cfa.transaction_count,
    ROUND(cfa.revenue_per_customer::numeric, 2) as revenue_per_customer,
    
    -- Customer insights
    ROUND(cfa.avg_customer_satisfaction::numeric, 2) as avg_customer_satisfaction,
    cfa.unique_customers,
    ROUND(cfa.ltv_to_revenue_ratio::numeric, 3) as ltv_to_revenue_ratio,
    
    -- Operational efficiency
    ROUND(cfa.avg_inventory_turnover::numeric, 2) as avg_inventory_turnover,
    cfa.total_stockouts,
    ROUND(cfa.avg_days_in_inventory::numeric, 1) as avg_days_in_inventory,
    
    -- Marketing effectiveness  
    ROUND(cfa.marketing_roi::numeric, 2) as marketing_roi,
    ROUND(cfa.click_through_rate::numeric, 4) as click_through_rate,
    ROUND(cfa.net_profit_margin::numeric, 3) as net_profit_margin,
    
    -- Performance indicators
    ca.performance_quartile,
    ROUND(ca.contribution_to_level::numeric, 4) as contribution_to_level,
    ca.market_position,
    ca.growth_trend,
    
    -- Advanced analytics
    ROUND(ca.performance_z_score::numeric, 2) as performance_z_score,
    ROUND(ca.cumulative_sales_pct::numeric, 3) as cumulative_sales_pct,
    ROUND(ca.seasonal_index::numeric, 2) as seasonal_index,
    
    -- Strategic insights
    CASE 
        WHEN ca.performance_quartile = 4 AND ca.market_position = 'MARKET_LEADER' THEN 'STAR_PERFORMER'
        WHEN ca.performance_quartile >= 3 AND ca.growth_trend = 'GROWING' THEN 'RISING_STAR'
        WHEN ca.performance_quartile <= 2 AND ca.market_position IN ('NICHE_PLAYER', 'EMERGING') THEN 'QUESTION_MARK'
        WHEN ca.growth_trend = 'DECLINING' AND ca.performance_quartile <= 2 THEN 'DOG'
        ELSE 'CASH_COW'
    END as bcg_matrix_classification,
    
    -- OLAP navigation metadata
    JSONB_BUILD_OBJECT(
        'drill_down_available', CASE WHEN cfa.cube_level != 'DETAILED' THEN true ELSE false END,
        'roll_up_available', CASE WHEN cfa.cube_level != 'GRAND_TOTAL' THEN true ELSE false END,
        'slice_dimensions', ARRAY['TIME', 'PRODUCT', 'GEOGRAPHY'],
        'available_measures', ARRAY['sales', 'quantity', 'customers', 'profit_margin']
    ) as olap_metadata

FROM cross_fact_analysis cfa
JOIN cube_analytics ca USING (cube_level, total_sales, transaction_count)

WHERE cfa.total_sales > 0  -- Filter empty cells

ORDER BY 
    CASE cfa.cube_level
        WHEN 'GRAND_TOTAL' THEN 1
        WHEN 'TIME_ROLLUP' THEN 2
        WHEN 'PRODUCT_ROLLUP' THEN 3
        WHEN 'GEOGRAPHY_ROLLUP' THEN 4
        WHEN 'PARTIAL_ROLLUP' THEN 5
        WHEN 'DETAILED' THEN 6
    END,
    cfa.total_sales DESC

LIMIT 1000;
```

### 7. **Genomic Sequence Analysis with Bioinformatics Algorithms** ⭐⭐⭐⭐⭐
**Complexity: EXTREME | Lines: ~60 | Features: Bioinformatics, Pattern Matching, Sequence Analysis**

```sql
WITH RECURSIVE
-- DNA sequence pattern matching and analysis
sequence_analysis AS (
    SELECT 
        sequence_id,
        organism_name,
        chromosome,
        start_position,
        end_position,
        dna_sequence,
        
        -- Basic sequence statistics
        LENGTH(dna_sequence) as sequence_length,
        
        -- Nucleotide composition analysis
        (LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'A', '')))::float / LENGTH(dna_sequence) as adenine_content,
        (LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'T', '')))::float / LENGTH(dna_sequence) as thymine_content,
        (LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'G', '')))::float / LENGTH(dna_sequence) as guanine_content,
        (LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'C', '')))::float / LENGTH(dna_sequence) as cytosine_content,
        
        -- GC content (important for genomic analysis)
        ((LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'G', ''))) +
         (LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'C', ''))))::float / LENGTH(dna_sequence) as gc_content,
        
        -- Purine/Pyrimidine ratio
        ((LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'A', ''))) +
         (LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'G', ''))))::float /
        NULLIF(((LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'T', ''))) +
                (LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'C', '')))), 0) as purine_pyrimidine_ratio,
        
        -- Find potential gene start sites (ATG codons)
        ARRAY_LENGTH(STRING_TO_ARRAY(dna_sequence, 'ATG'), 1) - 1 as potential_start_codons,
        
        -- Find potential gene stop sites (TAA, TAG, TGA codons)
        (ARRAY_LENGTH(STRING_TO_ARRAY(dna_sequence, 'TAA'), 1) - 1 +
         ARRAY_LENGTH(STRING_TO_ARRAY(dna_sequence, 'TAG'), 1) - 1 +
         ARRAY_LENGTH(STRING_TO_ARRAY(dna_sequence, 'TGA'), 1) - 1) as potential_stop_codons,
        
        -- CpG islands detection (important for gene regulation)
        ARRAY_LENGTH(STRING_TO_ARRAY(dna_sequence, 'CG'), 1) - 1 as cpg_sites,
        
        -- Repeat sequence detection
        CASE 
            WHEN dna_sequence ~ '(A{4,}|T{4,}|G{4,}|C{4,})' THEN 'HOMOPOLYMER_REPEAT'
            WHEN dna_sequence ~ '(AT){3,}|(TA){3,}|(GC){3,}|(CG){3,}' THEN 'DINUCLEOTIDE_REPEAT'
            WHEN dna_sequence ~ '(CAG){3,}|(CTG){3,}|(GAA){3,}' THEN 'TRINUCLEOTIDE_REPEAT'
            ELSE 'NO_MAJOR_REPEATS'
        END as repeat_type,
        
        -- Complexity score (Shannon entropy approximation)
        -((LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'A', '')))::float / LENGTH(dna_sequence) * 
          LN(NULLIF((LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'A', '')))::float / LENGTH(dna_sequence), 0)) +
          (LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'T', '')))::float / LENGTH(dna_sequence) * 
          LN(NULLIF((LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'T', '')))::float / LENGTH(dna_sequence), 0)) +
          (LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'G', '')))::float / LENGTH(dna_sequence) * 
          LN(NULLIF((LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'G', '')))::float / LENGTH(dna_sequence), 0)) +
          (LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'C', '')))::float / LENGTH(dna_sequence) * 
          LN(NULLIF((LENGTH(dna_sequence) - LENGTH(REPLACE(dna_sequence, 'C', '')))::float / LENGTH(dna_sequence), 0))) 
        as sequence_entropy
        
    FROM genomic_sequences
    WHERE LENGTH(dna_sequence) >= 100  -- Minimum length for meaningful analysis
),

-- Open Reading Frame (ORF) detection
orf_detection AS (
    SELECT 
        sa.sequence_id,
        sa.organism_name,
        sa.dna_sequence,
        
        -- Generate all possible reading frames (6 total: 3 forward, 3 reverse complement)
        generate_series(0, 2) as reading_frame,
        
        -- Extract codons in each reading frame
        SUBSTRING(sa.dna_sequence FROM reading_frame + 1) as frame_sequence,
        
        -- Translate to amino acids (simplified genetic code)
        TRANSLATE(
            TRANSLATE(
                TRANSLATE(
                    TRANSLATE(
                        TRANSLATE(
                            TRANSLATE(
                                SUBSTRING(sa.dna_sequence FROM reading_frame + 1),
                                'TTT TTC TTA TTG TCT TCC TCA TCG TAT TAC TAA TAG TGT TGC TGA TGG',
                                'F   F   L   L   S   S   S   S   Y   Y   *   *   C   C   *   W'
                            ),
                            'CTT CTC CTA CTG CCT CCC CCA CCG CAT CAC CAA CAG CGT CGC CGA CGG',
                            'L   L   L   L   P   P   P   P   H   H   Q   Q   R   R   R   R'
                        ),
                        'ATT ATC ATA ATG ACT ACC ACA ACG AAT AAC AAA AAG AGT AGC AGA AGG',
                        'I   I   I   M   T   T   T   T   N   N   K   K   S   S   R   R'
                    ),
                    'GTT GTC GTA GTG GCT GCC GCA GCG GAT GAC GAA GAG GGT GGC GGA GGG',
                    'V   V   V   V   A   A   A   A   D   D   E   E   G   G   G   G'
                ),
                ' ', ''
            ),
            'ACDEFGHIKLMNPQRSTVWY*', 'ACDEFGHIKLMNPQRSTVWY*'
        ) as amino_acid_sequence,
        
        -- Calculate ORF length
        LENGTH(SUBSTRING(sa.dna_sequence FROM reading_frame + 1)) / 3 as orf_length_codons,
        
        -- ORF quality metrics
        CASE 
            WHEN LENGTH(SUBSTRING(sa.dna_sequence FROM reading_frame + 1)) >= 300 THEN 'LONG_ORF'
            WHEN LENGTH(SUBSTRING(sa.dna_sequence FROM reading_frame + 1)) >= 150 THEN 'MEDIUM_ORF'
            WHEN LENGTH(SUBSTRING(sa.dna_sequence FROM reading_frame + 1)) >= 75 THEN 'SHORT_ORF'
            ELSE 'VERY_SHORT_ORF'
        END as orf_classification
        
    FROM sequence_analysis sa
    CROSS JOIN generate_series(0, 2) as reading_frame
),

-- Phylogenetic analysis and sequence similarity
phylogenetic_analysis AS (
    SELECT 
        sa1.sequence_id as seq1_id,
        sa2.sequence_id as seq2_id,
        sa1.organism_name as organism1,
        sa2.organism_name as organism2,
        
        -- Hamming distance (for sequences of same length)
        CASE 
            WHEN LENGTH(sa1.dna_sequence) = LENGTH(sa2.dna_sequence) THEN
                SUM(CASE WHEN SUBSTRING(sa1.dna_sequence, i, 1) != SUBSTRING(sa2.dna_sequence, i, 1) THEN 1 ELSE 0 END)
            ELSE NULL
        END as hamming_distance,
        
        -- Sequence similarity percentage
        CASE 
            WHEN LENGTH(sa1.dna_sequence) = LENGTH(sa2.dna_sequence) THEN
                (1.0 - SUM(CASE WHEN SUBSTRING(sa1.dna_sequence, i, 1) != SUBSTRING(sa2.dna_sequence, i, 1) THEN 1 ELSE 0 END)::float / LENGTH(sa1.dna_sequence)) * 100
            ELSE NULL  
        END as similarity_percentage,
        
        -- GC content difference (taxonomic indicator)
        ABS(sa1.gc_content - sa2.gc_content) as gc_content_difference,
        
        -- Composition similarity (Euclidean distance in ATGC space)
        SQRT(
            POWER(sa1.adenine_content - sa2.adenine_content, 2) +
            POWER(sa1.thymine_content - sa2.thymine_content, 2) +
            POWER(sa1.guanine_content - sa2.guanine_content, 2) +
            POWER(sa1.cytosine_content - sa2.cytosine_content, 2)
        ) as composition_distance,
        
        -- Complexity similarity
        ABS(sa1.sequence_entropy - sa2.sequence_entropy) as entropy_difference,
        
        -- Functional similarity based on ORF patterns
        ABS(sa1.potential_start_codons - sa2.potential_start_codons) as start_codon_difference,
        ABS(sa1.potential_stop_codons - sa2.potential_stop_codons) as stop_codon_difference
        
    FROM sequence_analysis sa1
    CROSS JOIN sequence_analysis sa2
    CROSS JOIN generate_series(1, LEAST(LENGTH(sa1.dna_sequence), LENGTH(sa2.dna_sequence))) as i
    WHERE sa1.sequence_id < sa2.sequence_id  -- Avoid duplicate comparisons
    AND LENGTH(sa1.dna_sequence) = LENGTH(sa2.dna_sequence)  -- Only compare same-length sequences
    GROUP BY 
        sa1.sequence_id, sa2.sequence_id, sa1.organism_name, sa2.organism_name,
        sa1.dna_sequence, sa2.dna_sequence, sa1.gc_content, sa2.gc_content,
        sa1.adenine_content, sa2.adenine_content, sa1.thymine_content, sa2.thymine_content,
        sa1.guanine_content, sa2.guanine_content, sa1.cytosine_content, sa2.cytosine_content,
        sa1.sequence_entropy, sa2.sequence_entropy, sa1.potential_start_codons, sa2.potential_start_codons,
        sa1.potential_stop_codons, sa2.potential_stop_codons
),

-- Motif discovery and pattern analysis
motif_analysis AS (
    SELECT 
        sa.sequence_id,
        sa.organism_name,
        sa.dna_sequence,
        
        -- Common regulatory motifs
        ARRAY_LENGTH(STRING_TO_ARRAY(sa.dna_sequence, 'TATAAA'), 1) - 1 as tata_box_count,  -- Promoter element
        ARRAY_LENGTH(STRING_TO_ARRAY(sa.dna_sequence, 'CCAAT'), 1) - 1 as ccaat_box_count,   -- Promoter element
        ARRAY_LENGTH(STRING_TO_ARRAY(sa.dna_sequence, 'GGGCGG'), 1) - 1 as sp1_binding_sites, -- Transcription factor binding
        ARRAY_LENGTH(STRING_TO_ARRAY(sa.dna_sequence, 'AAGCTT'), 1) - 1 as hindiii_sites,     -- Restriction enzyme site
        
        -- Splice site motifs (for eukaryotic genes)
        ARRAY_LENGTH(STRING_TO_ARRAY(sa.dna_sequence, 'GTAAGT'), 1) - 1 as donor_splice_sites,
        ARRAY_LENGTH(STRING_TO_ARRAY(sa.dna_sequence, 'TTTCAG'), 1) - 1 as acceptor_splice_sites,
        
        -- DNA structural motifs
        ARRAY_LENGTH(STRING_TO_ARRAY(sa.dna_sequence, 'AAAAA'), 1) - 1 as poly_a_tracts,    -- DNA bending
        ARRAY_LENGTH(STRING_TO_ARRAY(sa.dna_sequence, 'CGGCGG'), 1) - 1 as cpg_islands,     -- Methylation sites
        
        -- Pathogenic mutation hotspots
        ARRAY_LENGTH(STRING_TO_ARRAY(sa.dna_sequence, 'CGA'), 1) - 1 as cga_sites,          -- Deamination hotspots
        ARRAY_LENGTH(STRING_TO_ARRAY(sa.dna_sequence, 'CTCF'), 1) - 1 as ctcf_binding,      -- Insulator elements
        
        -- Calculate motif density (motifs per kb)
        (ARRAY_LENGTH(STRING_TO_ARRAY(sa.dna_sequence, 'TATAAA'), 1) - 1 + 
         ARRAY_LENGTH(STRING_TO_ARRAY(sa.dna_sequence, 'CCAAT'), 1) - 1 +
         ARRAY_LENGTH(STRING_TO_ARRAY(sa.dna_sequence, 'GGGCGG'), 1) - 1) * 1000.0 / LENGTH(sa.dna_sequence) as regulatory_motif_density
        
    FROM sequence_analysis sa
)

-- Final comprehensive genomic analysis
SELECT 
    sa.sequence_id,
    sa.organism_name,
    sa.chromosome,
    sa.sequence_length,
    
    -- Composition analysis
    ROUND(sa.gc_content::numeric, 3) as gc_content,
    ROUND(sa.purine_pyrimidine_ratio::numeric, 3) as purine_pyrimidine_ratio,
    ROUND(sa.sequence_entropy::numeric, 3) as sequence_complexity,
    sa.repeat_type,
    
    -- Gene structure predictions
    sa.potential_start_codons,
    sa.potential_stop_codons,
    sa.cpg_sites,
    
    -- ORF analysis summary
    COUNT(od.orf_length_codons) FILTER (WHERE od.orf_classification = 'LONG_ORF') as long_orfs,
    COUNT(od.orf_length_codons) FILTER (WHERE od.orf_classification = 'MEDIUM_ORF') as medium_orfs,
    MAX(od.orf_length_codons) as longest_orf_codons,
    
    -- Regulatory element predictions
    ma.tata_box_count,
    ma.ccaat_box_count, 
    ma.sp1_binding_sites,
    ma.donor_splice_sites,
    ma.acceptor_splice_sites,
    ROUND(ma.regulatory_motif_density::numeric, 2) as regulatory_density_per_kb,
    
    -- Phylogenetic context (average similarity to other sequences)
    AVG(pa.similarity_percentage) as avg_similarity_to_others,
    MIN(pa.composition_distance) as closest_composition_match,
    
    -- Functional classification
    CASE 
        WHEN sa.gc_content > 0.6 THEN 'GC_RICH_REGION'
        WHEN sa.gc_content < 0.4 THEN 'AT_RICH_REGION'  
        WHEN ma.tata_box_count > 0 OR ma.ccaat_box_count > 0 THEN 'PROMOTER_REGION'
        WHEN sa.potential_start_codons > 2 AND sa.potential_stop_codons > 2 THEN 'CODING_REGION'
        WHEN ma.cpg_islands > 5 THEN 'CPG_ISLAND'
        WHEN sa.repeat_type != 'NO_MAJOR_REPEATS' THEN 'REPETITIVE_REGION'
        ELSE 'INTERGENIC_REGION'
    END as predicted_function,
    
    -- Pathogenicity risk assessment
    CASE
        WHEN ma.cga_sites > LENGTH(sa.dna_sequence) / 50 THEN 'HIGH_MUTATION_RISK'
        WHEN sa.repeat_type = 'TRINUCLEOTIDE_REPEAT' THEN 'REPEAT_EXPANSION_RISK'
        WHEN sa.sequence_entropy < 1.0 THEN 'LOW_COMPLEXITY_REGION'
        ELSE 'STANDARD_RISK'
    END as pathogenicity_assessment,
    
    -- Evolutionary conservation score (based on composition similarity)
    CASE 
        WHEN AVG(pa.composition_distance) < 0.1 THEN 'HIGHLY_CONSERVED'
        WHEN AVG(pa.composition_distance) < 0.2 THEN 'MODERATELY_CONSERVED'  
        WHEN AVG(pa.composition_distance) < 0.4 THEN 'VARIABLE'
        ELSE 'HIGHLY_VARIABLE'
    END as conservation_status
    
FROM sequence_analysis sa
LEFT JOIN orf_detection od ON sa.sequence_id = od.sequence_id
LEFT JOIN motif_analysis ma ON sa.sequence_id = ma.sequence_id  
LEFT JOIN phylogenetic_analysis pa ON sa.sequence_id IN (pa.seq1_id, pa.seq2_id)

WHERE sa.sequence_length >= 200  -- Focus on meaningful sequences

GROUP BY 
    sa.sequence_id, sa.organism_name, sa.chromosome, sa.sequence_length,
    sa.gc_content, sa.purine_pyrimidine_ratio, sa.sequence_entropy, sa.repeat_type,
    sa.potential_start_codons, sa.potential_stop_codons, sa.cpg_sites,
    ma.tata_box_count, ma.ccaat_box_count, ma.sp1_binding_sites,
    ma.donor_splice_sites, ma.acceptor_splice_sites, ma.regulatory_motif_density,
    ma.cga_sites, sa.dna_sequence

ORDER BY 
    sa.sequence_length DESC,
    sa.gc_content DESC,
    ma.regulatory_motif_density DESC

LIMIT 500;
```

### 8. **IoT Sensor Data Stream Analytics with Anomaly Detection** ⭐⭐⭐⭐⭐
**Complexity: EXTREME | Lines: ~65 | Features: IoT Analytics, Stream Processing, Anomaly Detection**

```sql
WITH
-- Real-time IoT sensor data preprocessing and enrichment
sensor_data_enriched AS (
    SELECT 
        sd.sensor_id,
        sd.device_id,
        sd.location_id,
        sd.sensor_type,
        sd.timestamp,
        sd.value as raw_value,
        sd.unit,
        sd.quality_indicator,
        
        -- Time-based features
        EXTRACT(EPOCH FROM sd.timestamp) as unix_timestamp,
        EXTRACT(HOUR FROM sd.timestamp) as hour_of_day,
        EXTRACT(DOW FROM sd.timestamp) as day_of_week,
        EXTRACT(DOY FROM sd.timestamp) as day_of_year,
        
        -- Data quality assessment
        CASE 
            WHEN sd.quality_indicator < 0.5 THEN 'POOR'
            WHEN sd.quality_indicator < 0.8 THEN 'ACCEPTABLE'
            WHEN sd.quality_indicator < 0.95 THEN 'GOOD'  
            ELSE 'EXCELLENT'
        END as quality_category,
        
        -- Sensor value normalization and validation
        CASE 
            WHEN sd.sensor_type = 'TEMPERATURE' AND sd.value BETWEEN -50 AND 100 THEN sd.value
            WHEN sd.sensor_type = 'HUMIDITY' AND sd.value BETWEEN 0 AND 100 THEN sd.value  
            WHEN sd.sensor_type = 'PRESSURE' AND sd.value BETWEEN 800 AND 1200 THEN sd.value
            WHEN sd.sensor_type = 'VIBRATION' AND sd.value BETWEEN 0 AND 1000 THEN sd.value
            ELSE NULL  -- Mark as invalid
        END as validated_value,
        
        -- Temporal ordering and gaps detection
        LAG(sd.timestamp) OVER (
            PARTITION BY sd.sensor_id 
            ORDER BY sd.timestamp
        ) as prev_timestamp,
        
        EXTRACT(EPOCH FROM (
            sd.timestamp - LAG(sd.timestamp) OVER (
                PARTITION BY sd.sensor_id ORDER BY sd.timestamp
            )
        )) as seconds_since_last_reading,
        
        -- Sequential change detection
        sd.value - LAG(sd.value) OVER (
            PARTITION BY sd.sensor_id 
            ORDER BY sd.timestamp
        ) as value_change,
        
        -- Rate of change calculation  
        (sd.value - LAG(sd.value) OVER (
            PARTITION BY sd.sensor_id ORDER BY sd.timestamp
        )) / NULLIF(EXTRACT(EPOCH FROM (
            sd.timestamp - LAG(sd.timestamp) OVER (
                PARTITION BY sd.sensor_id ORDER BY sd.timestamp  
            )
        )), 0) as rate_of_change
        
    FROM iot_sensor_data sd
    WHERE sd.timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    AND sd.value IS NOT NULL
),

-- Advanced statistical analysis and anomaly detection  
statistical_analysis AS (
    SELECT 
        sde.*,
        
        -- Rolling statistical measures (1-hour window)
        AVG(sde.validated_value) OVER (
            PARTITION BY sde.sensor_id
            ORDER BY sde.unix_timestamp
            RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
        ) as rolling_1h_avg,
        
        STDDEV(sde.validated_value) OVER (
            PARTITION BY sde.sensor_id  
            ORDER BY sde.unix_timestamp
            RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
        ) as rolling_1h_stddev,
        
        -- Percentile-based anomaly detection
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY sde.validated_value) OVER (
            PARTITION BY sde.sensor_id
            ORDER BY sde.unix_timestamp  
            RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW  -- 2-hour window
        ) as rolling_q1,
        
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY sde.validated_value) OVER (
            PARTITION BY sde.sensor_id
            ORDER BY sde.unix_timestamp
            RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW
        ) as rolling_q3,
        
        -- Seasonal baselines (same hour of day, historical)
        AVG(sde.validated_value) OVER (
            PARTITION BY sde.sensor_id, sde.hour_of_day
            ORDER BY sde.unix_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as historical_hourly_avg,
        
        STDDEV(sde.validated_value) OVER (
            PARTITION BY sde.sensor_id, sde.hour_of_day  
            ORDER BY sde.unix_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as historical_hourly_stddev,
        
        -- Weekly seasonal patterns
        AVG(sde.validated_value) OVER (
            PARTITION BY sde.sensor_id, sde.day_of_week, sde.hour_of_day
            ORDER BY sde.unix_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as weekly_seasonal_avg,
        
        -- Extreme value detection
        COUNT(CASE WHEN ABS(sde.value_change) > 10 THEN 1 END) OVER (
            PARTITION BY sde.sensor_id
            ORDER BY sde.unix_timestamp
            RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW  -- 30-minute window
        ) as extreme_changes_30min,
        
        -- Data gap analysis
        CASE 
            WHEN sde.seconds_since_last_reading > 300 THEN 'DATA_GAP'  -- > 5 minutes
            WHEN sde.seconds_since_last_reading > 120 THEN 'DELAYED_READING'  -- > 2 minutes
            WHEN sde.seconds_since_last_reading < 10 THEN 'HIGH_FREQUENCY'  -- < 10 seconds
            ELSE 'NORMAL_FREQUENCY'
        END as reading_frequency_status
        
    FROM sensor_data_enriched sde
    WHERE sde.validated_value IS NOT NULL
),

-- Multi-dimensional anomaly detection using various algorithms
anomaly_detection AS (
    SELECT 
        sa.*,
        
        -- Z-score based anomaly detection
        ABS(sa.validated_value - sa.rolling_1h_avg) / 
        NULLIF(sa.rolling_1h_stddev, 0) as z_score,
        
        -- IQR-based outlier detection  
        CASE 
            WHEN sa.validated_value < (sa.rolling_q1 - 1.5 * (sa.rolling_q3 - sa.rolling_q1)) THEN 'OUTLIER_LOW'
            WHEN sa.validated_value > (sa.rolling_q3 + 1.5 * (sa.rolling_q3 - sa.rolling_q1)) THEN 'OUTLIER_HIGH'
            ELSE 'NORMAL'
        END as iqr_outlier_status,
        
        -- Seasonal anomaly detection
        ABS(sa.validated_value - sa.weekly_seasonal_avg) / 
        NULLIF(sa.historical_hourly_stddev, 0) as seasonal_z_score,
        
        -- Rate of change anomaly
        CASE 
            WHEN ABS(sa.rate_of_change) > 5 THEN 'RAPID_CHANGE'
            WHEN ABS(sa.rate_of_change) > 2 THEN 'MODERATE_CHANGE'
            ELSE 'STABLE'
        END as change_rate_status,
        
        -- Isolation Forest approximation (distance-based)
        SQRT(
            POWER((sa.validated_value - sa.rolling_1h_avg) / NULLIF(sa.rolling_1h_stddev, 1), 2) +
            POWER((sa.rate_of_change - AVG(sa.rate_of_change) OVER (
                PARTITION BY sa.sensor_id
                ORDER BY sa.unix_timestamp  
                RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
            )) / NULLIF(STDDEV(sa.rate_of_change) OVER (
                PARTITION BY sa.sensor_id
                ORDER BY sa.unix_timestamp
                RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW  
            ), 1), 2)
        ) as isolation_score,
        
        -- Pattern-based anomaly detection
        CASE
            WHEN sa.extreme_changes_30min > 5 THEN 'ERRATIC_BEHAVIOR'
            WHEN sa.reading_frequency_status = 'DATA_GAP' THEN 'SENSOR_MALFUNCTION'
            WHEN sa.quality_category = 'POOR' AND ABS(sa.validated_value - sa.rolling_1h_avg) > 2 * sa.rolling_1h_stddev THEN 'QUALITY_ANOMALY'
            ELSE 'PATTERN_NORMAL'
        END as pattern_anomaly_status,
        
        -- Multivariate anomaly score (combined indicators)
        (CASE WHEN ABS(sa.validated_value - sa.rolling_1h_avg) / NULLIF(sa.rolling_1h_stddev, 0) > 3 THEN 3.0 ELSE 0.0 END +
         CASE WHEN sa.validated_value < (sa.rolling_q1 - 1.5 * (sa.rolling_q3 - sa.rolling_q1)) OR 
                   sa.validated_value > (sa.rolling_q3 + 1.5 * (sa.rolling_q3 - sa.rolling_q1)) THEN 2.0 ELSE 0.0 END +
         CASE WHEN ABS(sa.rate_of_change) > 5 THEN 2.0 ELSE 0.0 END +
         CASE WHEN sa.extreme_changes_30min > 3 THEN 1.5 ELSE 0.0 END +
         CASE WHEN sa.quality_category = 'POOR' THEN 1.0 ELSE 0.0 END
        ) as composite_anomaly_score
        
    FROM statistical_analysis sa
),

-- Sensor correlation analysis and system-wide anomaly detection
correlation_analysis AS (
    SELECT 
        ad.sensor_id,
        ad.device_id,  
        ad.location_id,
        ad.timestamp,
        ad.validated_value,
        ad.composite_anomaly_score,
        
        -- Cross-sensor correlation for same device
        CORR(ad.validated_value, ad2.validated_value) OVER (
            PARTITION BY ad.device_id
            ORDER BY ad.unix_timestamp
            RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
        ) as device_sensor_correlation,
        
        -- Location-based correlation (sensors in same area)
        AVG(ad2.validated_value) FILTER (WHERE ad2.sensor_type = ad.sensor_type) OVER (
            PARTITION BY ad.location_id, ad.sensor_type  
            ORDER BY ad.unix_timestamp
            RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW
        ) as location_type_avg,
        
        STDDEV(ad2.validated_value) FILTER (WHERE ad2.sensor_type = ad.sensor_type) OVER (
            PARTITION BY ad.location_id, ad.sensor_type
            ORDER BY ad.unix_timestamp  
            RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW
        ) as location_type_stddev,
        
        -- System-wide anomaly indicators
        COUNT(CASE WHEN ad2.composite_anomaly_score > 5 THEN 1 END) OVER (
            PARTITION BY ad.device_id
            ORDER BY ad.unix_timestamp
            RANGE BETWEEN 600 PRECEDING AND CURRENT ROW  -- 10-minute window
        ) as device_anomaly_count,
        
        COUNT(CASE WHEN ad2.composite_anomaly_score > 3 THEN 1 END) OVER (
            PARTITION BY ad.location_id
            ORDER BY ad.unix_timestamp  
            RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW  -- 30-minute window
        ) as location_anomaly_count,
        
        -- Environmental context
        CASE ad.sensor_type
            WHEN 'TEMPERATURE' THEN 
                CASE 
                    WHEN ad.validated_value > 35 THEN 'HIGH_TEMP_EVENT'
                    WHEN ad.validated_value < 5 THEN 'LOW_TEMP_EVENT'
                    ELSE 'NORMAL_TEMP'
                END
            WHEN 'HUMIDITY' THEN
                CASE
                    WHEN ad.validated_value > 80 THEN 'HIGH_HUMIDITY_EVENT'  
                    WHEN ad.validated_value < 20 THEN 'LOW_HUMIDITY_EVENT'
                    ELSE 'NORMAL_HUMIDITY'
                END
            WHEN 'VIBRATION' THEN
                CASE 
                    WHEN ad.validated_value > 500 THEN 'HIGH_VIBRATION_EVENT'
                    WHEN ad.rate_of_change > 100 THEN 'VIBRATION_SPIKE'
                    ELSE 'NORMAL_VIBRATION'
                END
            ELSE 'UNDEFINED_EVENT'
        END as environmental_event
        
    FROM anomaly_detection ad
    LEFT JOIN anomaly_detection ad2 ON (
        (ad.device_id = ad2.device_id OR ad.location_id = ad2.location_id) 
        AND ABS(EXTRACT(EPOCH FROM (ad.timestamp - ad2.timestamp))) <= 300  -- 5-minute window
    )
),

-- Predictive analytics and trend analysis
predictive_analytics AS (
    SELECT 
        ca.*,
        
        -- Trend prediction using linear regression over window
        REGR_SLOPE(ca.validated_value, EXTRACT(EPOCH FROM ca.timestamp)) OVER (
            PARTITION BY ca.sensor_id
            ORDER BY ca.timestamp
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW  -- 20-point trend
        ) as trend_slope,
        
        -- Predicted next value based on trend
        ca.validated_value + 
        (REGR_SLOPE(ca.validated_value, EXTRACT(EPOCH FROM ca.timestamp)) OVER (
            PARTITION BY ca.sensor_id
            ORDER BY ca.timestamp  
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) * 300) as predicted_value_5min,  -- 5 minutes ahead
        
        -- Confidence in prediction based on R-squared
        REGR_R2(ca.validated_value, EXTRACT(EPOCH FROM ca.timestamp)) OVER (
            PARTITION BY ca.sensor_id
            ORDER BY ca.timestamp
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as trend_confidence,
        
        -- Failure prediction indicators
        CASE 
            WHEN ca.composite_anomaly_score > 7 THEN 'IMMEDIATE_ATTENTION'
            WHEN ca.device_anomaly_count > 5 AND ca.composite_anomaly_score > 3 THEN 'DEVICE_DEGRADATION'
            WHEN ca.location_anomaly_count > 10 THEN 'ENVIRONMENTAL_ISSUE'
            WHEN ABS(REGR_SLOPE(ca.validated_value, EXTRACT(EPOCH FROM ca.timestamp)) OVER (
                PARTITION BY ca.sensor_id ORDER BY ca.timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            )) > 0.1 THEN 'TRENDING_ANOMALY'
            ELSE 'STABLE_OPERATION'
        END as health_prediction,
        
        -- Maintenance scheduling indicators
        CASE
            WHEN ca.composite_anomaly_score > 5 AND ca.trend_confidence < 0.5 THEN 'SCHEDULE_MAINTENANCE'
            WHEN ca.device_anomaly_count > 3 THEN 'MONITOR_CLOSELY'  
            WHEN ca.environmental_event LIKE '%_EVENT' THEN 'CHECK_CONDITIONS'
            ELSE 'ROUTINE_MONITORING'
        END as maintenance_recommendation
        
    FROM correlation_analysis ca
)

-- Final comprehensive IoT analytics dashboard
SELECT 
    pa.sensor_id,
    pa.device_id,
    pa.location_id,
    pa.sensor_type,
    pa.timestamp,
    ROUND(pa.validated_value::numeric, 2) as sensor_value,
    pa.unit,
    
    -- Anomaly detection results
    ROUND(pa.composite_anomaly_score::numeric, 2) as anomaly_score,
    pa.iqr_outlier_status,
    pa.change_rate_status,
    pa.pattern_anomaly_status,
    
    -- System health indicators  
    pa.device_anomaly_count,
    pa.location_anomaly_count,
    ROUND(pa.device_sensor_correlation::numeric, 3) as device_correlation,
    pa.environmental_event,
    
    -- Predictive analytics
    pa.health_prediction,
    pa.maintenance_recommendation,
    ROUND(pa.trend_slope::numeric, 6) as trend_slope_per_second,
    ROUND(pa.predicted_value_5min::numeric, 2) as predicted_value_5min,
    ROUND(pa.trend_confidence::numeric, 3) as prediction_confidence,
    
    -- Data quality indicators
    pa.quality_category,
    pa.reading_frequency_status,
    
    -- Real-time alerting flags
    CASE 
        WHEN pa.composite_anomaly_score > 8 THEN 'CRITICAL_ALERT'
        WHEN pa.composite_anomaly_score > 5 THEN 'WARNING_ALERT'
        WHEN pa.health_prediction = 'IMMEDIATE_ATTENTION' THEN 'MAINTENANCE_ALERT'
        WHEN pa.environmental_event LIKE '%HIGH%' OR pa.environmental_event LIKE '%SPIKE%' THEN 'ENVIRONMENTAL_ALERT'
        ELSE 'NORMAL_STATUS'  
    END as alert_level,
    
    -- Contextual information for operators
    CONCAT(
        'Sensor ', pa.sensor_id, ' (', pa.sensor_type, ') at location ', pa.location_id,
        ' showing value ', ROUND(pa.validated_value::numeric, 2), ' ', pa.unit,
        CASE 
            WHEN pa.composite_anomaly_score > 5 THEN ' - ANOMALY DETECTED'
            WHEN ABS(pa.trend_slope) > 0.05 THEN ' - TRENDING ' || 
                CASE WHEN pa.trend_slope > 0 THEN 'UP' ELSE 'DOWN' END
            ELSE ' - NORMAL'
        END
    ) as operator_summary,
    
    -- Machine learning features for advanced analytics
    JSONB_BUILD_OBJECT(
        'z_score', ROUND(pa.z_score::numeric, 3),
        'seasonal_z_score', ROUND(pa.seasonal_z_score::numeric, 3),
        'isolation_score', ROUND(pa.isolation_score::numeric, 3),
        'hour_of_day', pa.hour_of_day,
        'day_of_week', pa.day_of_week,
        'rate_of_change', ROUND(pa.rate_of_change::numeric, 4)
    ) as ml_features

FROM predictive_analytics pa

WHERE pa.timestamp >= CURRENT_TIMESTAMP - INTERVAL '2 hours'  -- Focus on recent data

ORDER BY 
    pa.composite_anomaly_score DESC,
    pa.timestamp DESC,
    pa.sensor_id

LIMIT 1000;
```

---

*continue with queries 9-10 and the targeted feature tests in the next file to complete the comprehensive SQL test suite.*