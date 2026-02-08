WITH raw_crm AS (
    SELECT revenue, order_id, order_date 
    FROM {{ source('stg_data', 'stg_crm_revenue') }}
),
cleansed_crm AS (
    SELECT revenue 
    FROM {{ ref('transform_crm_revenue') }}
),
duplicate_rows AS (
    SELECT 
        revenue
    FROM (
        SELECT 
            revenue,
            ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY order_date) as rn
        FROM raw_crm
    ) t
    WHERE rn > 1 
),
validation_metrics AS (
    SELECT 
        (SELECT SUM(revenue::numeric) FROM raw_crm) as raw_sum,
        (SELECT SUM(revenue::numeric) FROM cleansed_crm) as cleansed_sum,
        (SELECT COALESCE(SUM(revenue::numeric), 0) FROM duplicate_rows) as total_duplicated_revenue
),
result AS (
    SELECT 
        'Final Reconciliation' as test_name,
        raw_sum,
        cleansed_sum,
        (raw_sum - cleansed_sum) as total_difference,
        total_duplicated_revenue,
        CASE 
            WHEN ABS((raw_sum - cleansed_sum) - total_duplicated_revenue) < 1 THEN 'Match'
            ELSE 'Mismatch'
        END as status
    FROM validation_metrics
)

SELECT *
FROM result
WHERE status = 'Mismatch'