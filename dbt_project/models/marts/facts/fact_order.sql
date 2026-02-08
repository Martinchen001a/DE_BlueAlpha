{{ config(
    materialized='table'
) }}

select
    order_id,  -- PK
    order_date,
    customer_id,
    COALESCE(campaign_source, 'organic') AS campaign_id,
    revenue AS revenue_raw,
    CASE 
        WHEN revenue IS NULL THEN 0
        WHEN revenue < 0 THEN 0
        WHEN data_quality_tag = 'revenue_outlier' THEN 0
        ELSE revenue END AS revenue_clean,
    data_quality_tag AS revenue_quality_tag
from {{ ref('transform_crm_revenue') }}
