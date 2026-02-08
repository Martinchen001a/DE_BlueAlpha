{{ config(
    materialized='table'
) }}

SELECT 
    COALESCE(m.event_date, c.order_date) AS event_date,
    COALESCE(m.campaign_id, c.campaign_source) AS campaign_id,
    c.order_id,
    c.customer_id,
    COALESCE(m.campaign_name, 'organic') AS campaign_name,
    COALESCE(m.campaign_type, 'organic') AS campaign_type,
    COALESCE(m.platform, 'organic') AS platform,
    m.impression,
    m.clicks,
    m.spend,
    m.purchases,
    m.purchase_value,
    m.reach,
    m.avg_frequency,
    c.revenue,
    c.region,
    c.product_category,
    COALESCE(c.data_quality_tag, 'missing revenue')  AS revenue_quality_tag,
    CASE 
        WHEN m.campaign_id IS NOT NULL AND c.order_id IS NOT NULL THEN 'paid_converted'
        WHEN m.campaign_id IS NOT NULL AND c.order_id IS NULL THEN 'inefficient_spend'
        WHEN (m.campaign_id IS NULL OR m.campaign_type = 'organic') AND c.order_id IS NOT NULL THEN 'organic_conversion'
        ELSE 'other'
    END AS row_status
FROM {{ ref('marketing_master_view') }} m
FULL OUTER JOIN {{ ref('transform_crm_revenue') }} c  
ON m.event_date = c.order_date  
AND m.campaign_id = c.campaign_source