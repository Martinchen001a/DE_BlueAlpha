{{ config(
    materialized='table'
) }}

WITH base_data AS (
    SELECT 
        LOWER(TRIM(COALESCE(campaign_id, 'unknown'))) AS campaign_id,
        LOWER(TRIM(COALESCE(campaign_name, 'unknown'))) AS campaign_name,
        CAST(date AS DATE) AS date,
        COALESCE(impressions, 0) AS impression,
        COALESCE(clicks, 0) AS clicks,
        GREATEST(COALESCE(spend, 0), 0) AS spend,
        COALESCE(purchases, 0) AS purchases,
        COALESCE(purchase_value, 0) AS purchase_value,
        COALESCE(reach, 0) AS reach,
        COALESCE(frequency, 0) AS frequency
    FROM {{ source('stg_data', 'stg_facebook_ads') }}
),

calc_unit_price AS (
    SELECT 
        *,
        AVG(CASE WHEN purchases > 0 THEN purchase_value / purchases END) 
            OVER (PARTITION BY campaign_id) AS avg_unit_price
    FROM base_data
)

SELECT
    campaign_id,
    campaign_name,
    date,
    impression,
    clicks,
    spend,
    CASE 
        WHEN (purchases = 0 OR purchases IS NULL) AND purchase_value > 0 AND avg_unit_price > 0 
        THEN ROUND(purchase_value::numeric / avg_unit_price::numeric, 0)
        ELSE purchases::REAL
    END AS purchases,
    purchase_value,
    reach,
    frequency
FROM calc_unit_price






