{{ config(
    materialized='table'
) }}


SELECT 
    LOWER(TRIM(COALESCE(campaign_id, 'unknown'))) AS campaign_id,
    LOWER(TRIM(COALESCE(campaign_name, 'unknown'))) AS campaign_name,
	LOWER(TRIM(COALESCE(campaign_type, 'unknown'))) AS campaign_type,
    CAST(date AS DATE) AS date,
    SUM(COALESCE(impressions, 0)) AS impression,
    SUM(COALESCE(clicks, 0)) AS clicks,
    SUM(GREATEST(COALESCE(cost_micros, 0)/1000000, 0)) AS spend,
    SUM(COALESCE(conversions, 0)) AS purchases,
    SUM(COALESCE(conversion_value, 0)) AS purchase_value
FROM {{ source('staging_data', 'stg_google_ads') }}
GROUP BY 1, 2, 3, 4

