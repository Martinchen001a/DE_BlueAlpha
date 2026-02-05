{{ config(
    materialized='table'
) }}


SELECT 
    LOWER(TRIM(COALESCE(campaign_id, 'unknown')))  AS campaign_id,
    LOWER(TRIM(COALESCE(campaign_name, 'unknown')))  AS campaign_name,
    CAST(date AS DATE) AS date,
    SUM(COALESCE(impressions, 0)) AS impression,
    SUM(COALESCE(clicks, 0)) AS clicks,
    SUM(GREATEST(COALESCE(spend, 0), 0)) AS spend,
    SUM(COALESCE(purchases, 0)) AS purchases,
	SUM(COALESCE(purchase_value, 0)) AS purchase_value,
    SUM(COALESCE(reach, 0)) AS reach,
    AVG(COALESCE(frequency, 0)) AS frequency
FROM {{ source('staging_data', 'stg_facebook_ads') }}
GROUP BY 1, 2, 3



