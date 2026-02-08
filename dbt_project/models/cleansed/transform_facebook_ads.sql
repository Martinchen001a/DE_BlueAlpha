{{ config(
    materialized='table'
) }}


SELECT 
    LOWER(TRIM(COALESCE(campaign_id, 'unknown')))  AS campaign_id,
    LOWER(TRIM(COALESCE(campaign_name, 'unknown')))  AS campaign_name,
    CAST(date AS DATE) AS date,
    COALESCE(impressions, 0) AS impression,
    COALESCE(clicks, 0) AS clicks,
    GREATEST(COALESCE(spend, 0), 0) AS spend,
    COALESCE(purchases, 0) AS purchases,
	COALESCE(purchase_value, 0) AS purchase_value,
    COALESCE(reach, 0) AS reach,
    COALESCE(frequency, 0) AS frequency
FROM {{ source('stg_data', 'stg_facebook_ads') }}



