{{ config(
    materialized='table' 
) }}

WITH facebook_data AS (
    SELECT 
        LOWER(TRIM(campaign_id))::TEXT AS campaign_id,
        campaign_name::TEXT,
        'unknown'::TEXT AS campaign_type,
        'facebook'::TEXT AS platform,
        date AS event_date,
        SUM(impression)::BIGINT AS impression,
        SUM(clicks)::BIGINT AS clicks,
        SUM(spend)::REAL AS spend,
        SUM(purchases)::REAL AS purchases,
        SUM(purchase_value)::REAL AS purchase_value,
        SUM(reach)::BIGINT AS reach,
        ROUND(
            SUM(COALESCE(impression,0))::numeric / NULLIF(SUM(COALESCE(reach,0)),0),
            2
            ) AS avg_frequency
    FROM {{ ref('transform_facebook_ads') }}
    GROUP BY 1, 2, 3, 4, 5
),

google_data AS (
    SELECT 
        LOWER(TRIM(campaign_id))::TEXT AS campaign_id, 
        campaign_name::TEXT,
        campaign_type::TEXT,
        'google'::TEXT AS platform,
        date AS event_date,
        SUM(impression)::BIGINT AS impression,
        SUM(clicks)::BIGINT AS clicks,
        SUM(spend)::REAL AS spend,
        SUM(purchases)::REAL AS purchases,
        SUM(purchase_value)::REAL AS purchase_value,
        NULL::BIGINT AS reach,
        NULL::REAL AS avg_frequency
    FROM {{ ref('transform_google_ads') }}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM facebook_data
UNION ALL
SELECT * FROM google_data