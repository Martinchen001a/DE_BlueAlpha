{{ config(
    materialized = 'table'
) }}

SELECT 
	event_date,
	campaign_id,
	platform,
	SUM(COALESCE(impression, 0)) AS impression,
	SUM(COALESCE(clicks, 0)) AS clicks,
	SUM(COALESCE(spend, 0)) AS spend,
	SUM(COALESCE(purchases, 0)) AS purchases,
	SUM(COALESCE(purchase_value, 0)) AS purchase_value,
	SUM(COALESCE(reach, 0)) AS reach,
	ROUND(SUM(COALESCE(impression,0)) / NULLIF(SUM(reach), 0),2) AS avg_frequency
FROM {{ ref('marketing_master_view') }}
GROUP BY 1, 2, 3

