{{ config(
    materialized='table'
) }}


WITH campaign_funnel_agg AS(
SELECT 
	platform,
	campaign_id,
	campaign_name,
	campaign_type,
	SUM(impression) AS total_impressions,
	SUM(clicks) AS total_clicks,
	SUM(spend) AS total_spend,
	SUM(purchases) AS total_orders
FROM {{ ref('marketing_master_view') }}
GROUP BY 1, 2, 3, 4
)

SELECT 
	*,
    CASE WHEN total_impressions > 0 THEN ROUND((total_clicks::numeric / total_impressions)::numeric, 4) ELSE 0 END AS ctr,
    CASE WHEN total_clicks > 0 THEN ROUND((total_orders::numeric / total_clicks)::numeric, 4) ELSE 0 END AS cvr,
    CASE WHEN total_orders > 0 THEN ROUND((total_spend / total_orders)::numeric, 2) ELSE NULL END AS cpa
FROM campaign_funnel_agg
ORDER BY platform, campaign_id