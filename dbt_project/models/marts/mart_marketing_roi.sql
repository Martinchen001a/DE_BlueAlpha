{{ config(
    materialized='table'
) }}

WITH daily_agg AS (
    SELECT 
        COALESCE(a.event_date, r.order_date) AS event_date,
        COALESCE(a.platform, 'organic') AS platform,
		COALESCE(a.campaign_name, 'organic/direct') AS campaign_name,
        COALESCE(a.campaign_id, 'untracked') AS campaign_id,
		r.region,
		r.product_category,
        COALESCE(a.spend, 0) AS daily_spend,
		COALESCE(r.revenue, 0) AS clean_revenue,
        r.data_quality_tag
    FROM {{ ref('marketing_master_view') }} a
    FULL OUTER JOIN {{ ref('transform_crm_revenue') }} r 
		ON a.campaign_id = r.campaign_source
        AND a.event_date = r.order_date
		AND r.data_quality_tag = 'normal'
),
rolling_metrics AS (
    SELECT 
        *,
        SUM(daily_spend) OVER(
            PARTITION BY platform, campaign_id 
            ORDER BY event_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_spend,
        SUM(clean_revenue) OVER(
            PARTITION BY platform, campaign_id 
            ORDER BY event_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_revenue
    FROM daily_agg
)

SELECT 
    event_date,
    platform,
    campaign_name,
    campaign_id,
	COALESCE(region, 'unknown') AS region,
	COALESCE(product_category, 'unknown') AS product_category,
    daily_spend,
    clean_revenue,
    CASE 
		WHEN daily_spend > 0 THEN ROUND((clean_revenue / daily_spend)::numeric, 2) 
		ELSE NULL 
	END AS daily_roi,
    CASE 
		WHEN rolling_7d_spend > 0 THEN ROUND((rolling_7d_revenue / rolling_7d_spend)::numeric, 2) 
		ELSE NULL 
	END AS rolling_7d_roi,
    COALESCE(data_quality_tag, 'missing revenue') AS data_quality_tag
FROM rolling_metrics