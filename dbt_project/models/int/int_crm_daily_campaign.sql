{{ config(
    materialized = 'table'
) }}

SELECT 
	order_date,
	campaign_source AS campaign_id,
	channel_attributed AS platform,
	COUNT(DISTINCT order_id) AS orders_cnt,
	COUNT(DISTINCT customer_id) AS customers_cnt,
	SUM(COALESCE(revenue, 0)) AS revenue_raw,
	SUM(
		CASE 
			WHEN revenue IS NULL THEN 0
			WHEN revenue < 0 THEN 0
			WHEN data_quality_tag = 'revenue_outlier' THEN 0
			ELSE revenue
		END) AS revenue_clean,
	SUM(CASE WHEN data_quality_tag = 'normal' THEN 1 ELSE 0 END) AS normal_revenue_cnt,
	SUM(CASE WHEN data_quality_tag = 'revenue_outlier' THEN 1 ELSE 0 END) AS revenue_outlier_cnt,
	SUM(CASE WHEN data_quality_tag = 'negative_revenue' THEN 1 ELSE 0 END) AS negative_revenue_cnt,
	SUM(CASE WHEN data_quality_tag = 'missing_revenue' THEN 1 ELSE 0 END) AS missing_revenue_cnt
FROM {{ ref('transform_crm_revenue') }}
GROUP BY 1, 2, 3
