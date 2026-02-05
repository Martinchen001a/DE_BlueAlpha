{{ config(
    materialized='table'
) }}


SELECT DISTINCT ON (LOWER(TRIM(order_id)))
    LOWER(TRIM(order_id))::TEXT AS order_id,
	LOWER(TRIM(customer_id)) AS customer_id,
	CAST(order_date AS DATE) AS order_date,
	COALESCE(revenue, 0) AS revenue,
	LOWER(TRIM(channel_attributed)) AS channel_attributed,
	LOWER(TRIM(campaign_source)) AS campaign_source,
	LOWER(TRIM(product_category)) AS product_category,
	LOWER(TRIM(region)) AS region,
	CASE 
        WHEN revenue >= 1000000 THEN 'revenue outlier'
        WHEN revenue < 0 THEN 'negative revenue'
        WHEN customer_id IS NULL THEN 'missing customerid'
        ELSE 'normal'
    END AS data_quality_tag
FROM {{ source('staging_data', 'stg_crm_revenue') }}

