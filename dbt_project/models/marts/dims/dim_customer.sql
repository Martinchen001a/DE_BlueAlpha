-- Assume region is shipping_region

{{ config(
    materialized='table'
) }}

SELECT DISTINCT 
    customer_id, -- PK
    region
FROM {{ ref('transform_crm_revenue') }}