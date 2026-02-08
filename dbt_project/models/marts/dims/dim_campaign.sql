{{ config(
    materialized='table'
) }}

SELECT DISTINCT 
    campaign_id, -- PK
    platform,
    campaign_name,
    campaign_type
FROM {{ ref('marketing_master_view') }}