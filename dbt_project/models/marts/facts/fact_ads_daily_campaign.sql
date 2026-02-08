{{ config(
    materialized='table'
) }}

select
    event_date,
    campaign_id,
    platform,

    impression,
    clicks,
    spend,
    purchases,
    purchase_value,
    reach,
    avg_frequency

from {{ ref('int_ads_daily_campaign') }}