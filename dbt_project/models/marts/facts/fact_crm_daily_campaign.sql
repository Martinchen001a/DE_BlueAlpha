{{ config(
    materialized='table'
) }}

select
    order_date,
    campaign_id,

    orders_cnt,
    customers_cnt,
    revenue_raw,
    revenue_clean,
    normal_revenue_cnt,
    revenue_outlier_cnt,
    negative_revenue_cnt,
    missing_revenue_cnt

from {{ ref('int_crm_daily_campaign') }}