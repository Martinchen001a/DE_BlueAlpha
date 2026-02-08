{{ config(materialized='table') }}

with bounds as (
    select
        min(event_date) as min_date,
        max(event_date) as max_date
    from {{ ref('mart_marketing_attribution_daily') }}
),

date_spine as (
    select
        generate_series(
            (select min_date from bounds),
            (select max_date from bounds),
            interval '1 day'
        )::date as event_date
),

campaigns as (
    select distinct
        campaign_id,
        platform
    from {{ ref('mart_marketing_attribution_daily') }}
    where campaign_id <> 'organic'
),

grid as (
    select
        d.event_date,
        c.campaign_id,
        c.platform
    from date_spine d
    cross join campaigns c
)

select
    g.event_date,
    g.campaign_id,
    g.platform,

    coalesce(x.spend, 0) as spend,
    coalesce(x.revenue_clean, 0) as revenue_clean,
    coalesce(x.orders_cnt, 0) as orders_cnt,
    coalesce(x.clicks, 0) as clicks,
    coalesce(x.impression, 0) as impression

from grid g
left join {{ ref('mart_marketing_attribution_daily') }} x
  on g.event_date = x.event_date
 and g.campaign_id = x.campaign_id
 and g.platform = x.platform
