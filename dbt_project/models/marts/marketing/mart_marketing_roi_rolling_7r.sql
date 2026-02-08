{{ config(materialized='table') }}

with base as (
  select
    event_date,
    campaign_id,
    platform,
    spend,
    revenue_clean,
    orders_cnt,
    clicks,
    impression
  from {{ ref('mart_marketing_attribution_daily') }}
  where campaign_id <> 'organic'
),

rolling as (
  select
    event_date,
    campaign_id,
    platform,

    -- rolling sums (trailing 7 rows)
    sum(spend) over (
      partition by campaign_id, platform
      order by event_date
      rows between 6 preceding and current row
    ) as spend_7d,

    sum(revenue_clean) over (
      partition by campaign_id, platform
      order by event_date
      rows between 6 preceding and current row
    ) as revenue_7d,

    sum(orders_cnt) over (
      partition by campaign_id, platform
      order by event_date
      rows between 6 preceding and current row
    ) as orders_7d,

    sum(clicks) over (
      partition by campaign_id, platform
      order by event_date
      rows between 6 preceding and current row
    ) as clicks_7d,

    sum(impression) over (
      partition by campaign_id, platform
      order by event_date
      rows between 6 preceding and current row
    ) as impression_7d

  from base
)

select
  event_date,
  campaign_id,
  platform,

  spend_7d,
  revenue_7d,
  orders_7d,
  clicks_7d,
  impression_7d,

  -- rolling KPIs
  (revenue_7d::numeric / nullif(spend_7d, 0)) as roas_7d,
  ((revenue_7d - spend_7d)::numeric / nullif(spend_7d, 0)) as roi_7d,
  (spend_7d::numeric / nullif(orders_7d, 0)) as cpa_7d,
  (clicks_7d::numeric / nullif(impression_7d, 0)) as ctr_7d,
  (orders_7d::numeric / nullif(clicks_7d, 0)) as cvr_7d

from rolling
