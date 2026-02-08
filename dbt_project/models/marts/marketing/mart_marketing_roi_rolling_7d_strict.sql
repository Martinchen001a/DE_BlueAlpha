{{ config(materialized='table') }}

with base as (
  select *
  from {{ ref('mart_marketing_attribution_daily_spined') }}
),

rolling as (
  select
    event_date,
    campaign_id,
    platform,

    sum(spend) over (
      partition by campaign_id, platform
      order by event_date
      rows between 6 preceding and current row
    ) as spend_7d,

    sum(revenue_clean) over (
      partition by campaign_id, platform
      order by event_date
      rows between 6 preceding and current row
    ) as revenue_7d

  from base
)

select
  event_date,
  campaign_id,
  platform,
  spend_7d,
  revenue_7d,
  (revenue_7d::numeric / nullif(spend_7d, 0)) as roas_7d,
  ((revenue_7d - spend_7d)::numeric / nullif(spend_7d, 0)) as roi_7d
from rolling
where spend_7d > 0