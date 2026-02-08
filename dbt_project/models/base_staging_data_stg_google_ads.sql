with source as (
        select * from {{ source('stg_data', 'stg_google_ads') }}
  ),
  renamed as (
      select
          {{ adapter.quote("date") }},
        {{ adapter.quote("impressions") }},
        {{ adapter.quote("clicks") }},
        {{ adapter.quote("cost_micros") }},
        {{ adapter.quote("conversions") }},
        {{ adapter.quote("conversion_value") }},
        {{ adapter.quote("campaign_name") }},
        {{ adapter.quote("campaign_id") }},
        {{ adapter.quote("campaign_type") }}

      from source
  )
  select * from renamed
    