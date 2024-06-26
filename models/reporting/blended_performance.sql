{{ config (
    alias = target.database + '_blended_performance'
)}}

With meta as (
select date::date, date_granularity,
'Meta' as channel, 
SUM(coalesce(spend,0)) as spend, 
SUM(coalesce(purchases,0)) as paid_purchase,
SUM(coalesce(revenue,0)) as paid_revenue,
SUM(coalesce(link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions
FROM {{ source('reporting','facebook_ad_performance') }}
group by 1,2),

google as (select date::date, date_granularity,
'Google' as channel, 
SUM(coalesce(spend,0)) as spend, 
SUM(coalesce(purchases,0)) as paid_purchase,
SUM(coalesce(revenue,0)) as paid_revenue,
SUM(coalesce(clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions
FROM {{ source('reporting','googleads_campaign_performance') }}
group by 1,2)

select *
from meta 
union 
select *
from google
