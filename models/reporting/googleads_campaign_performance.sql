{{ config (
    alias = target.database + '_googleads_campaign_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
date,
DATE_TRUNC('week',date+3)-3 as custom_week,
date_granularity,
spend,
impressions,
clicks,
waldouspurchase as purchases,
waldouspurchase_value as revenue,
search_impression_share,
search_budget_lost_impression_share,
search_rank_lost_impression_share
FROM {{ ref('googleads_performance_by_campaign') }}
