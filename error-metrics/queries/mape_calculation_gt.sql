/*
    Code based on https://console.cloud.google.com/bigquery?ws=!1m7!1m6!12m5!1m3!1sdhh-ncr-stg!2sus-central1!3s1a065770-b5f4-4896-b229-10ec088ace5d!2e1&inv=1&invt=Ab5irw&project=dhh-ncr-stg
    This is used for MB meeting calculation

    We collect ground truth booking data for June 2025
*/
WITH
 il_cpc_cte AS (
  SELECT
    date_month, 
    management_entity,
    t1.global_entity_id,
    t1.vendor_id,
    campaign_id,
    ROUND(SUM(initial_budget),0) as booked_budget,
    SUM(cpc_clicks) as cpc_clicks,
    SUM(cpc_orders) as cpc_orders,
    SUM(cpc_revenue) as cpc_revenue,
    SUM(gmv_eur_direct) / SUM(cpc_revenue) as roas,
    ROUND(SAFE_DIVIDE(SUM(cpc_revenue), SUM(cpc_clicks)), 2) as bid_eur,
  FROM
    `dhh-ncr-live.analytics.ad_tech_il_cpc` AS t1
  WHERE TRUE
    # AND (global_entity_id LIKE 'PY_%' or global_entity_id LIKE 'TB_%')
    AND date_month = '2025-06-01' 
    AND vertical_parent = 'Food'  
    AND booking_source IN ('agent')
    AND DATE(created_date) >= '2025-06-01'
    GROUP BY ALL 
 ),

recos as (
  SELECT * FROM 
((SELECT *, "PEYA" as MARKET FROM `dhh-ncr-live.performance_estimation.LATAM_budget_recos` r
WHERE r.reco_date = '2025-05-29')
UNION ALL
(SELECT *, 
  IF (global_entity_id LIKE "TB_%", "TALABAT", "HS") as MARKET 
  FROM `dhh-ncr-live.performance_estimation.MENA_budget_recos` r
WHERE r.reco_date = '2025-05-07'))
LEFT JOIN 
il_cpc_cte
USING (global_entity_id, vendor_id)
WHERE il_cpc_cte.cpc_clicks > 0
  and il_cpc_cte.cpc_orders > 0
  and il_cpc_cte.cpc_revenue > 0
),

gaid_currency as
(select 
  global_entity_id, 
  currency_local as 
  currency_code
  from `fulfillment-dwh-production.curated_data_shared_adtech.dim_campaigns`
  group by all
),

recos_ceb as (
  SELECT 
    a.global_entity_id,
    vendor_id,
    ROUND(budget_tier_low/cast(fx_rate_eur as FLOAT64),0) as min_budget_rec_eur, 
    ROUND(budget_tier_high/cast(fx_rate_eur as FLOAT64),0) as max_budget_rec_eur,

    ROUND(ceb_low/cast(fx_rate_eur as FLOAT64), 2) as bid_price_low_eur,
    ROUND(ceb_high/cast(fx_rate_eur as FLOAT64), 2) as bid_price_high_eur,

    click_estimation_low as min_budget_clicks,
    click_estimation_high as max_budget_clicks,

    order_estimation_low as min_budget_orders,
    order_estimation_high as max_budget_orders,

    3 as min_budget_roas,
    3 as max_budget_roas

    FROM 
    `dhh-ncr-live.analytics.ceb_log_with_bookings`  as a 
    join gaid_currency b on a.global_entity_id=b.global_entity_id
    join fulfillment-dwh-production.curated_data_shared_coredata.fx_rates c on b.currency_code=c.currency_code and date(a.timestamp)=c.fx_rate_date
    WHERE DATE_TRUNC(DATE(timestamp), day) BETWEEN '2025-05-01' AND '2025-05-30'
    GROUP BY ALL
),

recos_ceb_t as (
  SELECT * FROM recos_ceb
  LEFT JOIN 
  il_cpc_cte
  USING (global_entity_id, vendor_id)
  WHERE il_cpc_cte.cpc_clicks > 0
    and il_cpc_cte.cpc_orders > 0
    and il_cpc_cte.cpc_revenue > 0
),

kb as (
  SELECT *,
    (max_budget_clicks - min_budget_clicks) / (max_budget_rec_eur - min_budget_rec_eur) as k_c,
  min_budget_clicks - (max_budget_clicks - min_budget_clicks) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur as b_c,
    (max_budget_orders - min_budget_orders) / (max_budget_rec_eur - min_budget_rec_eur) as k_o,
  min_budget_orders - (max_budget_orders - min_budget_orders) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur as b_o,
    (max_budget_roas - min_budget_roas) / (max_budget_rec_eur - min_budget_rec_eur) as k_r,
  min_budget_roas - (max_budget_roas - min_budget_roas) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur as b_r
  FROM recos_ceb_t
  WHERE max_budget_rec_eur > min_budget_rec_eur
),

adj as (
SELECT *,
  k_c * booked_budget + b_c as adj_e_clicks,
  k_o * booked_budget + b_o as adj_e_orders,
  k_r * booked_budget + b_r as adj_e_roas
FROM kb
),

ape_t as (
SELECT *,
  SAFE_DIVIDE(ABS(cpc_clicks - adj_e_clicks), cpc_clicks) * 100 as ape_c, 
  SAFE_DIVIDE(ABS(cpc_orders - adj_e_orders), cpc_orders) * 100 as ape_o, 
  SAFE_DIVIDE(ABS(roas - adj_e_roas), roas) * 100 as ape_r 
FROM adj
),

cl_groups as (
SELECT *,
   IF(cpc_clicks < 100, "<100", ">100")  as clicks_group 
FROM ape_t
)

SELECT 
  management_entity,
  clicks_group,
  COUNT(*) as total_campaigns,
  ROUND(AVG(ape_c), 0) as MAPE_clicks,
  ROUND(AVG(ape_o), 0) as MAPE_orders,
  ROUND(AVG(ape_r), 0) as MAPE_roas,
FROM cl_groups 
# AND booked_budget > min_budget_rec_eur
# AND booked_budget < max_budget_rec_eur
GROUP BY clicks_group, management_entity
ORDER BY management_entity, clicks_group