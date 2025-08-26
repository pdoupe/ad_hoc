/*
    Code based on https://console.cloud.google.com/bigquery?ws=!1m7!1m6!12m5!1m3!1sdhh-ncr-stg!2sus-central1!3s1a065770-b5f4-4896-b229-10ec088ace5d!2e1&inv=1&invt=Ab5irw&project=dhh-ncr-stg
    This is used for MB meeting calculation

    We collect ground truth booking data for June 2025
*/

DECLARE reco_source STRING DEFAULT '{RECO_SOURCE}'; -- Set to 'CEB' for CEB recos or 'ML' for ML recos.
DECLARE booking_source_param STRING DEFAULT '{BOOKING_SOURCE}'; -- Set to 'agent' or 'vendor'.

WITH
 outcomes AS (
  SELECT
    date_month, 
    management_entity,
    accounts_segment,
    long_tail_sub_category,
    t1.global_entity_id,
    t1.vendor_id,
    campaign_id,
    ROUND(SUM(gmv_eur_direct), 1) AS gmv_eur_direct,
    ROUND(SUM(initial_budget),0) as booked_budget,
    SUM(cpc_clicks) as cpc_clicks,
    SUM(cpc_orders) as cpc_orders,
    SUM(cpc_revenue) as cpc_revenue,
    SAFE_DIVIDE(SUM(gmv_eur_direct), SUM(cpc_revenue)) as roas,
    ROUND(SAFE_DIVIDE(SUM(cpc_revenue), SUM(cpc_clicks)), 2) as bid_eur,
  FROM
    `dhh-ncr-live.analytics.ad_tech_il_cpc` AS t1
  WHERE TRUE
    # AND (global_entity_id LIKE 'PY_%' or global_entity_id LIKE 'TB_%')
    AND date_month = '2025-06-01' 
    AND vertical_parent = 'Food'  
    AND booking_source IN (booking_source_param)
    AND DATE(created_date) >= '2025-06-01'
    GROUP BY ALL 
 ),

history AS (
  SELECT
    global_entity_id,
    vendor_id,
    SUM(value.order.gmv_eur) AS L3m_total_gmv_eur,
    SUM(value.order.gmv_eur) / COUNT(DISTINCT order_id) AS L3m_average_order_value_eur
  FROM `fulfillment-dwh-production.curated_data_shared_coredata_business.orders` 
  WHERE partition_date_local >= '2025-03-01' AND partition_date_local < '2025-06-01'
    AND vertical_parent = 'Food'  
    GROUP BY ALL 
 ),

il_cpc AS (
  SELECT
    o.*,
    COALESCE(h.L3m_total_gmv_eur, 0) AS L3m_total_gmv_eur,
    COALESCE(h.L3m_average_order_value_eur, 0) AS L3m_average_order_value_eur
  FROM outcomes o
  LEFT JOIN history h
  USING (global_entity_id, vendor_id)
),
 
recos_ml as (
  SELECT * FROM 
((SELECT *, "PEYA" as MARKET FROM `dhh-ncr-live.performance_estimation.LATAM_budget_recos` r
WHERE r.reco_date = '2025-05-29')
UNION ALL
(SELECT *, 
  IF (global_entity_id LIKE "TB_%", "TALABAT", "HS") as MARKET 
  FROM `dhh-ncr-live.performance_estimation.MENA_budget_recos` r
WHERE r.reco_date = '2025-05-07'))
LEFT JOIN  il_cpc USING (global_entity_id, vendor_id)
WHERE il_cpc.cpc_clicks is not Null
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
  LEFT JOIN il_cpc USING (global_entity_id, vendor_id)
  WHERE il_cpc.cpc_clicks is not Null
),

final_recos AS (
  SELECT
    global_entity_id,
    vendor_id,
    accounts_segment,
    long_tail_sub_category,
    L3m_average_order_value_eur,
    L3m_total_gmv_eur,
    min_budget_rec_eur,
    max_budget_rec_eur,
    min_budget_clicks,
    max_budget_clicks,
    min_budget_orders,
    max_budget_orders,
    min_budget_roas,
    max_budget_roas,
    date_month,
    management_entity,
    campaign_id,
    booked_budget,
    cpc_clicks,
    cpc_orders,
    cpc_revenue,
    roas,
    bid_eur
  FROM recos_ceb_t
  WHERE reco_source = 'CEB'

  UNION ALL

  SELECT
    global_entity_id,
    vendor_id,
    accounts_segment,
    long_tail_sub_category,
    L3m_average_order_value_eur,
    L3m_total_gmv_eur,
    min_budget_rec_eur,
    max_budget_rec_eur,
    min_budget_clicks,
    max_budget_clicks,
    min_budget_orders,
    max_budget_orders,
    min_budget_roas,
    max_budget_roas,
    date_month,
    management_entity,
    campaign_id,
    booked_budget,
    cpc_clicks,
    cpc_orders,
    cpc_revenue,
    roas,
    bid_eur
  FROM recos_ml
  WHERE reco_source = 'ML'
),

kb AS (
  SELECT
    global_entity_id,
    vendor_id,
    date_month,
    management_entity,
    accounts_segment,
    long_tail_sub_category,
    L3m_average_order_value_eur,
    L3m_total_gmv_eur,
    campaign_id,
    booked_budget,
    cpc_clicks,
    cpc_orders,
    cpc_revenue,
    roas,
    bid_eur,
    -- CEB-specific calculations for the six parameters
    (max_budget_clicks - min_budget_clicks) / (max_budget_rec_eur - min_budget_rec_eur) AS k_c,
    min_budget_clicks - (max_budget_clicks - min_budget_clicks) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur AS b_c,
    (max_budget_orders - min_budget_orders) / (max_budget_rec_eur - min_budget_rec_eur) AS k_o,
    min_budget_orders - (max_budget_orders - min_budget_orders) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur AS b_o,
    (max_budget_roas - min_budget_roas) / (max_budget_rec_eur - min_budget_rec_eur) AS k_r,
    min_budget_roas - (max_budget_roas - min_budget_roas) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur AS b_r
  FROM
    recos_ceb_t
  WHERE
    reco_source = 'CEB' AND max_budget_rec_eur > min_budget_rec_eur

  UNION ALL

  SELECT
    global_entity_id,
    vendor_id,
    date_month,
    management_entity,
    accounts_segment,
    long_tail_sub_category,
    L3m_average_order_value_eur,
    L3m_total_gmv_eur,
    campaign_id,
    booked_budget,
    cpc_clicks,
    cpc_orders,
    cpc_revenue,
    roas,
    bid_eur,
    (max_budget_clicks - min_budget_clicks) / (max_budget_rec_eur - min_budget_rec_eur) AS k_c,
    min_budget_clicks - (max_budget_clicks - min_budget_clicks) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur AS b_c,
    (max_budget_orders - min_budget_orders) / (max_budget_rec_eur - min_budget_rec_eur) AS k_o,
    min_budget_orders - (max_budget_orders - min_budget_orders) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur AS b_o,
    (max_budget_roas - min_budget_roas) / (max_budget_rec_eur - min_budget_rec_eur) AS k_r,
    min_budget_roas - (max_budget_roas - min_budget_roas) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur AS b_r
  FROM
    recos_ml
  WHERE
    reco_source = 'ML' AND max_budget_rec_eur > min_budget_rec_eur
),

adj as (
SELECT *,
  k_c * booked_budget + b_c as adj_e_clicks,
  k_o * booked_budget + b_o as adj_e_orders,
  k_r * booked_budget + b_r as adj_e_roas
FROM kb
)

SELECT 
  '{RECO_SOURCE}' AS reco_source, 
  '{BOOKING_SOURCE}' AS booking_source,
    *
FROM adj 