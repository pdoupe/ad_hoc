/*
    Code based on https://console.cloud.google.com/bigquery?ws=!1m7!1m6!12m5!1m3!1sdhh-ncr-stg!2sus-central1!3s1a065770-b5f4-4896-b229-10ec088ace5d!2e1&inv=1&invt=Ab5irw&project=dhh-ncr-stg
    This is used for MB meeting calculation

    We collect ground truth booking data for June 2025
*/

DECLARE OUTCOMES_MONTH DATE DEFAULT '{OUTCOMES_MONTH}';
DECLARE RECOMMENDATIONS_MONTH DATE DEFAULT '{RECOMMENDATIONS_MONTH}';

WITH
 il_cpc AS (
  SELECT
    date_month, 
    management_entity,
    accounts_segment,
    long_tail_sub_category,
    t1.global_entity_id,
    t1.vendor_id,
    campaign_id,
    booking_source,
    ROUND(SUM(gmv_eur_direct), 1) AS gmv_eur_direct,
    ROUND(SUM(initial_budget),0) as booked_budget,
    SUM(cpc_clicks) as cpc_clicks,
    SUM(cpc_orders) as cpc_orders,
    SUM(cpc_revenue) as cpc_revenue,
    SAFE_DIVIDE(SUM(gmv_eur_direct), SUM(cpc_revenue)) as roas,
    ROUND(SAFE_DIVIDE(SUM(cpc_revenue), SUM(cpc_clicks)), 2) as bid_eur,
  FROM
    `dhh-ncr-live.analytics.ad_tech_il_cpc` AS t1
  WHERE --(global_entity_id LIKE 'PY_%' or global_entity_id LIKE 'TB_%')
    date_month = OUTCOMES_MONTH
    AND vertical_parent = 'Food'  
    AND booking_source IN ('agent', 'vendor')
    AND DATE(created_date) >= OUTCOMES_MONTH 
    GROUP BY ALL 
 ),

recos_ml as (
  SELECT 
    *, 
    'ML' AS reco_source
  FROM (
    (
        SELECT *, "PEYA" as MARKET 
        FROM `dhh-ncr-live.performance_estimation.LATAM_budget_recos` r
        -- select the most recent recommendations from the month
        WHERE r.reco_date = (SELECT MAX(reco_date) FROM `dhh-ncr-live.performance_estimation.LATAM_budget_recos` WHERE DATE_TRUNC(reco_date, month) = DATE(RECOMMENDATIONS_MONTH))
    )
    UNION ALL (
        SELECT 
            *, 
            IF (global_entity_id LIKE "TB_%", "TALABAT", "HS") as MARKET 
        FROM `dhh-ncr-live.performance_estimation.MENA_budget_recos` r
        -- select the most recent recommendations from the month
        WHERE r.reco_date = (SELECT MAX(reco_date) FROM `dhh-ncr-live.performance_estimation.MENA_budget_recos` WHERE DATE_TRUNC(reco_date, month) = DATE(RECOMMENDATIONS_MONTH))
    )
    )
  LEFT JOIN il_cpc USING (global_entity_id, vendor_id)
  WHERE il_cpc.cpc_clicks IS NOT Null
),

gaid_currency as (
    SELECT 
      global_entity_id, 
      currency_local as 
      currency_code
    FROM `fulfillment-dwh-production.curated_data_shared_adtech.dim_campaigns`
    GROUP BY ALL
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

    FROM `dhh-ncr-live.analytics.ceb_log_with_bookings`  AS a 
    JOIN gaid_currency b on a.global_entity_id=b.global_entity_id
    JOIN fulfillment-dwh-production.curated_data_shared_coredata.fx_rates c on b.currency_code=c.currency_code and date(a.timestamp)=c.fx_rate_date
    WHERE DATE_TRUNC(DATE(timestamp), MONTH) = RECOMMENDATIONS_MONTH
    GROUP BY ALL
),

recos_ceb_t as (
  SELECT 
    *, 
    'CEB' AS reco_source,
  FROM recos_ceb
  LEFT JOIN il_cpc USING (global_entity_id, vendor_id)
  WHERE il_cpc.cpc_clicks is not Null
),

final_recos AS (
  SELECT
    global_entity_id,
    vendor_id,
    accounts_segment,
    long_tail_sub_category,
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
    bid_eur,
    reco_source,
    booking_source
  FROM recos_ceb_t

  UNION ALL

  SELECT
    global_entity_id,
    vendor_id,
    accounts_segment,
    long_tail_sub_category,
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
    bid_eur,
    reco_source,
    booking_source
  FROM recos_ml
),

kb AS (
  SELECT
    global_entity_id,
    vendor_id,
    date_month,
    management_entity,
    accounts_segment,
    long_tail_sub_category,
    campaign_id,
    booked_budget,
    cpc_clicks,
    cpc_orders,
    cpc_revenue,
    roas,
    bid_eur,
    reco_source,
    booking_source,
    -- CEB-specific calculations for the six parameters
    (max_budget_clicks - min_budget_clicks) / (max_budget_rec_eur - min_budget_rec_eur) AS k_c,
    min_budget_clicks - (max_budget_clicks - min_budget_clicks) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur AS b_c,
    (max_budget_orders - min_budget_orders) / (max_budget_rec_eur - min_budget_rec_eur) AS k_o,
    min_budget_orders - (max_budget_orders - min_budget_orders) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur AS b_o,
    (max_budget_roas - min_budget_roas) / (max_budget_rec_eur - min_budget_rec_eur) AS k_r,
    min_budget_roas - (max_budget_roas - min_budget_roas) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur AS b_r
  FROM
    recos_ceb_t
  WHERE max_budget_rec_eur > min_budget_rec_eur

  UNION ALL

  SELECT
    global_entity_id,
    vendor_id,
    date_month,
    management_entity,
    accounts_segment,
    long_tail_sub_category,
    campaign_id,
    booked_budget,
    cpc_clicks,
    cpc_orders,
    cpc_revenue,
    roas,
    bid_eur,
    reco_source,
    booking_source,
    (max_budget_clicks - min_budget_clicks) / (max_budget_rec_eur - min_budget_rec_eur) AS k_c,
    min_budget_clicks - (max_budget_clicks - min_budget_clicks) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur AS b_c,
    (max_budget_orders - min_budget_orders) / (max_budget_rec_eur - min_budget_rec_eur) AS k_o,
    min_budget_orders - (max_budget_orders - min_budget_orders) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur AS b_o,
    (max_budget_roas - min_budget_roas) / (max_budget_rec_eur - min_budget_rec_eur) AS k_r,
    min_budget_roas - (max_budget_roas - min_budget_roas) / (max_budget_rec_eur - min_budget_rec_eur) * min_budget_rec_eur AS b_r
  FROM
    recos_ml
  WHERE max_budget_rec_eur > min_budget_rec_eur
),

adj as (
SELECT *,
  k_c * booked_budget + b_c as adj_e_clicks,
  k_o * booked_budget + b_o as adj_e_orders,
  k_r * booked_budget + b_r as adj_e_roas
FROM kb
),

base AS (
    SELECT 
        *
    FROM adj 
)

SELECT 
    management_entity,
    reco_source,
    100*SAFE_DIVIDE(AVG(ABS(cpc_clicks - adj_e_clicks)),  AVG(cpc_clicks)) AS SMAE_clicks,
    100*SAFE_DIVIDE(AVG(ABS(cpc_orders - adj_e_orders)), AVG(cpc_orders)) AS SMAE_orders,
    100*SAFE_DIVIDE(AVG(ABS(roas - adj_e_roas)), AVG(roas)) AS SMAE_roas,
    100*AVG(SAFE_DIVIDE(ABS(cpc_clicks - adj_e_clicks), cpc_clicks)) AS MAPE_clicks,
    100*AVG(SAFE_DIVIDE(ABS(cpc_orders - adj_e_orders), cpc_orders)) AS MAPE_orders,
    100*AVG(SAFE_DIVIDE(ABS(roas - adj_e_roas), roas)) AS MAPE_roas,
    AVG(ABS(cpc_clicks - adj_e_clicks)) AS MAE_clicks,
    AVG(ABS(cpc_orders - adj_e_orders))AS MAE_orders,
    AVG(ABS(roas - adj_e_roas)) AS MAE_roas
FROM base
WHERE adj_e_clicks > 100
AND reco_source = 'CEB'
GROUP BY ALL
ORDER BY 2, 1

