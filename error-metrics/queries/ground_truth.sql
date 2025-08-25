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
 )

SELECT * FROM il_cpc_cte
 