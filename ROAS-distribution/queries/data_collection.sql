/*
    Code based on https://console.cloud.google.com/bigquery?ws=!1m7!1m6!12m5!1m3!1sdhh-ncr-stg!2sus-central1!3s1a065770-b5f4-4896-b229-10ec088ace5d!2e1&inv=1&invt=Ab5irw&project=dhh-ncr-stg
    This is used for MB meeting calculation
*/

WITH
 il_cpc_cte AS (
  SELECT
    date_month, 
    created_date,
    management_entity,
    t1.global_entity_id,
    t1.vendor_id,
    campaign_id,
    booking_source,
    ROUND(SUM(gmv_eur_direct), 1) AS gmv_eur_direct,
    ROUND(SUM(initial_budget),0) as booked_budget,
    SAFE_DIVIDE(SUM(gmv_eur_direct), SUM(cpc_revenue)) as roas
  FROM
    `dhh-ncr-live.analytics.ad_tech_il_cpc` AS t1
  WHERE date_month = '{ANALYSIS_MONTH}'
    AND DATE(created_date) >= '{ANALYSIS_MONTH}'
    AND vertical_parent = 'Food'  
    AND booking_source IN ('agent', 'vendor')
    GROUP BY ALL 
)

SELECT *
FROM il_cpc_cte 
