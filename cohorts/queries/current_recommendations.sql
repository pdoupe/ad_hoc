DECLARE REPORT_DATE date DEFAULT '2025-05-01';

CREATE OR REPLACE TABLE `dhh-ncr-stg.patrick_doupe.current_recommendations` AS

WITH base AS (
    SELECT 
      global_entity_id
      , vendor_id
      , created_at
      , recommend_cpc
      , recommend_cpc_top_up
      , recommend_joker
      , recommend_ops_avoidable_waiting_time
      , recommend_ops_contact_rate
      , recommend_ops_fail_rate
      , recommend_ops_menu_content_score
      , recommend_ops_offline_rate
      , recommend_ops_online_markup
      , recommend_ops_ratings
      , recommend_targeted_vfd
      , recommend_vfd
      , CASE 
           WHEN recommend_ops_avoidable_waiting_time THEN TRUE
           WHEN recommend_ops_contact_rate THEN TRUE
           WHEN recommend_ops_fail_rate THEN TRUE
           WHEN recommend_ops_menu_content_score THEN TRUE
           WHEN recommend_ops_offline_rate THEN TRUE
           WHEN recommend_ops_online_markup THEN TRUE
           WHEN recommend_ops_ratings THEN TRUE ELSE FALSE END AS recommend_ops
      , CASE
        WHEN recommend_cpc THEN TRUE 
        WHEN recommend_cpc_top_up THEN TRUE
        WHEN recommend_joker THEN TRUE
        WHEN recommend_targeted_vfd THEN TRUE
        WHEN recommend_vfd THEN TRUE ELSE FALSE END AS recommend_growth
    FROM `fulfillment-dwh-production.cl_vendor._growth_vendor_smart_recommendations`
    WHERE created_at='2025-05-01'
)

SELECT * FROM base;