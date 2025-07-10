DECLARE REPORT_DATE date DEFAULT '2025-07-02';

CREATE OR REPLACE TABLE `dhh-ncr-stg.patrick_doupe.current_recommendations` AS

WITH base AS (
    SELECT 
      entity_id AS global_entity_id
      , vendor_code AS vendor_id
      , created_date
      , CASE WHEN recommend_cpc THEN 1 ELSE 0 END AS cpc
      , CASE WHEN recommend_joker THEN 1 ELSE 0 END AS joker
      , CASE WHEN recommend_targeted_vfd THEN 1 ELSE 0 END AS targetd_vfd
      , CASE WHEN recommend_vfd THEN 1 ELSE 0 END AS vfd
      , CASE WHEN recommend_ops_avoidable_waiting_time THEN 1 ELSE 0 END AS avoidable_waiting_time
      , CASE WHEN recommend_ops_contact_rate THEN 1 ELSE 0 END AS contact_rate
      , CASE WHEN recommend_ops_fail_rate THEN 1 ELSE 0 END AS fail_rate
      , CASE WHEN recommend_ops_menu_content_score THEN 1 ELSE 0 END AS menu_content_score
      , CASE WHEN recommend_ops_offline_rate THEN 1 ELSE 0 END AS offline_rate
      , CASE WHEN recommend_ops_online_markup THEN 1 ELSE 0 END AS online_markup
      , CASE WHEN recommend_ops_ratings THEN 1 ELSE 0 END AS ratings
      , CASE
            WHEN recommend_cpc THEN 1 
            WHEN recommend_joker THEN 1
            WHEN recommend_targeted_vfd THEN 1
            WHEN recommend_vfd THEN 1 ELSE 0 END AS any_growth
      , CASE 
           WHEN recommend_ops_avoidable_waiting_time THEN 1
           WHEN recommend_ops_contact_rate THEN 1
           WHEN recommend_ops_fail_rate THEN 1
           WHEN recommend_ops_menu_content_score THEN 1
           WHEN recommend_ops_offline_rate THEN 1
           WHEN recommend_ops_online_markup THEN 1
           WHEN recommend_ops_ratings THEN 1 ELSE 0 END AS any_ops
      , CASE 
            WHEN recommend_cpc THEN 1 
            WHEN recommend_joker THEN 1
            WHEN recommend_targeted_vfd THEN 1
            WHEN recommend_vfd THEN 1 
            WHEN recommend_ops_avoidable_waiting_time THEN 1
            WHEN recommend_ops_contact_rate THEN 1
            WHEN recommend_ops_fail_rate THEN 1
            WHEN recommend_ops_menu_content_score THEN 1
            WHEN recommend_ops_offline_rate THEN 1
            WHEN recommend_ops_online_markup THEN 1
            WHEN recommend_ops_ratings THEN 1 ELSE 0 END AS any_recommendation
    FROM `fulfillment-dwh-production.curated_data_shared_vendor.growth_vendor_smart_recommendations`
    WHERE created_date=REPORT_DATE
)
SELECT * FROM base;