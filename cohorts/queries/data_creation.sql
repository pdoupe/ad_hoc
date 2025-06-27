/* 

  We're getting the cohort information for Vendors

    Largely based on: https://github.com/deliveryhero/datahub-airflow/blob/main/dags/vendor/vendor_performance/smart_recommendations/analytics_queries/vendor_cohorts.sql

*/

DECLARE REPORT_DATE date DEFAULT '2025-06-15';
DECLARE target_entities ARRAY<STRING>; 
DECLARE LOOKBACK_RANGE_DAYS INT64 DEFAULT 30;
SET target_entities = ['FP_SG', 'TB_AE']; 

CREATE OR REPLACE TABLE `dhh-ncr-stg.patrick_doupe.cohort_vendor_base` AS

WITH growth_entities AS (
    -- https://data.catalog.deliveryhero.net/table_detail/fulfillment-dwh-production/bigquery/curated_data_shared_vendor/growth_entities
    -- country level
    -- we only want to get the region and entity id for the entities we're looking at
    SELECT region
      , platforms.entity_id
      , is_active
    FROM `fulfillment-dwh-production.cl_vendor.growth_entities`
    LEFT JOIN UNNEST(platforms) AS platforms
    WHERE platforms.entity_id IN UNNEST(target_entities)
)

, growth_vendor_attributes_latest AS (
    -- https://data.catalog.deliveryhero.net/table_detail/fulfillment-dwh-production/bigquery/curated_data_shared_vendor/growth_vendor_attributes
    -- partition is creation date
    -- at entity_id-vendor_code level
    SELECT region
      , entity_id
      , vendor_code
      , cuisine_name
      , city_name
      , is_active
      , attributes.vertical_food
      , attributes.fixed_vendor_grade
      , attributes.fixed_is_new_vendor
      , key_account_sub_category
    FROM `fulfillment-dwh-production.cl_vendor.growth_vendor_attributes`
    WHERE created_date = REPORT_DATE -- check that this makes sense
)

, coredata_business_vendors AS (
    -- https://data.catalog.deliveryhero.net/table_detail/fulfillment-dwh-production/bigquery/curated_data_shared_coredata_business/vendors
    -- keys: vendor_uid
    -- clustering on global_entity_id
    SELECT global_entity_id
      , vendor_id
      , location.area
      , chain_id
      , chain_name
    FROM `fulfillment-dwh-production.curated_data_shared_coredata_business.vendors`
)

, vendor_stream AS (
    -- https://data.catalog.deliveryhero.net/table_detail/fulfillment-dwh-production/bigquery/curated_data_shared_data_stream/vendor_stream
    -- https://docs.api.thedatafridge.com/#tag/vendor-events/operation/Vendor
    -- want to check this
    -- there are multiple rows for vendor id. I don't fully understand it. In the meantime, 
    -- this provides one row per vendor, i believe
    SELECT DISTINCT
      content.global_entity_id
      , content.vendor_id
      , ARRAY_AGG(DISTINCT c) AS cuisine_ids
      , ARRAY_AGG(DISTINCT CASE WHEN content.budget IN ('1','2','3') THEN content.budget ELSE 'NA' END) AS budget
    FROM `fulfillment-dwh-production.curated_data_shared_data_stream.vendor_stream`
    , UNNEST(content.cuisine_ids) AS c
    GROUP BY ALL
) 

, orders AS (
    -- https://data.catalog.deliveryhero.net/table_detail/fulfillment-dwh-production/bigquery/curated_data_shared_data_stream/orders
    -- at transaction level
    -- we aggregate this to vendor level
  SELECT global_entity_id
    , vendor_id
    , COUNT(DISTINCT order_id) AS total_orders
    , COUNT(DISTINCT CASE WHEN order_status = 'DELIVERED' THEN order_id END) AS successful_orders
    , COUNT(DISTINCT CASE WHEN analytical_customer_vendor_rank = 1 AND order_status = 'DELIVERED' THEN order_id END) AS new_customer_orders
    , COUNT(DISTINCT CASE WHEN analytical_customer_vendor_rank != 1 AND order_status = 'DELIVERED' THEN analytical_customer_id END) AS retained_customers
    , COUNT(DISTINCT CASE WHEN order_status = 'DELIVERED' THEN analytical_customer_id END) AS successful_customers
  FROM `fulfillment-dwh-production.curated_data_shared_coredata_business.orders`
  WHERE partition_date_local >= DATE_SUB(REPORT_DATE, INTERVAL LOOKBACK_RANGE_DAYS DAY)
    AND global_entity_id IN UNNEST(TARGET_ENTITIES)
  GROUP BY ALL
)

, output AS (
  SELECT 
        ge.entity_id
        , cbv.vendor_id
        , chain_id
        , chain_name
        , LOWER(ge.entity_id) AS entity
        , COALESCE(gval.city_name, 'UNK') AS city
        , COALESCE(area, 'UNK') AS area
        , fixed_vendor_grade
        , fixed_is_new_vendor
        , COALESCE(cuisine_name, 'UNK') AS cuisine
        , IFNULL(budget, []) AS budget
        , COALESCE(key_account_sub_category, 'UNK') AS key_account_sub_category
        , cuisine_ids
        , o.successful_orders
        , o.total_orders
        , o.new_customer_orders
        , o.retained_customers
        , o.successful_customers
  FROM growth_entities AS ge
  LEFT JOIN growth_vendor_attributes_latest AS gval
  ON ge.entity_id = gval.entity_id
  LEFT JOIN coredata_business_vendors AS cbv
  ON gval.entity_id = cbv.global_entity_id
    AND gval.vendor_code = cbv.vendor_id
  LEFT JOIN vendor_stream AS vs
  ON gval.entity_id = vs.global_entity_id
    AND gval.vendor_code = vs.vendor_id
  LEFT JOIN orders AS o
  ON gval.entity_id = o.global_entity_id
    AND gval.vendor_code = o.vendor_id
  WHERE ge.is_active IS TRUE
    AND gval.vertical_food = 'FOOD'
    AND gval.is_active IS TRUE
    AND gval.fixed_is_new_vendor IS FALSE
)

SELECT * FROM output