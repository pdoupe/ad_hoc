/* 

  We're getting the cohort information for Vendors

    Largely based on: https://github.com/deliveryhero/datahub-airflow/blob/main/dags/vendor/vendor_performance/smart_recommendations/analytics_queries/vendor_cohorts.sql

*/

DECLARE REPORT_DATE DATE DEFAULT DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH);
DECLARE target_entities ARRAY<STRING>; 

SET target_entities = ['TB_JO', 'TB_BH', 'TB_KW', 'TB_QA', 'TB_IQ', 'TB_AE',
'TB_OM', 'HF_EG', 'YS_TR', 'HS_SA', 'DJ_CZ', 'GV_RS', 'GV_HR', 'YS_TR', 'FP_SG',
'PY_AR'];

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

, growth_vendor_attributes AS (
    -- https://data.catalog.deliveryhero.net/table_detail/fulfillment-dwh-production/bigquery/curated_data_shared_vendor/growth_vendor_attributes
    -- partition is creation date
    -- at entity_id-vendor_code level
    SELECT region
      , entity_id
      , vendor_code
      , DATE_TRUNC(created_date, MONTH) AS created_month
      , cuisine_name
      , city_name
      , is_active
      , attributes.vertical_food
      , attributes.fixed_vendor_grade AS vendor_grade
      , attributes.fixed_is_new_vendor AS is_new_vendor
      , key_account_sub_category
    FROM `fulfillment-dwh-production.cl_vendor.growth_vendor_attributes`
    -- WHERE created_date = REPORT_DATE -- check that this makes sense
    QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id, vendor_code, DATE_TRUNC(created_date, MONTH) ORDER BY created_date ASC) = 1
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
    , DATE_TRUNC(DATE(placed_at_local), MONTH) AS created_month
    , COUNT(DISTINCT order_id) AS total_orders
    , COUNT(DISTINCT CASE WHEN order_status = 'DELIVERED' THEN order_id END) AS successful_orders
    , COUNT(DISTINCT CASE WHEN analytical_customer_vendor_rank = 1 AND order_status = 'DELIVERED' THEN order_id END) AS new_customer_orders
    , COUNT(DISTINCT CASE WHEN analytical_customer_vendor_rank != 1 AND order_status = 'DELIVERED' THEN analytical_customer_id END) AS retained_customers
    , COUNT(DISTINCT CASE WHEN order_status = 'DELIVERED' THEN analytical_customer_id END) AS successful_customers
    , SUM(value.order.gmv_eur) AS total_orders_gmv
    , SUM(CASE WHEN order_status = 'DELIVERED' THEN value.order.gmv_eur  ELSE 0 END) AS successful_orders_gmv
  FROM `fulfillment-dwh-production.curated_data_shared_coredata_business.orders`
  WHERE partition_date_local >= DATE_SUB(REPORT_DATE, INTERVAL 1 DAY)
    AND global_entity_id IN UNNEST(TARGET_ENTITIES)
  GROUP BY ALL
)
-- NEW CTE: All relevant vendor-month combinations
, vendor_month_combinations AS (
    SELECT DISTINCT
        ge.region,
        gval.entity_id,
        gval.vendor_code,
        cbv.chain_id,
        cbv.chain_name,
        LOWER(gval.entity_id) AS entity,
        COALESCE(gval.city_name, 'UNK') AS city,
        COALESCE(cbv.area, 'UNK') AS area,
        gval.vendor_grade,
        gval.is_new_vendor,
        COALESCE(gval.cuisine_name, 'UNK') AS cuisine,
        COALESCE(gval.key_account_sub_category, 'UNK') AS key_account_sub_category,
        months.created_month -- This is the month from the cross-join
    FROM
        growth_entities AS ge
    JOIN
        growth_vendor_attributes AS gval
    ON
        ge.entity_id = gval.entity_id
    LEFT JOIN
        coredata_business_vendors AS cbv
    ON
        gval.entity_id = cbv.global_entity_id
        AND gval.vendor_code = cbv.vendor_id
    -- Removed vendor_stream join for this initial combination, as it's not strictly
    -- needed for the core entity-vendor-month unique key.
    -- If you need vendor_stream attributes for *every* month, even if they don't change
    -- month-to-month, you'd join it here. However, typical usage is for current attributes.
    CROSS JOIN
        (SELECT DISTINCT created_month FROM orders WHERE created_month >= DATE_TRUNC(REPORT_DATE, MONTH)) AS months
        -- Added a filter here to only cross-join with months that are relevant to your report.
        -- Adjust the WHERE clause as needed to define your desired range of months.   WHERE
      WHERE        
        ge.is_active IS TRUE
        AND gval.vertical_food = 'FOOD'
        AND gval.is_active IS TRUE
        AND gval.is_new_vendor IS FALSE
),

 output AS (
    SELECT
        vmc.region,
        vmc.entity_id,
        vmc.vendor_code,
        vmc.created_month, -- Use the month from the VendorMonthCombinations CTE
        vmc.chain_id,
        vmc.chain_name,
        vmc.entity,
        vmc.city,
        vmc.area,
        vmc.vendor_grade,
        vmc.is_new_vendor,
        vmc.cuisine,
        vmc.key_account_sub_category,
        -- Now left join to vendor_stream (if you need attributes that might not change monthly,
        -- you might want to join it to growth_vendor_attributes directly, or select its
        -- latest values. For this context, assuming it's for current vendor info.)
        -- COALESCE(vs.budget, []) AS budget -- Re-add if needed, considering how it changes over time
        -- , vs.cuisine_ids -- Re-add if needed
        COALESCE(o.successful_orders, 0) AS successful_orders,
        COALESCE(o.total_orders, 0) AS total_orders,
        COALESCE(o.new_customer_orders, 0) AS new_customer_orders,
        COALESCE(o.retained_customers, 0) AS retained_customers,
        COALESCE(o.successful_customers, 0) AS successful_customers,
        COALESCE(o.total_orders_gmv, 0) AS total_orders_gmv,
        COALESCE(o.successful_orders_gmv, 0) AS successful_orders_gmv
    FROM
        vendor_month_combinations AS vmc
    LEFT JOIN
        orders AS o
    ON
        vmc.entity_id = o.global_entity_id
        AND vmc.vendor_code = o.vendor_id
        AND vmc.created_month = o.created_month
    LEFT JOIN
        vendor_stream AS vs -- Re-join vendor_stream here if you need its data
    ON
        vmc.entity_id = vs.global_entity_id
        AND vmc.vendor_code = vs.vendor_id
    -- Removed the WHERE clauses that were previously in the output CTE,
    -- as filtering is now done in VendorMonthCombinations.
    -- If there's an additional filter required for the final output, add it here.
)

SELECT * FROM output
ORDER BY entity_id, vendor_code, created_month DESC