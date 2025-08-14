DECLARE REPORT_DATE date DEFAULT '2025-07-01';
DECLARE target_entities ARRAY<STRING>; 
DECLARE LOOKBACK_RANGE_DAYS INT64 DEFAULT 365;
SET target_entities = ['TB_JO', 'TB_BH', 'TB_KW', 'TB_QA', 'TB_IQ', 'TB_AE',
'TB_OM', 'HF_EG', 'YS_TR', 'HS_SA', 'DJ_CZ', 'GV_RS', 'GV_HR', 'YS_TR', 'FP_SG',
'PY_AR'];

CREATE OR REPLACE TABLE `dhh-ncr-stg.patrick_doupe.current_cohort_vendor_base` AS

WITH current_cohorts AS (
    SELECT 
        entity_id AS global_entity_id
        , vendor_code AS vendor_id
        , cohort_id
    FROM `fulfillment-dwh-production.cl_vendor.growth_vendor_smart_recommendations`
    WHERE created_date = REPORT_DATE 
    AND entity_id IN UNNEST(target_entities)
)

, orders AS (
    -- https://data.catalog.deliveryhero.net/table_detail/fulfillment-dwh-production/bigquery/curated_data_shared_data_stream/orders
    -- at transaction level
    -- we aggregate this to vendor level
  SELECT global_entity_id
    , vendor_id
    , DATE_TRUNC(DATE(placed_at_local), MONTH) AS created_month
    , COALESCE(COUNT(DISTINCT CASE WHEN order_status = 'DELIVERED' THEN order_id END), 0) AS orders
    , COALESCE(COUNT(DISTINCT CASE WHEN order_status = 'DELIVERED' THEN analytical_customer_id END), 0) AS customers
    , COALESCE(SUM(CASE WHEN order_status = 'DELIVERED' THEN value.order.gmv_eur  ELSE 0 END), 0) AS gmv
  FROM `fulfillment-dwh-production.curated_data_shared_coredata_business.orders`
  WHERE partition_date_local >= DATE_SUB(REPORT_DATE, INTERVAL LOOKBACK_RANGE_DAYS DAY)
    AND global_entity_id IN UNNEST(TARGET_ENTITIES)
    AND placed_at_local >= DATE_SUB(REPORT_DATE, INTERVAL LOOKBACK_RANGE_DAYS DAY)
  GROUP BY ALL
),

all_combinations AS (
    SELECT DISTINCT
        c.global_entity_id,
        c.vendor_id,
        c.cohort_id,
        months.created_month
    FROM
        current_cohorts AS c
    CROSS JOIN (SELECT DISTINCT created_month FROM orders) AS months
)

, joined AS (
SELECT
    ac.global_entity_id,
    ac.vendor_id,
    ac.cohort_id,
    ac.created_month,
    COALESCE(o.gmv, 0) AS gmv,
    COALESCE(o.customers, 0) AS customers,
    COALESCE(o.orders, 0) AS orders
FROM
    all_combinations AS ac
LEFT JOIN
    orders AS o
ON
    ac.global_entity_id = o.global_entity_id
    AND ac.vendor_id = o.vendor_id
    AND ac.created_month = o.created_month
)

SELECT * FROM joined