/*

    Collecting VFD self bookings

    Thanks to Istiak Rahman for the base of the code

    TODO: 
        JOIN ONTO ALL VENDOR CODES FOR THESE ENTITIES
        THEN WE HAVE A PANEL SO THAT WE CAN GET BASE ESTIMATES
        THEN REVIEW
        THEN CREATE A TABLE AND STORE IN GOOGLE CLOUD

*/

-- '2025-04-03' was the launch date for Smart Reco
DECLARE START_DATE DATE DEFAULT DATE_SUB('2025-04-03', INTERVAL 62 DAY);
DECLARE END_DATE DATE DEFAULT '2025-06-04';
DECLARE ENTITIES ARRAY<STRING> DEFAULT ['TB_BH', 'TB_AE', 'TB_JO', 'TB_KW', 'TB_OM', 'HF_EG', 'TB_IQ', 'TB_QA', 'DJ_CZ'];

WITH vfd_dataset AS (
  SELECT *
  FROM `fulfillment-dwh-production.cl_vendor.customer_incentives`
  WHERE
    -- we need a larger lookback to identify and eliminate deals that were booked before the lookback period and were adjusted during the lookback period
    event_date BETWEEN DATE_SUB(END_DATE, INTERVAL 15 MONTH) AND END_DATE
    AND global_entity_id IN UNNEST(ENTITIES)
), 

stream_incentives_dataset AS (
  SELECT *
  FROM `fulfillment-dwh-production.curated_data_shared_data_stream.customer_incentives`
  WHERE
    DATE(updated_at) BETWEEN START_DATE AND END_DATE
    AND global_entity_id IN UNNEST(ENTITIES)
), 

customer_incentive_transactions_dataset AS (
  SELECT *
  FROM `fulfillment-dwh-production.cl_vendor.customer_incentive_transactions`
  WHERE
    transaction_date BETWEEN START_DATE AND END_DATE
    AND global_entity_id IN UNNEST(ENTITIES)
), 

vfd_bookings AS (
  SELECT
    a.event_date AS created_date
    , global_entity_id AS entity_id
    , vendor_id
    , a.create_source
    , a.create_user_id
    , a.customer_incentive_id
  FROM vfd_dataset a
  WHERE
    TRUE
    -- Filter for VFDs
    AND sponsor_ratio_vendor > 0
    -- Filter for first record of a VFD
  QUALIFY ROW_NUMBER() OVER (PARTITION BY global_entity_id, customer_incentive_id ORDER BY create_timestamp) = 1
), 

add_vfd_booking_source AS (
  SELECT
    a.*
    , v as vendor_code
    , COALESCE(create_source IN ('godroid-promo-package', 'global-godroid-godroid'), FALSE) AS godroid
    , COALESCE(create_source IN ('vp-dashboard-smart_reco') , FALSE) AS is_smart_reco_booking
    , COALESCE(create_source IN ('vp-promo-package', 'godroid-promo-package',
            'global-godroid-godroid', 'vp-dashboard-package', 'vp-cpc-package',
            'vp-dashboard-smart_reco'), 
        FALSE) AS is_oneweb_one_click_booking
    , COALESCE(create_source IN ('vp-promo-form', 'vp-promo-package',
            'vp-promo-platform_event', 'vp-onboarding-package',
            'godroid-promo-package', 'global-godroid-godroid',
            'vendor-deals-global', 'vp-dashboard-package', 'vp-cpc-package',
            'vp-dashboard-smart_reco') , FALSE) AS is_vendor_self_booking
  FROM vfd_bookings a
  LEFT JOIN UNNEST(vendor_id) v
  WHERE v IS NOT NULL
    AND create_source NOT IN ('System')
),

ss_bookings AS (
    SELECT 
        entity_id,
        vendor_code AS vendor_id,
        DATE_TRUNC(created_date, MONTH) AS month,
        -- customer_incentive_id,
        is_vendor_self_booking,
        is_smart_reco_booking
    FROM add_vfd_booking_source
    WHERE is_vendor_self_booking IS TRUE
),

all_vendors AS (
    SELECT DISTINCT
        entity_id,
        vendor_id
    FROM ss_bookings
),

all_months AS (
    SELECT DISTINCT month
    FROM ss_bookings
),

panel AS (
    SELECT
      v.entity_id,
      v.vendor_id,
      m.month
    FROM
      all_vendors AS v
      CROSS JOIN all_months AS m
)

SELECT
    p.entity_id,
    p.vendor_id,
    p.month,
    COALESCE(ss.is_vendor_self_booking, FALSE) AS ss_booking,
    COALESCE(ss.is_smart_reco_booking, FALSE) AS smart_reco_booking
FROM panel AS p
LEFT JOIN ss_bookings as ss
ON p.entity_id = ss.entity_id AND p.vendor_id = ss.vendor_id AND p.month = ss.month
ORDER BY
    p.entity_id,
    p.vendor_id,
    p.month
