/*

    Collecting CPC and Joker self bookings for today

    Thanks to Istiak Rahman for the bulk of the code

*/


DECLARE ENTITIES ARRAY<STRING> DEFAULT ['TB_BH', 'TB_AE', 'TB_JO', 'TB_KW', 'TB_OM', 'HF_EG', 'TB_IQ', 'TB_QA', 'DJ_CZ'];

WITH 

-- Get campaigns data
campaigns_dataset AS (
  SELECT *
  FROM `fulfillment-dwh-production.curated_data_shared_adtech.dim_campaigns`
  WHERE
    global_entity_id IN  UNNEST(ENTITIES)
), 

-- get metadata and campaign type
campaigns AS (
  SELECT
    a.global_entity_id AS entity_id
    , a.campaign_id
    , av.id AS vendor_code
    , a.pricing.budget_eur
    , a.status
    , a.origin
    , a.created_by_service
    , a.created_by_actor
    , a.tool
    , DATE(a.modified_at_utc) AS created_date
    , DATETIME(a.modified_at_utc) AS created_at
    , product
    , CASE
      WHEN product IN ('premium_placements', 'organic_placements', 'featured_products', 'kwords_search')
        THEN 'cpc'
      WHEN product IN ('sponsored_deals_joker', 'promotion')
        THEN 'joker'
      ELSE 'other'
    END AS campaign_type
    , ROW_NUMBER() OVER (PARTITION BY global_entity_id, campaign_id ORDER BY modified_at_utc) AS _version
  FROM campaigns_dataset a
  LEFT JOIN UNNEST(advertiser.vendors) av
), 

-- get booking source 

add_cpc_booking_source AS (
  SELECT
    *
    , COALESCE(
      product IN ('premium_placements', 'organic_placements')
      AND created_by_actor = 'vendor'
      AND tool = 'vp-dashboard-smart_reco'
      , FALSE
    ) AS is_smart_reco_booking
    , COALESCE(
      product IN ('premium_placements', 'organic_placements')
      AND created_by_actor = 'vendor'
      AND (
        created_by_service IN ('vp-vfd-adoption')
        AND tool IN ('vp-promo-package_success_page', 'vp-dashboard-package', 'vp-dashboard-smart_reco')
      )
      , FALSE
    ) AS is_oneweb_one_click_booking
    , COALESCE(
      product IN ('premium_placements', 'organic_placements')
      AND created_by_actor = 'vendor'
      AND created_by_service NOT IN ('at-agent-campaigns-gateway')
      AND tool not in ('vp-ag-package')
      , FALSE
    ) AS is_vendor_self_booking
    , COALESCE(
      created_by_actor = 'agent'
      OR created_by_service = 'at-agent-campaigns-gateway'
      , FALSE
    ) AS is_agent_booking
  FROM campaigns
  -- filter for campaign creation events
  WHERE
    _version = 1
    AND status != 'cancelled'
)

SELECT *
FROM add_cpc_booking_source
WHERE is_vendor_self_booking IS TRUE
