{{
  config(materialized='table')
}}

WITH carrier_raw AS (
    SELECT DISTINCT carrier_code
    FROM {{ ref('stg_shipments') }}
)
SELECT
    MD5(carrier_code)   AS carrier_key,
    carrier_code,
    CASE carrier_code
        WHEN 'MAERSK'       THEN 'Maersk Line'
        WHEN 'MSC'          THEN 'Mediterranean Shipping Company'
        WHEN 'EVERGREEN'    THEN 'Evergreen Marine'
        WHEN 'CMA CGM'      THEN 'CMA CGM'
        WHEN 'OOCL'         THEN 'Orient Overseas Container Line'
        WHEN 'COSCO SHIPPING' THEN 'COSCO Shipping Lines'
        WHEN 'HAPAG-LLOYD'  THEN 'Hapag-Lloyd'
        WHEN 'ONE'          THEN 'Ocean Network Express'
        WHEN 'YANG MING'    THEN 'Yang Ming Marine Transport'
        WHEN 'ZIM'          THEN 'Zim Integrated Shipping'
        WHEN 'PIL'          THEN 'Pacific International Lines'
        WHEN 'WAN HAI'      THEN 'Wan Hai Lines'
        WHEN 'HMM'          THEN 'Hyundai Merchant Marine'
        WHEN 'MCC TRANSPORT' THEN 'MCC Transport'
        WHEN 'SITC'         THEN 'SITC Container Lines'
        WHEN 'TS LINES'     THEN 'TS Lines'
        WHEN 'X-PRESS FEEDERS' THEN 'X-Press Feeders'
        WHEN 'KMTC'         THEN 'Korea Marine Transport'
        WHEN 'MATSON'       THEN 'Matson Navigation'
        WHEN 'SEABOARD MARINE' THEN 'Seaboard Marine'
        WHEN 'GRIMALDI'     THEN 'Grimaldi Lines'
        WHEN 'ARKAS'        THEN 'Arkas Line'
        WHEN 'SAFMARINE'    THEN 'Safmarine'
        WHEN 'EMIRATES SHIPPING LINE' THEN 'Emirates Shipping Line'
        WHEN 'IRISL'        THEN 'Islamic Republic of Iran Shipping Lines'
        ELSE carrier_code
    END                 AS carrier_name,
    CASE carrier_code
        WHEN 'MAERSK'       THEN '2M'
        WHEN 'MSC'          THEN '2M'
        WHEN 'EVERGREEN'    THEN 'OCEAN'
        WHEN 'CMA CGM'      THEN 'OCEAN'
        WHEN 'OOCL'         THEN 'OCEAN'
        WHEN 'COSCO SHIPPING' THEN 'OCEAN'
        WHEN 'HAPAG-LLOYD'  THEN 'THE'
        WHEN 'ONE'          THEN 'THE'
        WHEN 'YANG MING'    THEN 'THE'
        WHEN 'HMM'          THEN 'THE'
        WHEN 'ZIM'          THEN 'INDEPENDENT'
        WHEN 'PIL'          THEN 'INDEPENDENT'
        WHEN 'WAN HAI'      THEN 'INDEPENDENT'
        WHEN 'MCC TRANSPORT' THEN 'INDEPENDENT'
        WHEN 'SITC'         THEN 'INDEPENDENT'
        WHEN 'TS LINES'     THEN 'INDEPENDENT'
        WHEN 'X-PRESS FEEDERS' THEN 'INDEPENDENT'
        WHEN 'KMTC'         THEN 'INDEPENDENT'
        WHEN 'MATSON'       THEN 'INDEPENDENT'
        WHEN 'SEABOARD MARINE' THEN 'INDEPENDENT'
        WHEN 'GRIMALDI'     THEN 'INDEPENDENT'
        WHEN 'ARKAS'        THEN 'INDEPENDENT'
        WHEN 'SAFMARINE'    THEN 'INDEPENDENT'
        WHEN 'EMIRATES SHIPPING LINE' THEN 'INDEPENDENT'
        WHEN 'IRISL'        THEN 'INDEPENDENT'
        ELSE 'INDEPENDENT'
    END                 AS alliance,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM carrier_raw