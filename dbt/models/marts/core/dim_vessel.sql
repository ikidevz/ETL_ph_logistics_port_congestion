{{
  config(materialized='table')
}}

WITH vessel_raw AS (
    SELECT DISTINCT
        vessel_imo,
        cargo_type      AS primary_cargo_type
    FROM {{ source('bronze', 'raw_vessel_calls') }}
    WHERE vessel_imo IS NOT NULL
),
ranked AS (
    SELECT
        vessel_imo,
        primary_cargo_type,
        ROW_NUMBER() OVER (PARTITION BY vessel_imo
                           ORDER BY primary_cargo_type)  AS rn
    FROM vessel_raw
)
SELECT
    MD5(vessel_imo)             AS vessel_key,
    vessel_imo,
    -- IMO lookup fields (populated from AIS enrichment in prod)
    NULL::VARCHAR               AS vessel_name,
    NULL::VARCHAR               AS flag_country,
    primary_cargo_type          AS vessel_type,
    NULL::INTEGER               AS capacity_teu,
    CURRENT_TIMESTAMP()         AS dbt_updated_at
FROM ranked
WHERE rn = 1