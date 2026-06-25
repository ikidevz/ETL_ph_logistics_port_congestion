{{
  config(materialized='table')
}}

SELECT
    MD5(port_code)                      AS port_key,
    port_code,
    port_name,
    region,
    port_type,
    lat,
    lon,
    -- H3 hex index at resolution 5 (~8.5 km edge) for geospatial clustering
    H3_LATLNG_TO_CELL_STRING(lat, lon, 5)  AS h3_res5,
    H3_LATLNG_TO_CELL_STRING(lat, lon, 7)  AS h3_res7,
    CURRENT_TIMESTAMP()                 AS dbt_updated_at
FROM {{ ref('ph_ports') }}