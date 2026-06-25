{{
  config(
    materialized='table',
    schema='gold'
  )
}}

SELECT
    s.shipment_id,
    p.port_code                         AS dest_port_code,
    p.port_name,
    ST_DISTANCE(
        ST_MAKEPOINT(p.lon, p.lat),
        ST_MAKEPOINT(p2.lon, p2.lat)
    ) / 1000                            AS distance_km,
    p2.port_name                        AS nearest_alt_port
FROM {{ ref('fct_shipment_events') }} s
JOIN {{ ref('dim_port') }}            p  ON s.dest_port_key  = p.port_key
JOIN {{ ref('dim_port') }}            p2 ON p2.port_key != p.port_key
WHERE ST_DISTANCE(
    ST_MAKEPOINT(p.lon, p.lat),
    ST_MAKEPOINT(p2.lon, p2.lat)
) / 1000 <= 200
  AND s.delay_category IN ('MODERATE_DELAY', 'SEVERE_DELAY')
ORDER BY distance_km