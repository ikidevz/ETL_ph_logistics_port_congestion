{{
  config(
    materialized='table',
    schema='gold'
  )
}}

SELECT
    c.carrier_code,
    c.carrier_name,
    c.alliance,
    sh.origin_port_code,
    sh.dest_port_code,
    sh.origin_port_code                                         AS origin_port_name,
    p_dest.port_name                                            AS dest_port_name,

    COUNT(*)                                                    AS total_shipments,

    ROUND(
        SUM(CASE WHEN s.delay_category = 'ON_TIME' THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                           AS on_time_pct,

    SUM(CASE WHEN s.delay_category = 'MINOR_DELAY'    THEN 1 ELSE 0 END) AS minor_delay_count,
    SUM(CASE WHEN s.delay_category = 'MODERATE_DELAY' THEN 1 ELSE 0 END) AS moderate_delay_count,
    SUM(CASE WHEN s.delay_category = 'SEVERE_DELAY'   THEN 1 ELSE 0 END) AS severe_delay_count,

    ROUND(AVG(s.delay_hours), 1)                               AS avg_delay_hours,
    ROUND(AVG(CASE WHEN s.delay_hours > 0 THEN s.delay_hours END), 1) AS avg_delay_hours_when_late,
    MAX(s.delay_hours)                                         AS max_delay_hours,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.delay_hours) AS p50_delay_hours,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY s.delay_hours) AS p90_delay_hours,

    ROUND(
        SUM(s.delay_hours * s.declared_value_usd)
        / NULLIF(SUM(s.declared_value_usd), 0), 1
    )                                                           AS value_weighted_delay_hours,

    CURRENT_TIMESTAMP()                                         AS dbt_updated_at

FROM {{ ref('fct_shipment_events') }}   s
JOIN {{ ref('dim_carrier') }}           c      ON s.carrier_key      = c.carrier_key
JOIN {{ ref('dim_shipment') }}          sh     ON s.shipment_key     = sh.shipment_key
LEFT JOIN {{ ref('dim_port') }}         p_dest ON s.dest_port_key    = p_dest.port_key

GROUP BY
    c.carrier_code, c.carrier_name, c.alliance,
    sh.origin_port_code, sh.dest_port_code,
    p_dest.port_name