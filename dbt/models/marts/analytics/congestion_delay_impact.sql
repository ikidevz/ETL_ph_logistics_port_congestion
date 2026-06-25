-- models/marts/analytics/congestion_delay_impact.sql

{{
  config(
    materialized='incremental',
    unique_key='port_code || cast(week_start_date as varchar)',
    schema='gold'
  )
}}

WITH weekly_dwell AS (
    SELECT
        p.port_code,
        p.port_name,
        DATE_TRUNC('week', f.arrival_date_key::DATE)            AS week_start_date,
        COUNT(*)                                                 AS vessel_calls,
        ROUND(AVG(f.anchorage_wait_h), 1)                      AS avg_anchorage_wait_hours,
        ROUND(AVG(f.dwell_hours), 1)                            AS avg_dwell_hours,
        ROUND(
            SUM(CASE WHEN f.is_congested THEN 1 ELSE 0 END)::FLOAT
            / NULLIF(COUNT(*), 0) * 100, 1
        )                                                        AS congestion_rate_pct,
        SUM(f.teu_count)                                         AS total_teu_handled

    FROM {{ ref('fct_port_dwell') }}    f
    JOIN {{ ref('dim_port') }}          p ON f.port_key = p.port_key
    GROUP BY p.port_code, p.port_name, DATE_TRUNC('week', f.arrival_date_key::DATE)
),

weekly_shipments AS (
    SELECT
        p.port_code                                          AS port_code, 
        DATE_TRUNC('week', s.eta_date_key::DATE)            AS week_start_date,
        COUNT(*)                                             AS shipments_arriving,
        ROUND(AVG(s.delay_hours), 1)                        AS avg_shipment_delay_hours,
        ROUND(
            SUM(CASE WHEN s.delay_category = 'ON_TIME' THEN 1 ELSE 0 END)::FLOAT
            / NULLIF(COUNT(*), 0) * 100, 1
        )                                                    AS on_time_rate_pct,
        SUM(CASE WHEN s.delay_category = 'SEVERE_DELAY' THEN 1 ELSE 0 END) AS severe_delay_count

    FROM {{ ref('fct_shipment_events') }} s
    JOIN {{ ref('dim_port') }} p ON s.dest_port_key = p.port_key 
    GROUP BY p.port_code, DATE_TRUNC('week', s.eta_date_key::DATE)
)

SELECT
    wd.port_code,
    wd.port_name,
    wd.week_start_date,
    wd.vessel_calls,
    wd.total_teu_handled,
    wd.avg_anchorage_wait_hours,
    wd.avg_dwell_hours,
    wd.congestion_rate_pct,
    ws.shipments_arriving,
    ws.avg_shipment_delay_hours,
    ws.on_time_rate_pct,
    ws.severe_delay_count,

    -- Congestion severity label (mirrors dt_port_congestion logic)
    CASE
        WHEN wd.congestion_rate_pct >= 60 THEN 'CRITICAL'
        WHEN wd.congestion_rate_pct >= 35 THEN 'HIGH'
        WHEN wd.congestion_rate_pct >= 15 THEN 'MODERATE'
        ELSE 'NORMAL'
    END                                                         AS congestion_level,

    ROUND(
        ws.avg_shipment_delay_hours / NULLIF(wd.avg_anchorage_wait_hours, 0)
    , 2)                                                        AS delay_to_wait_ratio,

    CURRENT_TIMESTAMP()                                         AS dbt_updated_at

FROM weekly_dwell wd
LEFT JOIN weekly_shipments ws
    ON wd.port_code        = ws.port_code
    AND wd.week_start_date = ws.week_start_date

{% if is_incremental() %}
WHERE wd.week_start_date > (
    SELECT MAX(week_start_date) FROM {{ this }}
)
{% endif %}