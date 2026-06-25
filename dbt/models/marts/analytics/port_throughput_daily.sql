-- models/marts/analytics/port_throughput_daily.sql

{{
  config(
    materialized='incremental',
    unique_key='port_code || cast(arrival_date_key as varchar)',
    schema='gold'
  )
}}

SELECT
    p.port_code,
    p.port_name,
    p.region,
    p.port_type,
    f.arrival_date_key                                          AS report_date,
    d.year_num,
    d.month_num,
    d.week_num,
    d.year_month,

    -- Throughput
    COUNT(*)                                                    AS vessel_calls,
    SUM(f.teu_count)                                            AS total_teu,
    ROUND(AVG(f.teu_count), 0)                                 AS avg_teu_per_call,

    -- Dwell
    ROUND(AVG(f.dwell_hours), 1)                               AS avg_dwell_hours,
    ROUND(MIN(f.dwell_hours), 1)                               AS min_dwell_hours,
    ROUND(MAX(f.dwell_hours), 1)                               AS max_dwell_hours,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.dwell_hours) AS p50_dwell_hours,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f.dwell_hours) AS p90_dwell_hours,

    -- Congestion
    ROUND(AVG(f.anchorage_wait_h), 1)                         AS avg_anchorage_wait_hours,
    SUM(CASE WHEN f.is_congested THEN 1 ELSE 0 END)           AS congested_vessel_calls,
    ROUND(
        SUM(CASE WHEN f.is_congested THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                           AS congestion_rate_pct,

    -- 7-day rolling average dwell (for trend smoothing in dashboards)
    ROUND(
        AVG(AVG(f.dwell_hours)) OVER (
            PARTITION BY p.port_code
            ORDER BY f.arrival_date_key
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 1
    )                                                           AS rolling_7d_avg_dwell_hours,

    CURRENT_TIMESTAMP()                                         AS dbt_updated_at

FROM {{ ref('fct_port_dwell') }}    f
JOIN {{ ref('dim_port') }}          p ON f.port_key       = p.port_key
JOIN {{ ref('dim_date') }}          d ON f.arrival_date_key = d.date_key

{% if is_incremental() %}
WHERE f.arrival_date_key > (
    SELECT MAX(report_date) FROM {{ this }}
)
{% endif %}

GROUP BY
    p.port_code, p.port_name, p.region, p.port_type,
    f.arrival_date_key,
    d.year_num, d.month_num, d.week_num, d.year_month