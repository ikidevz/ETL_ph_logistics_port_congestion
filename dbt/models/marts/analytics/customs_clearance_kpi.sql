-- models/marts/analytics/customs_clearance_kpi.sql
{{
  config(
    materialized='table',
    schema='gold'
  )
}}

WITH base AS (
    SELECT
        ce.port_code,
        ce.entry_type,
        ce.payment_mode,
        ce.processing_days,
        ce.tariff_rate_pct,
        ce.total_tax_usd,
        ce.declared_value_usd,

        -- SLA flag: BOC target is 5 working days for formal entries
        CASE WHEN ce.processing_days > 5 THEN TRUE ELSE FALSE END AS is_sla_breach,

        -- Clearance speed band
        CASE
            WHEN ce.processing_days <= 2  THEN 'FAST'
            WHEN ce.processing_days <= 5  THEN 'STANDARD'
            WHEN ce.processing_days <= 10 THEN 'SLOW'
            ELSE 'CRITICAL'
        END                                                         AS clearance_band

    FROM {{ ref('dim_customs_entry') }} ce
    WHERE ce.entry_number IS NOT NULL
)

SELECT
    b.port_code,
    p.port_name,
    b.entry_type,
    b.payment_mode,

    COUNT(*)                                                    AS total_entries,

    -- Processing time distribution
    ROUND(AVG(b.processing_days), 1)                           AS avg_processing_days,
    ROUND(MIN(b.processing_days), 1)                           AS min_processing_days,
    ROUND(MAX(b.processing_days), 1)                           AS max_processing_days,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.processing_days) AS p50_processing_days,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY b.processing_days) AS p90_processing_days,

    -- SLA performance
    SUM(CASE WHEN b.is_sla_breach THEN 1 ELSE 0 END)          AS sla_breach_count,
    ROUND(
        SUM(CASE WHEN b.is_sla_breach THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                           AS sla_breach_pct,

    -- Clearance speed bands
    SUM(CASE WHEN b.clearance_band = 'FAST'     THEN 1 ELSE 0 END) AS fast_count,
    SUM(CASE WHEN b.clearance_band = 'STANDARD' THEN 1 ELSE 0 END) AS standard_count,
    SUM(CASE WHEN b.clearance_band = 'SLOW'     THEN 1 ELSE 0 END) AS slow_count,
    SUM(CASE WHEN b.clearance_band = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,

    -- Tax metrics
    ROUND(AVG(b.tariff_rate_pct), 2)                           AS avg_tariff_rate_pct,
    ROUND(AVG(b.total_tax_usd), 2)                             AS avg_total_tax_usd,
    ROUND(SUM(b.total_tax_usd), 2)                             AS total_tax_collected_usd,
    ROUND(AVG(b.declared_value_usd), 2)                        AS avg_declared_value_usd,

    CURRENT_TIMESTAMP()                                         AS dbt_updated_at

FROM base b
LEFT JOIN {{ ref('dim_port') }} p ON b.port_code = p.port_code

GROUP BY
    b.port_code, p.port_name,
    b.entry_type, b.payment_mode