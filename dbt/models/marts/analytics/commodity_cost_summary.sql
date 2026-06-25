{{
  config(
    materialized='table',
    schema='gold'
  )
}}

WITH cost_base AS (
    SELECT
        lc.shipment_id,
        lc.dest_port_code,
        lc.eta_date_key,
        lc.cif_usd,
        lc.duty_rate_pct,
        lc.duties_usd,
        lc.vat_usd,
        lc.total_landed_cost_php,
        lc.usd_php_rate,
        s.cargo_type,
        s.commodity_code,
        t.description                                           AS commodity_desc,
        t.atiga_rate_pct
    FROM {{ ref('fct_landed_cost') }}       lc
    JOIN {{ ref('dim_shipment') }}          s  ON lc.shipment_id    = s.shipment_id
    LEFT JOIN {{ ref('ph_tariff_rates') }}  t  ON s.commodity_code  = t.commodity_code
)

SELECT
    cb.commodity_code,
    cb.commodity_desc,
    cb.cargo_type,
    cb.dest_port_code,
    p.port_name                                                 AS dest_port_name,
    COUNT(*)                                                    AS shipment_count,
    ROUND(AVG(cb.cif_usd), 2)                                  AS avg_cif_usd,
    ROUND(AVG(cb.duties_usd), 2)                               AS avg_duties_usd,
    ROUND(AVG(cb.vat_usd), 2)                                  AS avg_vat_usd,
    ROUND(AVG(cb.cif_usd + cb.duties_usd + cb.vat_usd), 2)    AS avg_total_tax_inclusive_usd,
    ROUND(AVG(cb.total_landed_cost_php), 2)                    AS avg_landed_cost_php,
    ROUND(SUM(cb.total_landed_cost_php), 2)                    AS total_landed_cost_php,
    ROUND(MIN(cb.total_landed_cost_php), 2)                    AS min_landed_cost_php,
    ROUND(MAX(cb.total_landed_cost_php), 2)                    AS max_landed_cost_php,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cb.total_landed_cost_php) AS p50_landed_cost_php,
    ROUND(AVG(cb.duty_rate_pct), 2)                            AS avg_duty_rate_pct,
    MAX(cb.atiga_rate_pct)                                      AS atiga_preferential_rate_pct,
    ROUND(
        AVG((cb.duties_usd + cb.vat_usd) / NULLIF(cb.cif_usd + cb.duties_usd + cb.vat_usd, 0)) * 100
    , 1)                                                        AS avg_tax_burden_pct,
    ROUND(AVG(cb.usd_php_rate), 4)                             AS avg_usd_php_rate,
    CURRENT_TIMESTAMP()                                         AS dbt_updated_at

FROM cost_base cb
LEFT JOIN {{ ref('dim_port') }} p ON cb.dest_port_code = p.port_code

GROUP BY
    cb.commodity_code, cb.commodity_desc, cb.cargo_type,
    cb.dest_port_code, p.port_name