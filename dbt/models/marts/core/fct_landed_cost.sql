-- models/marts/core/fct_landed_cost.sql
{{
  config(
    materialized='incremental',
    unique_key='shipment_id'
  )
}}

WITH shipments AS (
    SELECT * FROM {{ ref('stg_shipments') }}
),
duties AS (
    SELECT commodity_code, mfn_rate_pct AS duty_rate_pct
    FROM {{ ref('ph_tariff_rates') }}
),
fx AS (
    SELECT rate_date, usd_php_close AS usd_php_rate
    FROM {{ ref('usd_php_rates') }}
),
fx_filled AS (
    SELECT
        s.shipment_id,
        MAX(CASE WHEN f.rate_date <= COALESCE(s.eta::DATE, CURRENT_DATE())
                 THEN f.usd_php_rate END)                       AS usd_php_rate
    FROM shipments s
    CROSS JOIN fx f
    GROUP BY s.shipment_id
)
SELECT
    s.shipment_id,
    s.bl_number,
    s.dest_port_code,
    s.eta::DATE                                                 AS eta_date_key,
    s.declared_value_usd,
    s.freight_usd,
    s.insurance_usd,
    s.declared_value_usd + s.freight_usd + s.insurance_usd     AS cif_usd,
    COALESCE(d.duty_rate_pct, 0)                                AS duty_rate_pct,
    ROUND((s.declared_value_usd + s.freight_usd + s.insurance_usd)
          * COALESCE(d.duty_rate_pct, 0) / 100, 2)             AS duties_usd,
    ROUND(((s.declared_value_usd + s.freight_usd + s.insurance_usd)
           + (s.declared_value_usd + s.freight_usd + s.insurance_usd)
             * COALESCE(d.duty_rate_pct, 0) / 100)
          * 0.12, 2)                                            AS vat_usd,
    ROUND(((s.declared_value_usd + s.freight_usd + s.insurance_usd)
           * (1 + COALESCE(d.duty_rate_pct, 0) / 100) * 1.12)
          * COALESCE(ff.usd_php_rate, 56.0), 2)                AS total_landed_cost_php,
    COALESCE(ff.usd_php_rate, 56.0)                            AS usd_php_rate,
    s._loaded_at
FROM shipments s
LEFT JOIN duties    d  ON s.commodity_code = d.commodity_code
LEFT JOIN fx_filled ff ON s.shipment_id    = ff.shipment_id

{% if is_incremental() %}
WHERE s._loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}