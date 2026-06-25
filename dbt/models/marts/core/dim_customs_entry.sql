{{
  config(materialized='table')
}}

SELECT
    MD5(entry_number)           AS customs_key,
    entry_number,
    shipment_id,
    bl_number,
    commodity_code,
    entry_type,
    declared_value_usd,
    tariff_rate_pct,
    duties_usd,
    vat_usd,
    total_tax_usd,
    lodge_date,
    assessment_date,
    release_date,
    processing_days,
    payment_mode,
    port_code,
    CURRENT_TIMESTAMP()         AS dbt_updated_at
FROM {{ ref('stg_customs_entries') }}
WHERE entry_number IS NOT NULL