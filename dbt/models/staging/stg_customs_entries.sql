WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_customs_entries') }}
)
SELECT
    entry_number,
    shipment_id,
    bl_number,
    UPPER(TRIM(commodity_code))         AS commodity_code,
    UPPER(TRIM(entry_type))             AS entry_type,
    declared_value_usd,
    tariff_rate_pct,
    duties_usd,
    vat_usd,
    total_tax_usd,
    lodge_date,
    assessment_date,
    release_date,
    COALESCE(processing_days,
        DATEDIFF('day', lodge_date, release_date)) AS processing_days,
    UPPER(TRIM(payment_mode))           AS payment_mode,
    UPPER(TRIM(port_code))              AS port_code,
    _loaded_at
FROM source
WHERE entry_number IS NOT NULL
  AND shipment_id  IS NOT NULL