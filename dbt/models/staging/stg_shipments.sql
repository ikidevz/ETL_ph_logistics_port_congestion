WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_shipments') }}
)
SELECT
    shipment_id,
    bl_number,
    UPPER(origin_port)                          AS origin_port_code,
    UPPER(dest_port)                            AS dest_port_code,
    UPPER(carrier_code)                         AS carrier_code,
    UPPER(vessel_imo)                           AS vessel_imo,
    etd,
    eta,
    ata,
    GREATEST(0, DATEDIFF('hour', eta, ata))     AS delay_hours,
    CASE
        WHEN DATEDIFF('hour', eta, ata) = 0     THEN 'ON_TIME'
        WHEN DATEDIFF('hour', eta, ata) <= 24   THEN 'MINOR_DELAY'
        WHEN DATEDIFF('hour', eta, ata) <= 72   THEN 'MODERATE_DELAY'
        ELSE 'SEVERE_DELAY'
    END                                         AS delay_category,
    cargo_type,
    commodity_code,
    declared_value_usd,
    freight_usd,
    insurance_usd,
    _loaded_at
FROM source
WHERE shipment_id IS NOT NULL