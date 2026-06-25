-- models/marts/core/fct_shipment_events.sql
{{
  config(
    materialized='incremental',
    unique_key='shipment_id'
  )
}}

SELECT
    s.shipment_id,
    sh.shipment_key,
    d_etd.date_key                              AS etd_date_key,
    d_eta.date_key                              AS eta_date_key,
    d_ata.date_key                              AS ata_date_key,
    p_orig.port_key                             AS origin_port_key,
    p_dest.port_key                             AS dest_port_key,
    c.carrier_key,
    s.etd,
    s.eta,
    s.ata,
    s.delay_hours,
    s.delay_category,
    -- Event type derived from ATA vs ETA
    CASE
        WHEN s.ata IS NULL              THEN 'IN_TRANSIT'
        WHEN s.delay_hours = 0          THEN 'ARRIVED_ON_TIME'
        WHEN s.delay_category = 'MINOR_DELAY'    THEN 'ARRIVED_MINOR_DELAY'
        WHEN s.delay_category = 'MODERATE_DELAY' THEN 'ARRIVED_MODERATE_DELAY'
        ELSE                                      'ARRIVED_SEVERE_DELAY'
    END                                         AS event_type,
    s.declared_value_usd,
    s.freight_usd,
    s.insurance_usd,
    s._loaded_at
FROM {{ ref('stg_shipments') }}                 s
LEFT JOIN {{ ref('dim_shipment') }}             sh  ON s.shipment_id     = sh.shipment_id
LEFT JOIN {{ ref('dim_date') }}                 d_etd ON s.etd::DATE     = d_etd.date_key
LEFT JOIN {{ ref('dim_date') }}                 d_eta ON s.eta::DATE     = d_eta.date_key
LEFT JOIN {{ ref('dim_date') }}                 d_ata ON s.ata::DATE     = d_ata.date_key
LEFT JOIN {{ ref('dim_port') }}                 p_orig ON s.origin_port_code = p_orig.port_code
LEFT JOIN {{ ref('dim_port') }}                 p_dest ON s.dest_port_code   = p_dest.port_code
LEFT JOIN {{ ref('dim_carrier') }}              c  ON s.carrier_code     = c.carrier_code

{% if is_incremental() %}
WHERE s._loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}