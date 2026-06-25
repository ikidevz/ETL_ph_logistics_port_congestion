{{
  config(
    materialized='incremental',
    unique_key='dwell_key'
  )
}}

SELECT
    MD5(v.vessel_imo || CAST(v.berth_ts AS VARCHAR))    AS dwell_key,
    v.vessel_imo,
    vd.vessel_key,
    d.date_key                                          AS arrival_date_key,
    p.port_key,
    v.berth_ts,
    v.cargo_type,
    v.dwell_hours,
    v.anchorage_wait_h,
    v.teu_count,
    p.h3_res5                                           AS port_h3_res5,
    CASE WHEN v.anchorage_wait_h > 24
         THEN TRUE ELSE FALSE END                       AS is_congested,
    v._loaded_at
FROM {{ ref('stg_vessel_calls') }}      v
LEFT JOIN {{ ref('dim_vessel') }}       vd ON v.vessel_imo      = vd.vessel_imo
LEFT JOIN {{ ref('dim_date') }}         d  ON v.arrival_ts::DATE = d.date_key
LEFT JOIN {{ ref('dim_port') }}         p  ON v.port_code        = p.port_code

{% if is_incremental() %}
WHERE v._loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}