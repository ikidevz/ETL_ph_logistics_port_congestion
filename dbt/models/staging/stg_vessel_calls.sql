WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_vessel_calls') }}
),
cleaned AS (
    SELECT
        vessel_imo,
        UPPER(TRIM(port_code))                              AS port_code,
        arrival_ts,
        berth_ts,
        departure_ts,
        COALESCE(dwell_hours,
            DATEDIFF('hour', arrival_ts, departure_ts))    AS dwell_hours,
        anchorage_wait_h,
        UPPER(cargo_type)                                   AS cargo_type,
        teu_count,
        _loaded_at
    FROM source
    WHERE arrival_ts IS NOT NULL
      AND COALESCE(dwell_hours, DATEDIFF('hour', arrival_ts, departure_ts)) > 0
)
SELECT * FROM cleaned