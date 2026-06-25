-- =============================================================================
-- monitoring.sql
-- PH Logistics & Port Congestion Intelligence Platform
-- Snowflake Monitoring Queries
-- =============================================================================
-- Usage: run individual sections as needed, or schedule via Snowflake Tasks.
-- All queries target logistics_db. Switch context first:
--   USE DATABASE logistics_db;
--   USE WAREHOUSE portfolio_wh;
-- =============================================================================


-- =============================================================================
-- SECTION 1 — PIPELINE FRESHNESS
-- Is the data current? Did the last Airflow run actually land new records?
-- =============================================================================

-- 1.1 Latest _loaded_at per bronze table
-- Red flag: any table > 5 hours behind (DAG runs every 4 hours + 1h buffer)
SELECT
    'bronze.raw_vessel_calls'    AS table_name,
    MAX(_loaded_at)              AS latest_load_ts,
    DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()) AS hours_since_load,
    CASE
        WHEN DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()) > 5  THEN 'STALE'
        WHEN DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()) > 8  THEN 'CRITICAL'
        ELSE 'OK'
    END                          AS freshness_status
FROM bronze.raw_vessel_calls

UNION ALL

SELECT
    'bronze.raw_shipments',
    MAX(_loaded_at),
    DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()),
    CASE
        WHEN DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()) > 5  THEN 'STALE'
        WHEN DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()) > 8  THEN 'CRITICAL'
        ELSE 'OK'
    END
FROM bronze.raw_shipments

UNION ALL

SELECT
    'bronze.raw_customs_entries',
    MAX(_loaded_at),
    DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()),
    CASE
        WHEN DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()) > 5  THEN 'STALE'
        WHEN DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()) > 8  THEN 'CRITICAL'
        ELSE 'OK'
    END
FROM bronze.raw_customs_entries

ORDER BY hours_since_load DESC;


-- 1.2 Latest _loaded_at per gold fact table
-- Facts are incremental — confirms dbt actually processed new bronze records
SELECT
    'gold.fct_port_dwell'        AS table_name,
    MAX(_loaded_at)              AS latest_load_ts,
    DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()) AS hours_since_load
FROM gold.fct_port_dwell

UNION ALL

SELECT
    'gold.fct_shipment_events',
    MAX(_loaded_at),
    DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP())
FROM gold.fct_shipment_events

UNION ALL

SELECT
    'gold.fct_landed_cost',
    MAX(_loaded_at),
    DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP())
FROM gold.fct_landed_cost

ORDER BY hours_since_load DESC;


-- 1.3 Dynamic Table lag check
-- dt_active_shipments TARGET_LAG = 5 min; dt_port_congestion = 15 min
-- If scheduled_lag_seconds >> target, the Dynamic Table is falling behind
SELECT
    name                                                    AS dynamic_table_name,
    scheduling_state,
    last_completed_dependency_update_time,
    DATEDIFF(
        'minute',
        last_completed_dependency_update_time,
        CURRENT_TIMESTAMP()
    )                                                       AS minutes_since_refresh,
    target_lag_sec / 60                                     AS target_lag_minutes,
    CASE
        WHEN DATEDIFF('minute', last_completed_dependency_update_time, CURRENT_TIMESTAMP())
             > (target_lag_sec / 60) * 3
        THEN 'LAGGING'
        ELSE 'OK'
    END                                                     AS lag_status
FROM information_schema.dynamic_tables
WHERE table_schema = 'REALTIME'
  AND name IN ('DT_ACTIVE_SHIPMENTS', 'DT_PORT_CONGESTION')
ORDER BY minutes_since_refresh DESC;


-- =============================================================================
-- SECTION 2 — ROW COUNTS & VOLUME
-- Are row counts in the expected range? Did a load silently drop records?
-- =============================================================================

-- 2.1 Row counts across all layers
SELECT
    'bronze.raw_vessel_calls'       AS table_name, COUNT(*) AS row_count FROM bronze.raw_vessel_calls    UNION ALL
SELECT 'bronze.raw_shipments',                               COUNT(*) FROM bronze.raw_shipments           UNION ALL
SELECT 'bronze.raw_customs_entries',                         COUNT(*) FROM bronze.raw_customs_entries     UNION ALL
SELECT 'silver.stg_vessel_calls',                            COUNT(*) FROM silver.stg_vessel_calls        UNION ALL
SELECT 'silver.stg_shipments',                               COUNT(*) FROM silver.stg_shipments           UNION ALL
SELECT 'silver.stg_customs_entries',                         COUNT(*) FROM silver.stg_customs_entries     UNION ALL
SELECT 'gold.dim_port',                                      COUNT(*) FROM gold.dim_port                  UNION ALL
SELECT 'gold.dim_vessel',                                    COUNT(*) FROM gold.dim_vessel                UNION ALL
SELECT 'gold.dim_carrier',                                   COUNT(*) FROM gold.dim_carrier               UNION ALL
SELECT 'gold.dim_shipment',                                  COUNT(*) FROM gold.dim_shipment              UNION ALL
SELECT 'gold.dim_customs_entry',                             COUNT(*) FROM gold.dim_customs_entry         UNION ALL
SELECT 'gold.fct_port_dwell',                                COUNT(*) FROM gold.fct_port_dwell            UNION ALL
SELECT 'gold.fct_shipment_events',                           COUNT(*) FROM gold.fct_shipment_events       UNION ALL
SELECT 'gold.fct_landed_cost',                               COUNT(*) FROM gold.fct_landed_cost           UNION ALL
SELECT 'gold.carrier_performance',                           COUNT(*) FROM gold.carrier_performance       UNION ALL
SELECT 'gold.port_throughput_daily',                         COUNT(*) FROM gold.port_throughput_daily     UNION ALL
SELECT 'gold.customs_clearance_kpi',                         COUNT(*) FROM gold.customs_clearance_kpi    UNION ALL
SELECT 'gold.commodity_cost_summary',                        COUNT(*) FROM gold.commodity_cost_summary    UNION ALL
SELECT 'gold.congestion_delay_impact',                       COUNT(*) FROM gold.congestion_delay_impact   UNION ALL
SELECT 'gold.port_catchment_area',                           COUNT(*) FROM gold.port_catchment_area       UNION ALL
SELECT 'realtime.dt_active_shipments',                       COUNT(*) FROM realtime.dt_active_shipments   UNION ALL
SELECT 'realtime.dt_port_congestion',                        COUNT(*) FROM realtime.dt_port_congestion
ORDER BY table_name;


-- 2.2 Layer pass-through ratio
-- Silver should retain >= 98% of bronze rows (small drop expected from null filters)
-- Gold fact rows should be <= silver shipments (1:1 grain for fct_shipment_events)
WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM bronze.raw_vessel_calls)    AS bronze_vessel,
        (SELECT COUNT(*) FROM bronze.raw_shipments)       AS bronze_shipments,
        (SELECT COUNT(*) FROM bronze.raw_customs_entries) AS bronze_customs,
        (SELECT COUNT(*) FROM silver.stg_vessel_calls)    AS silver_vessel,
        (SELECT COUNT(*) FROM silver.stg_shipments)       AS silver_shipments,
        (SELECT COUNT(*) FROM silver.stg_customs_entries) AS silver_customs,
        (SELECT COUNT(*) FROM gold.fct_port_dwell)        AS gold_dwell,
        (SELECT COUNT(*) FROM gold.fct_shipment_events)   AS gold_shipments,
        (SELECT COUNT(*) FROM gold.fct_landed_cost)       AS gold_landed
)
SELECT
    ROUND(silver_vessel    / NULLIF(bronze_vessel,    0) * 100, 2) AS vessel_bronze_to_silver_pct,
    ROUND(silver_shipments / NULLIF(bronze_shipments, 0) * 100, 2) AS shipment_bronze_to_silver_pct,
    ROUND(silver_customs   / NULLIF(bronze_customs,   0) * 100, 2) AS customs_bronze_to_silver_pct,
    ROUND(gold_dwell       / NULLIF(silver_vessel,    0) * 100, 2) AS vessel_silver_to_gold_pct,
    ROUND(gold_shipments   / NULLIF(silver_shipments, 0) * 100, 2) AS shipment_silver_to_gold_pct,
    ROUND(gold_landed      / NULLIF(silver_shipments, 0) * 100, 2) AS landed_silver_to_gold_pct
FROM counts;


-- 2.3 Daily load volume — last 14 days (bronze)
-- Sudden drop = generator script failed or Airflow task silently skipped
SELECT
    DATE(_loaded_at)             AS load_date,
    COUNT(*)                     AS vessel_calls_loaded
FROM bronze.raw_vessel_calls
WHERE _loaded_at >= CURRENT_DATE() - 14
GROUP BY load_date
ORDER BY load_date DESC;

SELECT
    DATE(_loaded_at)             AS load_date,
    COUNT(*)                     AS shipments_loaded
FROM bronze.raw_shipments
WHERE _loaded_at >= CURRENT_DATE() - 14
GROUP BY load_date
ORDER BY load_date DESC;


-- =============================================================================
-- SECTION 3 — DATA QUALITY
-- Nulls, duplicates, out-of-range values, referential integrity.
-- =============================================================================

-- 3.1 Null rate on critical columns — bronze layer
SELECT
    'raw_vessel_calls.vessel_imo'    AS column_check,
    ROUND(SUM(CASE WHEN vessel_imo IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS null_pct
FROM bronze.raw_vessel_calls UNION ALL
SELECT 'raw_vessel_calls.arrival_ts',
    ROUND(SUM(CASE WHEN arrival_ts IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)
FROM bronze.raw_vessel_calls UNION ALL
SELECT 'raw_shipments.shipment_id',
    ROUND(SUM(CASE WHEN shipment_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)
FROM bronze.raw_shipments UNION ALL
SELECT 'raw_shipments.eta',
    ROUND(SUM(CASE WHEN eta IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)
FROM bronze.raw_shipments UNION ALL
SELECT 'raw_customs_entries.entry_number',
    ROUND(SUM(CASE WHEN entry_number IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)
FROM bronze.raw_customs_entries UNION ALL
SELECT 'raw_customs_entries.shipment_id',
    ROUND(SUM(CASE WHEN shipment_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)
FROM bronze.raw_customs_entries
ORDER BY null_pct DESC;


-- 3.2 Duplicate primary keys — bronze layer
-- Any row > 0 means the generator or COPY INTO created dupes
SELECT 'raw_shipments.shipment_id duplicates' AS check_name,
    COUNT(*) - COUNT(DISTINCT shipment_id) AS duplicate_count
FROM bronze.raw_shipments
UNION ALL
SELECT 'raw_customs_entries.entry_number duplicates',
    COUNT(*) - COUNT(DISTINCT entry_number)
FROM bronze.raw_customs_entries
UNION ALL
SELECT 'gold.fct_shipment_events.shipment_id duplicates',
    COUNT(*) - COUNT(DISTINCT shipment_id)
FROM gold.fct_shipment_events
UNION ALL
SELECT 'gold.fct_landed_cost.shipment_id duplicates',
    COUNT(*) - COUNT(DISTINCT shipment_id)
FROM gold.fct_landed_cost
UNION ALL
SELECT 'gold.dim_port.port_code duplicates',
    COUNT(*) - COUNT(DISTINCT port_code)
FROM gold.dim_port
UNION ALL
SELECT 'gold.dim_vessel.vessel_imo duplicates',
    COUNT(*) - COUNT(DISTINCT vessel_imo)
FROM gold.dim_vessel;


-- 3.3 Out-of-range value checks
-- dwell_hours, delay_hours, processing_days should never be negative
-- declared_value_usd / total_landed_cost_php should never be zero or negative
SELECT
    'fct_port_dwell: negative dwell_hours'      AS check_name,
    COUNT(*)                                     AS violation_count
FROM gold.fct_port_dwell
WHERE dwell_hours < 0

UNION ALL SELECT
    'fct_port_dwell: zero dwell_hours',
    COUNT(*)
FROM gold.fct_port_dwell
WHERE dwell_hours = 0

UNION ALL SELECT
    'fct_port_dwell: anchorage_wait_h > dwell_hours (impossible)',
    COUNT(*)
FROM gold.fct_port_dwell
WHERE anchorage_wait_h > dwell_hours

UNION ALL SELECT
    'fct_shipment_events: negative delay_hours',
    COUNT(*)
FROM gold.fct_shipment_events
WHERE delay_hours < 0

UNION ALL SELECT
    'fct_shipment_events: ATA before ETA but delay_hours > 0',
    COUNT(*)
FROM gold.fct_shipment_events
WHERE ata < eta AND delay_hours > 0

UNION ALL SELECT
    'fct_landed_cost: total_landed_cost_php <= 0',
    COUNT(*)
FROM gold.fct_landed_cost
WHERE total_landed_cost_php <= 0

UNION ALL SELECT
    'fct_landed_cost: duties_usd > cif_usd (duty rate > 100%)',
    COUNT(*)
FROM gold.fct_landed_cost
WHERE duties_usd > cif_usd

UNION ALL SELECT
    'dim_customs_entry: negative processing_days',
    COUNT(*)
FROM gold.dim_customs_entry
WHERE processing_days < 0

UNION ALL SELECT
    'dim_customs_entry: release_date before lodge_date',
    COUNT(*)
FROM gold.dim_customs_entry
WHERE release_date < lodge_date

ORDER BY violation_count DESC;


-- 3.4 Accepted values check — categorical columns
SELECT
    'stg_vessel_calls: invalid cargo_type'       AS check_name,
    COUNT(*)                                      AS violation_count
FROM silver.stg_vessel_calls
WHERE cargo_type NOT IN ('FCL', 'LCL', 'RORO', 'BULK')

UNION ALL SELECT
    'stg_shipments: invalid delay_category',
    COUNT(*)
FROM silver.stg_shipments
WHERE delay_category NOT IN ('ON_TIME', 'MINOR_DELAY', 'MODERATE_DELAY', 'SEVERE_DELAY')

UNION ALL SELECT
    'stg_customs_entries: invalid entry_type',
    COUNT(*)
FROM silver.stg_customs_entries
WHERE entry_type NOT IN ('FORMAL', 'INFORMAL', 'WAREHOUSING')

UNION ALL SELECT
    'stg_customs_entries: invalid payment_mode',
    COUNT(*)
FROM silver.stg_customs_entries
WHERE payment_mode NOT IN ('CASH', 'SURETY_BOND', 'DEFERRED')

UNION ALL SELECT
    'congestion_delay_impact: invalid congestion_level',
    COUNT(*)
FROM gold.congestion_delay_impact
WHERE congestion_level NOT IN ('CRITICAL', 'HIGH', 'MODERATE', 'NORMAL')

ORDER BY violation_count DESC;


-- 3.5 Referential integrity — orphan records
-- Shipments in fct_shipment_events with no matching dim_shipment
SELECT
    'fct_shipment_events → dim_shipment orphans'  AS check_name,
    COUNT(*)                                       AS orphan_count
FROM gold.fct_shipment_events f
LEFT JOIN gold.dim_shipment s ON f.shipment_key = s.shipment_key
WHERE s.shipment_key IS NULL

UNION ALL SELECT
    'fct_port_dwell → dim_port orphans',
    COUNT(*)
FROM gold.fct_port_dwell f
LEFT JOIN gold.dim_port p ON f.port_key = p.port_key
WHERE p.port_key IS NULL

UNION ALL SELECT
    'fct_port_dwell → dim_vessel orphans',
    COUNT(*)
FROM gold.fct_port_dwell f
LEFT JOIN gold.dim_vessel v ON f.vessel_key = v.vessel_key
WHERE v.vessel_key IS NULL

UNION ALL SELECT
    'fct_landed_cost → dim_shipment orphans',
    COUNT(*)
FROM gold.fct_landed_cost f
LEFT JOIN gold.dim_shipment s ON f.shipment_id = s.shipment_id
WHERE s.shipment_id IS NULL

UNION ALL SELECT
    'stg_customs_entries → stg_shipments FK orphans',
    COUNT(*)
FROM silver.stg_customs_entries ce
LEFT JOIN silver.stg_shipments s ON ce.shipment_id = s.shipment_id
WHERE s.shipment_id IS NULL

ORDER BY orphan_count DESC;


-- 3.6 Seed coverage — commodity codes in shipments with no tariff rate match
-- These shipments will get duty_rate_pct = 0 in fct_landed_cost (COALESCE default)
SELECT
    s.commodity_code,
    COUNT(*)             AS shipment_count,
    'no tariff rate match — duty defaults to 0%' AS note
FROM silver.stg_shipments s
LEFT JOIN gold.ph_tariff_rates t ON s.commodity_code = t.commodity_code
WHERE t.commodity_code IS NULL
GROUP BY s.commodity_code
ORDER BY shipment_count DESC;


-- 3.7 FX rate coverage — shipments with ETA before earliest seed rate
-- These will get NULL usd_php_rate in fct_landed_cost → total_landed_cost_php = NULL
SELECT
    COUNT(*)                                     AS shipments_missing_fx_rate,
    MIN(s.eta::DATE)                             AS earliest_eta,
    (SELECT MIN(rate_date) FROM gold.usd_php_rates) AS earliest_fx_date
FROM silver.stg_shipments s
WHERE s.eta::DATE < (SELECT MIN(rate_date) FROM gold.usd_php_rates);


-- =============================================================================
-- SECTION 4 — BUSINESS METRIC SANITY
-- Do the numbers look reasonable? Catch data generation anomalies
-- that pass DQ tests but produce nonsense analytics.
-- =============================================================================

-- 4.1 Shipment delay distribution
-- Expected per generator: ~60% ON_TIME, ~40% delayed across MINOR/MODERATE/SEVERE
SELECT
    delay_category,
    COUNT(*)                                              AS shipment_count,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 1)    AS pct_of_total
FROM gold.fct_shipment_events
GROUP BY delay_category
ORDER BY shipment_count DESC;


-- 4.2 Port congestion rate by port
-- Expected: ~30% of vessel calls have anchorage_wait_h > 0 (per generator logic)
-- >60% congestion at any port is worth investigating
SELECT
    p.port_code,
    p.port_name,
    COUNT(*)                                                   AS total_calls,
    SUM(CASE WHEN f.is_congested THEN 1 ELSE 0 END)           AS congested_calls,
    ROUND(
        SUM(CASE WHEN f.is_congested THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                          AS congestion_rate_pct,
    ROUND(AVG(f.dwell_hours), 1)                              AS avg_dwell_hours,
    ROUND(AVG(f.anchorage_wait_h), 1)                         AS avg_anchorage_wait_hours
FROM gold.fct_port_dwell    f
JOIN gold.dim_port           p ON f.port_key = p.port_key
GROUP BY p.port_code, p.port_name
ORDER BY congestion_rate_pct DESC;


-- 4.3 Carrier on-time rate
-- Significant differences between carriers are expected and valid
-- All carriers at 100% or 0% = generator bug
SELECT
    c.carrier_code,
    c.carrier_name,
    c.alliance,
    COUNT(*)                                                   AS total_shipments,
    ROUND(
        SUM(CASE WHEN f.delay_category = 'ON_TIME' THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                          AS on_time_pct,
    ROUND(AVG(f.delay_hours), 1)                              AS avg_delay_hours
FROM gold.fct_shipment_events   f
JOIN gold.dim_carrier            c ON f.carrier_key = c.carrier_key
GROUP BY c.carrier_code, c.carrier_name, c.alliance
ORDER BY on_time_pct DESC;


-- 4.4 Average landed cost by cargo type (PHP)
-- Should reflect tariff rates: Textiles (~15%) most expensive, Electronics (~0%) cheapest
SELECT
    s.cargo_type,
    COUNT(*)                                    AS shipment_count,
    ROUND(AVG(lc.cif_usd), 2)                  AS avg_cif_usd,
    ROUND(AVG(lc.duty_rate_pct), 2)            AS avg_duty_rate_pct,
    ROUND(AVG(lc.total_landed_cost_php), 2)    AS avg_landed_cost_php,
    ROUND(AVG(lc.usd_php_rate), 4)             AS avg_fx_rate
FROM gold.fct_landed_cost    lc
JOIN gold.dim_shipment        s ON lc.shipment_id = s.shipment_id
GROUP BY s.cargo_type
ORDER BY avg_landed_cost_php DESC;


-- 4.5 Customs processing time by entry type
-- FORMAL entries are expected to take longest; INFORMAL fastest
SELECT
    entry_type,
    payment_mode,
    COUNT(*)                                    AS entry_count,
    ROUND(AVG(processing_days), 1)             AS avg_processing_days,
    ROUND(MIN(processing_days), 1)             AS min_processing_days,
    ROUND(MAX(processing_days), 1)             AS max_processing_days,
    SUM(CASE WHEN processing_days > 5
             THEN 1 ELSE 0 END)                AS sla_breach_count
FROM gold.dim_customs_entry
GROUP BY entry_type, payment_mode
ORDER BY avg_processing_days DESC;


-- 4.6 Dynamic table content sanity
-- dt_active_shipments should only contain in-transit shipments (ata IS NULL)
-- dt_port_congestion should have one row per port (8 ports in seed)
SELECT 'dt_active_shipments row count'        AS check_name,
    COUNT(*)                                   AS value
FROM realtime.dt_active_shipments
UNION ALL
SELECT 'dt_port_congestion row count',
    COUNT(*)
FROM realtime.dt_port_congestion
UNION ALL
SELECT 'dt_active_shipments: severe delays',
    COUNT(*)
FROM realtime.dt_active_shipments
WHERE delay_category = 'SEVERE_DELAY'
UNION ALL
SELECT 'dt_port_congestion: CRITICAL ports',
    COUNT(*)
FROM realtime.dt_port_congestion
WHERE congestion_level = 'CRITICAL';


-- =============================================================================
-- SECTION 5 — SNOWFLAKE WAREHOUSE & COST
-- Credit consumption, query performance, warehouse efficiency.
-- =============================================================================

-- 5.1 Credit consumption by day — last 30 days
-- Spike on a non-pipeline day = unexpected query or Dynamic Table runaway
SELECT
    TO_DATE(start_time)          AS usage_date,
    warehouse_name,
    ROUND(SUM(credits_used), 4)  AS credits_used,
    COUNT(*)                     AS query_count
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= CURRENT_DATE() - 30
  AND warehouse_name = 'PORTFOLIO_WH'
GROUP BY usage_date, warehouse_name
ORDER BY usage_date DESC;


-- 5.2 Longest running queries — last 7 days
-- Candidates for optimisation: missing clustering keys, missing filters
SELECT
    query_id,
    query_text,
    warehouse_name,
    ROUND(total_elapsed_time / 1000, 1)        AS elapsed_seconds,
    ROUND(compilation_time / 1000, 1)          AS compile_seconds,
    ROUND(execution_time / 1000, 1)            AS execute_seconds,
    bytes_scanned,
    rows_produced,
    start_time
FROM snowflake.account_usage.query_history
WHERE start_time >= CURRENT_DATE() - 7
  AND warehouse_name = 'PORTFOLIO_WH'
  AND total_elapsed_time > 30000               -- > 30 seconds
ORDER BY total_elapsed_time DESC
LIMIT 20;


-- 5.3 Full table scans (no partition pruning)
-- bytes_scanned >> bytes_written suggests missing clustering or overkill SELECT *
SELECT
    query_id,
    SUBSTR(query_text, 1, 120)                 AS query_preview,
    bytes_scanned,
    partitions_scanned,
    partitions_total,
    ROUND(partitions_scanned / NULLIF(partitions_total, 0) * 100, 1) AS pct_partitions_scanned,
    start_time
FROM snowflake.account_usage.query_history
WHERE start_time >= CURRENT_DATE() - 7
  AND warehouse_name = 'PORTFOLIO_WH'
  AND partitions_total > 0
  AND partitions_scanned / NULLIF(partitions_total, 0) > 0.9   -- scanning > 90% of partitions
ORDER BY bytes_scanned DESC
LIMIT 20;


-- 5.4 Warehouse idle vs active time ratio — last 7 days
-- AUTO_SUSPEND = 60s; high idle time = warehouse oversized for workload
SELECT
    warehouse_name,
    SUM(CASE WHEN credits_used > 0 THEN 1 ELSE 0 END)   AS active_intervals,
    SUM(CASE WHEN credits_used = 0 THEN 1 ELSE 0 END)   AS idle_intervals,
    ROUND(
        SUM(CASE WHEN credits_used > 0 THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                     AS active_pct,
    ROUND(SUM(credits_used), 4)                          AS total_credits
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= CURRENT_DATE() - 7
  AND warehouse_name = 'PORTFOLIO_WH'
GROUP BY warehouse_name;


-- 5.5 Dynamic Table credit consumption
-- Dynamic Tables refresh on TARGET_LAG schedule — credits accumulate even with no upstream changes
SELECT
    table_name,
    TO_DATE(start_time)          AS usage_date,
    ROUND(SUM(credits_used), 6)  AS credits_used,
    COUNT(*)                     AS refresh_count
FROM snowflake.account_usage.dynamic_table_refresh_history
WHERE start_time >= CURRENT_DATE() - 30
  AND table_schema = 'REALTIME'
GROUP BY table_name, usage_date
ORDER BY usage_date DESC, credits_used DESC;


-- 5.6 COPY INTO history — last 14 days
-- Confirms bronze loads succeeded; surfaces file errors and rows rejected
SELECT
    table_name,
    file_name,
    row_count,
    row_parsed,
    first_error_message,
    first_error_line_number,
    status,
    last_load_time
FROM information_schema.load_history
WHERE last_load_time >= CURRENT_DATE() - 14
  AND table_schema = 'BRONZE'
ORDER BY last_load_time DESC;


-- =============================================================================
-- SECTION 6 — STORAGE
-- Table sizes, micro-partition counts, clustering efficiency.
-- =============================================================================

-- 6.1 Table storage by schema
SELECT
    table_schema,
    table_name,
    ROW_COUNT                                                  AS row_count,
    ROUND(BYTES / 1024 / 1024, 2)                             AS size_mb,
    ROUND(BYTES_FAILURE / 1024 / 1024, 2)                     AS failsafe_mb,
    LAST_ALTERED                                               AS last_modified
FROM information_schema.tables
WHERE table_catalog = 'LOGISTICS_DB'
  AND table_schema IN ('BRONZE', 'SILVER', 'GOLD', 'REALTIME')
ORDER BY BYTES DESC;


-- 6.2 Micro-partition and clustering health for incremental fact tables
-- High average_overlaps = poor clustering = slower incremental scans
SELECT
    system$clustering_information('gold.fct_port_dwell',      '(_loaded_at)') AS fct_port_dwell_clustering,
    system$clustering_information('gold.fct_shipment_events', '(_loaded_at)') AS fct_shipment_events_clustering,
    system$clustering_information('gold.fct_landed_cost',     '(_loaded_at)') AS fct_landed_cost_clustering;


-- 6.3 Time Travel storage cost
-- Default = 1 day retention; increase on gold facts only if needed for rollback
SELECT
    table_schema,
    table_name,
    retention_time                                             AS time_travel_days,
    ROUND(TIME_TRAVEL_BYTES / 1024 / 1024, 2)                AS time_travel_mb
FROM information_schema.tables
WHERE table_catalog  = 'LOGISTICS_DB'
  AND table_schema  IN ('BRONZE', 'SILVER', 'GOLD', 'REALTIME')
  AND TIME_TRAVEL_BYTES > 0
ORDER BY TIME_TRAVEL_BYTES DESC;


-- =============================================================================
-- SECTION 7 — PIPELINE HEALTH SUMMARY
-- Single-query executive view — red rows need attention.
-- =============================================================================

WITH freshness AS (
    SELECT
        MAX(_loaded_at) AS latest_bronze_ts
    FROM bronze.raw_shipments
),
counts AS (
    SELECT
        (SELECT COUNT(*) FROM bronze.raw_shipments)       AS bronze_rows,
        (SELECT COUNT(*) FROM silver.stg_shipments)       AS silver_rows,
        (SELECT COUNT(*) FROM gold.fct_shipment_events)   AS gold_rows,
        (SELECT COUNT(*) FROM realtime.dt_active_shipments) AS rt_rows
),
dq AS (
    SELECT
        SUM(CASE WHEN shipment_id IS NULL THEN 1 ELSE 0 END)  AS null_shipment_ids,
        COUNT(*) - COUNT(DISTINCT shipment_id)                 AS dup_shipment_ids,
        SUM(CASE WHEN delay_hours < 0 THEN 1 ELSE 0 END)      AS negative_delays
    FROM gold.fct_shipment_events
)
SELECT
    -- Freshness
    f.latest_bronze_ts,
    DATEDIFF('hour', f.latest_bronze_ts, CURRENT_TIMESTAMP())  AS hours_since_bronze_load,
    CASE WHEN DATEDIFF('hour', f.latest_bronze_ts, CURRENT_TIMESTAMP()) > 5
         THEN 'STALE' ELSE 'OK' END                            AS freshness_status,

    -- Volume
    c.bronze_rows,
    c.silver_rows,
    c.gold_rows,
    c.rt_rows                                                   AS realtime_rows,
    ROUND(c.silver_rows / NULLIF(c.bronze_rows, 0) * 100, 1)  AS silver_retention_pct,
    ROUND(c.gold_rows   / NULLIF(c.silver_rows, 0) * 100, 1)  AS gold_retention_pct,

    -- Data quality
    d.null_shipment_ids,
    d.dup_shipment_ids,
    d.negative_delays,
    CASE
        WHEN d.null_shipment_ids > 0
          OR d.dup_shipment_ids  > 0
          OR d.negative_delays   > 0
        THEN 'DQ VIOLATIONS FOUND'
        ELSE 'OK'
    END                                                         AS dq_status,

    CURRENT_TIMESTAMP()                                         AS checked_at

FROM freshness f
CROSS JOIN counts c
CROSS JOIN dq d;