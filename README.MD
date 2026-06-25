# ph-logistics-port-congestion

End-to-end ELT pipeline tracking shipment delays, port congestion, and landed cost across five major Philippine international ports (MICT, Batangas, Cebu, Davao, Subic). Implements a Kimball star schema on Snowflake with Dynamic Tables for near-real-time congestion scoring — no Kafka, no streaming infrastructure.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Design Decisions](#2-design-decisions)
3. [Stack](#3-stack)
4. [Folder Structure](#4-folder-structure)
5. [Snowflake Setup — Run Once](#5-snowflake-setup--run-once)
6. [Prerequisites & Environment](#6-prerequisites--environment)
7. [Step 1 — Simulate Source Data](#7-step-1--simulate-source-data)
8. [Step 2 — Load Bronze (Snowflake Ingest)](#8-step-2--load-bronze-snowflake-ingest)
9. [Step 3 — dbt Project Configuration](#9-step-3--dbt-project-configuration)
10. [Step 4 — Seed Reference Data](#10-step-4--seed-reference-data)
11. [Step 5 — Silver Layer (Staging Models)](#11-step-5--silver-layer-staging-models)
12. [Step 6 — Gold Layer (Dimension Models)](#12-step-6--gold-layer-dimension-models)
13. [Step 7 — Gold Layer (Fact Models)](#13-step-7--gold-layer-fact-models)
14. [Step 8 — Snowflake Dynamic Tables](#14-step-8--snowflake-dynamic-tables)
15. [Step 9 — Analytics Layer (Geospatial)](#15-step-9--analytics-layer-geospatial)
16. [Step 10 — dbt Tests & Data Quality](#16-step-10--dbt-tests--data-quality)
17. [Step 11 — Airflow DAG](#17-step-11--airflow-dag)
18. [Idempotency Contract](#18-idempotency-contract)
19. [Data Model Reference](#19-data-model-reference)
20. [Dashboard Targets](#20-dashboard-targets)
21. [Delivery Order (80/20)](#21-delivery-order-8020)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCE SIMULATION                                                   │
│  generate_port_events.py   generate_shipments.py                    │
│  generate_customs_entries.py                                        │
│  → data/raw_vessel_calls.csv                                        │
│  → data/raw_shipments.csv                                           │
│  → data/raw_customs_entries.csv                                     │
└────────────────────────┬────────────────────────────────────────────┘
                         │ PUT + COPY INTO (load_bronze.py)
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE  logistics_db.bronze  (exact copy, append-only)             │
│  raw_vessel_calls · raw_shipments · raw_customs_entries             │
│  _loaded_at watermark on every table                                │
└────────────────────────┬────────────────────────────────────────────┘
                         │ dbt run --select staging
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER  logistics_db.silver  (views, always idempotent)            │
│  stg_vessel_calls · stg_shipments · stg_customs_entries             │
│  Cleans nulls, uppercases enums, recomputes delay_hours &           │
│  processing_days, coalesces missing fields                          │
└────────────────────────┬────────────────────────────────────────────┘
                         │ dbt seed + dbt run --select marts.core
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD  logistics_db.gold  (Kimball star schema)                     │
│                                                                     │
│  Seeds:   ph_ports · ph_tariff_rates · usd_php_rates               │
│                                                                     │
│  Dims:    dim_date · dim_port (H3 geohash) · dim_vessel             │
│           dim_carrier · dim_shipment · dim_customs_entry            │
│                                                                     │
│  Facts:   fct_port_dwell (incremental, _loaded_at watermark)       │
│           fct_shipment_events (incremental, _loaded_at watermark)   │
│           fct_landed_cost (incremental, duty × FX calculation)     │
│                                                                     │
│  Analytics: port_catchment_area (ST_DISTANCE geospatial)           │
└────────────────────────┬────────────────────────────────────────────┘
                         │ auto-refresh (no Airflow trigger)
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  REALTIME  logistics_db.realtime  (Snowflake Dynamic Tables)        │
│  dt_active_shipments  TARGET_LAG = 5 min                            │
│  dt_port_congestion   TARGET_LAG = 15 min                           │
└─────────────────────────────────────────────────────────────────────┘
```

**Airflow Schedule:** `0 */4 * * *` — every 4 hours
**Snowflake Database:** `logistics_db`
**Dynamic Tables:** self-managing, not triggered by Airflow

---

## 2. Design Decisions

### Why Kimball, not Data Vault?

The business questions here are well-defined and stable: delay metrics, landed cost per shipment, port throughput by cargo type. Data Vault's full historization of every attribute is unnecessary overhead — sources are limited (port manifests, carrier data, BOC customs) and historical tracking at the attribute level is not a regulatory requirement. Kimball star schema delivers simpler, faster analytical queries.

### Why Dynamic Tables, not Kafka?

Port logistics has two distinct query patterns that require different latencies:

- **Historical analysis** ("average MICT dwell time last quarter") → served by the Kimball star schema, refreshed every 4 hours by Airflow.
- **Near-real-time tracking** ("which shipments are delayed right now?") → served by Snowflake Dynamic Tables that auto-refresh when upstream silver data changes.

Dynamic Tables eliminate stream processing infrastructure (no Kafka brokers, consumer lag management, dead-letter queues, or schema registry). For structured port data arriving every few minutes, they are the correct cost-complexity tradeoff.

### Why no dbt packages?

This project uses only Snowflake-native functions: `ST_DISTANCE`, `ST_MAKEPOINT`, `H3_LATLNG_TO_CELL_STRING`, `GENERATOR`. No `dbt_utils` or `dbt_expectations` are required. **`dbt deps` is not needed.** The custom `expression_is_true` tests in `schema.yml` use Snowflake SQL directly.

### Idempotency by design

Every layer is designed to be safely re-run without producing duplicates or data loss. See the full [Idempotency Contract](#18-idempotency-contract) section.

### Medallion boundary rule

Gold models (`marts/core/`, `marts/analytics/`) must **never** read directly from bronze. All bronze access flows through a silver staging view. `dim_customs_entry` reads from `stg_customs_entries`, not `source('bronze', 'raw_customs_entries')`. This keeps cleaning logic in one place and makes the lineage auditable.

### Shared VESSEL_POOL for join integrity

Both `generate_port_events.py` and `generate_shipments.py` draw vessel IMOs from the same deterministic pool (`IMO9000001`–`IMO9000200`). This ensures `vessel_imo` in `raw_shipments` actually joins to `vessel_imo` in `raw_vessel_calls` — producing realistic overlap in `dim_vessel` and `fct_port_dwell` instead of ~0% join hits.

---

## 3. Stack

| Layer             | Tool                                                                |
| ----------------- | ------------------------------------------------------------------- |
| Source simulation | Python (Faker, pandas) — three generator scripts                    |
| Ingestion         | `snowflake.connector` — PUT to table stage + COPY INTO              |
| Batch modeling    | dbt Core + Snowflake SQL                                            |
| Near-real-time    | Snowflake Dynamic Tables (auto-refresh, no Airflow needed)          |
| Geospatial        | Snowflake `ST_DISTANCE`, `ST_MAKEPOINT`, `H3_LATLNG_TO_CELL_STRING` |
| Landed cost calc  | dbt model joining tariff seed + FX seed                             |
| Orchestration     | Apache Airflow 2.x (Task SDK `@task` decorator)                     |
| Dashboard         | Metabase / Preset with map visualization                            |
| Data quality      | dbt schema tests + `Iki_DQ_Check`                                   |

---

## 4. Folder Structure

```
ph_logistics_port_congestion/
├── README.md
├── .env                              # Snowflake credentials — never commit
├── .gitignore
│
├── snowflake/
│   ├── setup.sql                     # Warehouse + database + schema DDL
│   └── dynamic_tables.sql            # dt_active_shipments, dt_port_congestion
│
├── data/                             # Generated CSVs — gitignored
│   ├── raw_vessel_calls.csv
│   ├── raw_shipments.csv
│   └── raw_customs_entries.csv
│
├── seeds/                            # Static reference data loaded by dbt seed
│   ├── ph_ports.csv                  # 8 ports with lat/lon
│   ├── ph_tariff_rates.csv           # CMTA/ASEAN ATIGA duty rates by HS code
│   └── usd_php_rates.csv             # Historical BSP FX rates
│
├── scripts/
│   ├── generate_port_events.py       # → data/raw_vessel_calls.csv  (2,000 rows)
│   ├── generate_shipments.py         # → data/raw_shipments.csv     (10,000 rows)
│   ├── generate_customs_entries.py   # reads raw_shipments → raw_customs_entries.csv
│   └── load_bronze.py                # PUT + COPY INTO all three bronze tables
│
├── logistics_dbt/
│   ├── dbt_project.yml               # materialization + schema routing config
│   ├── profiles.yml                  # Snowflake connection (reference from ~/.dbt/)
│   ├── macros/
│   │   └── generate_schema_name.sql  # routes staging→silver, marts→gold
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml           # bronze source definitions (all 3 tables, freshness)
│   │   │   ├── schema.yml            # stg_* model docs + tests
│   │   │   ├── stg_vessel_calls.sql
│   │   │   ├── stg_shipments.sql
│   │   │   └── stg_customs_entries.sql
│   │   └── marts/
│   │       ├── core/
│   │       │   ├── schema.yml        # dim_* + fct_* docs + column tests
│   │       │   ├── dim_date.sql
│   │       │   ├── dim_port.sql
│   │       │   ├── dim_vessel.sql
│   │       │   ├── dim_carrier.sql
│   │       │   ├── dim_shipment.sql
│   │       │   ├── dim_customs_entry.sql
│   │       │   ├── fct_port_dwell.sql
│   │       │   ├── fct_shipment_events.sql
│   │       │   └── fct_landed_cost.sql
│   │       └── analytics/
│   │           ├── schema.yml        # port_catchment_area docs + expression tests
│   │           └── port_catchment_area.sql
│   └── tests/
│       └── assert_positive_dwell.sql # singular test: dwell_hours >= 0
│
├── dags/
│   └── logistics_etl_dag.py          # Airflow Task SDK DAG, every 4 hours
│
└── dashboard/
    └── port_congestion_map.json      # Metabase map visualization config
```

---

## 5. Snowflake Setup — Run Once

Run this once per Snowflake account before anything else. These are shared resources used across the portfolio.

```sql
-- portfolio-infra/snowflake/setup.sql
CREATE WAREHOUSE IF NOT EXISTS portfolio_wh
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  COMMENT        = 'Shared warehouse for all portfolio projects';

-- Project database + medallion schemas
CREATE DATABASE IF NOT EXISTS logistics_db;
USE DATABASE logistics_db;

CREATE SCHEMA IF NOT EXISTS logistics_db.bronze;    -- raw ingest
CREATE SCHEMA IF NOT EXISTS logistics_db.silver;    -- cleaned views
CREATE SCHEMA IF NOT EXISTS logistics_db.gold;      -- dims, facts, seeds, analytics
CREATE SCHEMA IF NOT EXISTS logistics_db.realtime;  -- Dynamic Tables
```

Then run the bronze DDL from `snowflake/setup.sql` in this repo:

```sql
-- snowflake/setup.sql (bronze table DDL)
CREATE OR REPLACE TABLE bronze.raw_vessel_calls (
    vessel_imo          VARCHAR,
    port_code           VARCHAR,
    arrival_ts          TIMESTAMP_NTZ,
    berth_ts            TIMESTAMP_NTZ,
    departure_ts        TIMESTAMP_NTZ,
    dwell_hours         INTEGER,
    anchorage_wait_h    INTEGER,
    cargo_type          VARCHAR,
    teu_count           INTEGER,
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE bronze.raw_shipments (
    shipment_id         VARCHAR,
    bl_number           VARCHAR,
    origin_port         VARCHAR,
    dest_port           VARCHAR,
    carrier_code        VARCHAR,
    vessel_imo          VARCHAR,
    etd                 TIMESTAMP_NTZ,
    eta                 TIMESTAMP_NTZ,
    ata                 TIMESTAMP_NTZ,
    delay_hours         INTEGER,
    cargo_type          VARCHAR,
    commodity_code      VARCHAR,
    declared_value_usd  NUMERIC(15,2),
    freight_usd         NUMERIC(15,2),
    insurance_usd       NUMERIC(15,2),
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE bronze.raw_customs_entries (
    entry_number        VARCHAR,
    shipment_id         VARCHAR,
    bl_number           VARCHAR,
    commodity_code      VARCHAR,
    entry_type          VARCHAR,
    declared_value_usd  NUMERIC(15,2),
    tariff_rate_pct     NUMERIC(5,2),
    duties_usd          NUMERIC(15,2),
    vat_usd             NUMERIC(15,2),
    total_tax_usd       NUMERIC(15,2),
    lodge_date          DATE,
    assessment_date     DATE,
    release_date        DATE,
    processing_days     INTEGER,
    payment_mode        VARCHAR,
    port_code           VARCHAR,
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

> **Cost tip:** Dynamic Tables consume Snowflake credits on every refresh cycle. Set `TARGET_LAG = '1 hour'` during development. Only tighten to `'5 minutes'` when demoing.

---

## 6. Prerequisites & Environment

```
portfolio-infra/snowflake/setup.sql   — warehouse created
portfolio-infra/.env                  — SNOWFLAKE_* vars filled in
portfolio-infra/dbt/profiles.yml      — logistics profile exists
~/.dbt/profiles.yml                   — symlinked or copied from above
ph-logistics-port-congestion/         — symlinked into portfolio-infra/airflow/include/
```

**.env format** (never commit):

```bash
SNOWFLAKE_ACCOUNT=your_account.us-east-1
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=portfolio_wh
SNOWFLAKE_ROLE=SYSADMIN
```

**Activate shared venv:**

```bash
source ../portfolio-infra/.venv/bin/activate
pip install faker pandas python-dotenv snowflake-connector-python
```

---

## 7. Step 1 — Simulate Source Data

Three scripts simulate three independent event streams. All scripts follow `if __name__ == '__main__'` convention so they work standalone and are importable by `load_bronze.py`.

### Why a shared VESSEL_POOL?

`generate_port_events.py` and `generate_shipments.py` both draw `vessel_imo` from the same deterministic pool of 200 vessels (`IMO9000001`–`IMO9000200`). Without this, randomly generated IMOs across two independent scripts produce ~0% join overlap in `dim_vessel` and `fct_port_dwell`. The shared pool gives realistic multi-call vessel histories.

### generate_port_events.py

Simulates AIS vessel port call events. Generates `raw_vessel_calls.csv`.

- 2,000 vessel calls across 5 PH ports
- 30% of calls have a congestion delay of 12–72 hours at anchorage
- `anchorage_wait_h > 24` → `is_congested = TRUE` in `fct_port_dwell`
- Outputs: `vessel_imo`, `port_code`, `arrival_ts`, `berth_ts`, `departure_ts`, `dwell_hours`, `anchorage_wait_h`, `cargo_type`, `teu_count`

```bash
python scripts/generate_port_events.py
# → data/raw_vessel_calls.csv  (2,000 rows)
```

### generate_shipments.py

Simulates import shipment lifecycle records. Generates `raw_shipments.csv`.

- 10,000 shipments from 5 origin LOCODEs (CNSHA, SGSIN, JPOSA, USLAX, NLRTM)
- 5 carriers: MAERSK, MSC, EVERGREEN, CMA, OOCL
- Transit days: 3–25 days
- 40% of shipments arrive late by 24–120 hours (`delay_h` sampled with `weights=[60, 40]`)
- CIF value: USD 500–150,000 per shipment
- Freight: 8% of declared value; Insurance: 0.5%
- Commodity codes aligned with `seeds/ph_tariff_rates.csv` so duty joins resolve correctly

```bash
python scripts/generate_shipments.py
# → data/raw_shipments.csv  (10,000 rows)
```

### generate_customs_entries.py

Reads `raw_shipments.csv` and generates one BOC customs entry per shipment. **Must run after `generate_shipments.py`.**

- Customs lodged 0–3 days before ATA (pre-lodgement is standard BOC practice)
- Assessment: 1–5 days after lodgement
- Release: 4–72 hours after assessment
- Duty rates: 0, 3, 5, 7, 10, or 15% (matching `ph_tariff_rates.csv` values)
- 12% Philippine VAT applied on `(CIF + duties)`
- Entry types: FORMAL, INFORMAL, WAREHOUSING
- Payment modes: CASH, SURETY_BOND, DEFERRED

```bash
python scripts/generate_customs_entries.py
# reads  data/raw_shipments.csv
# → data/raw_customs_entries.csv  (10,000 rows, 1 entry per shipment)
```

---

## 8. Step 2 — Load Bronze (Snowflake Ingest)

`load_bronze.py` uses the Snowflake Python connector with **PUT + COPY INTO** — no external stage, no Snowpipe, no cloud storage required for local development.

### How it works

1. **PUT** — uploads the local CSV to the table's internal stage (`@%table_name`) with `OVERWRITE=TRUE`. This re-stages the file on every run; Snowflake deduplicates by content hash at the COPY step.
2. **COPY INTO** — ingests from the stage with `FORCE=FALSE` (default). Snowflake tracks loaded file hashes in its load history and skips already-loaded files. Running `load_bronze.py` twice does not produce duplicates.
3. **Auto-generate** — if a CSV does not exist, `load_bronze.py` calls the appropriate generator script before attempting PUT.

### Key settings in COPY INTO

```sql
FILE_FORMAT = (
    TYPE             = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER      = 1
    NULL_IF          = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
    DATE_FORMAT      = 'YYYY-MM-DD'
)
ON_ERROR = 'CONTINUE'   -- logs bad rows, does not abort the load
```

```bash
python scripts/load_bronze.py
# Loads all three bronze tables
# Idempotent: safe to re-run
```

---

## 9. Step 3 — dbt Project Configuration

### dbt_project.yml

Controls materialization and schema routing for every model group:

```yaml
models:
  logistics_dbt:
    staging:
      +materialized: view # silver — always idempotent
      +schema: silver
    marts:
      core:
        +materialized: table # dims are full-refresh tables
        +schema: gold
      analytics:
        +materialized: table
        +schema: gold
seeds:
  logistics_dbt:
    +schema: gold # seeds land alongside dims in gold
```

### generate_schema_name macro

Without this macro, dbt prepends the target schema name to custom schemas (e.g. `silver_silver`). This override writes models directly to the schema specified in `+schema`:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

**Result:** `staging` models → `logistics_db.silver.*`, `marts` models → `logistics_db.gold.*`

### profiles.yml

Keep this file out of version control. Reference it via `DBT_PROFILES_DIR` or copy to `~/.dbt/profiles.yml`:

```yaml
logistics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE', 'SYSADMIN') }}"
      database: logistics_db
      warehouse: portfolio_wh
      schema: silver # default; overridden per-model by generate_schema_name
      threads: 4
```

---

## 10. Step 4 — Seed Reference Data

Seeds are static CSV files that dbt loads into `logistics_db.gold`. They are the join targets for duty rates and FX conversion in `fct_landed_cost`.

```bash
cd logistics_dbt
dbt seed --full-refresh \
  --profiles-dir ../../portfolio-infra/dbt \
  --profile logistics
```

`--full-refresh` truncates and reloads the seed tables every time — seeds are deterministic CSVs so this is always safe.

### ph_ports.csv

8 Philippine ports. 5 international (used in scripts), 3 domestic (for geospatial catchment queries).

| port_code | port_name                               | region              | port_type     | lat     | lon      |
| --------- | --------------------------------------- | ------------------- | ------------- | ------- | -------- |
| MICT      | Manila International Container Terminal | NCR                 | international | 14.5820 | 120.9650 |
| BCT       | Batangas Container Terminal             | CALABARZON          | international | 13.7565 | 121.0583 |
| CEBU      | Cebu International Port                 | Central Visayas     | international | 10.2939 | 123.8988 |
| DAVAO     | Davao International Port                | Davao Region        | international | 7.0708  | 125.6127 |
| SUBIC     | Subic Bay International Terminal        | Central Luzon       | international | 14.7969 | 120.2705 |
| ILOILO    | Iloilo Commercial Port Complex          | Western Visayas     | domestic      | 10.6969 | 122.5764 |
| CAGAYAN   | Cagayan de Oro Port                     | Northern Mindanao   | domestic      | 8.4822  | 124.6472 |
| ZAMBOANGA | Zamboanga Port                          | Zamboanga Peninsula | domestic      | 6.9214  | 122.0790 |

### ph_tariff_rates.csv

15 HS commodity codes covering all 5 cargo types. CMTA and ASEAN ATIGA rates. Used in `fct_landed_cost` to apply correct duty rates by commodity.

| commodity_code | duty_rate_pct | atiga_rate_pct | cargo type  |
| -------------- | ------------- | -------------- | ----------- |
| 8471.30        | 0             | 0              | Electronics |
| 8517.12        | 0             | 0              | Electronics |
| 8429.51        | 5             | 0              | Machinery   |
| 6109.10        | 15            | 5              | Textiles    |
| 1902.19        | 10            | 5              | Food        |
| 2901.10        | 3             | 0              | Chemicals   |
| _(+ 9 more)_   |               |                |             |

### usd_php_rates.csv

15 monthly BSP exchange rates from Jan 2024 to Mar 2025. Used in `fct_landed_cost` for USD→PHP conversion. `fct_landed_cost` forward-fills using `MAX(rate_date) <= eta_date` to avoid look-ahead bias.

---

## 11. Step 5 — Silver Layer (Staging Models)

Staging models are **views** in `logistics_db.silver`. They are recreated on every `dbt run` with zero side effects. Their only job is to clean, type, and validate bronze data before anything reaches gold.

**Rule:** Staging models must never reference gold dims (`ref('dim_*')`). Any enrichment that requires a gold join (e.g. H3 index from `dim_port`) is deferred to the fact model.

### sources.yml

Defines `bronze` as the dbt source for all three raw tables. Sets `_loaded_at` as the freshness field with `warn_after: 8h`, `error_after: 24h`.

```bash
# Check source freshness
dbt source freshness --profiles-dir ../../portfolio-infra/dbt --profile logistics
```

### stg_vessel_calls.sql

Key transformations:

- `UPPER(TRIM(port_code))` — standardises port codes for joins
- `COALESCE(dwell_hours, DATEDIFF('hour', arrival_ts, departure_ts))` — recomputes dwell if raw value is null
- Filters: `arrival_ts IS NOT NULL` and `dwell_hours > 0` (removes data quality noise)
- `UPPER(cargo_type)` — ensures FCL/LCL/RORO/BULK consistency

### stg_shipments.sql

Key transformations:

- `GREATEST(0, DATEDIFF('hour', eta, ata))` — `delay_hours` is clamped to 0 for early arrivals; raw bronze can have negative values when ATA < ETA
- `delay_category` CASE: `ON_TIME` (0h) / `MINOR_DELAY` (1–24h) / `MODERATE_DELAY` (25–72h) / `SEVERE_DELAY` (>72h)
- `UPPER(origin_port)`, `UPPER(dest_port)`, `UPPER(carrier_code)` — join safety

### stg_customs_entries.sql

Key transformations:

- `UPPER(TRIM(entry_type))` and `UPPER(TRIM(payment_mode))` — enum normalisation
- `COALESCE(processing_days, DATEDIFF('day', lodge_date, release_date))` — recomputes clearance cycle time if raw field is null
- Filters: `entry_number IS NOT NULL AND shipment_id IS NOT NULL`

```bash
dbt run --select staging \
  --profiles-dir ../../portfolio-infra/dbt \
  --profile logistics
```

---

## 12. Step 6 — Gold Layer (Dimension Models)

All dims are `materialized='table'` in `logistics_db.gold`. They are fully replaced on every run — safe because they are derived from silver views or seeds, which are themselves idempotent.

### dim_date

Generated entirely from Snowflake's `GENERATOR(ROWCOUNT => 4018)` — no source or seed required. Covers `2020-01-01` to `2030-12-31`. Provides `date_key`, `year_num`, `quarter_num`, `month_num`, `month_name`, `week_num`, `day_of_month`, `day_of_week`, `day_name`, `is_weekend`, `year_month`, `year_quarter`.

### dim_port

Sourced from `seeds/ph_ports.csv`. Adds two H3 geospatial indexes using Snowflake-native functions:

- `H3_LATLNG_TO_CELL_STRING(lat, lon, 5)` → `h3_res5` (~8.5 km edge, regional clustering)
- `H3_LATLNG_TO_CELL_STRING(lat, lon, 7)` → `h3_res7` (~1.2 km edge, port-level clustering)

Surrogate key: `MD5(port_code)`.

### dim_vessel

Deduplicated from `bronze.raw_vessel_calls`. One row per `vessel_imo`. `vessel_name`, `flag_country`, and `capacity_teu` are `NULL` placeholders for AIS enrichment in production. `vessel_type` is the first `cargo_type` seen for that IMO (`ROW_NUMBER() OVER (PARTITION BY vessel_imo)` deduplication).

### dim_carrier

Derived from distinct `carrier_code` values in `bronze.raw_shipments`. Full carrier name and alliance hardcoded for the 5 simulated carriers (MAERSK/MSC → 2M, EVERGREEN/CMA/OOCL → OCEAN). Extend with a seed file when adding real carrier data.

### dim_shipment

Sourced from `stg_shipments`. Carries descriptive attributes (ports, carrier, cargo type, incoterm, declared value). Incoterm is inferred from `cargo_type`: Electronics/Food → CIF, Machinery → CFR, others → FOB. Surrogate key: `MD5(shipment_id)`.

### dim_customs_entry

Sourced from `stg_customs_entries` (not bronze directly — medallion boundary). One row per `entry_number`. Carries the full BOC clearance record: tariff rate, duties, VAT, lodge/assess/release dates, `processing_days`, `payment_mode`. Surrogate key: `MD5(entry_number)`.

```bash
dbt run --select marts.core \
  --profiles-dir ../../portfolio-infra/dbt \
  --profile logistics
```

---

## 13. Step 7 — Gold Layer (Fact Models)

All facts are `materialized='incremental'` using `_loaded_at` watermarks. On each run, only records newer than `MAX(_loaded_at)` in the existing table are processed. Re-running is safe — no duplicates — because of `unique_key` upsert handling.

To fully rebuild a fact from scratch:

```bash
dbt run --full-refresh --select fct_port_dwell \
  --profiles-dir ../../portfolio-infra/dbt \
  --profile logistics
```

### fct_port_dwell

**Grain:** one row per vessel port call.
**unique_key:** `vessel_imo || cast(berth_ts as varchar)` — composite key handles vessels that call the same port multiple times.

Key measures:

- `dwell_hours` — total port time (arrival to departure)
- `anchorage_wait_h` — time waiting at anchorage before berth assignment
- `is_congested` — `TRUE` when `anchorage_wait_h > 24`
- `port_h3_res5` — H3 index pulled from `dim_port` (deferred from staging as per medallion rule)

Joins: `stg_vessel_calls` → `dim_vessel`, `dim_date`, `dim_port`

### fct_shipment_events

**Grain:** one row per shipment.
**unique_key:** `shipment_id`

Key measures:

- `delay_hours` — recomputed non-negative delay from silver
- `delay_category` — ON_TIME / MINOR_DELAY / MODERATE_DELAY / SEVERE_DELAY
- `event_type` — IN_TRANSIT / ARRIVED_ON_TIME / ARRIVED_MINOR_DELAY / ARRIVED_MODERATE_DELAY / ARRIVED_SEVERE_DELAY
- `declared_value_usd`, `freight_usd`, `insurance_usd`

Joins: `stg_shipments` → `dim_shipment`, `dim_date` (×3 for ETD/ETA/ATA), `dim_port` (×2 for origin/dest), `dim_carrier`

### fct_landed_cost

**Grain:** one row per shipment.
**unique_key:** `shipment_id`

Computes the full import cost in PHP:

```
CIF = declared_value_usd + freight_usd + insurance_usd
duties_usd = CIF × tariff_rate_pct / 100          (from ph_tariff_rates seed)
vat_usd = (CIF + duties_usd) × 0.12               (Philippine customs law: 12% VAT)
total_landed_cost_php = (CIF + duties_usd + vat_usd) × usd_php_rate
```

**FX forward-fill logic:** uses `MAX(rate_date) <= eta_date` to find the closest available rate on or before the shipment's ETA — avoids look-ahead bias.

**Duty lookup:** `LEFT JOIN` on `commodity_code` to `ph_tariff_rates`. `COALESCE(duty_rate_pct, 0)` handles commodities not in the seed (duty-free by default).

---

## 14. Step 8 — Snowflake Dynamic Tables

Dynamic Tables refresh automatically when upstream data changes. **Airflow does not trigger them.** Run the DDL once in a Snowflake worksheet after the first successful `dbt run`.

```bash
# In Snowflake worksheet:
# snowflake/dynamic_tables.sql
```

### dt_active_shipments — TARGET_LAG = 5 minutes

Shows all shipments currently in transit (`ata IS NULL`) with delay status and hours to ETA.

Reads from: `logistics_db.silver.stg_shipments` + `logistics_db.gold.dim_port`

Key columns: `shipment_id`, `bl_number`, `origin_port_code`, `dest_port_code`, `carrier_code`, `eta`, `delay_hours`, `delay_category`, `dest_port_name`, `dest_lat`, `dest_lon`, `hours_to_eta`

### dt_port_congestion — TARGET_LAG = 15 minutes

Computes a rolling 24-hour congestion score per port based on `fct_port_dwell`. Uses an inner CTE (`agg`) to compute `congestion_pct` before the outer SELECT applies the `CASE` classification — required because Snowflake does not allow referencing a `SELECT`-level alias in the same `SELECT` clause.

Reads from: `logistics_db.gold.fct_port_dwell` + `logistics_db.gold.dim_port`

Congestion thresholds:

| congestion_pct | congestion_level |
| -------------- | ---------------- |
| ≥ 60%          | CRITICAL         |
| 35–59%         | HIGH             |
| 15–34%         | MODERATE         |
| < 15%          | NORMAL           |

Key columns: `port_code`, `port_name`, `lat`, `lon`, `vessels_in_port`, `avg_wait_hours`, `congested_vessels`, `congestion_pct`, `congestion_level`

> **Dashboard:** `dt_port_congestion` feeds the Metabase map visualization directly using `lat`/`lon` and `congestion_level` for color coding.

---

## 15. Step 9 — Analytics Layer (Geospatial)

### port_catchment_area.sql

Identifies alternative Philippine ports within 200 km of any congested destination port for MODERATE or SEVERE delayed shipments.

**Join pattern:** self-join on `dim_port` (alias `p` = congested port, alias `p2` = all other ports). `ST_DISTANCE` computes metres; divide by 1000 for km.

```sql
ST_DISTANCE(
    ST_MAKEPOINT(p.lon, p.lat),
    ST_MAKEPOINT(p2.lon, p2.lat)
) / 1000 AS distance_km
```

Filter: `distance_km <= 200 AND delay_category IN ('MODERATE_DELAY','SEVERE_DELAY')`

**Use case:** when MICT is at CRITICAL congestion, the output shows BCT (Batangas, ~90 km) as the nearest alternative — enabling logistics teams to decide whether to reroute in-transit vessels.

```bash
dbt run --select marts.analytics \
  --profiles-dir ../../portfolio-infra/dbt \
  --profile logistics
```

---

## 16. Step 10 — dbt Tests & Data Quality

### Schema tests (in `schema.yml` files)

Every key column has at least `not_null`. Natural keys (`shipment_id`, `entry_number`, `vessel_imo`, etc.) have `not_null + unique`. Enum columns use `accepted_values`. Numeric measures use `expression_is_true` with range guards.

Examples:

- `delay_category` → `accepted_values: ['ON_TIME', 'MINOR_DELAY', 'MODERATE_DELAY', 'SEVERE_DELAY']`
- `tariff_rate_pct` → `accepted_values: [0, 3, 5, 7, 10, 15]`
- `dwell_hours` → `expression_is_true: ">= 1"`
- `distance_km` → `expression_is_true: "> 0 AND distance_km <= 200"`
- `total_landed_cost_php` → `expression_is_true: "> 0"`

### Singular test (tests/assert_positive_dwell.sql)

```sql
-- Fails if any row in fct_port_dwell has dwell_hours < 0
SELECT * FROM {{ ref('fct_port_dwell') }}
WHERE dwell_hours < 0
```

### Iki_DQ_Check integration

Two domain-specific checks beyond standard dbt tests:

```python
from Iki_DQ_Check import check_standard, DQConfig
from datetime import datetime, timezone

cfg = DQConfig(
    latest_timestamp=datetime.now(timezone.utc),
    max_delay_hours=0.25,   # 15 min — matches dt_port_congestion TARGET_LAG
    columns=["dwell_hours", "delay_hours"],
)
report = check_standard(dt_active_shipments_df, config=cfg)
```

- `FreshnessCheck` on `dt_active_shipments` — fails if data age exceeds `TARGET_LAG`. This is the one Dynamic Table where staleness directly breaks the near-real-time claim.
- `OutlierCheck` on `dwell_hours` and `delay_hours` — catches data generation errors before they appear as congestion spikes in the dashboard.

```bash
dbt test --profiles-dir ../../portfolio-infra/dbt --profile logistics
```

---

## 17. Step 11 — Airflow DAG

**File:** `dags/logistics_etl_dag.py`
**Schedule:** `0 */4 * * *` — every 4 hours
**SDK:** Airflow Task SDK (`from airflow.sdk import DAG, task`) — not the deprecated `PythonOperator`
**catchup:** `False` — no historical backfill; only the current window runs

### Path constants

```python
DBT_PROJECT_DIR  = '/usr/local/airflow/include/ph-logistics-port-congestion/logistics_dbt'
DBT_PROFILES_DIR = '/usr/local/airflow/include/portfolio-infra/dbt'
DBT_PROFILE      = 'logistics'
SCRIPTS_DIR      = '/usr/local/airflow/include/ph-logistics-port-congestion/scripts'
```

### Helper functions

```python
def _dbt(*args):
    """Run a dbt command in the project directory."""
    subprocess.run(['dbt', *args,
                    '--profiles-dir', DBT_PROFILES_DIR,
                    '--profile', DBT_PROFILE],
                   cwd=DBT_PROJECT_DIR, check=True)

def _script(name):
    """Run a generator script. cwd=SCRIPTS_DIR resolves relative 'data/' paths."""
    subprocess.run(['python', f'{SCRIPTS_DIR}/{name}'],
                   cwd=SCRIPTS_DIR, check=True)
```

> **Note on `cwd`:** Generator scripts write to `data/raw_*.csv` using relative paths. Setting `cwd=SCRIPTS_DIR` in `_script()` ensures `data/` resolves correctly under Airflow's managed environment.

### Task dependency graph

```
generate_vessel_events ──┐
                         ├──→ generate_customs_entries ──→ load_bronze
generate_shipments ──────┘                                     │
                                                               ▼
                                                       seed_reference_data
                                                               │
                                                               ▼
                                                       run_staging_models
                                                               │
                                                               ▼
                                                       run_core_models
                                                               │
                                                               ▼
                                                       run_analytics_models
                                                               │
                                                               ▼
                                                           run_tests
```

**Parallel generation:** `generate_vessel_events` and `generate_shipments` run in parallel — they write to independent CSV files. `generate_customs_entries` waits for `generate_shipments` because it reads `raw_shipments.csv`.

**Seeds before staging:** `dim_port` references `ph_ports` seed, so seeds must complete before staging views are created.

**Dynamic Tables not in DAG:** `dt_active_shipments` and `dt_port_congestion` refresh on their own `TARGET_LAG` schedules. Triggering them from Airflow would defeat their purpose and double-count compute.

**Retry policy:** 1 retry with 5-minute delay on any task failure.

```python
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
```

---

## 18. Idempotency Contract

Running the full pipeline twice produces identical results. Here is how each layer guarantees this:

| Layer                        | Mechanism                                                                   | Safe to re-run? |
| ---------------------------- | --------------------------------------------------------------------------- | --------------- |
| `generate_*.py` scripts      | Overwrite CSV files unconditionally                                         | ✅ Yes          |
| `vessel_imo` joins           | Shared `VESSEL_POOL` — deterministic across reruns                          | ✅ Yes          |
| `load_bronze.py` — PUT       | `OVERWRITE=TRUE` — re-stages the file                                       | ✅ Yes          |
| `load_bronze.py` — COPY INTO | `FORCE=FALSE` (default) — Snowflake skips files by content hash             | ✅ Yes          |
| Bronze DDL (`setup.sql`)     | `CREATE OR REPLACE TABLE`                                                   | ✅ Yes          |
| `dbt seed`                   | `--full-refresh` truncates and reloads from CSV                             | ✅ Yes          |
| Silver staging models        | `materialized='view'` — recreated every run                                 | ✅ Yes          |
| Dim models (`marts.core`)    | `materialized='table'` — full replace every run                             | ✅ Yes          |
| Fact models (`marts.core`)   | `materialized='incremental'` + `unique_key` upsert + `_loaded_at` watermark | ✅ Yes          |
| Analytics model              | `materialized='table'` — full replace every run                             | ✅ Yes          |
| DAG                          | `catchup=False` — only latest scheduled interval runs                       | ✅ Yes          |
| Dynamic Tables               | Auto-refresh on `TARGET_LAG` — Snowflake manages                            | ✅ Yes          |

**Full reset (if needed):**

```bash
# Rebuild all incremental facts from scratch
dbt run --full-refresh --select marts.core \
  --profiles-dir ../../portfolio-infra/dbt \
  --profile logistics
```

---

## 19. Data Model Reference

### Star schema entity relationships

```
dim_date ──────────────────────┐
dim_port ──────────────────────┤
dim_vessel ────────────────────┼──── fct_port_dwell
dim_carrier ───────────────────┘

dim_date (etd/eta/ata) ────────┐
dim_shipment ──────────────────┤
dim_port (origin + dest) ──────┼──── fct_shipment_events
dim_carrier ───────────────────┘

dim_shipment ──────────────────┐
dim_customs_entry ─────────────┼──── fct_landed_cost
dim_date ──────────────────────┘

fct_shipment_events ───────────┐
dim_port ──────────────────────┴──── port_catchment_area (analytics)
```

### Fact tables

| Table                 | Grain                      | Materialization | unique_key                 | Key Measures                                                |
| --------------------- | -------------------------- | --------------- | -------------------------- | ----------------------------------------------------------- |
| `fct_port_dwell`      | 1 row per vessel port call | incremental     | `vessel_imo \|\| berth_ts` | `dwell_hours`, `anchorage_wait_h`, `is_congested`           |
| `fct_shipment_events` | 1 row per shipment         | incremental     | `shipment_id`              | `delay_hours`, `delay_category`, `event_type`               |
| `fct_landed_cost`     | 1 row per shipment         | incremental     | `shipment_id`              | `cif_usd`, `duties_usd`, `vat_usd`, `total_landed_cost_php` |

### Dimension tables

| Table               | Materialization | Surrogate Key       | Source                    |
| ------------------- | --------------- | ------------------- | ------------------------- |
| `dim_date`          | table           | `date_key` (DATE)   | Snowflake GENERATOR       |
| `dim_port`          | table           | `MD5(port_code)`    | `ph_ports` seed           |
| `dim_vessel`        | table           | `MD5(vessel_imo)`   | `bronze.raw_vessel_calls` |
| `dim_carrier`       | table           | `MD5(carrier_code)` | `bronze.raw_shipments`    |
| `dim_shipment`      | table           | `MD5(shipment_id)`  | `stg_shipments`           |
| `dim_customs_entry` | table           | `MD5(entry_number)` | `stg_customs_entries`     |

### Dynamic Tables

| Table                 | TARGET_LAG | Source                                   | Purpose                                        |
| --------------------- | ---------- | ---------------------------------------- | ---------------------------------------------- |
| `dt_active_shipments` | 5 min      | `silver.stg_shipments` + `gold.dim_port` | Current in-transit shipments with delay status |
| `dt_port_congestion`  | 15 min     | `gold.fct_port_dwell` + `gold.dim_port`  | Rolling 24-hour congestion score per port      |

---

## 20. Dashboard Targets

| Chart                      | Source Table                          | Metric                            | Notes                                                           |
| -------------------------- | ------------------------------------- | --------------------------------- | --------------------------------------------------------------- |
| Port congestion map        | `dt_port_congestion`                  | `congestion_level` by `lat`/`lon` | Color: NORMAL=green, MODERATE=yellow, HIGH=orange, CRITICAL=red |
| Delay trend by carrier     | `fct_shipment_events` + `dim_carrier` | `AVG(delay_hours)` over time      | Group by `carrier_name`, `year_month`                           |
| Active delayed shipments   | `dt_active_shipments`                 | Count by `delay_category`         | KPI tile, near-real-time                                        |
| Landed cost by commodity   | `fct_landed_cost`                     | `SUM(total_landed_cost_php)`      | Join to `dim_customs_entry` for commodity grouping              |
| Average dwell time by port | `fct_port_dwell` + `dim_port`         | `AVG(dwell_hours)` by port        | Bar chart ordered by dwell descending                           |

---

## 21. Delivery Order (80/20)

Build in this order. The first three priorities deliver the working pipeline and main dashboard tile. Items 4–7 add depth.

| Priority   | Deliverable                                                       | Why                                                             |
| ---------- | ----------------------------------------------------------------- | --------------------------------------------------------------- |
| 1 — must   | Bronze load + `stg_vessel_calls` + `fct_port_dwell`               | Proves the ingest pipeline works end-to-end                     |
| 2 — must   | `stg_shipments` + `fct_shipment_events` with delay classification | Core analytical value — delay by carrier/route                  |
| 3 — must   | `dt_port_congestion` Dynamic Table + congestion map               | The "near-real-time without Kafka" talking point                |
| 4 — should | `fct_landed_cost` with tariff seed + FX rates                     | Differentiates the project from basic delay tracking            |
| 5 — should | `dt_active_shipments` Dynamic Table                               | Enables the "delayed right now" KPI tile                        |
| 6 — nice   | Geospatial port catchment area analysis                           | Demonstrates ST\_ functions and rerouting use case              |
| 7 — nice   | Full Airflow DAG with 4-hour schedule                             | Shows orchestration; do last since pipeline can be run manually |

---

## Quick Start

```bash
# Activate shared venv
source ../portfolio-infra/.venv/bin/activate

# Step 1 — Generate raw data
python scripts/generate_port_events.py      # → data/raw_vessel_calls.csv
python scripts/generate_shipments.py        # → data/raw_shipments.csv
python scripts/generate_customs_entries.py  # reads raw_shipments → raw_customs_entries.csv

# Step 2 — Load to Snowflake bronze
python scripts/load_bronze.py

# Step 3 — Run dbt (no dbt deps needed — no packages used)
cd logistics_dbt

dbt seed --full-refresh \
  --profiles-dir ../../portfolio-infra/dbt --profile logistics

dbt run \
  --profiles-dir ../../portfolio-infra/dbt --profile logistics

dbt test \
  --profiles-dir ../../portfolio-infra/dbt --profile logistics

# Step 4 — Create Dynamic Tables (run once in Snowflake worksheet)
# Open snowflake/dynamic_tables.sql and execute in a Snowflake worksheet
# They will begin auto-refreshing immediately on their TARGET_LAG schedule
```
