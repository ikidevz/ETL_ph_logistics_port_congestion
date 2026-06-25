import os
import snowflake.connector
from pathlib import Path

PROJECT_ROOT = Path("/opt/airflow")
DATA_DIR = PROJECT_ROOT / "data"

SNOWFLAKE_WAREHOUSE = "portfolio_wh"
SNOWFLAKE_DATABASE = "logistics_db"

COLUMN_MAP = {
    "raw_vessel_calls": [
        'vessel_imo', 'port_code', 'arrival_ts', 'berth_ts',
        'departure_ts', 'dwell_hours', 'anchorage_wait_h',
        'cargo_type', 'teu_count'
    ],
    "raw_shipments": [
        'shipment_id', 'bl_number', 'origin_port', 'dest_port',
        'carrier_code', 'vessel_imo', 'etd', 'eta', 'ata',
        'delay_hours', 'cargo_type', 'commodity_code', 'declared_value_usd',
        'freight_usd', 'insurance_usd'
    ],
    "raw_customs_entries": [
        'entry_number', 'shipment_id', 'bl_number', 'commodity_code',
        'entry_type', 'declared_value_usd', 'tariff_rate_pct', 'duties_usd',
        'vat_usd', 'total_tax_usd', 'lodge_date', 'assessment_date',
        'release_date', 'processing_days', 'payment_mode', 'port_code'
    ]
}

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", SNOWFLAKE_WAREHOUSE),
    role=os.getenv("SNOWFLAKE_ROLE"),
    database=os.getenv("SNOWFLAKE_DB", SNOWFLAKE_DATABASE),
    schema="bronze",
)
cur = conn.cursor()

cur.execute("""
    CREATE STAGE IF NOT EXISTS bronze_ingestion
    COMMENT = 'Internal stage for bronze CSV ingestion'
""")


# ── static tables ─────────────────────────────────────────────────────
static_files = {
    "raw_vessel_calls": DATA_DIR / "raw_vessel_calls.csv",
    "raw_customs_entries": DATA_DIR / "raw_customs_entries.csv",
    "raw_shipments": DATA_DIR / "raw_shipments.csv",
}

for table, filepath in static_files.items():
    if not filepath.exists():
        print(f"[WARN] {filepath.name} not found — skipping.")
        continue
    cur.execute(
        f"PUT file://{filepath} @bronze_ingestion OVERWRITE=FALSE AUTO_COMPRESS=TRUE"
    )
    print(f"  ✓ PUT {filepath} → @bronze_ingestion")
    cur.execute(f"""
        COPY INTO {SNOWFLAKE_DATABASE}.bronze.{table} ({', '.join(COLUMN_MAP[table])})
        FROM @bronze_ingestion/{filepath.name}
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            EMPTY_FIELD_AS_NULL = TRUE
            NULL_IF = ('NULL', 'null', '')
        )
        ON_ERROR = CONTINUE
    """)

    print(f"  ✓ COPY INTO {table} (with default _loaded_at)")

conn.close()
