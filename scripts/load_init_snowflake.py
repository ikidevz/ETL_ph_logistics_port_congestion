import snowflake.connector
import os

SNOWFLAKE_WAREHOUSE = "portfolio_wh"
SNOWFLAKE_DATABASE = "logistics_db"
SNOWFLAKE_SCHEMAS = ["bronze", "silver", "gold", "gold"]

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
)
cur = conn.cursor()

# ── Warehouse ─────────────────────────────────────────────────────────
cur.execute(f"""
        CREATE WAREHOUSE IF NOT EXISTS {SNOWFLAKE_WAREHOUSE}
            WAREHOUSE_SIZE = 'X-SMALL'
            AUTO_SUSPEND   = 60
            AUTO_RESUME    = TRUE
            COMMENT        = 'Shared warehouse for all portfolio projects'
    """)
print(f"[OK] Warehouse {SNOWFLAKE_WAREHOUSE} ensured.")

# ── Database ──────────────────────────────────────────────────────────
cur.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
print(f"[OK] Database {SNOWFLAKE_DATABASE} ensured.")

# ── Schemas ───────────────────────────────────────────────────────────
for schema in SNOWFLAKE_SCHEMAS:
    cur.execute(
        f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{schema}")
    print(f"[OK] Schema {SNOWFLAKE_DATABASE}.{schema} ensured.")

bronze_tables = {
    "raw_vessel_calls": """
        vessel_imo VARCHAR, port_code VARCHAR, arrival_ts TIMESTAMP_NTZ,
        berth_ts TIMESTAMP_NTZ, departure_ts TIMESTAMP_NTZ, dwell_hours INTEGER,
        anchorage_wait_h INTEGER, cargo_type VARCHAR, teu_count INTEGER,
        _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()""",

    "raw_shipments": """
        shipment_id VARCHAR, bl_number VARCHAR, origin_port VARCHAR, dest_port VARCHAR,
        carrier_code VARCHAR, vessel_imo VARCHAR, etd TIMESTAMP_NTZ, eta TIMESTAMP_NTZ,
        ata TIMESTAMP_NTZ, delay_hours INTEGER, cargo_type VARCHAR, commodity_code VARCHAR,
        declared_value_usd NUMBER(15,2), freight_usd NUMBER(15,2), insurance_usd NUMBER(15,2),
        _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()""",

    "raw_customs_entries": """
        entry_number VARCHAR, shipment_id VARCHAR, bl_number VARCHAR, commodity_code VARCHAR,
        entry_type VARCHAR, declared_value_usd NUMBER(15,2), tariff_rate_pct NUMERIC(5,2),
        duties_usd NUMERIC(15,2), vat_usd NUMERIC(15,2), total_tax_usd NUMERIC(15,2),
        lodge_date DATE, assessment_date DATE, release_date DATE, processing_days INTEGER,
        payment_mode VARCHAR, port_code VARCHAR,
        _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"""
}

for table, columns in bronze_tables.items():
    cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.bronze.{table} ({columns})
        """)
    print(f"[OK] Table {SNOWFLAKE_DATABASE}.bronze.{table} ensured.")
