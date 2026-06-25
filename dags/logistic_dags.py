# dags/logistic_dags.py
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, task

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path("/opt/airflow")
DATA_DIR = PROJECT_ROOT / "data"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"
DBT_PROFILES_DIR = PROJECT_ROOT / "dbt"
DBT_PROFILE = "logistics"

# ---------------------------------------------------------------------------
# Snowflake constants
# ---------------------------------------------------------------------------
SNOWFLAKE_WAREHOUSE = "portfolio_wh"
SNOWFLAKE_DATABASE = "logistics_db"

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "ikidevs",
    "depends_on_past": False
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dbt(*args: str) -> None:
    """
    Run a dbt command inside DBT_PROJECT_DIR.
    Raises subprocess.CalledProcessError on non-zero exit — fails the task.
    """
    subprocess.run(
        [
            "dbt", *args,
            "--profiles-dir", str(DBT_PROFILES_DIR),
            "--profile",      DBT_PROFILE,
        ],
        cwd=str(DBT_PROJECT_DIR),
        check=True,
    )


with DAG(
    dag_id="ph_logistics_port_congestion",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["logistics", "ports", "etl"],
) as dag:

    # -----------------------------------------------------------------------
    # Stage 1 — Generate source CSVs (vessel + shipments run in parallel;
    #           customs must wait for raw_shipments.csv to exist)
    # -----------------------------------------------------------------------

    @task
    def generate_usd_php_rates():
        """Generate USD/PHP rates seed CSV."""
        script_path = SCRIPTS_DIR / "generate_usd_php_rates.py"
        subprocess.run(["python", str(script_path)], check=True)
        print("[OK] USD/PHP rates generated.")

    @task
    def generate_port_events():
        """Generate raw_vessel_calls.csv — overwrites previous run (idempotent)."""
        script_path = SCRIPTS_DIR / "generate_port_events.py"
        subprocess.run(["python", str(script_path)], check=True)
        print("[OK] Port events generated.")

    @task
    def generate_shipments():
        """Generate raw_shipments.csv — overwrites previous run (idempotent)."""
        script_path = SCRIPTS_DIR / "generate_shipments.py"
        subprocess.run(["python", str(script_path)], check=True)
        print("[OK] Shipments generated.")

    @task
    def generate_customs_entries():
        """
        Generate raw_customs_entries.csv from raw_shipments.csv.
        Must run after generate_shipments so the input CSV exists.
        """
        script_path = SCRIPTS_DIR / "generate_customs_entries.py"
        subprocess.run(["python", str(script_path)], check=True)
        print("[OK] Customs entries generated.")

    # -----------------------------------------------------------------------
    # Stage 2 — Snowflake setup + bronze load
    # -----------------------------------------------------------------------

    @task
    def load_init_snowflake():
        """
        CREATE OR REPLACE bronze/silver/gold/realtime schemas and tables.
        Safe to re-run — idempotent DDL.
        """
        script_path = SCRIPTS_DIR / "load_init_snowflake.py"
        subprocess.run(["python", str(script_path)], check=True)
        print("[OK] Initial Snowflake setup completed.")

    @task
    def load_data():
        """
        PUT + COPY INTO bronze tables for all three raw CSVs.
        PUT uses OVERWRITE=TRUE; COPY INTO skips already-loaded file hashes.
        """
        script_path = SCRIPTS_DIR / "load_data.py"
        subprocess.run(["python", str(script_path)], check=True)
        print("[OK] Data loaded into Snowflake bronze.")

    # -----------------------------------------------------------------------
    # Stage 3 — dbt: deps → seeds → staging → core → analytics → tests
    # -----------------------------------------------------------------------
    @task
    def dbt_deps():
        """
        Install dbt dependencies (packages.yml) into DBT_PROJECT_DIR.
        Safe to re-run — idempotent.
        """
        _dbt("deps")
        print("[OK] dbt dependencies installed.")

    @task
    def dbt_seed():
        """
        Load ph_ports, ph_tariff_rates, usd_php_rates into gold schema.
        --full-refresh keeps seeds in sync with CSV files on every run.
        dim_port refs ph_ports seed so seeds must land before staging runs.
        """
        _dbt("seed", "--full-refresh")
        print("[OK] dbt seeds loaded.")

    @task
    def dbt_run_staging():
        """
        Recreate stg_vessel_calls, stg_shipments, stg_customs_entries views
        over fresh bronze data. Views are always idempotent.
        """
        _dbt("run", "--select", "staging")
        print("[OK] dbt staging models complete.")

    @task
    def dbt_run_core():
        """
        Build dims (table) + facts (incremental) in marts.core.
        dbt resolves internal order via the ref() DAG.
        Incremental models watermark on _loaded_at — safe to re-run at any time.
        Models: dim_date, dim_port, dim_vessel, dim_carrier, dim_shipment,
                dim_customs_entry, fct_port_dwell, fct_shipment_events, fct_landed_cost.
        """
        _dbt("run", "--select", "marts.core")
        print("[OK] dbt core models complete.")

    @task
    def dbt_run_analytics():
        """
        Build marts.analytics models:
          - port_catchment_area      (table)   geospatial rerouting analysis
          - carrier_performance      (table)   on-time rate by carrier × lane
          - port_throughput_daily    (incremental) dwell + congestion trend
          - customs_clearance_kpi    (table)   BOC clearance SLA by entry type
          - commodity_cost_summary   (table)   landed cost by HS code × port
          - congestion_delay_impact  (incremental) congestion → delay correlation
        """
        _dbt("run", "--select", "marts.analytics")
        print("[OK] dbt analytics models complete.")

    @task
    def dbt_test():
        """
        Run all dbt schema tests (not_null, unique, accepted_values,
        dbt_utils.expression_is_true) + singular test assert_positive_dwell.
        Fails the DAG on any data quality violation.
        """
        _dbt("test")
        print("[OK] dbt tests passed.")

    t_fx = generate_usd_php_rates()
    t_vessels = generate_port_events()
    t_ships = generate_shipments()
    t_customs = generate_customs_entries()
    t_init = load_init_snowflake()
    t_bronze = load_data()
    t_deps = dbt_deps()
    t_seed = dbt_seed()
    t_staging = dbt_run_staging()
    t_core = dbt_run_core()
    t_analytics = dbt_run_analytics()
    t_tests = dbt_test()

    [t_fx, t_vessels, t_ships] >> t_customs
    t_customs >> t_init >> t_bronze >> t_deps
    t_deps >> t_seed >> t_staging >> t_core >> t_analytics >> t_tests
