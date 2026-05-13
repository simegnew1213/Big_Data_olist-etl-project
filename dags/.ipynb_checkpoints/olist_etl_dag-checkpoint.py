"""
Olist ETL Pipeline DAG — Apache Airflow Orchestration
=====================================================
Orchestrates the complete ETL pipeline on a daily schedule.

Pipeline stages (run in sequence):
  1. extract_data       → runs notebooks/01_extract.py
                          Pulls CSV files, Weather API data, and Parquet source
  2. spark_transform    → runs notebooks/02_transform_spark.py  (PySpark)
                          Falls back to 02_transform.py if Java is unavailable
  3. load_to_duckdb     → runs notebooks/03_load.py
                          Persists transformed Parquet files into DuckDB
  4. validate_quality   → inline data-quality checks on DuckDB tables
  5. generate_report    → writes pipeline_report.json with key KPIs
  6. cleanup_temp       → removes Spark temp dirs and notebook output files

─────────────────────────────────────────────────────────────────
DEPLOYMENT (Linux / WSL2 / Docker)
─────────────────────────────────────────────────────────────────
  export AIRFLOW_HOME=./airflow
  pip install apache-airflow
  airflow db init
  airflow users create --username admin --role Admin \\
      --firstname A --lastname B --email admin@example.com
  # copy this file to $AIRFLOW_HOME/dags/
  airflow dags list
  airflow dags trigger olist_etl_pipeline

─────────────────────────────────────────────────────────────────
DIRECT RUN (Windows — no Airflow needed)
─────────────────────────────────────────────────────────────────
  python dags/olist_etl_dag.py
  Executes all 6 tasks sequentially and prints a summary.

NOTE: Apache Airflow does NOT support Windows natively.
      Use WSL2, Docker, or run the pipeline directly as shown above.
─────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta

# ── Airflow imports ────────────────────────────────────────────────────────────
# Graceful fallback: if Airflow is not installed the module still runs
# standalone via __main__ (useful on Windows / CI without Airflow).
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    DAG = None
    PythonOperator = None

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOKS    = os.path.join(PROJECT_ROOT, 'notebooks')
DB_PATH      = os.path.join(PROJECT_ROOT, 'analytics.duckdb')

# ── Default DAG arguments ──────────────────────────────────────────────────────
default_args: dict = {
    'owner'           : 'olist-etl-team',
    'depends_on_past' : False,
    # days_ago(1) keeps the start_date timezone-aware (Airflow best practice).
    'start_date'      : days_ago(1) if AIRFLOW_AVAILABLE else datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry'  : False,
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes=5),
}


# ══════════════════════════════════════════════════════════════════════════════
# HELPER
# ══════════════════════════════════════════════════════════════════════════════

def run_script(script_name: str) -> int:
    """
    Execute *script_name* (relative to notebooks/) using the current Python
    interpreter so that the same virtualenv / packages are used.

    Raises
    ------
    FileNotFoundError
        If the script does not exist on disk.
    subprocess.CalledProcessError
        If the script exits with a non-zero return code.
    """
    script_path = os.path.join(NOTEBOOKS, script_name)
    if not os.path.exists(script_path):
        raise FileNotFoundError(
            f"Script not found: {script_path}\n"
            f"Make sure '{script_name}' exists inside the notebooks/ directory."
        )

    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=False,   # stream stdout/stderr directly to Airflow logs
        cwd=PROJECT_ROOT,
        check=True,             # raises CalledProcessError on non-zero exit
    )
    return result.returncode


# ══════════════════════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# Each function maps 1-to-1 with an Airflow PythonOperator task.
# They are also called directly when running the pipeline without Airflow.
# ══════════════════════════════════════════════════════════════════════════════

def task_extract() -> None:
    """
    STEP 1 — Extract
    ----------------
    Pulls data from three sources:
      • Multiple Olist CSV files  (local data/ directory)
      • Open-Meteo Weather API    (REST, no key required)
      • Olist reviews Parquet     (local data/ directory)
    Outputs raw Parquet files to data/raw/.
    """
    print('=' * 60)
    print('TASK 1/6 — extract_data')
    print('=' * 60)
    run_script('01_extract.py')
    print('✅  Extraction complete.')


def task_spark_transform() -> None:
    """
    STEP 2 — Transform
    ------------------
    Applies business-logic transformations using PySpark:
      • Joins orders, customers, payments, reviews, and weather data
      • Engineers delivery_days, was_late, weather buckets
      • Writes transformed data to data/processed/ as Parquet

    Falls back to a pandas implementation (02_transform.py) when Java /
    PySpark is unavailable (e.g. on Windows without WSL2).
    """
    print('=' * 60)
    print('TASK 2/6 — spark_transform')
    print('=' * 60)

    # Detect Java availability for PySpark
    java_available = False
    try:
        check = subprocess.run(
            ['java', '-version'],
            capture_output=True,
            timeout=10,
        )
        java_available = check.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    if java_available:
        print('Java detected → running PySpark transform (02_transform_spark.py)')
        run_script('02_transform_spark.py')
    else:
        print('Java not found → running pandas fallback (02_transform.py)')
        run_script('02_transform.py')

    print('✅  Transformation complete.')


def task_load() -> None:
    """
    STEP 3 — Load
    -------------
    Reads processed Parquet files from data/processed/ and loads them
    into DuckDB (analytics.duckdb) as analytical tables:
      • orders_full       — grain: one row per order
      • monthly_revenue   — grain: year / month
      • revenue_by_state  — grain: customer_state
      • weather_impact    — grain: weather_bucket
    """
    print('=' * 60)
    print('TASK 3/6 — load_to_duckdb')
    print('=' * 60)
    run_script('03_load.py')
    print('✅  Load complete.')


def task_validate() -> None:
    """
    STEP 4 — Validate
    -----------------
    Runs data-quality checks directly against DuckDB:
      • Row-count check  : every analytical table must have > 0 rows
      • Null check       : order_id, total_payment, customer_state must be non-null

    Raises ValueError if any check fails, which causes Airflow to mark
    the task — and the DAG run — as failed.
    """
    print('=' * 60)
    print('TASK 4/6 — validate_data_quality')
    print('=' * 60)

    import duckdb

    conn     = duckdb.connect(DB_PATH)
    tables   = ['orders_full', 'monthly_revenue', 'revenue_by_state', 'weather_impact']
    failures = []

    # ── Row-count check ───────────────────────────────────────
    for table in tables:
        try:
            count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
            if count == 0:
                failures.append(f'{table}: 0 rows (expected > 0)')
            else:
                print(f'  ✅  {table}: {count:,} rows')
        except Exception as exc:
            failures.append(f'{table}: query error — {exc}')

    # ── Null check on critical columns ────────────────────────
    null_df = conn.execute('''
        SELECT
            SUM(CASE WHEN order_id        IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
            SUM(CASE WHEN total_payment   IS NULL THEN 1 ELSE 0 END) AS null_payments,
            SUM(CASE WHEN customer_state  IS NULL THEN 1 ELSE 0 END) AS null_states
        FROM orders_full
    ''').fetchdf()

    print('\nNull check — orders_full critical columns:')
    print(null_df.to_string(index=False))

    for col in ['null_order_ids', 'null_payments', 'null_states']:
        val = int(null_df[col].iloc[0])
        if val > 0:
            failures.append(f'orders_full.{col.replace("null_", "")}: {val} NULL values')

    conn.close()

    if failures:
        raise ValueError('Data quality failures:\n' + '\n'.join(f'  • {f}' for f in failures))

    print('\n✅  All quality checks passed.')


def task_report() -> None:
    """
    STEP 5 — Report
    ---------------
    Queries DuckDB and writes pipeline_report.json to the project root.
    Includes:
      • Key KPIs  (total orders, revenue, avg order value, late-delivery rate)
      • Top 5 states by revenue
      • First 6 months of monthly revenue trend
    """
    print('=' * 60)
    print('TASK 5/6 — generate_pipeline_report')
    print('=' * 60)

    import duckdb

    conn = duckdb.connect(DB_PATH)

    # Key performance indicators
    kpis = conn.execute('''
        SELECT
            COUNT(order_id)                                       AS total_orders,
            ROUND(SUM(total_payment), 2)                          AS total_revenue,
            ROUND(AVG(total_payment), 2)                          AS avg_order_value,
            ROUND(AVG(delivery_days), 1)                          AS avg_delivery_days,
            SUM(was_late)                                         AS total_late_deliveries,
            ROUND(SUM(was_late) * 100.0 / COUNT(order_id), 2)    AS late_delivery_rate_pct
        FROM orders_full
    ''').fetchdf().to_dict('records')[0]

    # Top 5 revenue-generating states
    top_states = conn.execute('''
        SELECT customer_state, total_orders, total_revenue
        FROM   revenue_by_state
        ORDER  BY total_revenue DESC
        LIMIT  5
    ''').fetchdf().to_dict('records')

    # Monthly revenue trend (first 6 months for brevity)
    monthly_trend = conn.execute('''
        SELECT year, month, total_orders, total_revenue
        FROM   monthly_revenue
        ORDER  BY year, month
        LIMIT  6
    ''').fetchdf().to_dict('records')

    report = {
        'pipeline_run_time': datetime.now().isoformat(),
        'key_metrics'      : kpis,
        'top_states'       : top_states,
        'monthly_trend'    : monthly_trend,
    }

    report_path = os.path.join(PROJECT_ROOT, 'pipeline_report.json')
    with open(report_path, 'w') as fh:
        json.dump(report, fh, indent=2, default=str)

    conn.close()

    print(f'Report saved → {report_path}')
    print(f"  Total Orders       : {kpis['total_orders']:,}")
    print(f"  Total Revenue      : R${kpis['total_revenue']:,.2f}")
    print(f"  Avg Order Value    : R${kpis['avg_order_value']:,.2f}")
    print(f"  Avg Delivery Days  : {kpis['avg_delivery_days']}")
    print(f"  Late Delivery Rate : {kpis['late_delivery_rate_pct']}%")
    print('✅  Report complete.')


def task_cleanup() -> None:
    """
    STEP 6 — Cleanup
    ----------------
    Removes artefacts that should not persist between pipeline runs:
      • *_output.ipynb  — Jupyter output notebooks
      • *.pyc           — compiled Python bytecode
      • spark-warehouse/, metastore_db/, derby.log — Spark temp files
    """
    import glob
    import shutil

    print('=' * 60)
    print('TASK 6/6 — cleanup_temp_files')
    print('=' * 60)

    # Notebook output files and bytecode
    for pattern in ['*_output.ipynb', '*.pyc']:
        for f in glob.glob(os.path.join(NOTEBOOKS, pattern)):
            os.remove(f)
            print(f'  Removed file : {f}')

    # Spark / Derby artefacts
    for artefact in ['spark-warehouse', 'metastore_db', 'derby.log']:
        path = os.path.join(PROJECT_ROOT, artefact)
        if os.path.isdir(path):
            shutil.rmtree(path)
            print(f'  Removed dir  : {path}')
        elif os.path.isfile(path):
            os.remove(path)
            print(f'  Removed file : {path}')

    print('✅  Cleanup complete.')


# ══════════════════════════════════════════════════════════════════════════════
# AIRFLOW DAG DEFINITION
# Only registered when Airflow is installed (import succeeded above).
# ══════════════════════════════════════════════════════════════════════════════

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id='olist_etl_pipeline',
        default_args=default_args,
        description='Olist ETL Pipeline — PySpark + DuckDB + Airflow',
        # 'schedule' replaces the deprecated 'schedule_interval' in Airflow 2.4+
        schedule='@daily',
        catchup=False,
        max_active_runs=1,          # prevent overlapping runs
        tags=['etl', 'olist', 'pyspark', 'duckdb'],
    ) as dag:

        t_extract = PythonOperator(
            task_id='extract_data',
            python_callable=task_extract,
        )

        t_transform = PythonOperator(
            task_id='spark_transform',
            python_callable=task_spark_transform,
            execution_timeout=timedelta(minutes=30),
        )

        t_load = PythonOperator(
            task_id='load_to_duckdb',
            python_callable=task_load,
        )

        t_validate = PythonOperator(
            task_id='validate_data_quality',
            python_callable=task_validate,
        )

        t_report = PythonOperator(
            task_id='generate_pipeline_report',
            python_callable=task_report,
        )

        t_cleanup = PythonOperator(
            task_id='cleanup_temp_files',
            python_callable=task_cleanup,
            trigger_rule='all_done',    # run cleanup even if upstream tasks fail
        )

        # ── Dependency chain ──────────────────────────────────
        # extract → transform → load → validate → report → cleanup
        t_extract >> t_transform >> t_load >> t_validate >> t_report >> t_cleanup


# ══════════════════════════════════════════════════════════════════════════════
# DIRECT EXECUTION  (Windows / environments without Airflow)
# ══════════════════════════════════════════════════════════════════════════════
# Usage:
#   python dags/olist_etl_dag.py
#
# Runs all 6 pipeline tasks sequentially in the current Python process.
# Stops at the first failure and prints a summary table at the end.
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import traceback

    SEPARATOR = '=' * 60

    print(SEPARATOR)
    if AIRFLOW_AVAILABLE:
        print('NOTE: Airflow is installed.')
        print('To use the Airflow scheduler instead, run:')
        print('  airflow dags trigger olist_etl_pipeline')
        print('Running pipeline directly for local testing...')
    else:
        print('Airflow not installed — running pipeline directly.')
        print('(Airflow requires Linux / WSL2 / Docker on Windows)')
    print(SEPARATOR)

    PIPELINE: list[tuple[str, callable]] = [
        ('extract_data',          task_extract),
        ('spark_transform',       task_spark_transform),
        ('load_to_duckdb',        task_load),
        ('validate_data_quality', task_validate),
        ('generate_report',       task_report),
        ('cleanup_temp_files',    task_cleanup),
    ]

    results: dict[str, str] = {}

    for task_id, fn in PIPELINE:
        print(f'\n>>> Running task: {task_id}')
        try:
            fn()
            results[task_id] = '✅  PASS'
        except Exception as exc:
            results[task_id] = f'❌  FAIL: {exc}'
            traceback.print_exc()
            # Mark remaining tasks as skipped
            for tid, _ in PIPELINE[len(results):]:
                results[tid] = '⏭️  SKIPPED'
            print(f'\nTask "{task_id}" failed — pipeline stopped.')
            break

    print(f'\n{SEPARATOR}')
    print('PIPELINE SUMMARY')
    print(SEPARATOR)
    for task_id, status in results.items():
        print(f'  {status:<35}  {task_id}')
    print(SEPARATOR)

    # Exit with non-zero code if any task failed (useful for CI/CD)
    if any('FAIL' in s for s in results.values()):
        sys.exit(1)