"""
Olist ETL Pipeline DAG — Apache Airflow Orchestration
=====================================================
Orchestrates the complete ETL pipeline on a daily schedule.

Pipeline stages:
  1. extract_data       → runs notebooks/01_extract.py
  2. spark_transform    → runs notebooks/02_transform_spark.py  (PySpark)
                          falls back to 02_transform.py if Java unavailable
  3. load_to_duckdb     → runs notebooks/03_load.py
  4. validate_quality   → data quality checks on DuckDB
  5. generate_report    → writes pipeline_report.json
  6. cleanup_temp       → removes any temp Spark/notebook files

To deploy:
  export AIRFLOW_HOME=./airflow
  airflow db init
  airflow dags list
  airflow dags trigger olist_etl_pipeline
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Project root ─────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOKS    = os.path.join(PROJECT_ROOT, 'notebooks')
DB_PATH      = os.path.join(PROJECT_ROOT, 'analytics.duckdb')

# ── DAG defaults ─────────────────────────────────────────────
default_args = {
    'owner': 'olist-etl-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# ──────────────────────────────────────────────────────────────
# TASK FUNCTIONS
# ──────────────────────────────────────────────────────────────

def run_script(script_name: str) -> None:
    """Run a Python script inside the project virtualenv."""
    script_path = os.path.join(NOTEBOOKS, script_name)
    if not os.path.exists(script_path):
        raise FileNotFoundError(f'Script not found: {script_path}')

    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=False,      # stream stdout/stderr to Airflow logs
        cwd=PROJECT_ROOT,
        check=True,                # raise CalledProcessError on non-zero exit
    )
    return result.returncode


def task_extract() -> None:
    """STEP 1 — Extract data from CSV files, Weather API, and Parquet source."""
    print('=' * 60)
    print('AIRFLOW TASK: extract_data')
    print('=' * 60)
    run_script('01_extract.py')
    print('✅ Extraction complete.')


def task_spark_transform() -> None:
    """STEP 2 — Transform data with PySpark (falls back to pandas if no Java)."""
    print('=' * 60)
    print('AIRFLOW TASK: spark_transform')
    print('=' * 60)

    # Try PySpark first
    try:
        java_check = subprocess.run(
            ['java', '-version'], capture_output=True, timeout=10
        )
        java_available = java_check.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        java_available = False

    if java_available:
        print('Java detected → running PySpark transform')
        run_script('02_transform_spark.py')
    else:
        print('Java not found → running pandas fallback transform')
        run_script('02_transform.py')

    print('✅ Transformation complete.')


def task_load() -> None:
    """STEP 3 — Load transformed Parquet files into DuckDB."""
    print('=' * 60)
    print('AIRFLOW TASK: load_to_duckdb')
    print('=' * 60)
    run_script('03_load.py')
    print('✅ Load complete.')


def task_validate() -> None:
    """STEP 4 — Data quality validation against DuckDB."""
    print('=' * 60)
    print('AIRFLOW TASK: validate_data_quality')
    print('=' * 60)

    import duckdb

    conn = duckdb.connect(DB_PATH)
    tables = ['orders_full', 'monthly_revenue', 'revenue_by_state', 'weather_impact']
    failures = []

    for table in tables:
        try:
            count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
            if count == 0:
                failures.append(f'{table}: 0 rows (expected > 0)')
            else:
                print(f'  ✅ {table}: {count:,} rows')
        except Exception as exc:
            failures.append(f'{table}: {exc}')

    # Null check on critical columns
    null_check = conn.execute('''
        SELECT
            SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
            SUM(CASE WHEN total_payment IS NULL THEN 1 ELSE 0 END) AS null_payments,
            SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END) AS null_states
        FROM orders_full
    ''').fetchdf()
    print('\nNull check summary:')
    print(null_check.to_string(index=False))

    conn.close()

    if failures:
        raise ValueError('Data quality failures:\n' + '\n'.join(failures))

    print('\n✅ All quality checks passed.')


def task_report() -> None:
    """STEP 5 — Generate JSON pipeline report with key metrics."""
    print('=' * 60)
    print('AIRFLOW TASK: generate_pipeline_report')
    print('=' * 60)

    import duckdb

    conn = duckdb.connect(DB_PATH)

    kpis = conn.execute('''
        SELECT
            COUNT(order_id)                                       AS total_orders,
            ROUND(SUM(total_payment), 2)                          AS total_revenue,
            ROUND(AVG(total_payment), 2)                          AS avg_order_value,
            ROUND(AVG(delivery_days), 1)                          AS avg_delivery_days,
            SUM(was_late)                                         AS total_late_deliveries,
            ROUND(SUM(was_late) * 100.0 / COUNT(order_id), 2)    AS late_delivery_rate
        FROM orders_full
    ''').fetchdf().to_dict('records')[0]

    top_states = conn.execute('''
        SELECT customer_state, total_orders, total_revenue
        FROM revenue_by_state ORDER BY total_revenue DESC LIMIT 5
    ''').fetchdf().to_dict('records')

    monthly_trend = conn.execute('''
        SELECT year, month, total_orders, total_revenue
        FROM monthly_revenue ORDER BY year, month LIMIT 6
    ''').fetchdf().to_dict('records')

    report = {
        'pipeline_run_time': datetime.now().isoformat(),
        'key_metrics': kpis,
        'top_states': top_states,
        'monthly_trend': monthly_trend,
    }

    report_path = os.path.join(PROJECT_ROOT, 'pipeline_report.json')
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)

    conn.close()
    print(f'Report saved: {report_path}')
    print(f"Total Orders  : {kpis['total_orders']:,}")
    print(f"Total Revenue : R${kpis['total_revenue']:,.2f}")
    print(f"Late Rate     : {kpis['late_delivery_rate']}%")
    print('✅ Report complete.')


def task_cleanup() -> None:
    """STEP 6 — Remove temporary output notebooks and Spark temp dirs."""
    import glob
    import shutil

    print('=' * 60)
    print('AIRFLOW TASK: cleanup_temp_files')
    print('=' * 60)

    # Remove notebook output files
    for pattern in ['*_output.ipynb', '*.pyc']:
        for f in glob.glob(os.path.join(NOTEBOOKS, pattern)):
            os.remove(f)
            print(f'  Removed: {f}')

    # Remove Spark metastore
    for d in ['spark-warehouse', 'metastore_db', 'derby.log']:
        path = os.path.join(PROJECT_ROOT, d)
        if os.path.isdir(path):
            shutil.rmtree(path)
            print(f'  Removed dir: {path}')
        elif os.path.isfile(path):
            os.remove(path)
            print(f'  Removed file: {path}')

    print('✅ Cleanup complete.')


# ──────────────────────────────────────────────────────────────
# DAG DEFINITION
# ──────────────────────────────────────────────────────────────

with DAG(
    dag_id='olist_etl_pipeline',
    default_args=default_args,
    description='Complete Olist ETL Pipeline — PySpark + DuckDB + Airflow',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'olist', 'pyspark', 'duckdb'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=task_extract,
    )

    spark_transform_task = PythonOperator(
        task_id='spark_transform',
        python_callable=task_spark_transform,
        execution_timeout=timedelta(minutes=30),  # Spark can be slow on first run
    )

    load_task = PythonOperator(
        task_id='load_to_duckdb',
        python_callable=task_load,
    )

    validate_task = PythonOperator(
        task_id='validate_data_quality',
        python_callable=task_validate,
    )

    report_task = PythonOperator(
        task_id='generate_pipeline_report',
        python_callable=task_report,
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_temp_files',
        python_callable=task_cleanup,
    )

    # ── Task dependency chain ────────────────────────────────
    # extract → spark_transform → load → validate → report → cleanup
    extract_task >> spark_transform_task >> load_task >> validate_task >> report_task >> cleanup_task
