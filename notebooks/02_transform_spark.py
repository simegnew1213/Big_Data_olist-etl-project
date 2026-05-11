# ============================================================
# STEP 2 — TRANSFORM WITH APACHE PYSPARK
# Olist ETL Pipeline — Distributed Computing Version
#
# Sources consumed:
#   - data/raw/*.csv           (Olist CSV files)
#   - data/parquet/olist_orders.parquet  (Source 3 Parquet)
#   - data/parquet/sao_paulo_weather.csv (Source 2 Weather API)
#
# Output:
#   - data/transformed/orders_full.parquet
#   - data/transformed/monthly_revenue.parquet
#   - data/transformed/revenue_by_state.parquet
#   - data/transformed/weather_impact.parquet
#
# Requirements:
#   - Java 8+ must be installed and JAVA_HOME set
#   - pyspark >= 3.4.0 installed
#   - If Java is unavailable, run 02_transform.py (pandas fallback)
# ============================================================

import os
import sys

# ── Paths ────────────────────────────────────────────────────
BASE        = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
RAW         = os.path.join(BASE, 'data', 'raw')
PARQUET     = os.path.join(BASE, 'data', 'parquet')
TRANSFORMED = os.path.join(BASE, 'data', 'transformed')

os.makedirs(TRANSFORMED, exist_ok=True)

# ── Check Java availability ──────────────────────────────────
import subprocess
try:
    result = subprocess.run(['java', '-version'], capture_output=True, text=True, timeout=10)
    java_ok = result.returncode == 0 or 'version' in result.stderr
    if java_ok:
        print('✅ Java detected — PySpark will run in distributed mode')
    else:
        raise RuntimeError('Java not found')
except Exception:
    print('⚠️  Java not detected. Falling back to pandas transform.')
    print('   To use PySpark: install Java 8+ and set JAVA_HOME.')
    print('   Calling 02_transform.py (pandas fallback)...\n')
    import runpy
    runpy.run_path(os.path.join(BASE, 'notebooks', '02_transform.py'))
    sys.exit(0)

# ── PySpark imports ──────────────────────────────────────────
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType,
        IntegerType, TimestampType
    )
    from pyspark.sql.window import Window
except ImportError as e:
    print(f'❌ PySpark import error: {e}')
    print('   Install with: pip install pyspark')
    sys.exit(1)

# ============================================================
# 1. INITIALIZE SPARK SESSION
# ============================================================
print('\n' + '='*60)
print('Initialising SparkSession...')
print('='*60)

spark = (
    SparkSession.builder
    .appName('OlistETL')
    .master('local[*]')                       # Use all local CPU cores
    .config('spark.driver.memory', '2g')
    .config('spark.sql.shuffle.partitions', '4')  # Keep small for local mode
    .config('spark.sql.legacy.timeParserPolicy', 'LEGACY')
    .config('spark.driver.extraJavaOptions',
            '-Dlog4j.rootCategory=ERROR,console')  # Suppress verbose logs
    .getOrCreate()
)
spark.sparkContext.setLogLevel('ERROR')

print(f'Spark version : {spark.version}')
print(f'Master        : {spark.sparkContext.master}')
print(f'App Name      : {spark.sparkContext.appName}')

# ============================================================
# 2. LOAD DATA SOURCES
# ============================================================
print('\n' + '='*60)
print('Loading data sources into Spark...')
print('='*60)

# Source 1: Olist CSVs via Spark
orders_csv = spark.read.option('header', True).option('inferSchema', True)\
    .csv(os.path.join(RAW, 'olist_orders_dataset.csv').replace('\\', '/'))

customers = spark.read.option('header', True).option('inferSchema', True)\
    .csv(os.path.join(RAW, 'olist_customers_dataset.csv').replace('\\', '/'))

order_items = spark.read.option('header', True).option('inferSchema', True)\
    .csv(os.path.join(RAW, 'olist_order_items_dataset.csv').replace('\\', '/'))

payments = spark.read.option('header', True).option('inferSchema', True)\
    .csv(os.path.join(RAW, 'olist_order_payments_dataset.csv').replace('\\', '/'))

print(f'orders_csv   : {orders_csv.count():,} rows | {len(orders_csv.columns)} cols')
print(f'customers    : {customers.count():,} rows')
print(f'order_items  : {order_items.count():,} rows')
print(f'payments     : {payments.count():,} rows')

# Source 3: Parquet file (created by 01_extract.py)
parquet_path = os.path.join(PARQUET, 'olist_orders.parquet').replace('\\', '/')
orders_parquet = spark.read.parquet(parquet_path)
print(f'orders.parquet: {orders_parquet.count():,} rows  ← Parquet source ✅')

# Source 2: Weather CSV (created by 01_extract.py from Open-Meteo API)
weather_path = os.path.join(PARQUET, 'sao_paulo_weather.csv').replace('\\', '/')
weather = spark.read.option('header', True).option('inferSchema', True)\
    .csv(weather_path)
print(f'weather      : {weather.count():,} rows  ← API source ✅')

# ============================================================
# 3. TRANSFORMATION 1 — Clean Orders (use parquet source)
# ============================================================
print('\n' + '='*60)
print('TRANSFORMATION 1 — Cleaning Orders (PySpark)')
print('='*60)

# Parse timestamp columns
date_cols = [
    'order_purchase_timestamp',
    'order_approved_at',
    'order_delivered_carrier_date',
    'order_delivered_customer_date',
    'order_estimated_delivery_date'
]

orders_typed = orders_parquet
for col_name in date_cols:
    if col_name in orders_typed.columns:
        orders_typed = orders_typed.withColumn(
            col_name,
            F.to_timestamp(F.col(col_name))
        )

# Filter: delivered orders only
orders_clean = orders_typed.filter(F.col('order_status') == 'delivered')

# Derived columns
orders_clean = (
    orders_clean
    .withColumn('purchase_date',  F.to_date('order_purchase_timestamp'))
    .withColumn('delivery_date',  F.to_date('order_delivered_customer_date'))
    .withColumn('estimated_date', F.to_date('order_estimated_delivery_date'))
    .withColumn('delivery_days',
        F.datediff(F.col('delivery_date'), F.col('purchase_date')))
    .withColumn('was_late',
        (F.col('delivery_date') > F.col('estimated_date')).cast(IntegerType()))
    .withColumn('year',  F.year('purchase_date'))
    .withColumn('month', F.month('purchase_date'))
)

delivered_count = orders_clean.count()
print(f'Delivered orders: {delivered_count:,}')
print('Schema after cleaning:')
orders_clean.printSchema()

# ============================================================
# 4. TRANSFORMATION 2 — Joins
# ============================================================
print('\n' + '='*60)
print('TRANSFORMATION 2 — Distributed Joins (PySpark)')
print('='*60)

# Join with customers
orders_customers = orders_clean.join(
    customers.select('customer_id', 'customer_state', 'customer_city'),
    on='customer_id', how='left'
)

# Aggregate payments
payments_agg = (
    payments.groupBy('order_id')
    .agg(
        F.round(F.sum('payment_value'), 2).alias('total_payment'),
        F.count('payment_sequential').alias('payment_count')
    )
)

# Join payments
orders_payments = orders_customers.join(payments_agg, on='order_id', how='left')

# Aggregate items
items_agg = (
    order_items.groupBy('order_id')
    .agg(
        F.count('order_item_id').alias('item_count'),
        F.round(F.sum('price'), 2).alias('items_total'),
        F.round(F.sum('freight_value'), 2).alias('freight_total')
    )
)

# Join items
orders_full_spark = orders_payments.join(items_agg, on='order_id', how='left')
print(f'Orders after all joins: {orders_full_spark.count():,}')

# ============================================================
# 5. TRANSFORMATION 3 — Join with Weather
# ============================================================
print('\n' + '='*60)
print('TRANSFORMATION 3 — Enriching with Weather Data (PySpark)')
print('='*60)

weather_typed = (
    weather
    .withColumn('date',          F.to_date('date'))
    .withColumn('temp_max',      F.col('temp_max').cast(DoubleType()))
    .withColumn('precipitation', F.col('precipitation').cast(DoubleType()))
)

orders_weather = orders_full_spark.join(
    weather_typed,
    orders_full_spark['purchase_date'] == weather_typed['date'],
    how='left'
).drop('date')

# Add weather category column
orders_weather = orders_weather.withColumn(
    'rain_category',
    F.when(F.col('precipitation').isNull(), 'No Rain')
     .when(F.col('precipitation') == 0,    'No Rain')
     .when(F.col('precipitation') < 5,     'Light Rain')
     .otherwise('Heavy Rain')
)

print(f'Orders enriched with weather: {orders_weather.count():,}')
orders_weather.select(
    'order_id', 'purchase_date', 'total_payment',
    'temp_max', 'precipitation', 'rain_category'
).show(5, truncate=False)

# Cache for reuse in aggregations
orders_weather.cache()

# ============================================================
# 6. TRANSFORMATION 4 — Aggregated Summary Tables
# ============================================================
print('\n' + '='*60)
print('TRANSFORMATION 4 — Aggregations (PySpark)')
print('='*60)

# Monthly revenue
monthly_revenue = (
    orders_weather.groupBy('year', 'month')
    .agg(
        F.count('order_id').alias('total_orders'),
        F.round(F.sum('total_payment'),  2).alias('total_revenue'),
        F.round(F.avg('total_payment'),  2).alias('avg_order_value'),
        F.round(F.avg('delivery_days'),  1).alias('avg_delivery_days'),
        F.sum('was_late').alias('late_deliveries')
    )
    .orderBy('year', 'month')
)
print('Monthly revenue (first 5 rows):')
monthly_revenue.show(5)

# Revenue by state
revenue_by_state = (
    orders_weather.groupBy('customer_state')
    .agg(
        F.count('order_id').alias('total_orders'),
        F.round(F.sum('total_payment'),  2).alias('total_revenue'),
        F.round(F.avg('total_payment'),  2).alias('avg_order_value'),
        F.round(F.avg('delivery_days'),  1).alias('avg_delivery_days')
    )
    .orderBy(F.desc('total_revenue'))
)
print('Revenue by state (top 5):')
revenue_by_state.show(5)

# Weather impact
weather_impact = (
    orders_weather.groupBy('rain_category')
    .agg(
        F.count('order_id').alias('total_orders'),
        F.round(F.avg('total_payment'), 2).alias('avg_order_value')
    )
)
print('Weather impact:')
weather_impact.show()

# ============================================================
# 7. SAVE ALL TRANSFORMED TABLES AS PARQUET
# ============================================================
print('\n' + '='*60)
print('Saving Spark DataFrames as Parquet...')
print('='*60)

def save_parquet(df, name):
    path = os.path.join(TRANSFORMED, f'{name}.parquet').replace('\\', '/')
    # Coalesce to 1 file so pandas/DuckDB can read it easily
    df.coalesce(1).write.mode('overwrite').parquet(path)
    # Verify
    verify = spark.read.parquet(path)
    print(f'✅ {name}.parquet  →  {verify.count():,} rows saved at {path}')

save_parquet(orders_weather,    'orders_full')
save_parquet(monthly_revenue,   'monthly_revenue')
save_parquet(revenue_by_state,  'revenue_by_state')
save_parquet(weather_impact,    'weather_impact')

# ============================================================
# 8. SHOW SPARK EXECUTION PLAN (transparency)
# ============================================================
print('\n' + '='*60)
print('Spark Logical Plan for orders_weather:')
print('='*60)
orders_weather.explain(mode='simple')

# ── Stop Spark ───────────────────────────────────────────────
spark.stop()
print('\n' + '='*60)
print('PYSPARK TRANSFORMATION COMPLETE')
print('='*60)
print('All 4 Parquet files written to:', TRANSFORMED)
print('Ready for 03_load.py!')
