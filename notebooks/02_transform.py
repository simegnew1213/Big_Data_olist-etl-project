# ============================================================
# STEP 2 — TRANSFORM (PANDAS FALLBACK)
# Olist ETL Pipeline — pandas version
#
# ⚠️  This is the FALLBACK script used when Java is not available.
#     The PRIMARY transformation uses Apache PySpark (distributed):
#     → run notebooks/02_transform_spark.py instead.
#
# This script replicates the same logic (cleaning, joins,
# aggregations, Parquet output) using pandas so the pipeline
# remains runnable on machines without Java/PySpark.
# ============================================================

import os
import pandas as pd

# ── Paths ────────────────────────────────────────────────────
BASE        = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
RAW         = os.path.join(BASE, 'data', 'raw')
PARQUET     = os.path.join(BASE, 'data', 'parquet')
TRANSFORMED = os.path.join(BASE, 'data', 'transformed')

os.makedirs(PARQUET,     exist_ok=True)
os.makedirs(TRANSFORMED, exist_ok=True)

# ============================================================
# LOAD ALL FILES
# ============================================================
print('\n' + '='*50)
print('Loading files...')
print('='*50)

orders      = pd.read_csv(os.path.join(RAW, 'olist_orders_dataset.csv'))
customers   = pd.read_csv(os.path.join(RAW, 'olist_customers_dataset.csv'))
order_items = pd.read_csv(os.path.join(RAW, 'olist_order_items_dataset.csv'))
payments    = pd.read_csv(os.path.join(RAW, 'olist_order_payments_dataset.csv'))

# Load weather saved by 01_extract.py
weather = pd.read_csv(os.path.join(PARQUET, 'sao_paulo_weather.csv'))

# Load Parquet file saved by 01_extract.py
orders_parquet = pd.read_parquet(os.path.join(PARQUET, 'olist_orders.parquet'))

print(f'Orders:  {orders.shape[0]:,} rows')
print(f'Weather: {weather.shape[0]:,} rows')
print(f'Parquet: {orders_parquet.shape[0]:,} rows')
print('All files loaded!')

# ============================================================
# TRANSFORMATION 1 — Clean Orders
# ============================================================
print('\n' + '='*50)
print('TRANSFORMATION 1 — Cleaning Orders')
print('='*50)

# Parse date columns
date_cols = [
    'order_purchase_timestamp',
    'order_delivered_customer_date',
    'order_estimated_delivery_date',
    'order_approved_at',
    'order_delivered_carrier_date'
]
for col in date_cols:
    if col in orders.columns:
        orders[col] = pd.to_datetime(orders[col], errors='coerce')

# Keep only delivered orders
orders_clean = orders[orders['order_status'] == 'delivered'].copy()

# Add derived columns
orders_clean['purchase_date']  = orders_clean['order_purchase_timestamp'].dt.normalize()
orders_clean['delivery_date']  = orders_clean['order_delivered_customer_date'].dt.normalize()
orders_clean['estimated_date'] = orders_clean['order_estimated_delivery_date'].dt.normalize()

orders_clean['delivery_days'] = (
    orders_clean['delivery_date'] - orders_clean['purchase_date']
).dt.days

orders_clean['was_late'] = (
    orders_clean['delivery_date'] > orders_clean['estimated_date']
).astype(int)

orders_clean['year']  = orders_clean['purchase_date'].dt.year
orders_clean['month'] = orders_clean['purchase_date'].dt.month

print(f'Delivered orders: {len(orders_clean):,}')
print(orders_clean.dtypes)

# ============================================================
# TRANSFORMATION 2 — Join Orders + Customers + Payments + Items
# ============================================================
print('\n' + '='*50)
print('TRANSFORMATION 2 — Joining Tables')
print('='*50)

# Join with customers
orders_customers = orders_clean.merge(
    customers[['customer_id', 'customer_state', 'customer_city']],
    on='customer_id', how='left'
)

# Aggregate payments per order
payments_agg = payments.groupby('order_id').agg(
    total_payment=('payment_value', 'sum'),
    payment_count=('payment_sequential', 'count')
).reset_index()

# Join with payments
orders_payments = orders_customers.merge(payments_agg, on='order_id', how='left')

# Aggregate items per order
items_agg = order_items.groupby('order_id').agg(
    item_count=('order_item_id', 'count'),
    items_total=('price', 'sum'),
    freight_total=('freight_value', 'sum')
).reset_index()

# Join with items
orders_full = orders_payments.merge(items_agg, on='order_id', how='left')

print(f'Full joined orders: {len(orders_full):,}')
print('Columns:', list(orders_full.columns))

# ============================================================
# TRANSFORMATION 3 — Join with Weather
# ============================================================
print('\n' + '='*50)
print('TRANSFORMATION 3 — Joining with Weather')
print('='*50)

weather['date']          = pd.to_datetime(weather['date'])
weather['temp_max']      = pd.to_numeric(weather['temp_max'],      errors='coerce')
weather['precipitation'] = pd.to_numeric(weather['precipitation'], errors='coerce')

orders_weather = orders_full.merge(
    weather,
    left_on='purchase_date',
    right_on='date',
    how='left'
).drop(columns='date')

print(f'Orders with weather: {len(orders_weather):,}')
print(orders_weather[['order_id', 'purchase_date', 'total_payment', 'temp_max', 'precipitation']].head(5).to_string())

# ============================================================
# TRANSFORMATION 4 — Aggregated Summary Tables
# ============================================================
print('\n' + '='*50)
print('TRANSFORMATION 4 — Creating Summary Tables')
print('='*50)

# Table 1: Monthly revenue
monthly_revenue = orders_weather.groupby(['year', 'month']).agg(
    total_orders=('order_id', 'count'),
    total_revenue=('total_payment', 'sum'),
    avg_order_value=('total_payment', 'mean'),
    avg_delivery_days=('delivery_days', 'mean'),
    late_deliveries=('was_late', 'sum')
).reset_index()
monthly_revenue = monthly_revenue.round({'total_revenue': 2, 'avg_order_value': 2, 'avg_delivery_days': 1})
monthly_revenue = monthly_revenue.sort_values(['year', 'month'])

print('Monthly Revenue:')
print(monthly_revenue.to_string())

# Table 2: Revenue by state
revenue_by_state = orders_weather.groupby('customer_state').agg(
    total_orders=('order_id', 'count'),
    total_revenue=('total_payment', 'sum'),
    avg_order_value=('total_payment', 'mean'),
    avg_delivery_days=('delivery_days', 'mean')
).reset_index()
revenue_by_state = revenue_by_state.round({'total_revenue': 2, 'avg_order_value': 2, 'avg_delivery_days': 1})
revenue_by_state = revenue_by_state.sort_values('total_revenue', ascending=False)

print('\nRevenue by State:')
print(revenue_by_state.to_string())

# Table 3: Weather impact
orders_weather['rain_category'] = orders_weather['precipitation'].apply(
    lambda x: 'No Rain' if x == 0 else ('Light Rain' if x < 5 else 'Heavy Rain')
)
weather_impact = orders_weather.groupby('rain_category').agg(
    total_orders=('order_id', 'count'),
    avg_order_value=('total_payment', 'mean')
).reset_index()
weather_impact = weather_impact.round({'avg_order_value': 2})

print('\nWeather Impact:')
print(weather_impact.to_string())

# ============================================================
# SAVE ALL TRANSFORMED TABLES AS PARQUET
# ============================================================
print('\n' + '='*50)
print('Saving transformed tables...')
print('='*50)

orders_weather.to_parquet(   os.path.join(TRANSFORMED, 'orders_full.parquet'),     index=False)
monthly_revenue.to_parquet(  os.path.join(TRANSFORMED, 'monthly_revenue.parquet'), index=False)
revenue_by_state.to_parquet( os.path.join(TRANSFORMED, 'revenue_by_state.parquet'),index=False)
weather_impact.to_parquet(   os.path.join(TRANSFORMED, 'weather_impact.parquet'),  index=False)

print('='*50)
print('TRANSFORMATION COMPLETE')
print('='*50)
print('Saved: orders_full.parquet')
print('Saved: monthly_revenue.parquet')
print('Saved: revenue_by_state.parquet')
print('Saved: weather_impact.parquet')
print(f'\nFiles in {TRANSFORMED}:')
for f in os.listdir(TRANSFORMED):
    print(' -', f)
print('\nReady for 03_load.py!')
