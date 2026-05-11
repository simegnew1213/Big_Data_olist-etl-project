# ============================================================
# STEP 1 — EXTRACT
# Olist ETL Pipeline — Local Version
# Sources:
#   1. Olist CSV files  (data/raw)
#   2. Sao Paulo Weather (Open-Meteo REST API)
#   3. Olist Orders converted to Parquet
# ============================================================

import pandas as pd
import requests
import os

# ── Paths (relative to project root or notebooks folder) ────
BASE        = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
RAW         = os.path.join(BASE, 'data', 'raw')
PARQUET     = os.path.join(BASE, 'data', 'parquet')
TRANSFORMED = os.path.join(BASE, 'data', 'transformed')

os.makedirs(PARQUET,     exist_ok=True)
os.makedirs(TRANSFORMED, exist_ok=True)
print('Folders ready!')
print('RAW:        ', RAW)
print('PARQUET:    ', PARQUET)
print('TRANSFORMED:', TRANSFORMED)

# ── Check available files ────────────────────────────────────
print('\nAvailable files in dataset:')
for f in sorted(os.listdir(RAW)):
    print(' -', f)

# ============================================================
# SOURCE 1 — Load Olist CSV Files
# ============================================================
print('\n' + '='*50)
print('SOURCE 1 — Loading Olist CSV Files')
print('='*50)

orders      = pd.read_csv(os.path.join(RAW, 'olist_orders_dataset.csv'))
customers   = pd.read_csv(os.path.join(RAW, 'olist_customers_dataset.csv'))
products    = pd.read_csv(os.path.join(RAW, 'olist_products_dataset.csv'))
order_items = pd.read_csv(os.path.join(RAW, 'olist_order_items_dataset.csv'))
payments    = pd.read_csv(os.path.join(RAW, 'olist_order_payments_dataset.csv'))
reviews     = pd.read_csv(os.path.join(RAW, 'olist_order_reviews_dataset.csv'))
sellers     = pd.read_csv(os.path.join(RAW, 'olist_sellers_dataset.csv'))
category    = pd.read_csv(os.path.join(RAW, 'product_category_name_translation.csv'))

print(f'orders:      {orders.shape[0]:,} rows')
print(f'customers:   {customers.shape[0]:,} rows')
print(f'products:    {products.shape[0]:,} rows')
print(f'order_items: {order_items.shape[0]:,} rows')
print(f'payments:    {payments.shape[0]:,} rows')
print(f'reviews:     {reviews.shape[0]:,} rows')
print(f'sellers:     {sellers.shape[0]:,} rows')
print('All Olist CSV files loaded!')

print('\nOrders sample:')
print(orders.head(3))

# ============================================================
# SOURCE 2 — Weather API (Open-Meteo)
# ============================================================
print('\n' + '='*50)
print('SOURCE 2 — Fetching Weather API')
print('='*50)

url = 'https://archive-api.open-meteo.com/v1/archive'
params = {
    'latitude':   -23.5505,
    'longitude':  -46.6333,
    'start_date': '2017-01-01',
    'end_date':   '2018-08-31',
    'daily':      'temperature_2m_max,precipitation_sum',
    'timezone':   'America/Sao_Paulo'
}

response = requests.get(url, params=params)
print('API Status:', response.status_code)

if response.status_code != 200:
    raise RuntimeError(f'Weather API failed with status {response.status_code}')

data    = response.json()
weather = pd.DataFrame({
    'date':          data['daily']['time'],
    'temp_max':      data['daily']['temperature_2m_max'],
    'precipitation': data['daily']['precipitation_sum']
})
weather['date'] = pd.to_datetime(weather['date'])

weather.to_csv(os.path.join(PARQUET, 'sao_paulo_weather.csv'), index=False)
print(f'Weather saved: {len(weather)} rows')
print(weather.head(3).to_string())

# ============================================================
# SOURCE 3 — Convert Orders CSV to Parquet
# ============================================================
print('\n' + '='*50)
print('SOURCE 3 — Converting Orders to Parquet')
print('='*50)

orders['order_purchase_timestamp']      = pd.to_datetime(orders['order_purchase_timestamp'])
orders['order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'], errors='coerce')
orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'], errors='coerce')

orders.to_parquet(os.path.join(PARQUET, 'olist_orders.parquet'), index=False)

verify = pd.read_parquet(os.path.join(PARQUET, 'olist_orders.parquet'))
print(f'Parquet saved and verified: {verify.shape}')
print(verify.head(3).to_string())

# ============================================================
# SUMMARY
# ============================================================
print('\n' + '='*50)
print('EXTRACTION COMPLETE')
print('='*50)
print(f'Source 1 - Olist CSVs   : {orders.shape[0]:,} orders loaded')
print(f'Source 2 - Weather API  : {len(weather):,} days of weather data')
print(f'Source 3 - Parquet file : olist_orders.parquet created')
print('\nFiles saved to:', PARQUET)
for f in os.listdir(PARQUET):
    print(' -', f)
print('\nReady for 02_transform.py!')
