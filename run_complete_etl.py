#!/usr/bin/env python3
"""
Complete ETL Pipeline for Olist Data
Extracts, transforms, and loads data to create the dashboard database
"""

import pandas as pd
import requests
import os
import duckdb
from datetime import datetime

def extract_data():
    """Extract data from multiple sources"""
    print("=" * 60)
    print("STEP 1: DATA EXTRACTION")
    print("=" * 60)
    
    # Define paths
    RAW = 'data/raw'
    PARQUET = 'data/parquet'
    TRANSFORMED = 'data/transformed'
    
    # Create directories
    os.makedirs(PARQUET, exist_ok=True)
    os.makedirs(TRANSFORMED, exist_ok=True)
    
    print(f'Raw data path: {RAW}')
    print(f'Parquet output: {PARQUET}')
    print(f'Transformed output: {TRANSFORMED}')
    
    # Load Olist CSV files
    print('\nLoading Olist CSV files...')
    orders = pd.read_csv(f'{RAW}/olist_orders_dataset.csv')
    customers = pd.read_csv(f'{RAW}/olist_customers_dataset.csv')
    products = pd.read_csv(f'{RAW}/olist_products_dataset.csv')
    order_items = pd.read_csv(f'{RAW}/olist_order_items_dataset.csv')
    payments = pd.read_csv(f'{RAW}/olist_order_payments_dataset.csv')
    reviews = pd.read_csv(f'{RAW}/olist_order_reviews_dataset.csv')
    sellers = pd.read_csv(f'{RAW}/olist_sellers_dataset.csv')
    category = pd.read_csv(f'{RAW}/product_category_name_translation.csv')
    
    print(f'Orders:      {orders.shape[0]:,} rows')
    print(f'Customers:   {customers.shape[0]:,} rows')
    print(f'Products:    {products.shape[0]:,} rows')
    print(f'Order Items: {order_items.shape[0]:,} rows')
    print(f'Payments:    {payments.shape[0]:,} rows')
    
    # Fetch weather data
    print('\nFetching weather data from Open-Meteo API...')
    url = 'https://archive-api.open-meteo.com/v1/archive'
    params = {
        'latitude': -23.5505,  # São Paulo
        'longitude': -46.6333,
        'start_date': '2017-01-01',
        'end_date': '2018-08-31',
        'daily': 'temperature_2m_max,precipitation_sum',
        'timezone': 'America/Sao_Paulo'
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            weather = pd.DataFrame({
                'date': data['daily']['time'],
                'temp_max': data['daily']['temperature_2m_max'],
                'precipitation': data['daily']['precipitation_sum']
            })
            weather['date'] = pd.to_datetime(weather['date'])
            weather.to_csv(f'{PARQUET}/sao_paulo_weather.csv', index=False)
            print(f'Weather data saved: {len(weather)} rows')
        else:
            print(f'API request failed: {response.status_code}')
            return None
    except Exception as e:
        print(f'Error fetching weather: {e}')
        return None
    
    # Convert date columns and save as parquet
    print('\nConverting date columns...')
    date_columns = [
        'order_purchase_timestamp',
        'order_approved_at', 
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    
    for col in date_columns:
        if col in orders.columns:
            orders[col] = pd.to_datetime(orders[col], errors='coerce')
    
    orders.to_parquet(f'{PARQUET}/olist_orders.parquet', index=False)
    print(f'Parquet file created: {orders.shape}')
    
    return {
        'orders': orders, 'customers': customers, 'products': products,
        'order_items': order_items, 'payments': payments, 'reviews': reviews,
        'sellers': sellers, 'category': category, 'weather': weather
    }

def transform_data(data):
    """Transform data using pandas (simplified alternative to PySpark)"""
    print("\n" + "=" * 60)
    print("STEP 2: DATA TRANSFORMATION")
    print("=" * 60)
    
    orders = data['orders']
    customers = data['customers']
    order_items = data['order_items']
    payments = data['payments']
    weather = data['weather']
    
    # Clean orders - keep only delivered orders
    orders_clean = orders[orders['order_status'] == 'delivered'].copy()
    
    # Add date columns
    orders_clean['purchase_date'] = pd.to_datetime(orders_clean['order_purchase_timestamp'])
    orders_clean['delivery_date'] = pd.to_datetime(orders_clean['order_delivered_customer_date'])
    orders_clean['estimated_date'] = pd.to_datetime(orders_clean['order_estimated_delivery_date'])
    
    # Calculate delivery metrics
    orders_clean['delivery_days'] = (orders_clean['delivery_date'] - orders_clean['purchase_date']).dt.days
    orders_clean['was_late'] = (orders_clean['delivery_date'] > orders_clean['estimated_date']).astype(int)
    orders_clean['year'] = orders_clean['purchase_date'].dt.year
    orders_clean['month'] = orders_clean['purchase_date'].dt.month
    
    print(f'Cleaned orders: {len(orders_clean):,} rows')
    
    # Join with customers
    orders_customers = orders_clean.merge(
        customers[['customer_id', 'customer_state', 'customer_city']], 
        on='customer_id', how='left'
    )
    
    # Aggregate payments per order
    payments_agg = payments.groupby('order_id').agg({
        'payment_value': 'sum',
        'payment_sequential': 'count'
    }).rename(columns={
        'payment_value': 'total_payment',
        'payment_sequential': 'payment_count'
    }).reset_index()
    
    # Join with payments
    orders_payments = orders_customers.merge(payments_agg, on='order_id', how='left')
    
    # Aggregate items per order
    items_agg = order_items.groupby('order_id').agg({
        'order_item_id': 'count',
        'price': 'sum',
        'freight_value': 'sum'
    }).rename(columns={
        'order_item_id': 'item_count',
        'price': 'items_total',
        'freight_value': 'freight_total'
    }).reset_index()
    
    # Join with items
    orders_full = orders_payments.merge(items_agg, on='order_id', how='left')
    
    # Clean weather data
    weather_clean = weather.copy()
    weather_clean['date'] = pd.to_datetime(weather_clean['date'])
    
    # Join with weather
    orders_weather = orders_full.merge(
        weather_clean, 
        left_on='purchase_date', 
        right_on='date', 
        how='left'
    ).drop('date', axis=1)
    
    print(f'Final joined dataset: {len(orders_weather):,} rows')
    
    # Create aggregated tables
    # Monthly revenue
    monthly_revenue = orders_weather.groupby(['year', 'month']).agg({
        'order_id': 'count',
        'total_payment': 'sum',
        'delivery_days': 'mean',
        'was_late': 'sum'
    }).rename(columns={
        'order_id': 'total_orders',
        'total_payment': 'total_revenue',
        'delivery_days': 'avg_delivery_days',
        'was_late': 'late_deliveries'
    }).reset_index()
    
    monthly_revenue['avg_order_value'] = monthly_revenue['total_revenue'] / monthly_revenue['total_orders']
    monthly_revenue = monthly_revenue.round({'total_revenue': 2, 'avg_order_value': 2, 'avg_delivery_days': 1})
    
    # Revenue by state
    revenue_by_state = orders_weather.groupby('customer_state').agg({
        'order_id': 'count',
        'total_payment': 'sum',
        'delivery_days': 'mean'
    }).rename(columns={
        'order_id': 'total_orders',
        'total_payment': 'total_revenue',
        'delivery_days': 'avg_delivery_days'
    }).reset_index()
    
    revenue_by_state['avg_order_value'] = revenue_by_state['total_revenue'] / revenue_by_state['total_orders']
    revenue_by_state = revenue_by_state.round({'total_revenue': 2, 'avg_order_value': 2, 'avg_delivery_days': 1})
    revenue_by_state = revenue_by_state.sort_values('total_revenue', ascending=False)
    
    # Weather impact
    orders_weather['rain_category'] = orders_weather['precipitation'].apply(
        lambda x: 'No Rain' if x == 0 else ('Light Rain' if x < 5 else 'Heavy Rain')
    )
    
    weather_impact = orders_weather.groupby('rain_category').agg({
        'order_id': 'count',
        'total_payment': 'mean'
    }).rename(columns={
        'order_id': 'total_orders',
        'total_payment': 'avg_order_value'
    }).reset_index()
    weather_impact = weather_impact.round({'avg_order_value': 2})
    
    print('Transformation completed!')
    
    return {
        'orders_full': orders_weather,
        'monthly_revenue': monthly_revenue,
        'revenue_by_state': revenue_by_state,
        'weather_impact': weather_impact
    }

def load_data(transformed_data):
    """Load transformed data into DuckDB"""
    print("\n" + "=" * 60)
    print("STEP 3: DATA LOADING")
    print("=" * 60)
    
    DB_PATH = 'analytics.duckdb'
    
    # Remove existing database if it exists
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    # Connect to DuckDB
    conn = duckdb.connect(DB_PATH)
    print(f'Connected to DuckDB: {DB_PATH}')
    
    # Create tables
    tables = transformed_data
    
    for table_name, df in tables.items():
        conn.register(table_name, df)
        conn.execute(f'CREATE TABLE {table_name} AS SELECT * FROM {table_name}')
        print(f'Created table {table_name}: {len(df):,} rows')
    
    # Verify data
    print('\nVerifying loaded data:')
    for table_name in tables.keys():
        count = conn.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()[0]
        print(f'  {table_name}: {count:,} rows')
    
    # Get KPIs
    kpis = conn.execute('''
        SELECT
            COUNT(order_id) AS total_orders,
            ROUND(SUM(total_payment), 2) AS total_revenue,
            ROUND(AVG(total_payment), 2) AS avg_order_value,
            ROUND(AVG(delivery_days), 1) AS avg_delivery_days,
            SUM(was_late) AS total_late_deliveries,
            ROUND((SUM(was_late) * 100.0 / COUNT(order_id)), 2) AS late_delivery_rate
        FROM orders_full
    ''').fetchdf()
    
    print('\nBusiness KPIs:')
    print(kpis.to_string(index=False))
    
    conn.close()
    print(f'\nDatabase saved: {DB_PATH}')

def main():
    """Run complete ETL pipeline"""
    print("🚀 Starting Olist ETL Pipeline...")
    print(f"Timestamp: {datetime.now()}")
    
    try:
        # Step 1: Extract
        data = extract_data()
        if data is None:
            print("❌ Extraction failed!")
            return
        
        # Step 2: Transform
        transformed_data = transform_data(data)
        
        # Step 3: Load
        load_data(transformed_data)
        
        print("\n" + "=" * 60)
        print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("🎯 Ready to start the dashboard!")
        print("📊 Run: cd dashboard && python app.py")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
