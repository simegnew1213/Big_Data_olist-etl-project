# ============================================================
# STEP 3 — LOAD INTO DUCKDB + DASHBOARD
# Olist ETL Pipeline — Local Version
# ============================================================

import duckdb
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Paths ────────────────────────────────────────────────────
BASE        = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
TRANSFORMED = os.path.join(BASE, 'data', 'transformed')
DB_PATH     = os.path.join(BASE, 'analytics.duckdb')
DASHBOARD_DIR = os.path.join(BASE, 'dashboard')

os.makedirs(DASHBOARD_DIR, exist_ok=True)

# ── Connect to DuckDB ────────────────────────────────────────
# Remove existing DB so we start fresh
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = duckdb.connect(DB_PATH)
print('Connected to DuckDB!')
print('Database:', DB_PATH)

# ============================================================
# LOAD PARQUET FILES INTO DUCKDB
# ============================================================
print('\n' + '='*50)
print('Loading tables into DuckDB...')
print('='*50)

# Table 1: orders_full
conn.execute(f"""
    CREATE OR REPLACE TABLE orders_full AS
    SELECT * FROM read_parquet('{TRANSFORMED}/orders_full.parquet')
""")
count = conn.execute('SELECT COUNT(*) FROM orders_full').fetchone()[0]
print(f'orders_full loaded:      {count:,} rows')

# Table 2: monthly_revenue
conn.execute(f"""
    CREATE OR REPLACE TABLE monthly_revenue AS
    SELECT * FROM read_parquet('{TRANSFORMED}/monthly_revenue.parquet')
    ORDER BY year, month
""")
count = conn.execute('SELECT COUNT(*) FROM monthly_revenue').fetchone()[0]
print(f'monthly_revenue loaded:  {count:,} rows')

# Table 3: revenue_by_state
conn.execute(f"""
    CREATE OR REPLACE TABLE revenue_by_state AS
    SELECT * FROM read_parquet('{TRANSFORMED}/revenue_by_state.parquet')
    ORDER BY total_revenue DESC
""")
count = conn.execute('SELECT COUNT(*) FROM revenue_by_state').fetchone()[0]
print(f'revenue_by_state loaded: {count:,} rows')

# Table 4: weather_impact
conn.execute(f"""
    CREATE OR REPLACE TABLE weather_impact AS
    SELECT * FROM read_parquet('{TRANSFORMED}/weather_impact.parquet')
""")
count = conn.execute('SELECT COUNT(*) FROM weather_impact').fetchone()[0]
print(f'weather_impact loaded:   {count:,} rows')

# ============================================================
# VERIFY TABLES
# ============================================================
print('\n' + '='*50)
print('Tables in DuckDB:')
print('='*50)
tables = conn.execute('SHOW TABLES').fetchdf()
print(tables.to_string())

# ============================================================
# BUSINESS INSIGHT QUERIES
# ============================================================
print('\n' + '='*50)
print('BUSINESS INSIGHT QUERIES')
print('='*50)

# KPIs
kpis = conn.execute('''
    SELECT
        COUNT(order_id)               AS total_orders,
        ROUND(SUM(total_payment), 2)  AS total_revenue,
        ROUND(AVG(total_payment), 2)  AS avg_order_value,
        ROUND(AVG(delivery_days), 1)  AS avg_delivery_days,
        SUM(was_late)                 AS total_late_deliveries,
        ROUND((SUM(was_late) * 100.0 / COUNT(order_id)), 2) AS late_delivery_rate
    FROM orders_full
''').fetchdf()
print('\nOverall KPIs:')
print(kpis.to_string())

# Top 5 best months
best = conn.execute('''
    SELECT year, month, total_orders, total_revenue
    FROM monthly_revenue
    ORDER BY total_revenue DESC
    LIMIT 5
''').fetchdf()
print('\nTop 5 Best Months:')
print(best.to_string())

# Top 5 states
top_states = conn.execute('''
    SELECT customer_state, total_orders, total_revenue, avg_delivery_days
    FROM revenue_by_state
    LIMIT 5
''').fetchdf()
print('\nTop 5 States by Revenue:')
print(top_states.to_string())

# Weather impact
weather_df = conn.execute('SELECT * FROM weather_impact').fetchdf()
print('\nWeather Impact on Orders:')
print(weather_df.to_string())

# Slowest delivery states
slow = conn.execute('''
    SELECT customer_state, avg_delivery_days, total_orders
    FROM revenue_by_state
    ORDER BY avg_delivery_days DESC
    LIMIT 5
''').fetchdf()
print('\nSlower Delivery States:')
print(slow.to_string())

# ============================================================
# DASHBOARD — Plotly Charts
# ============================================================
print('\n' + '='*50)
print('Building Dashboard...')
print('='*50)

monthly  = conn.execute('SELECT * FROM monthly_revenue ORDER BY year, month').fetchdf()
by_state = conn.execute('SELECT * FROM revenue_by_state').fetchdf()
weather  = conn.execute('SELECT * FROM weather_impact').fetchdf()

monthly['period'] = monthly['year'].astype(str) + '-' + monthly['month'].astype(str).str.zfill(2)

# Build 4-chart dashboard
fig = make_subplots(
    rows=2, cols=2,
    subplot_titles=[
        'Monthly Revenue (R$)',
        'Monthly Orders',
        'Revenue by State (Top 10)',
        'Weather Impact on Orders'
    ]
)

# Chart 1 — Monthly Revenue line chart
fig.add_trace(go.Scatter(
    x=monthly['period'],
    y=monthly['total_revenue'],
    mode='lines+markers',
    name='Revenue',
    line=dict(color='#27ae60', width=2)
), row=1, col=1)

# Chart 2 — Monthly Orders bar chart
fig.add_trace(go.Bar(
    x=monthly['period'],
    y=monthly['total_orders'],
    name='Orders',
    marker_color='#2980b9'
), row=1, col=2)

# Chart 3 — Revenue by State bar chart
top10 = by_state.head(10)
fig.add_trace(go.Bar(
    x=top10['customer_state'],
    y=top10['total_revenue'],
    name='State Revenue',
    marker_color='#8e44ad'
), row=2, col=1)

# Chart 4 — Weather Impact bar chart
fig.add_trace(go.Bar(
    x=weather['rain_category'],
    y=weather['total_orders'],
    name='Weather',
    marker_color='#3498db'
), row=2, col=2)

fig.update_layout(
    height=700,
    title_text='Olist E-Commerce Analytics Dashboard',
    title_font_size=20,
    showlegend=False,
    template='plotly_white'
)

fig.show()

# Save dashboard as HTML
dashboard_html = os.path.join(DASHBOARD_DIR, 'dashboard.html')
fig.write_html(dashboard_html)
print('Dashboard saved to:', dashboard_html)

# ============================================================
# DONE
# ============================================================
conn.close()

print('\n' + '='*50)
print('PIPELINE COMPLETE!')
print('='*50)
print('Extract   done')
print('Transform done')
print('Load      done')
print('Dashboard done')
print()
print('DuckDB saved at:    ', DB_PATH)
print('Dashboard saved at: ', dashboard_html)
