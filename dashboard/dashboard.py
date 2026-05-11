import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import pandas as pd

# ── Connect to DuckDB ─────────────────────────────────────────────────────────
conn = duckdb.connect('../analytics.duckdb')

monthly    = conn.execute('SELECT * FROM monthly_revenue ORDER BY year, month').fetchdf()
by_state   = conn.execute('SELECT * FROM revenue_by_state ORDER BY total_revenue DESC').fetchdf()
weather    = conn.execute('SELECT * FROM weather_impact').fetchdf()
kpis       = conn.execute('''
    SELECT
        COUNT(order_id)              AS total_orders,
        ROUND(SUM(total_payment),2)  AS total_revenue,
        ROUND(AVG(total_payment),2)  AS avg_order_value,
        ROUND(AVG(delivery_days),1)  AS avg_delivery_days,
        SUM(was_late)                AS late_deliveries
    FROM orders_full
''').fetchdf()
conn.close()

# ── Prepare data ──────────────────────────────────────────────────────────────
monthly['period'] = monthly['year'].astype(str) + '-' + monthly['month'].astype(str).str.zfill(2)

# ── App layout ────────────────────────────────────────────────────────────────
app = dash.Dash(__name__)
app.title = 'Olist E-Commerce Dashboard'

CARD = {
    'background': '#f9f9f9',
    'border': '1px solid #ddd',
    'borderRadius': '10px',
    'padding': '20px',
    'textAlign': 'center',
    'flex': '1'
}

app.layout = html.Div([

    html.H1('Olist E-Commerce Analytics Dashboard',
            style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '10px'}),
    html.P('Brazilian E-Commerce | 2017 – 2018',
           style={'textAlign': 'center', 'color': '#7f8c8d', 'marginBottom': '30px'}),

    # ── KPI Cards ──────────────────────────────────────────────────────────────
    html.Div([
        html.Div([html.H4('Total Orders'),
                  html.H2(f"{int(kpis['total_orders'][0]):,}", style={'color':'#2980b9'})], style=CARD),
        html.Div([html.H4('Total Revenue'),
                  html.H2(f"R$ {kpis['total_revenue'][0]:,.0f}", style={'color':'#27ae60'})], style=CARD),
        html.Div([html.H4('Avg Order Value'),
                  html.H2(f"R$ {kpis['avg_order_value'][0]:,.2f}", style={'color':'#8e44ad'})], style=CARD),
        html.Div([html.H4('Avg Delivery Days'),
                  html.H2(f"{kpis['avg_delivery_days'][0]} days", style={'color':'#e67e22'})], style=CARD),
        html.Div([html.H4('Late Deliveries'),
                  html.H2(f"{int(kpis['late_deliveries'][0]):,}", style={'color':'#e74c3c'})], style=CARD),
    ], style={'display': 'flex', 'gap': '15px', 'margin': '0 20px 30px 20px'}),

    # ── Charts Row 1 ───────────────────────────────────────────────────────────
    html.Div([
        html.Div([
            html.H3('Monthly Revenue', style={'textAlign':'center'}),
            dcc.Graph(id='monthly-revenue')
        ], style={'flex':'1', 'background':'#fff', 'borderRadius':'10px',
                  'padding':'15px', 'border':'1px solid #ddd'}),

        html.Div([
            html.H3('Monthly Orders', style={'textAlign':'center'}),
            dcc.Graph(id='monthly-orders')
        ], style={'flex':'1', 'background':'#fff', 'borderRadius':'10px',
                  'padding':'15px', 'border':'1px solid #ddd'}),
    ], style={'display':'flex', 'gap':'15px', 'margin':'0 20px 20px 20px'}),

    # ── Charts Row 2 ───────────────────────────────────────────────────────────
    html.Div([
        html.Div([
            html.H3('Revenue by State (Top 10)', style={'textAlign':'center'}),
            dcc.Graph(id='state-revenue')
        ], style={'flex':'2', 'background':'#fff', 'borderRadius':'10px',
                  'padding':'15px', 'border':'1px solid #ddd'}),

        html.Div([
            html.H3('Weather Impact on Orders', style={'textAlign':'center'}),
            dcc.Graph(id='weather-chart')
        ], style={'flex':'1', 'background':'#fff', 'borderRadius':'10px',
                  'padding':'15px', 'border':'1px solid #ddd'}),
    ], style={'display':'flex', 'gap':'15px', 'margin':'0 20px 20px 20px'}),

    # ── Chart Row 3 ────────────────────────────────────────────────────────────
    html.Div([
        html.Div([
            html.H3('Avg Delivery Days by Month', style={'textAlign':'center'}),
            dcc.Graph(id='delivery-chart')
        ], style={'flex':'1', 'background':'#fff', 'borderRadius':'10px',
                  'padding':'15px', 'border':'1px solid #ddd'}),
    ], style={'margin':'0 20px 20px 20px'}),

], style={'fontFamily':'Arial, sans-serif', 'backgroundColor':'#f0f2f5', 'paddingBottom':'30px'})


# ── Callbacks ─────────────────────────────────────────────────────────────────
@app.callback(
    Output('monthly-revenue',  'figure'),
    Output('monthly-orders',   'figure'),
    Output('state-revenue',    'figure'),
    Output('weather-chart',    'figure'),
    Output('delivery-chart',   'figure'),
    Input('monthly-revenue',   'id')   # dummy trigger on load
)
def update_all(_):
    fig1 = px.line(monthly, x='period', y='total_revenue',
                   markers=True, template='plotly_white',
                   color_discrete_sequence=['#27ae60'])
    fig1.update_layout(xaxis_title='Month', yaxis_title='Revenue (R$)', margin=dict(t=10))

    fig2 = px.bar(monthly, x='period', y='total_orders',
                  template='plotly_white', color_discrete_sequence=['#2980b9'])
    fig2.update_layout(xaxis_title='Month', yaxis_title='Orders', margin=dict(t=10))

    top10 = by_state.head(10)
    fig3 = px.bar(top10, x='customer_state', y='total_revenue',
                  template='plotly_white', color_discrete_sequence=['#8e44ad'])
    fig3.update_layout(xaxis_title='State', yaxis_title='Revenue (R$)', margin=dict(t=10))

    fig4 = px.bar(weather, x='rain_category', y='total_orders',
                  template='plotly_white', color_discrete_sequence=['#3498db'])
    fig4.update_layout(xaxis_title='Weather', yaxis_title='Orders', margin=dict(t=10))

    fig5 = px.line(monthly, x='period', y='avg_delivery_days',
                   markers=True, template='plotly_white',
                   color_discrete_sequence=['#e67e22'])
    fig5.update_layout(xaxis_title='Month', yaxis_title='Days', margin=dict(t=10))

    return fig1, fig2, fig3, fig4, fig5


if __name__ == '__main__':
    print('Dashboard running at http://127.0.0.1:8050')
    app.run(debug=True)
