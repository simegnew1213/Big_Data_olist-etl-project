"""
Olist ETL Interactive Dashboard - Modern Design
Enhanced with modern UI, interactivity, and advanced visualizations
"""

import dash
from dash import dcc, html, Input, Output, callback, dash_table
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.figure_factory as ff
import duckdb
import pandas as pd
import os
from datetime import datetime
import dash_bootstrap_components as dbc

# Initialize Dash app with modern theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.themes.FLATLY])
app.title = "Olist E-Commerce Analytics Dashboard"

# Database connection
DB_PATH = '../analytics.duckdb'

def get_data_from_duckdb():
    """Load data from DuckDB database"""
    conn = duckdb.connect(DB_PATH)
    
    # Load all tables
    monthly_revenue = conn.execute('SELECT * FROM monthly_revenue ORDER BY year, month').fetchdf()
    revenue_by_state = conn.execute('SELECT * FROM revenue_by_state ORDER BY total_revenue DESC').fetchdf()
    weather_impact = conn.execute('SELECT * FROM weather_impact').fetchdf()
    
    # Get detailed orders data for advanced analysis
    orders_full = conn.execute('''
        SELECT * FROM orders_full 
        WHERE total_payment IS NOT NULL 
        ORDER BY purchase_date DESC 
        LIMIT 10000
    ''').fetchdf()
    
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
    ''').fetchdf().iloc[0]
    
    conn.close()
    
    return monthly_revenue, revenue_by_state, weather_impact, orders_full, kpis

# Load data
try:
    monthly_data, state_data, weather_data, orders_data, kpi_data = get_data_from_duckdb()
    
    # Create period column for monthly data
    monthly_data['period'] = monthly_data['year'].astype(str) + '-' + monthly_data['month'].astype(str).str.zfill(2)
    monthly_data['period_dt'] = pd.to_datetime(monthly_data['period'])
    
    # Format currency columns
    for df in [monthly_data, state_data]:
        if 'total_revenue' in df.columns:
            df['total_revenue_formatted'] = df['total_revenue'].apply(lambda x: f'R$ {x:,.2f}' if pd.notna(x) else 'R$ 0.00')
        if 'avg_order_value' in df.columns:
            df['avg_order_value_formatted'] = df['avg_order_value'].apply(lambda x: f'R$ {x:.2f}' if pd.notna(x) else 'R$ 0.00')
    
    # Create state performance categories
    state_data['performance_category'] = pd.cut(
        state_data['total_revenue'], 
        bins=[0, 500000, 1500000, float('inf')], 
        labels=['🥉 Bronze', '🥈 Silver', '🥇 Gold']
    )
    
    data_loaded = True
except Exception as e:
    print(f"Error loading data: {e}")
    data_loaded = False
    # Create empty dataframes as fallback
    monthly_data = pd.DataFrame()
    state_data = pd.DataFrame()
    weather_data = pd.DataFrame()
    orders_data = pd.DataFrame()
    kpi_data = pd.Series()

# Define modern color scheme
COLORS = {
    'primary': '#1f77b4',
    'secondary': '#ff7f0e', 
    'success': '#2ca02c',
    'danger': '#d62728',
    'warning': '#ff9800',
    'info': '#17a2b8',
    'light': '#f8f9fa',
    'dark': '#343a40',
    'gradient': ['#667eea', '#764ba2', '#f093fb', '#f5576c']
}

# Create KPI Cards with modern design
def create_kpi_card(title, value, subtitle, color="primary", icon="📊"):
    return dbc.Card([
        dbc.CardBody([
            html.Div([
                html.H2(icon, className="text-primary mb-2", style={'fontSize': '2rem'}),
                html.H4(value, className="card-title", style={'color': COLORS[color], 'fontWeight': 'bold', 'margin': '0', 'fontSize': '1.5rem'}),
                html.P(title, className="card-text", style={'fontSize': '0.9rem', 'margin': '0', 'color': '#2c3e50', 'fontWeight': '500'}),
                html.Small(subtitle, className="text-muted", style={'fontSize': '0.8rem'})
            ], style={'textAlign': 'center'})
        ])
    ], style={
        'boxShadow': '0 4px 6px rgba(0,0,0,0.1)',
        'border': '1px solid #e3e6f0',
        'borderRadius': '15px',
        'transition': 'transform 0.2s',
        'cursor': 'pointer',
        'backgroundColor': '#ffffff'
    }, className="mb-4")

# Define layout with modern design
app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.Div([
                html.H1("🛒 Olist E-Commerce Analytics", 
                       className="text-center mb-3", 
                       style={'color': 'white', 'fontWeight': 'bold', 'textShadow': '2px 2px 4px rgba(0,0,0,0.3)'}),
                html.H3("Real-time Business Intelligence Dashboard", 
                       className="text-center mb-4", 
                       style={'color': '#f8f9fa', 'fontWeight': '300'}),
                html.Div([
                    html.Span(f"📅 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                            className="badge bg-info text-white")
                ], className="text-center")
            ], style={
                'background': f'linear-gradient(135deg, {COLORS["gradient"][0]}, {COLORS["gradient"][1]})',
                'padding': '2rem',
                'borderRadius': '20px',
                'marginBottom': '2rem',
                'boxShadow': '0 8px 32px rgba(0,0,0,0.1)'
            })
        ])
    ]),
    
    # KPI Cards
    dbc.Row([
        dbc.Col([
            create_kpi_card(
                "Total Orders", 
                f"{kpi_data.get('total_orders', 0):,}",
                "Processed Orders",
                "info", "📦"
            )
        ], width=3),
        dbc.Col([
            create_kpi_card(
                "Total Revenue", 
                f"R$ {kpi_data.get('total_revenue', 0):,.2f}",
                "Gross Revenue",
                "success", "💰"
            )
        ], width=3),
        dbc.Col([
            create_kpi_card(
                "Avg Order Value", 
                f"R$ {kpi_data.get('avg_order_value', 0):.2f}",
                "Per Transaction",
                "warning", "🎯"
            )
        ], width=3),
        dbc.Col([
            create_kpi_card(
                "Late Delivery Rate", 
                f"{kpi_data.get('late_delivery_rate', 0)}%",
                "Delivery Performance",
                "danger", "🚚"
            )
        ], width=3)
    ]) if data_loaded else dbc.Row([
        dbc.Col([
            dbc.Alert("No data available. Please run the ETL pipeline first.", color="warning")
        ])
    ]),
    
    # Interactive Filters
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("🔍 Interactive Filters", className="card-title mb-3"),
                    dbc.Row([
                        dbc.Col([
                            html.Label("Select State:", className="form-label"),
                            dcc.Dropdown(
                                id='state-filter',
                                options=[{'label': 'All States', 'value': 'all'}] + 
                                       [{'label': state, 'value': state} for state in state_data['customer_state'].unique()] if data_loaded else [],
                                value='all',
                                className="mb-3"
                            )
                        ], width=4),
                        dbc.Col([
                            html.Label("Date Range:", className="form-label"),
                            dcc.DatePickerRange(
                                id='date-range-picker',
                                start_date=monthly_data['period_dt'].min() if data_loaded else '2017-01-01',
                                end_date=monthly_data['period_dt'].max() if data_loaded else '2018-12-31',
                                className="mb-3"
                            )
                        ], width=4),
                        dbc.Col([
                            html.Label("Weather Condition:", className="form-label"),
                            dcc.Dropdown(
                                id='weather-filter',
                                options=[{'label': 'All Conditions', 'value': 'all'}] + 
                                       [{'label': condition, 'value': condition} for condition in weather_data['rain_category'].unique()] if data_loaded else [],
                                value='all',
                                className="mb-3"
                            )
                        ], width=4)
                    ])
                ])
            ], style={'borderRadius': '15px', 'boxShadow': '0 4px 6px rgba(0,0,0,0.1)'})
        ], width=12)
    ], className="mb-4") if data_loaded else None,
    
    # Main Charts Row 1
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("📈 Monthly Revenue Trend", className="card-title mb-3"),
                    dcc.Graph(id='monthly-revenue-chart', config={'displayModeBar': False})
                ])
            ], style={'borderRadius': '15px', 'boxShadow': '0 4px 6px rgba(0,0,0,0.1)'})
        ], width=6),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("📊 Order Volume Analysis", className="card-title mb-3"),
                    dcc.Graph(id='monthly-orders-chart', config={'displayModeBar': False})
                ])
            ], style={'borderRadius': '15px', 'boxShadow': '0 4px 6px rgba(0,0,0,0.1)'})
        ], width=6)
    ], className="mb-4"),
    
    # Main Charts Row 2
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("🗺️ State Performance Map", className="card-title mb-3"),
                    dcc.Graph(id='state-revenue-chart', config={'displayModeBar': False})
                ])
            ], style={'borderRadius': '15px', 'boxShadow': '0 4px 6px rgba(0,0,0,0.1)'})
        ], width=6),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("🌧️ Weather Impact Analysis", className="card-title mb-3"),
                    dcc.Graph(id='weather-impact-chart', config={'displayModeBar': False})
                ])
            ], style={'borderRadius': '15px', 'boxShadow': '0 4px 6px rgba(0,0,0,0.1)'})
        ], width=6)
    ], className="mb-4"),
    
    # Advanced Analytics Section
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("📈 Performance Metrics", className="card-title mb-3"),
                    html.Div(id='performance-metrics')
                ])
            ], style={'borderRadius': '15px', 'boxShadow': '0 4px 6px rgba(0,0,0,0.1)'})
        ], width=12)
    ], className="mb-4"),
    
    # Data Table Section
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("📋 Top Performing States", className="card-title mb-3"),
                    html.Div(id='state-table-container')
                ])
            ], style={'borderRadius': '15px', 'boxShadow': '0 4px 6px rgba(0,0,0,0.1)'})
        ], width=12)
    ]),
    
    # Footer
    dbc.Row([
        dbc.Col([
            html.Div([
                html.P([
                    html.Span("🚀 Powered by PySpark & DuckDB | "),
                    html.Span("© 2024 Olist Analytics Team")
                ], className="text-center text-muted", style={'fontSize': '0.9rem'})
            ], style={'padding': '2rem 0'})
        ])
    ])
], fluid=True, style={'backgroundColor': '#f5f6fa', 'padding': '2rem'})

# Callbacks for dynamic charts
@app.callback(
    [Output('monthly-revenue-chart', 'figure'),
     Output('monthly-orders-chart', 'figure'),
     Output('state-revenue-chart', 'figure'),
     Output('weather-impact-chart', 'figure'),
     Output('performance-metrics', 'children'),
     Output('state-table-container', 'children')],
    [Input('state-filter', 'value'),
     Input('date-range-picker', 'start_date'),
     Input('date-range-picker', 'end_date'),
     Input('weather-filter', 'value')]
)
def update_charts(selected_state, start_date, end_date, weather_filter):
    if not data_loaded:
        empty_fig = go.Figure()
        empty_fig.add_annotation(text="No data available", 
                               xref="paper", yref="paper",
                               x=0.5, y=0.5, showarrow=False)
        return empty_fig, empty_fig, empty_fig, empty_fig, html.P("No data available"), html.P("No data available")
    
    # Filter data based on selections
    filtered_monthly = monthly_data.copy()
    filtered_state = state_data.copy()
    filtered_weather = weather_data.copy()
    
    # Apply date filter
    if start_date and end_date:
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        filtered_monthly = filtered_monthly[
            (filtered_monthly['period_dt'] >= start_dt) & 
            (filtered_monthly['period_dt'] <= end_dt)
        ]
    
    # Apply state filter
    if selected_state != 'all':
        filtered_state = filtered_state[filtered_state['customer_state'] == selected_state]
    
    # Apply weather filter
    if weather_filter != 'all':
        filtered_weather = filtered_weather[filtered_weather['rain_category'] == weather_filter]
    
    # Monthly Revenue Chart with gradient
    fig_revenue = go.Figure()
    fig_revenue.add_trace(go.Scatter(
        x=filtered_monthly['period'],
        y=filtered_monthly['total_revenue'],
        mode='lines+markers',
        name='Revenue',
        line=dict(color=COLORS['gradient'][0], width=4),
        marker=dict(size=10, color=COLORS['gradient'][1]),
        fill='tonexty',
        fillcolor=f'rgba(102, 126, 234, 0.2)'
    ))
    fig_revenue.update_layout(
        title="Monthly Revenue Trend",
        xaxis_title="Period",
        yaxis_title="Revenue (R$)",
        hovermode='x unified',
        showlegend=False,
        plot_bgcolor='#ffffff',
        paper_bgcolor='#ffffff',
        font=dict(color='#2c3e50', size=12),
        title_font=dict(size=16, color='#2c3e50'),
        xaxis=dict(gridcolor='#e0e0e0'),
        yaxis=dict(gridcolor='#e0e0e0')
    )
    
    # Monthly Orders Chart with bars
    fig_orders = go.Figure()
    fig_orders.add_trace(go.Bar(
        x=filtered_monthly['period'],
        y=filtered_monthly['total_orders'],
        name='Orders',
        marker=dict(
            color=filtered_monthly['total_orders'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="Orders")
        ),
        text=filtered_monthly['total_orders'],
        textposition='auto'
    ))
    fig_orders.update_layout(
        title="Monthly Orders Volume",
        xaxis_title="Period",
        yaxis_title="Number of Orders",
        showlegend=False,
        plot_bgcolor='#ffffff',
        paper_bgcolor='#ffffff',
        font=dict(color='#2c3e50', size=12),
        title_font=dict(size=16, color='#2c3e50'),
        xaxis=dict(gridcolor='#e0e0e0'),
        yaxis=dict(gridcolor='#e0e0e0')
    )
    
    # State Revenue Chart with performance categories
    fig_states = go.Figure()
    color_map = {'🥇 Gold': '#FFD700', '🥈 Silver': '#C0C0C0', '🥉 Bronze': '#CD7F32'}
    
    for category in filtered_state['performance_category'].unique():
        category_data = filtered_state[filtered_state['performance_category'] == category]
        fig_states.add_trace(go.Bar(
            x=category_data['customer_state'],
            y=category_data['total_revenue'],
            name=category,
            marker_color=color_map.get(category, COLORS['primary']),
            text=category_data['total_revenue_formatted'],
            textposition='auto'
        ))
    
    fig_states.update_layout(
        title="State Performance by Revenue",
        xaxis_title="State",
        yaxis_title="Revenue (R$)",
        showlegend=True,
        plot_bgcolor='#ffffff',
        paper_bgcolor='#ffffff',
        font=dict(color='#2c3e50', size=12),
        title_font=dict(size=16, color='#2c3e50'),
        xaxis=dict(gridcolor='#e0e0e0'),
        yaxis=dict(gridcolor='#e0e0e0')
    )
    
    # Weather Impact Chart with pie chart
    fig_weather = go.Figure()
    fig_weather.add_trace(go.Pie(
        labels=filtered_weather['rain_category'],
        values=filtered_weather['total_orders'],
        name="Orders by Weather",
        hole=0.4,
        marker=dict(colors=[COLORS['warning'], COLORS['info'], COLORS['primary']])
    ))
    fig_weather.update_layout(
        title="Orders Distribution by Weather",
        showlegend=True,
        font=dict(color='#2c3e50', size=12),
        title_font=dict(size=16, color='#2c3e50'),
        paper_bgcolor='#ffffff'
    )
    
    # Performance Metrics
    metrics_cards = dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("📊 Average Order Value", className="card-title"),
                    html.H4(f"R$ {filtered_monthly['avg_order_value'].mean():.2f}", 
                           className="text-success", style={'fontWeight': 'bold'})
                ])
            ], color="light", outline=True)
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("📈 Growth Rate", className="card-title"),
                    html.H4(f"+{((filtered_monthly['total_revenue'].iloc[-1] / filtered_monthly['total_revenue'].iloc[0] - 1) * 100):.1f}%", 
                           className="text-info", style={'fontWeight': 'bold'})
                ])
            ], color="light", outline=True)
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("🏆 Top State", className="card-title"),
                    html.H4(filtered_state.iloc[0]['customer_state'] if len(filtered_state) > 0 else "N/A", 
                           className="text-warning", style={'fontWeight': 'bold'})
                ])
            ], color="light", outline=True)
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("🌤️ Weather Effect", className="card-title"),
                    html.H4(f"{filtered_weather['avg_order_value'].max():.2f}" if len(filtered_weather) > 0 else "N/A", 
                           className="text-primary", style={'fontWeight': 'bold'})
                ])
            ], color="light", outline=True)
        ], width=3)
    ])
    
    # Enhanced Data Table
    table_data = filtered_state.head(10).to_dict('records')
    table = dash_table.DataTable(
        columns=[
            {'name': 'State', 'id': 'customer_state'},
            {'name': 'Performance', 'id': 'performance_category'},
            {'name': 'Orders', 'id': 'total_orders', 'type': 'numeric', 'format': {'specifier': ','}},
            {'name': 'Revenue', 'id': 'total_revenue_formatted'},
            {'name': 'Avg Order Value', 'id': 'avg_order_value_formatted'},
            {'name': 'Avg Delivery Days', 'id': 'avg_delivery_days', 'type': 'numeric', 'format': {'specifier': '.1f'}}
        ],
        data=table_data,
        style_cell={'textAlign': 'center', 'padding': '10px'},
        style_header={
            'backgroundColor': COLORS['primary'],
            'color': 'white',
            'fontWeight': 'bold',
            'fontSize': '14px'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgba(0,0,0,0.05)'
            }
        ],
        style_table={'borderRadius': '10px', 'overflow': 'hidden'}
    )
    
    return fig_revenue, fig_orders, fig_states, fig_weather, metrics_cards, table

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=8050)
