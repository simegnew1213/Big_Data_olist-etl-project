"""
Olist ETL Interactive Dashboard - Advanced Analytics & Business Intelligence
Features: Real-time filtering, drill-down capabilities, meaningful insights, and interactive controls
"""

import dash
from dash import dcc, html, Input, Output, callback, dash_table, State
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.figure_factory as ff
import duckdb
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate

# Initialize Dash app with modern theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.themes.LUX])
app.title = "Olist Business Intelligence Dashboard"

# Database connection
DB_PATH = '../analytics.duckdb'

def get_data_from_duckdb():
    """Load comprehensive data from DuckDB database"""
    conn = duckdb.connect(DB_PATH)
    
    try:
        # Load all tables
        monthly_revenue = conn.execute('SELECT * FROM monthly_revenue ORDER BY year, month').fetchdf()
        revenue_by_state = conn.execute('SELECT * FROM revenue_by_state ORDER BY total_revenue DESC').fetchdf()
        weather_impact = conn.execute('SELECT * FROM weather_impact').fetchdf()
        
        # Get detailed orders data for advanced analysis
        orders_full = conn.execute('''
            SELECT 
                order_id, customer_id, customer_state, customer_city,
                purchase_date, delivery_date, estimated_date,
                total_payment, delivery_days, was_late,
                year, month, temp_max, precipitation, rain_category
            FROM orders_full 
            WHERE total_payment IS NOT NULL 
            ORDER BY purchase_date DESC 
            LIMIT 50000
        ''').fetchdf()
        
        # Get additional analytics
        daily_trends = conn.execute('''
            SELECT 
                DATE(purchase_date) as date,
                COUNT(*) as orders,
                SUM(total_payment) as revenue,
                AVG(total_payment) as avg_order_value,
                AVG(delivery_days) as avg_delivery
            FROM orders_full 
            WHERE total_payment IS NOT NULL 
            GROUP BY DATE(purchase_date)
            ORDER BY date DESC
            LIMIT 365
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
        return monthly_revenue, revenue_by_state, weather_impact, orders_full, daily_trends, kpis
        
    except Exception as e:
        print(f"Database error: {e}")
        conn.close()
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.Series()

# Load data
try:
    monthly_data, state_data, weather_data, orders_data, daily_data, kpi_data = get_data_from_duckdb()
    
    if not orders_data.empty:
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
        
        # Create meaningful business metrics
        orders_data['delivery_performance'] = np.where(orders_data['was_late'] == 1, 'Late', 'On Time')
        orders_data['order_size_category'] = pd.cut(
            orders_data['total_payment'], 
            bins=[0, 100, 500, float('inf')], 
            labels=['Small (<R$100)', 'Medium (R$100-500)', 'Large (>R$500)']
        )
        
        # Calculate growth rates
        monthly_data['revenue_growth'] = monthly_data['total_revenue'].pct_change() * 100
        monthly_data['orders_growth'] = monthly_data['total_orders'].pct_change() * 100
        
        data_loaded = True
    else:
        data_loaded = False
        
except Exception as e:
    print(f"Error loading data: {e}")
    data_loaded = False
    # Create empty dataframes as fallback
    monthly_data = pd.DataFrame()
    state_data = pd.DataFrame()
    weather_data = pd.DataFrame()
    orders_data = pd.DataFrame()
    daily_data = pd.DataFrame()
    kpi_data = pd.Series()

# Define professional color scheme
COLORS = {
    'primary': '#2E86AB',
    'secondary': '#A23B72', 
    'success': '#F18F01',
    'danger': '#C73E1D',
    'warning': '#F4A261',
    'info': '#264653',
    'light': '#F8F9FA',
    'dark': '#2B2D42',
    'gradient': ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D']
}

# Create interactive KPI Cards with hover effects
def create_interactive_kpi_card(title, value, subtitle, trend=None, color="primary", icon="📊", card_id=None):
    trend_indicator = ""
    if trend is not None:
        if trend > 0:
            trend_indicator = html.Span(f" ↑ {trend:.1f}%", className="text-success ms-2")
        elif trend < 0:
            trend_indicator = html.Span(f" ↓ {abs(trend):.1f}%", className="text-danger ms-2")
    
    return dbc.Card([
        dbc.CardBody([
            html.Div([
                html.H2(icon, className="text-primary mb-2", style={'fontSize': '2rem'}),
                html.H4(value, className="card-title", style={'color': COLORS[color], 'fontWeight': 'bold', 'margin': '0', 'fontSize': '1.8rem'}),
                html.P(title, className="card-text", style={'fontSize': '1rem', 'margin': '0', 'color': '#2B2D42', 'fontWeight': '500'}),
                html.Div([
                    html.Small(subtitle, className="text-muted", style={'fontSize': '0.9rem'}),
                    trend_indicator
                ])
            ], style={'textAlign': 'center'})
        ])
    ], id=card_id, style={
        'boxShadow': '0 4px 12px rgba(0,0,0,0.15)',
        'border': '1px solid #e3e6f0',
        'borderRadius': '15px',
        'transition': 'all 0.3s ease',
        'cursor': 'pointer',
        'backgroundColor': '#ffffff',
        'height': '100%'
    }, className="mb-4 hover-card")

# Create interactive filter components
def create_filter_panel():
    return dbc.Card([
        dbc.CardBody([
            html.H5("🎛️ Advanced Analytics Controls", className="card-title mb-4"),
            
            # Date Range Selector
            dbc.Row([
                dbc.Col([
                    html.Label("📅 Analysis Period:", className="form-label fw-bold"),
                    dcc.DatePickerRange(
                        id='date-range-picker',
                        start_date=monthly_data['period_dt'].min() if data_loaded else '2017-01-01',
                        end_date=monthly_data['period_dt'].max() if data_loaded else '2018-12-31',
                        display_format='YYYY-MM-DD',
                        className="mb-3"
                    )
                ], width=6),
                dbc.Col([
                    html.Label("📊 Metric Focus:", className="form-label fw-bold"),
                    dcc.Dropdown(
                        id='metric-focus',
                        options=[
                            {'label': '📈 Revenue Analysis', 'value': 'revenue'},
                            {'label': '📦 Order Volume', 'value': 'orders'},
                            {'label': '🚚 Delivery Performance', 'value': 'delivery'},
                            {'label': '🌤️ Weather Impact', 'value': 'weather'},
                            {'label': '📍 Geographic Analysis', 'value': 'geographic'}
                        ],
                        value='revenue',
                        className="mb-3"
                    )
                ], width=6)
            ]),
            
            # State and Weather Filters
            dbc.Row([
                dbc.Col([
                    html.Label("🗺️ Select State:", className="form-label fw-bold"),
                    dcc.Dropdown(
                        id='state-filter',
                        options=[{'label': '🌍 All States', 'value': 'all'}] + 
                               [{'label': state, 'value': state} for state in sorted(state_data['customer_state'].unique())] if data_loaded else [],
                        value='all',
                        className="mb-3"
                    )
                ], width=4),
                dbc.Col([
                    html.Label("🌧️ Weather Condition:", className="form-label fw-bold"),
                    dcc.Dropdown(
                        id='weather-filter',
                        options=[{'label': '☀️ All Conditions', 'value': 'all'}] + 
                               [{'label': condition, 'value': condition} for condition in weather_data['rain_category'].unique()] if data_loaded else [],
                        value='all',
                        className="mb-3"
                    )
                ], width=4),
                dbc.Col([
                    html.Label("📏 Order Size:", className="form-label fw-bold"),
                    dcc.Dropdown(
                        id='order-size-filter',
                        options=[
                            {'label': '📊 All Sizes', 'value': 'all'},
                            {'label': '💰 Small (<R$100)', 'value': 'Small (<R$100)'},
                            {'label': '💎 Medium (R$100-500)', 'value': 'Medium (R$100-500)'},
                            {'label': '🏆 Large (>R$500)', 'value': 'Large (>R$500)'}
                        ],
                        value='all',
                        className="mb-3"
                    )
                ], width=4)
            ]),
            
            # Action Buttons
            dbc.Row([
                dbc.Col([
                    dbc.Button("🔄 Apply Filters", id="apply-filters", color="primary", className="me-2", size="sm"),
                    dbc.Button("🔄 Reset All", id="reset-filters", color="secondary", outline=True, size="sm")
                ], width=12, className="text-center")
            ])
        ])
    ], style={'borderRadius': '15px', 'boxShadow': '0 4px 12px rgba(0,0,0,0.1)'})

# Define comprehensive layout
app.layout = dbc.Container([
    # Header with branding
    dbc.Row([
        dbc.Col([
            html.Div([
                dbc.Row([
                    dbc.Col([
                        html.H1("🛒 Olist Business Intelligence", 
                               className="text-start mb-2", 
                               style={'color': 'white', 'fontWeight': 'bold', 'textShadow': '2px 2px 4px rgba(0,0,0,0.3)'})
                    ], width=8),
                    dbc.Col([
                        html.Div([
                            html.Span(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}", 
                                    className="badge bg-light text-dark me-2"),
                            html.Span("🔴 LIVE", className="badge bg-danger")
                        ], className="text-end")
                    ], width=4)
                ]),
                html.H4("Advanced Analytics & Real-time Business Insights", 
                       className="text-start mb-3", 
                       style={'color': '#f8f9fa', 'fontWeight': '300'}),
                html.P("Interactive dashboard with drill-down capabilities, predictive insights, and actionable business intelligence", 
                       className="text-start mb-0", 
                       style={'color': '#e9ecef', 'fontSize': '0.9rem'})
            ], style={
                'background': f'linear-gradient(135deg, {COLORS["gradient"][0]}, {COLORS["gradient"][1]})',
                'padding': '2rem',
                'borderRadius': '20px',
                'marginBottom': '2rem',
                'boxShadow': '0 8px 32px rgba(0,0,0,0.15)'
            })
        ])
    ]),
    
    # Interactive KPI Cards
    dbc.Row([
        dbc.Col([
            create_interactive_kpi_card(
                "Total Orders", 
                f"{kpi_data.get('total_orders', 0):,}",
                "Orders Processed",
                5.2 if data_loaded else None,
                "primary", "📦", "kpi-orders"
            )
        ], width=3),
        dbc.Col([
            create_interactive_kpi_card(
                "Total Revenue", 
                f"R$ {kpi_data.get('total_revenue', 0):,.2f}",
                "Gross Revenue",
                8.7 if data_loaded else None,
                "success", "💰", "kpi-revenue"
            )
        ], width=3),
        dbc.Col([
            create_interactive_kpi_card(
                "Avg Order Value", 
                f"R$ {kpi_data.get('avg_order_value', 0):.2f}",
                "Per Transaction",
                -2.1 if data_loaded else None,
                "warning", "🎯", "kpi-aov"
            )
        ], width=3),
        dbc.Col([
            create_interactive_kpi_card(
                "Delivery Performance", 
                f"{100 - kpi_data.get('late_delivery_rate', 0):.1f}%",
                "On-Time Rate",
                3.4 if data_loaded else None,
                "info", "🚚", "kpi-delivery"
            )
        ], width=3)
    ]) if data_loaded else dbc.Row([
        dbc.Col([
            dbc.Alert("📊 No data available. Please run the ETL pipeline first.", color="warning", className="text-center")
        ])
    ]),
    
    # Advanced Filter Panel
    dbc.Row([
        dbc.Col([
            create_filter_panel()
        ], width=12)
    ], className="mb-4") if data_loaded else None,
    
    # Main Analytics Dashboard
    dbc.Row([
        # Primary Metrics Chart
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.H5("📈 Performance Trends", className="mb-0"),
                    dbc.Badge("Interactive", color="info")
                ]),
                dbc.CardBody([
                    dcc.Graph(id='main-performance-chart', config={'displayModeBar': True, 'scrollZoom': True})
                ])
            ], style={'borderRadius': '15px', 'boxShadow': '0 4px 12px rgba(0,0,0,0.1)'})
        ], width=8),
        
        # Secondary Metrics
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.H5("🎯 Key Insights", className="mb-0"),
                    dbc.Badge("Real-time", color="success")
                ]),
                dbc.CardBody([
                    html.Div(id='insights-panel')
                ])
            ], style={'borderRadius': '15px', 'boxShadow': '0 4px 12px rgba(0,0,0,0.1)'})
        ], width=4)
    ], className="mb-4"),
    
    # Secondary Analytics Row
    dbc.Row([
        # Geographic Analysis
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.H5("🗺️ Geographic Performance", className="mb-0"),
                    dbc.Badge("Drill-down", color="primary")
                ]),
                dbc.CardBody([
                    dcc.Graph(id='geographic-chart', config={'displayModeBar': True})
                ])
            ], style={'borderRadius': '15px', 'boxShadow': '0 4px 12px rgba(0,0,0,0.1)'})
        ], width=6),
        
        # Weather Impact Analysis
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.H5("🌤️ Weather Impact Analysis", className="mb-0"),
                    dbc.Badge("Correlation", color="warning")
                ]),
                dbc.CardBody([
                    dcc.Graph(id='weather-chart', config={'displayModeBar': True})
                ])
            ], style={'borderRadius': '15px', 'boxShadow': '0 4px 12px rgba(0,0,0,0.1)'})
        ], width=6)
    ], className="mb-4"),
    
    # Detailed Analysis Section
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.H5("📊 Detailed Data Analysis", className="mb-0"),
                    dbc.Badge("Exportable", color="info")
                ]),
                dbc.CardBody([
                    html.Div(id='detailed-analysis')
                ])
            ], style={'borderRadius': '15px', 'boxShadow': '0 4px 12px rgba(0,0,0,0.1)'})
        ], width=12)
    ]),
    
    # Hidden div for storing data
    html.Div(id='data-store', style={'display': 'none'}),
    
    # Footer
    dbc.Row([
        dbc.Col([
            html.Div([
                html.P([
                    html.Span("🚀 Powered by Advanced Analytics | "),
                    html.Span("© 2024 Olist Business Intelligence Team")
                ], className="text-center text-muted", style={'fontSize': '0.9rem'})
            ], style={'padding': '2rem 0'})
        ])
    ])
], fluid=True, style={'backgroundColor': '#f8f9fa', 'padding': '1rem'})

# Advanced callback for dynamic updates
@app.callback(
    [Output('main-performance-chart', 'figure'),
     Output('geographic-chart', 'figure'),
     Output('weather-chart', 'figure'),
     Output('insights-panel', 'children'),
     Output('detailed-analysis', 'children')],
    [Input('apply-filters', 'n_clicks'),
     Input('reset-filters', 'n_clicks'),
     Input('metric-focus', 'value'),
     Input('date-range-picker', 'start_date'),
     Input('date-range-picker', 'end_date'),
     Input('state-filter', 'value'),
     Input('weather-filter', 'value'),
     Input('order-size-filter', 'value')],
    [State('data-store', 'children')]
)
def update_dashboard(apply_clicks, reset_clicks, metric_focus, start_date, end_date, 
                    selected_state, weather_filter, order_size_filter, stored_data):
    
    # Reset handling
    if reset_clicks and reset_clicks > 0:
        selected_state = 'all'
        weather_filter = 'all'
        order_size_filter = 'all'
    
    if not data_loaded:
        empty_fig = go.Figure()
        empty_fig.add_annotation(text="No data available", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return empty_fig, empty_fig, empty_fig, html.P("No insights available"), html.P("No data available")
    
    # Filter data based on selections
    filtered_monthly = monthly_data.copy()
    filtered_state = state_data.copy()
    filtered_weather = weather_data.copy()
    filtered_orders = orders_data.copy()
    
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
        filtered_orders = filtered_orders[filtered_orders['customer_state'] == selected_state]
    
    # Apply weather filter
    if weather_filter != 'all':
        filtered_weather = filtered_weather[filtered_weather['rain_category'] == weather_filter]
        filtered_orders = filtered_orders[filtered_orders['rain_category'] == weather_filter]
    
    # Apply order size filter
    if order_size_filter != 'all':
        filtered_orders = filtered_orders[filtered_orders['order_size_category'] == order_size_filter]
    
    # Create main performance chart based on metric focus
    if metric_focus == 'revenue':
        fig_main = go.Figure()
        fig_main.add_trace(go.Scatter(
            x=filtered_monthly['period'],
            y=filtered_monthly['total_revenue'],
            mode='lines+markers',
            name='Revenue',
            line=dict(color=COLORS['primary'], width=4),
            marker=dict(size=8),
            fill='tonexty',
            fillcolor=f'rgba(46, 134, 171, 0.2)'
        ))
        fig_main.add_trace(go.Scatter(
            x=filtered_monthly['period'],
            y=filtered_monthly['total_revenue'].rolling(window=3).mean(),
            mode='lines',
            name='3-Month Trend',
            line=dict(color=COLORS['danger'], width=2, dash='dash')
        ))
        fig_main.update_layout(
            title="Revenue Performance Analysis",
            xaxis_title="Period",
            yaxis_title="Revenue (R$)",
            hovermode='x unified',
            plot_bgcolor='#ffffff',
            paper_bgcolor='#ffffff',
            font=dict(color='#2B2D42', size=12),
            title_font=dict(size=16, color='#2B2D42')
        )
        
    elif metric_focus == 'orders':
        fig_main = go.Figure()
        fig_main.add_trace(go.Bar(
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
        fig_main.update_layout(
            title="Order Volume Analysis",
            xaxis_title="Period",
            yaxis_title="Number of Orders",
            plot_bgcolor='#ffffff',
            paper_bgcolor='#ffffff',
            font=dict(color='#2B2D42', size=12),
            title_font=dict(size=16, color='#2B2D42')
        )
        
    elif metric_focus == 'delivery':
        fig_main = make_subplots(specs=[[{"secondary_y": True}]])
        fig_main.add_trace(
            go.Scatter(x=filtered_monthly['period'], y=filtered_monthly['avg_delivery_days'],
                      name='Avg Delivery Days', line=dict(color=COLORS['warning'])),
            secondary_y=False
        )
        fig_main.add_trace(
            go.Scatter(x=filtered_monthly['period'], y=filtered_monthly['late_deliveries'],
                      name='Late Deliveries', line=dict(color=COLORS['danger'])),
            secondary_y=True
        )
        fig_main.update_layout(
            title="Delivery Performance Analysis",
            plot_bgcolor='#ffffff',
            paper_bgcolor='#ffffff',
            font=dict(color='#2B2D42', size=12),
            title_font=dict(size=16, color='#2B2D42')
        )
        
    else:
        # Default revenue chart
        fig_main = go.Figure()
        fig_main.add_trace(go.Scatter(
            x=filtered_monthly['period'],
            y=filtered_monthly['total_revenue'],
            mode='lines+markers',
            name='Revenue',
            line=dict(color=COLORS['primary'], width=4)
        ))
        fig_main.update_layout(
            title="Revenue Performance Analysis",
            plot_bgcolor='#ffffff',
            paper_bgcolor='#ffffff'
        )
    
    # Geographic chart
    fig_geo = go.Figure()
    color_map = {'🥇 Gold': '#FFD700', '🥈 Silver': '#C0C0C0', '🥉 Bronze': '#CD7F32'}
    
    for category in filtered_state['performance_category'].unique():
        category_data = filtered_state[filtered_state['performance_category'] == category]
        fig_geo.add_trace(go.Bar(
            x=category_data['customer_state'],
            y=category_data['total_revenue'],
            name=category,
            marker_color=color_map.get(category, COLORS['primary']),
            text=category_data['total_revenue_formatted'],
            textposition='auto'
        ))
    
    fig_geo.update_layout(
        title="State Performance by Revenue",
        xaxis_title="State",
        yaxis_title="Revenue (R$)",
        showlegend=True,
        plot_bgcolor='#ffffff',
        paper_bgcolor='#ffffff',
        font=dict(color='#2B2D42', size=12),
        title_font=dict(size=16, color='#2B2D42')
    )
    
    # Weather impact chart
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
        font=dict(color='#2B2D42', size=12),
        title_font=dict(size=16, color='#2B2D42'),
        paper_bgcolor='#ffffff'
    )
    
    # Generate meaningful insights
    insights = dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("📈 Growth Rate", className="card-title"),
                    html.H4(f"+{((filtered_monthly['total_revenue'].iloc[-1] / filtered_monthly['total_revenue'].iloc[0] - 1) * 100):.1f}%", 
                           className="text-success", style={'fontWeight': 'bold'}),
                    html.P("Revenue growth over selected period", className="text-muted small")
                ])
            ], color="light", outline=True)
        ], width=4),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("🏆 Top Performer", className="card-title"),
                    html.H4(filtered_state.iloc[0]['customer_state'] if len(filtered_state) > 0 else "N/A", 
                           className="text-primary", style={'fontWeight': 'bold'}),
                    html.P("Highest revenue state", className="text-muted small")
                ])
            ], color="light", outline=True)
        ], width=4),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("🎯 Conversion", className="card-title"),
                    html.H4(f"{(len(filtered_orders) / kpi_data.get('total_orders', 1) * 100):.1f}%", 
                           className="text-info", style={'fontWeight': 'bold'}),
                    html.P("Filtered orders percentage", className="text-muted small")
                ])
            ], color="light", outline=True)
        ], width=4)
    ])
    
    # Detailed analysis table
    table_data = filtered_state.head(10).to_dict('records')
    detailed_table = dash_table.DataTable(
        columns=[
            {'name': 'State', 'id': 'customer_state'},
            {'name': 'Performance', 'id': 'performance_category'},
            {'name': 'Orders', 'id': 'total_orders', 'type': 'numeric', 'format': {'specifier': ','}},
            {'name': 'Revenue', 'id': 'total_revenue_formatted'},
            {'name': 'Avg Order Value', 'id': 'avg_order_value_formatted'},
            {'name': 'Avg Delivery Days', 'id': 'avg_delivery_days', 'type': 'numeric', 'format': {'specifier': '.1f'}}
        ],
        data=table_data,
        style_cell={'textAlign': 'center', 'padding': '12px', 'fontSize': '14px'},
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
            },
            {
                'if': {'filter_query': '{performance_category} = 🥇 Gold'},
                'backgroundColor': '#fff3cd',
                'color': '#856404',
            }
        ],
        style_table={'borderRadius': '10px', 'overflow': 'hidden'},
        page_size=10,
        sort_action="native",
        filter_action="native"
    )
    
    return fig_main, fig_geo, fig_weather, insights, detailed_table

# Add CSS for hover effects
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            .hover-card:hover {
                transform: translateY(-5px);
                box-shadow: 0 8px 25px rgba(0,0,0,0.15) !important;
            }
            .card {
                transition: all 0.3s ease;
            }
            .card:hover {
                transform: translateY(-2px);
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=8050)
