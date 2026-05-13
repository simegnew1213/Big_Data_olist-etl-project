"""
Olist E-Commerce Analytics — Fully Interactive Dashboard
Dark theme | Filters | 8 chart types | KPI gauges
"""

import dash
from dash import dcc, html, Input, Output, State
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import pandas as pd
import os

app = dash.Dash(__name__, title="Olist Analytics")

BASE    = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE, 'analytics.duckdb')

conn = duckdb.connect(DB_PATH, read_only=True)
monthly  = conn.execute('SELECT * FROM monthly_revenue ORDER BY year, month').fetchdf()
states   = conn.execute('SELECT * FROM revenue_by_state ORDER BY total_revenue DESC').fetchdf()
weather  = conn.execute('SELECT * FROM weather_impact').fetchdf()
orders   = conn.execute('''
    SELECT order_id, purchase_date, total_payment, delivery_days,
           was_late, customer_state, temp_max, precipitation, year, month
    FROM orders_full WHERE total_payment IS NOT NULL
''').fetchdf()
kpis = conn.execute('''
    SELECT COUNT(order_id) AS total_orders,
           ROUND(SUM(total_payment),2) AS total_revenue,
           ROUND(AVG(total_payment),2) AS avg_order_value,
           ROUND(AVG(delivery_days),1) AS avg_delivery_days,
           SUM(was_late) AS late_orders,
           ROUND(SUM(was_late)*100.0/COUNT(order_id),2) AS late_rate
    FROM orders_full
''').fetchdf().iloc[0]
conn.close()

monthly['period'] = monthly['year'].astype(str) + '-' + monthly['month'].astype(str).str.zfill(2)
orders['year'] = pd.to_numeric(orders['year'], errors='coerce')
orders['rain_category'] = orders['precipitation'].apply(
    lambda x: 'No Rain' if pd.isna(x) or x == 0 else ('Light Rain' if x < 5 else 'Heavy Rain'))
orders['was_late_str'] = orders['was_late'].map({0: 'On Time', 1: 'Late'}).fillna('Unknown')

years     = sorted([int(y) for y in monthly['year'].dropna().unique()])
all_years = [{'label': str(y), 'value': y} for y in years]

BR_STATES = {
    'AC':'Acre','AL':'Alagoas','AM':'Amazonas','AP':'Amapá','BA':'Bahia',
    'CE':'Ceará','DF':'Distrito Federal','ES':'Espírito Santo','GO':'Goiás',
    'MA':'Maranhão','MG':'Minas Gerais','MS':'Mato Grosso do Sul',
    'MT':'Mato Grosso','PA':'Pará','PB':'Paraíba','PE':'Pernambuco',
    'PI':'Piauí','PR':'Paraná','RJ':'Rio de Janeiro','RN':'Rio Grande do Norte',
    'RO':'Rondônia','RR':'Roraima','RS':'Rio Grande do Sul','SC':'Santa Catarina',
    'SE':'Sergipe','SP':'São Paulo','TO':'Tocantins'
}
states['state_name'] = states['customer_state'].map(BR_STATES).fillna(states['customer_state'])

# Theme
BG, CARD, BORDER = '#0f1117', '#1a1d27', '#2a2d3e'
ACCENT, GREEN, ORANGE, BLUE, RED, TEXT, MUTED = (
    '#6c63ff','#00d4aa','#ff7f50','#4da6ff','#ff4d6d','#e0e0e0','#8888aa')

FILL = {'total_revenue': 'rgba(0,212,170,0.12)',
        'total_orders':  'rgba(77,166,255,0.12)',
        'avg_order_value':'rgba(255,127,80,0.12)'}
CMAP = {'total_revenue': GREEN, 'total_orders': BLUE, 'avg_order_value': ORANGE}

def base_layout(height=320):
    return dict(
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color=TEXT, family='Inter,sans-serif', size=12),
        margin=dict(l=50, r=20, t=36, b=40),
        height=height,
        xaxis=dict(gridcolor='#1f2233', showgrid=True, zeroline=False, linecolor=BORDER),
        yaxis=dict(gridcolor='#1f2233', showgrid=True, zeroline=False, linecolor=BORDER),
        hoverlabel=dict(bgcolor=CARD, bordercolor=BORDER, font_color=TEXT),
    )

def card(children, extra=None):
    style = {'backgroundColor': CARD, 'borderRadius': '12px',
             'border': f'1px solid {BORDER}', 'padding': '20px', 'marginBottom': '20px'}
    if extra:
        style.update(extra)
    return html.Div(children, style=style)

def kpi_box(icon, label, value, color):
    return html.Div([
        html.Div(icon, style={'fontSize': '26px'}),
        html.Div(label, style={'color': MUTED, 'fontSize': '12px', 'margin': '4px 0 2px'}),
        html.Div(value, style={'color': color, 'fontSize': '22px', 'fontWeight': '700'}),
    ], style={'backgroundColor': CARD, 'border': f'1px solid {BORDER}', 'borderRadius': '12px',
              'padding': '20px', 'textAlign': 'center', 'flex': '1', 'margin': '0 8px'})

# ── Toggle CSS injected via index_string ─────────────────────
app.index_string = '''
<!DOCTYPE html>
<html>
<head>
{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<style>
  @import url("https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap");
  .toggle-wrap { display:flex; align-items:center; gap:10px; }
  .toggle-label { font-size:13px; color:#8888aa; }
  .switch { position:relative; display:inline-block; width:48px; height:26px; }
  .switch input { opacity:0; width:0; height:0; }
  .slider { position:absolute; cursor:pointer; top:0;left:0;right:0;bottom:0;
            background:#2a2d3e; border-radius:26px; transition:.3s; }
  .slider:before { position:absolute; content:""; height:18px; width:18px;
                   left:4px; bottom:4px; background:#8888aa;
                   border-radius:50%; transition:.3s; }
  input:checked + .slider { background:#6c63ff; }
  input:checked + .slider:before { transform:translateX(22px); background:white; }
</style>
</head>
<body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body>
</html>
'''

app.layout = html.Div(id='app-wrapper',
    style={'backgroundColor': BG, 'minHeight': '100vh',
           'fontFamily': 'Inter,sans-serif', 'color': TEXT, 'padding': '24px'},
    children=[
    dcc.Store(id='theme-store', data='dark'),

    html.Div([
        html.Div([
            html.H1('🛒 Olist E-Commerce Analytics',
                    style={'margin': 0, 'fontSize': '26px', 'fontWeight': '800'}),
            html.P('Interactive Business Intelligence Dashboard',
                   style={'margin': '4px 0 0', 'color': MUTED, 'fontSize': '13px'}),
        ]),
        # Dark / Light toggle
        html.Div([
            html.Span('🌙', className='toggle-label', id='theme-icon'),
            html.Label([
                dcc.Input(id='theme-toggle', type='checkbox', style={'display': 'none'}),
                html.Span(className='slider'),
            ], className='switch', htmlFor='theme-toggle'),
            html.Span('☀️', className='toggle-label'),
        ], className='toggle-wrap'),
    ], style={'display': 'flex', 'justifyContent': 'space-between',
              'alignItems': 'flex-start', 'marginBottom': '24px'}),

    # KPIs
    html.Div([
        kpi_box('📦', 'Total Orders',      f"{int(kpis['total_orders']):,}",       GREEN),
        kpi_box('💰', 'Total Revenue',     f"R${kpis['total_revenue']:,.0f}",      ACCENT),
        kpi_box('🛍️', 'Avg Order Value',  f"R${kpis['avg_order_value']:.2f}",     BLUE),
        kpi_box('🚚', 'Avg Delivery Days', f"{kpis['avg_delivery_days']} days",    ORANGE),
        kpi_box('⚠️', 'Late Delivery Rate',f"{kpis['late_rate']}%",               RED),
    ], style={'display': 'flex', 'marginBottom': '20px'}),

    # Filters
    card([
        html.Div([
            html.Div([
                html.Label('📅 Year', style={'color': MUTED, 'fontSize': '12px', 'display': 'block', 'marginBottom': '6px'}),
                dcc.Checklist(id='year-filter', options=all_years, value=years, inline=True,
                              inputStyle={'marginRight': '4px'},
                              labelStyle={'color': TEXT, 'marginRight': '14px', 'fontSize': '14px'}),
            ], style={'flex': '1'}),
            html.Div([
                html.Label('📊 Revenue Range (R$)', style={'color': MUTED, 'fontSize': '12px', 'display': 'block', 'marginBottom': '6px'}),
                dcc.RangeSlider(id='rev-slider',
                                min=0, max=int(orders['total_payment'].max())+1, step=50,
                                value=[0, int(orders['total_payment'].max())+1],
                                marks={0:'R$0', 500:'R$500', 1000:'R$1k', 3000:'R$3k'},
                                tooltip={'placement': 'bottom', 'always_visible': False}),
            ], style={'flex': '2', 'paddingLeft': '28px'}),
        ], style={'display': 'flex', 'alignItems': 'center'}),
    ]),

    # Row 1: Trend + Heatmap
    html.Div([
        html.Div([card([
            html.Div([
                html.Span('📈 Revenue & Orders Trend', style={'fontWeight': '600', 'fontSize': '15px'}),
                dcc.RadioItems(id='trend-metric',
                    options=[{'label': ' Revenue', 'value': 'total_revenue'},
                             {'label': ' Orders',  'value': 'total_orders'},
                             {'label': ' Avg Value','value': 'avg_order_value'}],
                    value='total_revenue', inline=True,
                    inputStyle={'marginRight': '4px'},
                    labelStyle={'color': TEXT, 'marginRight': '12px', 'fontSize': '13px'},
                    style={'marginTop': '2px'}),
            ], style={'display': 'flex', 'justifyContent': 'space-between', 'marginBottom': '8px'}),
            dcc.Graph(id='trend-chart', config={'displayModeBar': False}),
        ])], style={'flex': '6', 'marginRight': '10px'}),
        html.Div([card([
            html.Div('🕐 Orders Heatmap (Month × Year)',
                     style={'fontWeight': '600', 'fontSize': '15px', 'marginBottom': '8px'}),
            dcc.Graph(id='heatmap-chart', config={'displayModeBar': False}),
        ])], style={'flex': '4'}),
    ], style={'display': 'flex'}),

    # Row 2: Map + Pie
    html.Div([
        html.Div([card([
            html.Div([
                html.Span('🗺️ Revenue by Brazilian State', style={'fontWeight': '600', 'fontSize': '15px'}),
                dcc.RadioItems(id='map-metric',
                    options=[{'label': ' Revenue',  'value': 'total_revenue'},
                             {'label': ' Orders',   'value': 'total_orders'},
                             {'label': ' Delivery', 'value': 'avg_delivery_days'}],
                    value='total_revenue', inline=True,
                    inputStyle={'marginRight': '4px'},
                    labelStyle={'color': TEXT, 'marginRight': '12px', 'fontSize': '13px'},
                    style={'marginTop': '2px'}),
            ], style={'display': 'flex', 'justifyContent': 'space-between', 'marginBottom': '8px'}),
            dcc.Graph(id='map-chart', config={'displayModeBar': False}),
        ])], style={'flex': '6', 'marginRight': '10px'}),
        html.Div([card([
            html.Div('🍰 Top 8 States — Revenue Share',
                     style={'fontWeight': '600', 'fontSize': '15px', 'marginBottom': '8px'}),
            dcc.Graph(id='pie-chart', config={'displayModeBar': False}),
        ])], style={'flex': '4'}),
    ], style={'display': 'flex'}),

    # Row 3: Scatter + Weather
    html.Div([
        html.Div([card([
            html.Div([
                html.Span('🔵 Temperature vs Order Value', style={'fontWeight': '600', 'fontSize': '15px'}),
                dcc.Dropdown(id='scatter-color',
                    options=[{'label': 'Rain Category', 'value': 'rain_category'},
                             {'label': 'Was Late',      'value': 'was_late_str'},
                             {'label': 'State',         'value': 'customer_state'}],
                    value='rain_category', clearable=False,
                    style={'width': '160px', 'fontSize': '13px', 'backgroundColor': '#1a1d27'}),
            ], style={'display': 'flex', 'justifyContent': 'space-between', 'alignItems': 'center', 'marginBottom': '8px'}),
            dcc.Graph(id='scatter-chart', config={'displayModeBar': False}),
        ])], style={'flex': '5', 'marginRight': '10px'}),
        html.Div([card([
            html.Div('🌧️ Weather Impact on Orders',
                     style={'fontWeight': '600', 'fontSize': '15px', 'marginBottom': '8px'}),
            dcc.Graph(id='weather-chart', config={'displayModeBar': False}),
        ])], style={'flex': '5'}),
    ], style={'display': 'flex'}),

    # Row 4: Histogram + Gauge
    html.Div([
        html.Div([card([
            html.Div('📦 Delivery Time Distribution',
                     style={'fontWeight': '600', 'fontSize': '15px', 'marginBottom': '8px'}),
            dcc.Graph(id='histogram-chart', config={'displayModeBar': False}),
        ])], style={'flex': '6', 'marginRight': '10px'}),
        html.Div([card([
            html.Div('⏱️ On-Time vs Late Deliveries',
                     style={'fontWeight': '600', 'fontSize': '15px', 'marginBottom': '8px'}),
            dcc.Graph(id='gauge-chart', config={'displayModeBar': False}),
        ])], style={'flex': '4'}),
    ], style={'display': 'flex'}),

    # Data Table
    card([
        html.Div([
            html.Span('📋 States Performance Table', style={'fontWeight': '600', 'fontSize': '15px'}),
            dcc.Input(id='table-search', type='text', placeholder='🔍 Search state...',
                      debounce=True,
                      style={'backgroundColor': BG, 'color': TEXT, 'border': f'1px solid {BORDER}',
                             'borderRadius': '6px', 'padding': '6px 12px', 'fontSize': '13px'}),
        ], style={'display': 'flex', 'justifyContent': 'space-between', 'marginBottom': '12px'}),
        html.Div(id='data-table'),
    ]),

    html.Div('Olist ETL Pipeline · Pandas + DuckDB + Dash',
             style={'textAlign': 'center', 'color': MUTED, 'fontSize': '12px', 'marginTop': '8px'}),
])


# ══════════════════════════════════════════════════════
# CALLBACKS
# ══════════════════════════════════════════════════════

@app.callback(Output('trend-chart', 'figure'),
              [Input('year-filter', 'value'), Input('trend-metric', 'value')])
def cb_trend(years_sel, metric):
    df = monthly[monthly['year'].isin(years_sel)].copy()
    labels = {'total_revenue': 'Revenue (R$)', 'total_orders': 'Orders', 'avg_order_value': 'Avg Value (R$)'}
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['period'], y=df[metric],
        mode='lines+markers',
        line=dict(color=CMAP[metric], width=2.5),
        marker=dict(size=7, color=CMAP[metric], line=dict(color=BG, width=1.5)),
        fill='tozeroy', fillcolor=FILL[metric],
        hovertemplate='<b>%{x}</b><br>' + labels[metric] + ': %{y:,.1f}<extra></extra>',
    ))
    layout = base_layout(300)
    layout['yaxis']['title'] = labels[metric]
    fig.update_layout(**layout)
    return fig


@app.callback(Output('heatmap-chart', 'figure'), Input('year-filter', 'value'))
def cb_heatmap(years_sel):
    df = monthly[monthly['year'].isin(years_sel)]
    pivot = df.pivot_table(index='month', columns='year', values='total_orders', aggfunc='sum').fillna(0)
    mnames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    y_labels = [mnames[m-1] for m in pivot.index]
    fig = go.Figure(go.Heatmap(
        z=pivot.values, x=[str(c) for c in pivot.columns], y=y_labels,
        colorscale='Viridis',
        hovertemplate='<b>%{y} %{x}</b><br>Orders: %{z:,}<extra></extra>',
    ))
    fig.update_layout(**base_layout(300))
    return fig


@app.callback(Output('map-chart', 'figure'), Input('map-metric', 'value'))
def cb_map(metric):
    labels = {'total_revenue': 'Revenue (R$)', 'total_orders': 'Orders', 'avg_delivery_days': 'Avg Delivery Days'}
    fig = px.choropleth(
        states,
        geojson='https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson',
        locations='customer_state', featureidkey='properties.sigla',
        color=metric, hover_name='state_name',
        color_continuous_scale='Plasma', labels={metric: labels[metric]},
    )
    fig.update_geos(fitbounds='locations', visible=False, bgcolor='rgba(0,0,0,0)')
    layout = base_layout(350)
    layout.pop('xaxis', None)
    layout.pop('yaxis', None)
    fig.update_layout(**layout,
                      coloraxis_colorbar=dict(title=labels[metric], tickfont=dict(color=TEXT)))
    fig.update_traces(marker_line_color=BORDER, marker_line_width=0.5)
    return fig


@app.callback(Output('pie-chart', 'figure'), Input('year-filter', 'value'))
def cb_pie(_):
    top8  = states.head(8)[['state_name','total_revenue']].copy()
    rest  = states.iloc[8:]['total_revenue'].sum()
    df_pie = pd.concat([top8, pd.DataFrame([{'state_name':'Others','total_revenue':rest}])],
                       ignore_index=True)
    fig = go.Figure(go.Pie(
        labels=df_pie['state_name'], values=df_pie['total_revenue'],
        hole=0.45, textinfo='label+percent',
        hovertemplate='<b>%{label}</b><br>R$%{value:,.0f} (%{percent})<extra></extra>',
        marker=dict(colors=px.colors.qualitative.Vivid),
    ))
    layout = base_layout(350)
    layout.pop('xaxis', None); layout.pop('yaxis', None)
    fig.update_layout(**layout, showlegend=False)
    return fig


@app.callback(Output('scatter-chart', 'figure'),
              [Input('year-filter', 'value'), Input('rev-slider', 'value'), Input('scatter-color', 'value')])
def cb_scatter(years_sel, rev_range, color_col):
    mask = (
        orders['year'].isin(years_sel) &
        orders['total_payment'].between(rev_range[0], rev_range[1]) &
        orders['temp_max'].notna() & orders['precipitation'].notna()
    )
    df = orders[mask]
    n  = min(2500, len(df))
    df = df.sample(n, random_state=42) if n > 0 else df
    fig = px.scatter(
        df, x='temp_max', y='total_payment', color=color_col,
        opacity=0.6,
        labels={'temp_max': 'Max Temp (°C)', 'total_payment': 'Order Value (R$)'},
        color_discrete_sequence=px.colors.qualitative.Bold,
    )
    fig.update_traces(marker=dict(size=5))
    layout = base_layout(320)
    layout['legend'] = dict(bgcolor='rgba(0,0,0,0)', font=dict(color=TEXT))
    fig.update_layout(**layout)
    return fig


@app.callback(Output('weather-chart', 'figure'), Input('year-filter', 'value'))
def cb_weather(_):
    order_map = {'No Rain': 0, 'Light Rain': 1, 'Heavy Rain': 2}
    clrs      = [GREEN, BLUE, ACCENT]
    df = weather.copy()
    df['_sort'] = df['rain_category'].map(order_map).fillna(0)
    df = df.sort_values('_sort')
    fig = go.Figure()
    for _, row in df.iterrows():
        idx = int(row.get('_sort', 0))
        fig.add_trace(go.Bar(
            x=[row['rain_category']], y=[row['total_orders']],
            name=row['rain_category'],
            marker_color=clrs[idx],
            text=[f"{int(row['total_orders']):,}"],
            textposition='outside', textfont=dict(color=TEXT),
            hovertemplate=f"<b>{row['rain_category']}</b><br>Orders: {int(row['total_orders']):,}<br>Avg: R${row['avg_order_value']:,.2f}<extra></extra>",
        ))
    fig.update_layout(**base_layout(320), showlegend=False, barmode='group')
    return fig


@app.callback(Output('histogram-chart', 'figure'),
              [Input('year-filter', 'value'), Input('rev-slider', 'value')])
def cb_histogram(years_sel, rev_range):
    mask = (
        orders['year'].isin(years_sel) &
        orders['total_payment'].between(rev_range[0], rev_range[1]) &
        orders['delivery_days'].notna() &
        orders['delivery_days'].between(0, 60)
    )
    df = orders[mask]
    fig = go.Figure(go.Histogram(
        x=df['delivery_days'], nbinsx=40,
        marker=dict(color=ACCENT, opacity=0.85, line=dict(color=BORDER, width=0.4)),
        hovertemplate='Days: %{x}<br>Orders: %{y:,}<extra></extra>',
    ))
    layout = base_layout(310)
    layout['xaxis']['title'] = 'Delivery Days'
    layout['yaxis']['title'] = 'Number of Orders'
    fig.update_layout(**layout)
    return fig


@app.callback(Output('gauge-chart', 'figure'), Input('year-filter', 'value'))
def cb_gauge(years_sel):
    df       = orders[orders['year'].isin(years_sel)]
    total    = len(df)
    on_time  = int((df['was_late'] == 0).sum())
    late     = int((df['was_late'] == 1).sum())
    pct      = round(on_time * 100 / total, 1) if total > 0 else 0
    fig = go.Figure(go.Indicator(
        mode='gauge+number+delta',
        value=pct,
        delta={'reference': 90, 'valueformat': '.1f', 'suffix': '%'},
        number={'suffix': '%', 'font': {'color': GREEN, 'size': 38}},
        gauge={
            'axis': {'range': [0, 100], 'tickcolor': MUTED},
            'bar':  {'color': GREEN},
            'bgcolor': 'rgba(0,0,0,0)',
            'bordercolor': BORDER,
            'steps': [
                {'range': [0,  70], 'color': 'rgba(255,77,109,0.15)'},
                {'range': [70, 90], 'color': 'rgba(255,127,80,0.15)'},
                {'range': [90,100], 'color': 'rgba(0,212,170,0.15)'},
            ],
            'threshold': {'line': {'color': GREEN, 'width': 3}, 'value': 90},
        },
        title={'text': f'On-Time Rate<br><span style="font-size:12px;color:{MUTED}">On-time: {on_time:,} | Late: {late:,}</span>',
               'font': {'color': TEXT}},
        domain={'x': [0,1], 'y': [0,1]},
    ))
    layout = base_layout(310)
    layout.pop('xaxis', None); layout.pop('yaxis', None)
    fig.update_layout(**layout)
    return fig


@app.callback(Output('data-table', 'children'),
              [Input('table-search', 'value'), Input('year-filter', 'value')])
def cb_table(search, _):
    df = states.copy()
    if search:
        q = search.upper()
        df = df[df['customer_state'].str.upper().str.contains(q, na=False) |
                df['state_name'].str.upper().str.contains(q, na=False)]

    th = {'padding': '9px 12px', 'backgroundColor': ACCENT + '22',
          'color': MUTED, 'fontSize': '11px', 'textTransform': 'uppercase',
          'letterSpacing': '0.06em', 'fontWeight': '600', 'textAlign': 'left'}
    th_r = {**th, 'textAlign': 'right'}
    td_base = {'padding': '9px 12px', 'fontSize': '13px', 'color': TEXT,
               'borderBottom': f'1px solid {BORDER}'}

    rows = []
    for i, r in df.iterrows():
        bg = BG if i % 2 == 0 else '#151820'
        rows.append(html.Tr(style={'backgroundColor': bg}, children=[
            html.Td(r['customer_state'], style={**td_base, 'fontWeight': '600', 'color': ACCENT}),
            html.Td(r['state_name'],     style=td_base),
            html.Td(f"{r['total_orders']:,}", style={**td_base, 'textAlign': 'right'}),
            html.Td(f"R${r['total_revenue']:,.0f}", style={**td_base, 'textAlign': 'right', 'color': GREEN}),
            html.Td(f"R${r['avg_order_value']:,.2f}", style={**td_base, 'textAlign': 'right', 'color': BLUE}),
            html.Td(f"{r['avg_delivery_days']:.1f}d", style={**td_base, 'textAlign': 'right', 'color': ORANGE}),
        ]))

    return html.Table(style={'width': '100%', 'borderCollapse': 'collapse'}, children=[
        html.Thead(html.Tr([
            html.Th('Code',     style=th),
            html.Th('State',    style=th),
            html.Th('Orders',   style=th_r),
            html.Th('Revenue',  style=th_r),
            html.Th('Avg Value',style=th_r),
            html.Th('Delivery', style=th_r),
        ])),
        html.Tbody(rows),
    ])


# ── Theme toggle callback ─────────────────────────────────────
@app.callback(
    [Output('app-wrapper', 'style'),
     Output('theme-store', 'data')],
    Input('theme-toggle', 'value'),
    State('theme-store', 'data'),
    prevent_initial_call=True,
)
def cb_toggle_theme(checked, current_theme):
    if current_theme == 'dark':
        # Switch to light
        new_bg   = '#f0f2f5'
        new_card = '#ffffff'
        new_text = '#1a1a2e'
        theme    = 'light'
    else:
        # Switch to dark
        new_bg   = '#0f1117'
        new_card = '#1a1d27'
        new_text = '#e0e0e0'
        theme    = 'dark'
    wrapper_style = {
        'backgroundColor': new_bg,
        'minHeight': '100vh',
        'fontFamily': 'Inter,sans-serif',
        'color': new_text,
        'padding': '24px',
        'transition': 'background-color 0.3s ease, color 0.3s ease',
    }
    return wrapper_style, theme


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=8050)
