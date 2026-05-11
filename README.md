# 🛒 Olist E-Commerce ETL Pipeline

> **Advanced Data Engineering Assignment** — ETL pipeline using PySpark, DuckDB, Apache Airflow, dbt, and an interactive BI dashboard.

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.4%2B-orange)](https://spark.apache.org)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10%2B-yellow)](https://duckdb.org)
[![Airflow](https://img.shields.io/badge/Airflow-2.6%2B-green)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.6%2B-red)](https://www.getdbt.com)

**GitHub Repository:** https://github.com/simegnew1213/Big_Data_olist-etl-project

---

## 🎯 Project Overview

### Business Problem

[Olist](https://olist.com) is Brazil's largest department store marketplace, connecting small merchants to major e-commerce channels. This pipeline answers key business questions:

- **Which Brazilian states drive the most revenue — and why?**
- **How do weather conditions in São Paulo affect consumer purchasing behaviour?**
- **What are the late delivery rates, and which regions perform worst?**
- **What is the monthly revenue growth trend from 2017–2018?**

### Dataset

The [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) contains **~100,000 orders** from 2016–2018 across 8 relational CSV files, enriched with real São Paulo weather data fetched from the [Open-Meteo Archive API](https://open-meteo.com/).

---

## 🏗️ Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES (Extract)                    │
│                                                                  │
│  ┌──────────────┐   ┌───────────────────┐   ┌───────────────┐   │
│  │ Olist CSVs   │   │ Open-Meteo REST   │   │ Parquet File  │   │
│  │ (8 tables)   │   │ API — Weather     │   │ olist_orders  │   │
│  │ Source 1 ✅  │   │ Source 2 ✅       │   │ Source 3 ✅   │   │
│  └──────┬───────┘   └────────┬──────────┘   └──────┬────────┘   │
└─────────┼────────────────────┼─────────────────────┼────────────┘
          │                    │                      │
          └────────────────────┴──────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   TRANSFORM — Apache PySpark                     │
│                                                                  │
│  • SparkSession (local[*] — all CPU cores)                       │
│  • Data cleaning & type casting                                  │
│  • Distributed joins (orders + customers + payments + items)     │
│  • Weather enrichment join                                       │
│  • Aggregations → monthly revenue, state revenue, weather impact │
│  • Output: 4 × Parquet files in data/transformed/               │
│                                                                  │
│  Fallback: 02_transform.py (pandas) if Java not available        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      LOAD — DuckDB                               │
│                                                                  │
│  • Read Parquet → CREATE TABLE (columnar, in-process)            │
│  • Tables: orders_full, monthly_revenue,                         │
│            revenue_by_state, weather_impact                      │
│  • Business insight queries & KPI validation                     │
│  • analytics.duckdb (~12 MB analytical database)                 │
└──────────────────────────────┬──────────────────────────────────┘
                               │
               ┌───────────────┴───────────────┐
               │                               │
               ▼                               ▼
┌──────────────────────────┐   ┌───────────────────────────────┐
│  ORCHESTRATION — Airflow │   │  TRANSFORMATION MODELS — dbt  │
│                          │   │                               │
│  DAG: olist_etl_pipeline │   │  Staging:                     │
│  Schedule: @daily        │   │    stg_orders (view)          │
│                          │   │    stg_weather (view)         │
│  Tasks:                  │   │  Marts:                       │
│  1. extract_data         │   │    monthly_revenue (table)    │
│  2. spark_transform      │   │    revenue_by_state (table)   │
│  3. load_to_duckdb       │   │    weather_impact (table)     │
│  4. validate_quality     │   │  Tests: 3 custom SQL tests    │
│  5. generate_report      │   │                               │
│  6. cleanup_temp         │   └───────────────────────────────┘
└──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────┐
│               DASHBOARD — Dash + Plotly (BI Tool)                │
│                                                                  │
│  • 5 KPI cards (orders, revenue, avg value, delivery, late rate) │
│  • Revenue & orders trend (line chart with area fill)            │
│  • Orders heatmap (month × year)                                 │
│  • Choropleth map — revenue by Brazilian state                   │
│  • Donut chart — top 8 states revenue share                      │
│  • Scatter — temperature vs order value                          │
│  • Bar chart — weather impact on orders                          │
│  • Histogram — delivery time distribution                        │
│  • Gauge — on-time delivery rate                                 │
│  • Interactive data table with search                            │
│  • Year filter + revenue range slider                            │
│  • Dark / Light theme toggle                                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
olist-etl-project/
│
├── 📁 notebooks/                       # ETL scripts
│   ├── 01_extract.py                  # Extract: CSVs + Weather API + Parquet
│   ├── 02_transform_spark.py          # Transform: Apache PySpark (primary)
│   ├── 02_transform.py                # Transform: pandas fallback (no Java)
│   └── 03_load.py                     # Load: DuckDB + Plotly dashboard HTML
│
├── 📁 dags/                            # Airflow orchestration
│   └── olist_etl_dag.py               # DAG: 6-task daily pipeline
│
├── 📁 dbt/                             # dbt transformation models (bonus)
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml            # DuckDB source definitions + tests
│   │   │   ├── stg_orders.sql         # Cleaned orders view
│   │   │   └── stg_weather.sql        # Weather observations view
│   │   ├── marts/
│   │   │   ├── monthly_revenue.sql    # Monthly KPI table
│   │   │   ├── revenue_by_state.sql   # State performance table
│   │   │   └── weather_impact.sql     # Weather impact table
│   │   └── schema.yml                 # Column-level tests for all models
│   └── tests/
│       ├── assert_positive_revenue.sql
│       ├── assert_late_rate_reasonable.sql
│       └── assert_sp_top_state.sql
│
├── 📁 dashboard/                       # Interactive BI dashboard
│   ├── app.py                         # Dash app (8 chart types, dark mode)
│   └── screenshots/
│       └── dashboard.png
│
├── 📁 data/                            # Data storage (not committed to git)
│   ├── raw/                           # Original Olist CSV files
│   ├── parquet/                       # Intermediate Parquet + weather CSV
│   └── transformed/                   # Final Parquet files from PySpark
│
├── 📄 run_complete_etl.py             # Single-command pipeline runner
├── 📄 requirements.txt                # Python dependencies
├── 📄 .gitignore
└── 📄 analytics.duckdb                # Analytical database (not committed)
```

---

## 🛠️ Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Data Processing** | Apache PySpark | ≥ 3.4 | Distributed transformation |
| **Analytics DB** | DuckDB | ≥ 0.10 | High-performance OLAP |
| **Orchestration** | Apache Airflow | ≥ 2.6 | Scheduled pipeline (bonus) |
| **Transformation Models** | dbt + dbt-duckdb | ≥ 1.6 | SQL modelling + tests (bonus) |
| **Dashboard** | Dash + Plotly | ≥ 2.14 | Interactive BI |
| **Storage** | Apache Parquet | — | Columnar intermediate storage |
| **Weather API** | Open-Meteo | — | São Paulo historical weather |
| **Language** | Python | ≥ 3.9 | All pipeline code |

---

## 📦 Installation & Setup

### Prerequisites

- **Python 3.9+**
- **Java 8 or 11** — required for PySpark  
  Install: https://adoptium.net/ → set `JAVA_HOME`
- **Git**

### Step 1 — Clone Repository

```bash
git clone https://github.com/simegnew1213/Big_Data_olist-etl-project.git
cd Big_Data_olist-etl-project
```

### Step 2 — Create Virtual Environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

### Step 3 — Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4 — Download the Dataset

Download the [Olist dataset from Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place all CSV files inside:

```
data/raw/
├── olist_orders_dataset.csv
├── olist_customers_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
└── product_category_name_translation.csv
```

### Step 5 — Run the Pipeline

#### ▶️ Option A: Single Command (recommended)

```bash
python run_complete_etl.py
```

#### ▶️ Option B: Step by Step

```bash
# 1. Extract
python notebooks/01_extract.py

# 2. Transform (PySpark — requires Java)
python notebooks/02_transform_spark.py

# 2b. Transform fallback (pandas — no Java needed)
python notebooks/02_transform.py

# 3. Load into DuckDB
python notebooks/03_load.py
```

#### ▶️ Option C: Airflow Orchestration (bonus)

```bash
export AIRFLOW_HOME=./airflow
airflow db init
airflow users create --username admin --password admin \
    --firstname Admin --lastname User --role Admin \
    --email admin@example.com

# Start scheduler (terminal 1)
airflow scheduler

# Start webserver (terminal 2)
airflow webserver --port 8080
# Open http://localhost:8080 → trigger DAG: olist_etl_pipeline
```

#### ▶️ Option D: Dashboard Only (data must already be loaded)

```bash
python dashboard/app.py
# Open http://localhost:8050
```

#### ▶️ Option E: dbt Models (bonus)

```bash
cd dbt
pip install dbt-duckdb

# Set path to your database
export DUCKDB_PATH=../analytics.duckdb   # Linux/macOS
set DUCKDB_PATH=..\analytics.duckdb      # Windows

dbt run       # Build all models
dbt test      # Run all schema + custom tests
dbt docs generate && dbt docs serve   # Browse documentation
```

---

## 📊 Key Business Insights

### 🎯 KPIs (from analytics.duckdb)

| Metric | Value |
|--------|-------|
| **Total Delivered Orders** | 96,478 |
| **Total Revenue** | R$ 15,422,461.77 |
| **Average Order Value** | R$ 159.86 |
| **Average Delivery Time** | 12.5 days |
| **Late Delivery Rate** | 6.77% |

### 🏆 Top 5 States by Revenue

| Rank | State | Revenue |
|------|-------|---------|
| 1 | São Paulo (SP) | R$ 5,770,000+ |
| 2 | Rio de Janeiro (RJ) | R$ 2,060,000+ |
| 3 | Minas Gerais (MG) | R$ 1,820,000+ |
| 4 | Rio Grande do Sul (RS) | R$ 800,000+ |
| 5 | Paraná (PR) | R$ 750,000+ |

### 🌤️ Weather Impact

| Condition | Avg Order Value |
|-----------|----------------|
| No Rain | R$ 161.81 |
| Light Rain | R$ 159.20 |
| Heavy Rain | R$ 157.40 |

### 📈 Monthly Trend

- **Peak Month**: November 2017 — R$ 1.15M (Black Friday)
- **Growth**: Steady +15% month-over-month from Jan–Nov 2017
- **Seasonal Pattern**: Q4 outperforms Q1–Q3 consistently

---

## 📸 Dashboard Preview

![Dashboard Screenshot](dashboard/screenshots/dashboard.png)

**Dashboard features:**
- 🌙 Dark / ☀️ Light theme toggle
- 📅 Year filter + revenue range slider
- 📈 Revenue & orders trend line chart
- 🗓️ Orders heatmap (month × year)
- 🗺️ Choropleth map — revenue by Brazilian state
- 🍩 Donut chart — top 8 states revenue share
- 🔵 Scatter plot — temperature vs order value
- 🌧️ Weather impact bar chart
- 📦 Delivery time histogram
- ⏱️ On-time rate gauge
- 📋 Searchable states data table

---

## 🗺️ Airflow DAG

File: [`dags/olist_etl_dag.py`](dags/olist_etl_dag.py)

```
extract_data → spark_transform → load_to_duckdb → validate_quality → generate_report → cleanup_temp
```

- **Schedule**: `@daily`
- **Retries**: 1 (5 min delay)
- **Java fallback**: `spark_transform` automatically falls back to pandas if Java is unavailable
- **Validation**: Checks row counts and null values before generating report

---

## 🔄 dbt Transformation Models (Bonus)

File structure: [`dbt/`](dbt/)

```
Staging layer (views — no storage cost):
  stg_orders  → cleans orders_full: type casts, null filters, derived fields
  stg_weather → distinct daily weather observations

Marts layer (tables — materialized for fast queries):
  monthly_revenue    → monthly KPIs with median, late rate %, avg basket size
  revenue_by_state   → state performance with RANK() window function
  weather_impact     → orders split by rain category with avg temp

Custom tests (SQL — must return 0 rows):
  assert_positive_revenue      → every month must have revenue > 0
  assert_late_rate_reasonable  → all states must have late rate 0–50%
  assert_sp_top_state          → São Paulo must always be #1 by revenue
```

Run: `cd dbt && dbt run && dbt test`

---

## 👥 Team Contributions

| Member | Role | Primary Contributions |
|--------|------|-----------------------|
| **Simegnew** | Data Engineer & Team Lead | PySpark transformation pipeline (`02_transform_spark.py`), DuckDB loading (`03_load.py`), Airflow DAG design |
| **[Member 2]** | ETL Developer | Data extraction (`01_extract.py`), Weather API integration, Parquet pipeline |
| **[Member 3]** | BI Developer | Interactive Dash dashboard (`dashboard/app.py`), Plotly charts, dark/light theme |
| **[Member 4]** | Data Modelling | dbt models & tests, DuckDB schema design, data quality validation |

---

## 🔧 Environment Variables

```bash
# Optional overrides
JAVA_HOME=/path/to/java           # Required for PySpark
DUCKDB_PATH=./analytics.duckdb   # DuckDB file path
AIRFLOW_HOME=./airflow            # Airflow home directory
```

---

## 🧪 Data Quality Validation

```bash
# Quick validation from command line
python -c "
import duckdb
conn = duckdb.connect('analytics.duckdb')
for t in ['orders_full','monthly_revenue','revenue_by_state','weather_impact']:
    n = conn.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
    print(f'{t}: {n:,} rows')
conn.close()
"

# dbt tests
cd dbt && dbt test
```

---

## 📞 Troubleshooting

| Problem | Solution |
|---------|----------|
| `JAVA_HOME not set` | Install Java 8/11, set `JAVA_HOME`, re-run. Or use `02_transform.py` (pandas fallback) |
| `ModuleNotFoundError: pyspark` | Run `pip install pyspark` |
| `analytics.duckdb not found` | Run the full pipeline first: `python run_complete_etl.py` |
| `Dashboard shows no data` | Ensure `analytics.duckdb` exists and has tables loaded |
| `Airflow DAG not found` | Copy `dags/` to your `$AIRFLOW_HOME/dags/` folder |
| `dbt connection error` | Check `DUCKDB_PATH` env var points to the correct `.duckdb` file |

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

## 🙏 Acknowledgments

- [Olist](https://olist.com) — for the public e-commerce dataset (Kaggle)
- [Open-Meteo](https://open-meteo.com) — for free historical weather API
- [Apache Spark](https://spark.apache.org) — distributed computing
- [DuckDB](https://duckdb.org) — blazing-fast analytical database
- [dbt](https://www.getdbt.com) — data transformation modelling

---

*Built for the Advanced Data Engineering Assignment · May 2026*