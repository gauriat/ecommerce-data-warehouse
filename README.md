# E-Commerce Data Warehouse

A production-style dimensional data warehouse built on the Olist Brazilian
e-commerce dataset — raw ingestion, incremental loading, a dbt-modeled star
schema, Airflow orchestration, and CI, all running locally via Docker Compose.

## Architecture

Olist CSVs → Python ETL (raw, incremental) → Postgres (raw schema)
→ dbt (staging → intermediate → marts) → Postgres (star schema)
→ Airflow (orchestrates the above on a schedule)
→ GitHub Actions (validates every push against a fresh warehouse)


**Why these choices:**
- **Postgres** as both the raw landing zone and the warehouse — schema-separated (`raw` / `analytics` / `meta`) rather than three separate databases, to keep the local setup simple while still modeling the layers a real warehouse would have.
- **dbt** for transformation — SQL-based, testable, self-documenting. The star schema (facts + dimensions) is the standard shape for BI/analytics queries.
- **Airflow** for orchestration — retries, scheduling, and a UI for run history, instead of a cron job with no visibility into failures.
- **Incremental loading** on the `orders` table via a stored watermark, rather than full-refreshing a table that only grows — the reference tables (customers, products, etc.) are small enough to just full-refresh each run.

## Tech stack

PostgreSQL 16 · Python 3.11 (pandas, SQLAlchemy) · dbt-core 1.8 · Apache Airflow 2.9 · Docker Compose · GitHub Actions

## Project structure

├── docker-compose.yml # Postgres (warehouse + Airflow metadata), Airflow webserver/scheduler

├── Dockerfile.airflow # Custom Airflow image with dbt + Python deps

├── requirements.txt

├── sql/

│ └── raw_ddl.sql # raw / analytics / meta schemas + landing tables

├── scripts/

│ ├── generate_sample_data.py # synthetic Olist-shaped data (no Kaggle account needed)

│ ├── download_data.py # real Kaggle dataset download

│ ├── data_quality.py # reusable DQ check functions

│ └── load_raw.py # incremental extract-load with DQ checks + audit logging

├── dbt/

│ ├── profiles.yml

│ └── ecommerce_dwh/

│ ├── models/

│ │ ├── staging/ # 1:1 cleaned views over raw sources

│ │ ├── intermediate/ # reusable joined models

│ │ └── marts/core/ # star schema: dim_, fact_

│ └── tests/ # custom business-rule tests

├── dags/

│ └── ecommerce_elt_dag.py # Airflow DAG: extract → freshness → dbt run → dbt test

└── .github/workflows/ci.yml # dbt build against a fresh Postgres on every push

## The star schema

**Dimensions:** `dim_customers`, `dim_products`, `dim_sellers`, `dim_date`
**Facts:** `fact_orders` (order grain), `fact_order_items` (line-item grain), `fact_payments`, `fact_reviews`

`fact_orders` rolls up item/payment/review totals onto each order for
order-level analysis; `fact_order_items` keeps the finer grain for
product/seller-level analysis that `fact_orders` can't answer.

## Data quality & testing

- **Pre-load checks** (Python, in `load_raw.py`): not-null, uniqueness, accepted values, row-count sanity — logged to `meta.data_quality_results` and `meta.load_audit_log`.
- **dbt schema tests**: `not_null`, `unique`, `relationships` (referential integrity), `accepted_values`, `dbt_utils.accepted_range` — 76 tests across staging and marts.
- **Custom business-rule test**: `assert_order_value_matches_payments.sql` reconciles line-item totals against payments — this caught a real bug in the synthetic data generator during development (freight wasn't being included in payment totals).

## Running it locally

Prerequisites: Docker Desktop, Python 3.11+

```powershell
# 1. Clone and enter the repo
git clone https://github.com/gauriat/ecommerce-data-warehouse.git
cd ecommerce-data-warehouse

# 2. Bring up the full stack
docker compose up -d --build

# 3. Generate data and load it (or use scripts/download_data.py for the real dataset)
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install pandas SQLAlchemy psycopg2-binary
python scripts/generate_sample_data.py --n-orders 5000

$env:DWH_HOST="localhost"; $env:DWH_PORT="5433"; $env:DWH_DB="ecommerce_dwh"
$env:DWH_USER="dwh_user"; $env:DWH_PASSWORD="dwh_password"
python scripts/load_raw.py

# 4. Build the warehouse
pip install dbt-core==1.8.8 dbt-postgres==1.8.2
$env:DBT_PROFILES_DIR="$PWD\dbt"
cd dbt\ecommerce_dwh
dbt deps
dbt build
```

Then visit **http://localhost:8080** (admin/admin) to see the pipeline running in Airflow, or run `dbt docs generate && dbt docs serve` for the model lineage graph.

## CI

Every push to `main` runs `.github/workflows/ci.yml`: spins up a fresh Postgres, generates synthetic data, loads it, and runs `dbt build` — catching broken models or failing tests before they'd ever reach a scheduled Airflow run.

## Challenges & what I learned

- A dbt test caught a real bug in the sample data generator (freight charges weren't included in payment totals) — debugged by tracing the failing test back to the generator script.
- Installing dbt and Airflow into the same Docker image hit a pip dependency conflict (protobuf version mismatch); fixed using Airflow's official constraints file so pip resolves compatible versions instead of grabbing the newest Airflow version by default.

## Possible extensions

- Swap synthetic data for the real Kaggle Olist dataset via `scripts/download_data.py`
- Add a BI layer (Metabase/Superset) on top of the marts schema
- Partition `fact_order_items` by date as data volume grows
- Add Slack/email alerting to the Airflow failure callback
