"""
Extracts the Olist CSVs (real or synthetic — same shape either way) and
loads them into the `raw` schema of the warehouse Postgres.

Usage:
    python scripts/load_raw.py                 # incremental (normal daily run)
    python scripts/load_raw.py --full-refresh   # ignore watermark, reload all history
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import data_quality as dq

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# table_name -> (csv filename, primary key cols for the uniqueness check, required not-null cols)
FULL_REFRESH_TABLES = {
    "customers": ("olist_customers_dataset.csv", ["customer_id"], ["customer_id"]),
    "sellers": ("olist_sellers_dataset.csv", ["seller_id"], ["seller_id"]),
    "products": ("olist_products_dataset.csv", ["product_id"], ["product_id"]),
    "product_category_name_translation": ("product_category_name_translation.csv", ["product_category_name"], []),
    "geolocation": ("olist_geolocation_dataset.csv", [], []),
    "order_items": ("olist_order_items_dataset.csv", ["order_id", "order_item_id"], ["order_id", "product_id"]),
    "order_payments": ("olist_order_payments_dataset.csv", ["order_id", "payment_sequential"], ["order_id"]),
    "order_reviews": ("olist_order_reviews_dataset.csv", ["review_id"], ["order_id"]),
}


def get_engine() -> Engine:
    host = os.environ.get("DWH_HOST", "localhost")
    port = os.environ.get("DWH_PORT", "5433")
    db = os.environ.get("DWH_DB", "ecommerce_dwh")
    user = os.environ.get("DWH_USER", "dwh_user")
    password = os.environ.get("DWH_PASSWORD", "dwh_password")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def log_audit(engine: Engine, table: str, started: datetime, extracted: int,
              loaded: int, rejected: int, status: str, error: str | None) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO meta.load_audit_log
                (source_table, run_started_at, run_finished_at, rows_extracted,
                 rows_loaded, rows_rejected, status, error_message)
            VALUES
                (:table, :started, now(), :extracted, :loaded, :rejected, :status, :error)
        """), {
            "table": table, "started": started, "extracted": extracted,
            "loaded": loaded, "rejected": rejected, "status": status, "error": error,
        })


def log_dq_results(engine: Engine, results: list[dq.DQResult]) -> None:
    if not results:
        return
    with engine.begin() as conn:
        for r in results:
            conn.execute(text("""
                INSERT INTO meta.data_quality_results (check_name, table_name, passed, failed_rows, details)
                VALUES (:check_name, :table_name, :passed, :failed_rows, :details)
            """), vars(r))


def load_full_refresh(engine: Engine, table: str, csv_file: str,
                       pk_cols: list[str], not_null_cols: list[str]) -> bool:
    started = datetime.utcnow()
    path = DATA_DIR / csv_file
    if not path.exists():
        print(f"  [skip] {csv_file} not found in {DATA_DIR}")
        return True

    df = pd.read_csv(path)

    checks = []
    if not_null_cols:
        checks.append(lambda: dq.check_not_null(df, table, not_null_cols))
    if pk_cols:
        checks.append(lambda: dq.check_unique(df, table, pk_cols))
    checks.append(lambda: dq.check_row_count_min(df, table, minimum=1))
    results = dq.run_checks(checks)
    log_dq_results(engine, results)

    if dq.any_failed(results):
        failed = [r for r in results if not r.passed]
        error = "; ".join(f"{r.check_name}: {r.details}" for r in failed)
        print(f"  [FAIL] DQ checks failed for raw.{table}: {error}")
        log_audit(engine, f"raw.{table}", started, len(df), 0, sum(r.failed_rows for r in failed), "failed", error)
        return False

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE raw.{table}"))
    df.to_sql(table, engine, schema="raw", if_exists="append", index=False, method="multi", chunksize=1000)

    log_audit(engine, f"raw.{table}", started, len(df), len(df), 0, "success", None)
    print(f"  [ok] raw.{table}: {len(df)} rows (full refresh)")
    return True


def load_orders_incremental(engine: Engine, full_refresh: bool) -> bool:
    table = "orders"
    started = datetime.utcnow()
    path = DATA_DIR / "olist_orders_dataset.csv"
    if not path.exists():
        print(f"  [skip] orders csv not found in {DATA_DIR}")
        return True

    df = pd.read_csv(path, parse_dates=[
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ])

    with engine.begin() as conn:
        watermark = conn.execute(text(
            "SELECT last_watermark_value FROM meta.load_watermarks WHERE source_table = 'raw.orders'"
        )).scalar()

    if not full_refresh and watermark is not None:
        before = len(df)
        df = df[df["order_purchase_timestamp"] > watermark]
        print(f"  [incremental] watermark={watermark} -> {len(df)}/{before} new rows")

    results = dq.run_checks([
        lambda: dq.check_not_null(df, table, ["order_id", "customer_id", "order_purchase_timestamp"]),
        lambda: dq.check_unique(df, table, ["order_id"]),
        lambda: dq.check_values_in_set(df, table, "order_status", {
            "delivered", "shipped", "processing", "canceled", "invoiced",
            "unavailable", "created", "approved",
        }),
    ])
    log_dq_results(engine, results)

    if dq.any_failed(results):
        failed = [r for r in results if not r.passed]
        error = "; ".join(f"{r.check_name}: {r.details}" for r in failed)
        print(f"  [FAIL] DQ checks failed for raw.orders: {error}")
        log_audit(engine, "raw.orders", started, len(df), 0, sum(r.failed_rows for r in failed), "failed", error)
        return False

    if full_refresh:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE raw.orders"))

    if len(df) > 0:
        df.to_sql("orders", engine, schema="raw", if_exists="append", index=False, method="multi", chunksize=1000)
        new_watermark = df["order_purchase_timestamp"].max()
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE meta.load_watermarks
                SET last_watermark_value = :wm, last_run_at = now(),
                    last_run_row_count = :n, last_run_status = 'success'
                WHERE source_table = 'raw.orders'
            """), {"wm": new_watermark, "n": len(df)})

    log_audit(engine, "raw.orders", started, len(df), len(df), 0, "success", None)
    print(f"  [ok] raw.orders: {len(df)} rows loaded")
    return True


def main(full_refresh: bool) -> None:
    engine = get_engine()
    print(f"Loading raw data ({'FULL REFRESH' if full_refresh else 'incremental'}) from {DATA_DIR}")

    all_ok = True

    print("orders (incremental fact source):")
    all_ok &= load_orders_incremental(engine, full_refresh)

    print("reference / full-refresh tables:")
    for table, (csv_file, pk_cols, not_null_cols) in FULL_REFRESH_TABLES.items():
        all_ok &= load_full_refresh(engine, table, csv_file, pk_cols, not_null_cols)

    if not all_ok:
        print("\nOne or more tables failed data quality checks. See meta.data_quality_results / meta.load_audit_log.")
        sys.exit(1)

    print("\nAll raw tables loaded successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full-refresh", action="store_true", help="Ignore watermark and reload full order history")
    args = parser.parse_args()
    main(args.full_refresh)