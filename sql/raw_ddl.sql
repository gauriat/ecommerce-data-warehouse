-- ============================================================================
-- Raw / landing zone DDL
-- Schemas: raw (untouched landed data), analytics (dbt-built warehouse), meta (pipeline control)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS meta;

-- ----------------------------------------------------------------------------
-- meta.load_watermarks: tracks the last successfully loaded timestamp per
-- source table, enabling incremental (rather than full-refresh) extraction.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.load_watermarks (
    source_table          TEXT PRIMARY KEY,
    watermark_column      TEXT NOT NULL,
    last_watermark_value  TIMESTAMP NOT NULL DEFAULT '1970-01-01',
    last_run_at           TIMESTAMP,
    last_run_row_count    INTEGER,
    last_run_status       TEXT
);

-- ----------------------------------------------------------------------------
-- meta.load_audit_log: append-only log of every extract/load run, for
-- observability and debugging pipeline reliability issues.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.load_audit_log (
    id               SERIAL PRIMARY KEY,
    source_table     TEXT NOT NULL,
    run_started_at   TIMESTAMP NOT NULL,
    run_finished_at  TIMESTAMP,
    rows_extracted   INTEGER,
    rows_loaded      INTEGER,
    rows_rejected    INTEGER,
    status           TEXT,          -- 'success' | 'failed' | 'partial'
    error_message    TEXT
);

-- ----------------------------------------------------------------------------
-- meta.data_quality_results: results of DQ checks run after each load
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.data_quality_results (
    id             SERIAL PRIMARY KEY,
    check_name     TEXT NOT NULL,
    table_name     TEXT NOT NULL,
    run_at         TIMESTAMP NOT NULL DEFAULT now(),
    passed         BOOLEAN NOT NULL,
    failed_rows    INTEGER,
    details        TEXT
);

-- ----------------------------------------------------------------------------
-- Raw landing tables — column shapes mirror the Olist CSVs 1:1.
-- Kept intentionally "dumb" (mostly TEXT) since raw zone should not lose data
-- to premature casting; typing/cleaning happens in dbt staging models.
-- Each has a _loaded_at audit column stamped by the loader.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id                TEXT,
    customer_unique_id         TEXT,
    customer_zip_code_prefix   TEXT,
    customer_city              TEXT,
    customer_state             TEXT,
    _loaded_at                 TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id                       TEXT,
    customer_id                    TEXT,
    order_status                   TEXT,
    order_purchase_timestamp       TIMESTAMP,
    order_approved_at              TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP,
    _loaded_at                     TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_id             TEXT,
    order_item_id        TEXT,
    product_id           TEXT,
    seller_id            TEXT,
    shipping_limit_date  TIMESTAMP,
    price                NUMERIC,
    freight_value        NUMERIC,
    _loaded_at           TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.order_payments (
    order_id              TEXT,
    payment_sequential    TEXT,
    payment_type          TEXT,
    payment_installments  INTEGER,
    payment_value         NUMERIC,
    _loaded_at            TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.order_reviews (
    review_id                TEXT,
    order_id                 TEXT,
    review_score             TEXT,
    review_comment_title     TEXT,
    review_comment_message   TEXT,
    review_creation_date     TIMESTAMP,
    review_answer_timestamp  TIMESTAMP,
    _loaded_at                TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id                   TEXT,
    product_category_name        TEXT,
    product_name_lenght          TEXT,
    product_description_lenght   TEXT,
    product_photos_qty           TEXT,
    product_weight_g             NUMERIC,
    product_length_cm            NUMERIC,
    product_height_cm            NUMERIC,
    product_width_cm             NUMERIC,
    _loaded_at                   TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.sellers (
    seller_id                TEXT,
    seller_zip_code_prefix   TEXT,
    seller_city              TEXT,
    seller_state             TEXT,
    _loaded_at                TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.geolocation (
    geolocation_zip_code_prefix  TEXT,
    geolocation_lat              NUMERIC,
    geolocation_lng              NUMERIC,
    geolocation_city             TEXT,
    geolocation_state            TEXT,
    _loaded_at                   TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.product_category_name_translation (
    product_category_name          TEXT,
    product_category_name_english  TEXT,
    _loaded_at                     TIMESTAMP DEFAULT now()
);

-- Seed watermark rows (orders is the only table we load incrementally by
-- purchase timestamp; the rest are small reference/dimension-ish sources
-- that are cheap to fully refresh on every run)
INSERT INTO meta.load_watermarks (source_table, watermark_column)
VALUES ('raw.orders', 'order_purchase_timestamp')
ON CONFLICT (source_table) DO NOTHING;