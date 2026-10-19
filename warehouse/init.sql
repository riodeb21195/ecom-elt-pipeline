CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS pipeline_metadata;


CREATE TABLE IF NOT EXISTS pipeline_metadata.load_runs (
    load_run_id      VARCHAR PRIMARY KEY,
    source_file      VARCHAR NOT NULL,
    table_name       VARCHAR NOT NULL,
    rows_loaded      BIGINT NOT NULL,
    row_count_check  BOOLEAN NOT NULL,
    checksum         VARCHAR,
    started_at       TIMESTAMP NOT NULL,
    finished_at      TIMESTAMP,
    status           VARCHAR NOT NULL,  
    error_message    VARCHAR
);



CREATE TABLE IF NOT EXISTS raw.orders (
    order_id                       VARCHAR,
    customer_state                 VARCHAR,
    product_category               VARCHAR,
    order_status                   VARCHAR,
    order_status_v2                VARCHAR,
    order_purchase_timestamp       TIMESTAMP,
    order_delivered_timestamp      TIMESTAMP,
    order_estimated_delivery_days  INTEGER,
    review_score                   INTEGER,
    updated_at                     TIMESTAMP,
    _loaded_at                     TIMESTAMP DEFAULT current_timestamp,
    _load_run_id                   VARCHAR
);

ALTER TABLE pipeline_metadata.load_runs
ADD COLUMN IF NOT EXISTS file_sha256 VARCHAR;