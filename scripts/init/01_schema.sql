-- ====================================================
--  schema.sql
--  Database initialization for Silver & Gold layers
--  !! Bronze level should not be saved to the database
--  + initialization of pipeline state and outbox models
-- ====================================================

DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'admin1') THEN
      CREATE ROLE admin1 LOGIN PASSWORD 'admin1_password';
   END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS mart AUTHORIZATION admin1;
CREATE SCHEMA IF NOT EXISTS pipeline AUTHORIZATION admin1;

SET search_path TO mart, pipeline, public;


------------- SILVER LAYER

CREATE TABLE IF NOT EXISTS mart.silver_events (
    id SERIAL PRIMARY KEY,

    event_time      TIMESTAMP NOT NULL,
    session_id      VARCHAR(200) NOT NULL,
    user_id         VARCHAR(200),

    event_type      VARCHAR(100) NOT NULL,

    page_url        TEXT NOT NULL,
    page_path       TEXT NOT NULL,
    page_host       TEXT NOT NULL,

    page_section    TEXT,
    page_category   TEXT,
    page_item       TEXT,

    referrer_url    TEXT,

    product_id      VARCHAR(200),
    price           NUMERIC(10,2),

    ab_group        VARCHAR(50),

    scroll_depth    INTEGER,

    ip_address      VARCHAR(100),
    country         VARCHAR(100),
    city            VARCHAR(100),

    user_agent      TEXT,
    device_type     VARCHAR(100),
    browser_name    VARCHAR(100),

    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_session_id
    ON mart.silver_events(session_id);

CREATE INDEX IF NOT EXISTS idx_silver_event_time
    ON mart.silver_events(event_time);

CREATE INDEX IF NOT EXISTS idx_silver_event_type
    ON mart.silver_events(event_type);

CREATE INDEX IF NOT EXISTS idx_silver_product_id
    ON mart.silver_events(product_id);

CREATE INDEX IF NOT EXISTS idx_silver_page_url
    ON mart.silver_events(page_url);


------------- GOLD: PAGE VIEWS

CREATE TABLE IF NOT EXISTS mart.gold_page_views (
    id SERIAL PRIMARY KEY,

    event_time      TIMESTAMP NOT NULL,
    session_id      VARCHAR(200) NOT NULL,
    user_id         VARCHAR(200),

    page_url        TEXT NOT NULL,
    page_category   TEXT,
    page_item       TEXT,

    scroll_depth    INTEGER,
    ab_group        VARCHAR(50),

    created_at      TIMESTAMP DEFAULT NOW()

);

CREATE INDEX IF NOT EXISTS idx_gold_pageviews_session_id
    ON mart.gold_page_views(session_id);

CREATE INDEX IF NOT EXISTS idx_gold_pageviews_event_time
    ON mart.gold_page_views(event_time);

CREATE INDEX IF NOT EXISTS idx_gold_pageviews_page_url
    ON mart.gold_page_views(page_url);


------------- GOLD: PRODUCT EVENTS

CREATE TABLE IF NOT EXISTS mart.gold_product_events (
    id SERIAL PRIMARY KEY,

    event_time      TIMESTAMP NOT NULL,
    session_id      VARCHAR(200) NOT NULL,
    user_id         VARCHAR(200),

    product_id      VARCHAR(200) NOT NULL,
    price           NUMERIC(10,2),

    ab_group        VARCHAR(50),
    page_url        TEXT NOT NULL,

    created_at      TIMESTAMP DEFAULT NOW()

);

CREATE INDEX IF NOT EXISTS idx_gold_product_session_id
    ON mart.gold_product_events(session_id);

CREATE INDEX IF NOT EXISTS idx_gold_product_event_time
    ON mart.gold_product_events(event_time);

CREATE INDEX IF NOT EXISTS idx_gold_product_product_id
    ON mart.gold_product_events(product_id);


--------------------------------------------
------------- PIPELINE: PROCESSING STATE
--------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline.processing_state (
    id SERIAL PRIMARY KEY,

    dataset           TEXT    NOT NULL,
    layer             TEXT    NOT NULL,

    last_processed_ts TIMESTAMP,
    last_processed_id BIGINT,

    updated_at        TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_processing_state_dataset_layer
        UNIQUE (dataset, layer)
);

CREATE INDEX IF NOT EXISTS idx_processing_state_dataset_layer
    ON pipeline.processing_state(dataset, layer);


------------- PIPELINE: OUTBOX TASKS


CREATE TABLE IF NOT EXISTS pipeline.outbox_tasks (
    id BIGSERIAL PRIMARY KEY,

    event_type    TEXT    NOT NULL,
    dataset       TEXT    NOT NULL,
    layer         TEXT    NOT NULL,

    partition_key TEXT,


    status        TEXT    NOT NULL,
    attempts      INT     NOT NULL DEFAULT 0,

    next_retry_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_error    TEXT,

    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_outbox_tasks_event_dataset_layer_partition
        UNIQUE (event_type, dataset, layer, partition_key)
);


CREATE INDEX IF NOT EXISTS idx_outbox_tasks_status_next_retry
    ON pipeline.outbox_tasks(status, next_retry_at);


CREATE INDEX IF NOT EXISTS idx_outbox_tasks_dataset_layer_partition
    ON pipeline.outbox_tasks(dataset, layer, partition_key);


------------- PIPELINE: ETL RUNS

CREATE TABLE IF NOT EXISTS pipeline.etl_runs (
    id BIGSERIAL PRIMARY KEY,

    status                    TEXT NOT NULL,

    bronze_count              INT  NOT NULL DEFAULT 0,
    silver_count              INT  NOT NULL DEFAULT 0,
    gold_page_views_count     INT  NOT NULL DEFAULT 0,
    gold_product_events_count INT  NOT NULL DEFAULT 0,
    loaded_page_views_count   INT  NOT NULL DEFAULT 0,
    loaded_product_events_count INT NOT NULL DEFAULT 0,

    error_message             TEXT,

    started_at                TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at               TIMESTAMP,
    duration_seconds          NUMERIC(10,3)
);

CREATE INDEX IF NOT EXISTS idx_etl_runs_started_at
    ON pipeline.etl_runs(started_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_runs_status
    ON pipeline.etl_runs(status);


------------- GRANTS FOR admin1

GRANT USAGE ON SCHEMA mart TO admin1;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA mart TO admin1;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA mart TO admin1;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pipeline TO admin1;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA pipeline TO admin1;
