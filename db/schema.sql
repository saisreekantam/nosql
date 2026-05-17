-- Run this once to set up the reporting database
CREATE DATABASE nasa_etl;

\c nasa_etl;

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id            UUID PRIMARY KEY,
    pipeline_name     VARCHAR(20)  NOT NULL,
    run_timestamp     TIMESTAMP    NOT NULL DEFAULT NOW(),
    total_records     INTEGER,
    malformed_records INTEGER,
    batch_size        INTEGER,
    num_batches       INTEGER,
    avg_batch_size    FLOAT,
    runtime_seconds   FLOAT
);

CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id             SERIAL PRIMARY KEY,
    run_id         UUID        REFERENCES etl_runs(run_id),
    pipeline_name  VARCHAR(20),
    batch_id       INTEGER,
    execution_time TIMESTAMP,
    log_date       DATE,
    status_code    INTEGER,
    request_count  BIGINT,
    total_bytes    BIGINT
);

CREATE TABLE IF NOT EXISTS q2_top_resources (
    id                  SERIAL PRIMARY KEY,
    run_id              UUID        REFERENCES etl_runs(run_id),
    pipeline_name       VARCHAR(20),
    batch_id            INTEGER,
    execution_time      TIMESTAMP,
    resource_path       TEXT,
    request_count       BIGINT,
    total_bytes         BIGINT,
    distinct_host_count INTEGER
);

CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    id                   SERIAL PRIMARY KEY,
    run_id               UUID        REFERENCES etl_runs(run_id),
    pipeline_name        VARCHAR(20),
    batch_id             INTEGER,
    execution_time       TIMESTAMP,
    log_date             DATE,
    log_hour             SMALLINT,
    error_request_count  INTEGER,
    total_request_count  INTEGER,
    error_rate           FLOAT,
    distinct_error_hosts INTEGER
);

CREATE INDEX IF NOT EXISTS idx_q1_run      ON q1_daily_traffic(run_id);
CREATE INDEX IF NOT EXISTS idx_q2_run      ON q2_top_resources(run_id);
CREATE INDEX IF NOT EXISTS idx_q3_run      ON q3_hourly_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_runs_pipe   ON etl_runs(pipeline_name);
