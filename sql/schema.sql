-- File: sql/schema.sql
-- MySQL schema for ETL results storage

CREATE DATABASE IF NOT EXISTS etl_results;
USE etl_results;

-- Table 1: Run-level metadata (one row per execution invocation)
CREATE TABLE IF NOT EXISTS run_metadata (
    run_id              INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_name       VARCHAR(20) NOT NULL,
    query_name          VARCHAR(20) NOT NULL,
    batch_strategy      VARCHAR(20) NOT NULL,
    configured_batch_size INT,
    total_records_processed INT NOT NULL DEFAULT 0,
    total_malformed_records INT NOT NULL DEFAULT 0,
    total_batches       INT NOT NULL DEFAULT 0,
    average_batch_size  DECIMAL(12,2) NOT NULL DEFAULT 0,
    runtime_seconds     DECIMAL(10,3) NOT NULL DEFAULT 0,
    execution_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Table 2: Batch-level metadata (one row per batch per run)
CREATE TABLE IF NOT EXISTS batch_metadata (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    run_id              INT NOT NULL,
    batch_id            INT NOT NULL,
    batch_size          INT NOT NULL,
    malformed_count     INT NOT NULL DEFAULT 0,
    valid_records       INT NOT NULL,
    batch_runtime_seconds DECIMAL(10,3),
    FOREIGN KEY (run_id) REFERENCES run_metadata(run_id) ON DELETE CASCADE
);

-- Table 3: Query 1 results - Daily Traffic Summary
CREATE TABLE IF NOT EXISTS query1_daily_traffic (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    run_id              INT NOT NULL,
    batch_id            INT NOT NULL,
    log_date            VARCHAR(20) NOT NULL,
    status_code         INT NOT NULL,
    request_count       INT NOT NULL,
    total_bytes         BIGINT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES run_metadata(run_id) ON DELETE CASCADE
);

-- Table 4: Query 2 results - Top Requested Resources
CREATE TABLE IF NOT EXISTS query2_top_resources (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    run_id              INT NOT NULL,
    batch_id            INT NOT NULL,
    resource_path       VARCHAR(2048) NOT NULL,
    request_count       INT NOT NULL,
    total_bytes         BIGINT NOT NULL,
    distinct_host_count INT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES run_metadata(run_id) ON DELETE CASCADE
);

-- Table 5: Query 3 results - Hourly Error Analysis
CREATE TABLE IF NOT EXISTS query3_hourly_errors (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    run_id              INT NOT NULL,
    batch_id            INT NOT NULL,
    log_date            VARCHAR(20) NOT NULL,
    log_hour            VARCHAR(5) NOT NULL,
    error_request_count INT NOT NULL,
    total_request_count INT NOT NULL,
    error_rate          DECIMAL(8,6) NOT NULL,
    distinct_error_hosts INT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES run_metadata(run_id) ON DELETE CASCADE
);

-- Table 6: Malformed record summary per batch
CREATE TABLE IF NOT EXISTS malformed_summary (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    run_id              INT NOT NULL,
    batch_id            INT NOT NULL,
    malformed_count     INT NOT NULL,
    sample_lines        TEXT,
    FOREIGN KEY (run_id) REFERENCES run_metadata(run_id) ON DELETE CASCADE
);
