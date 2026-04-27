-- NASA Log Analytics — MySQL Reporting Schema (One Table Per Query)

CREATE DATABASE IF NOT EXISTS nasa_project;
USE nasa_project;

-- Metadata tracking for CLI reports
CREATE TABLE IF NOT EXISTS run_metadata (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_name   VARCHAR(50)    NOT NULL,
    run_id          VARCHAR(100)   NOT NULL,
    query_name      VARCHAR(100)   NOT NULL,
    execution_time  DOUBLE         NOT NULL  COMMENT 'Runtime in seconds',
    batch_size      INT            NOT NULL,
    total_records   INT            NOT NULL,
    num_batches     INT            NOT NULL,
    avg_batch_size  DOUBLE         NOT NULL,
    malformed_count INT            NOT NULL DEFAULT 0,
    executed_at     TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_run (run_id),
    INDEX idx_query (query_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Query 1 Results: Daily Traffic Summary
CREATE TABLE IF NOT EXISTS query1_results (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_name   VARCHAR(50)    NOT NULL,
    run_id          VARCHAR(100)   NOT NULL,
    execution_time  DOUBLE         NOT NULL,
    log_date        VARCHAR(20)    NOT NULL,
    status_code     INT            NOT NULL,
    request_count   BIGINT         NOT NULL,
    total_bytes     BIGINT         NOT NULL,
    
    INDEX idx_run (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Query 2 Results: Top 20 Requested Resources
CREATE TABLE IF NOT EXISTS query2_results (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_name       VARCHAR(50)    NOT NULL,
    run_id              VARCHAR(100)   NOT NULL,
    execution_time      DOUBLE         NOT NULL,
    resource_path       VARCHAR(1000)  NOT NULL,
    request_count       BIGINT         NOT NULL,
    total_bytes         BIGINT         NOT NULL,
    distinct_host_count BIGINT         NOT NULL,
    
    INDEX idx_run (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Query 3 Results: Hourly Error Analysis
CREATE TABLE IF NOT EXISTS query3_results (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_name       VARCHAR(50)    NOT NULL,
    run_id              VARCHAR(100)   NOT NULL,
    execution_time      DOUBLE         NOT NULL,
    log_date            VARCHAR(20)    NOT NULL,
    log_hour            INT            NOT NULL,
    error_request_count BIGINT         NOT NULL,
    total_request_count BIGINT         NOT NULL,
    error_rate          DOUBLE         NOT NULL,
    distinct_error_hosts BIGINT        NOT NULL,
    
    INDEX idx_run (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
