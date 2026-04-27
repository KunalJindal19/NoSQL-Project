-- NASA Log Analytics — MySQL Reporting Schema

CREATE DATABASE IF NOT EXISTS nasa_project;
USE nasa_project;

-- Single results table with JSON payload for flexibility
CREATE TABLE IF NOT EXISTS query_results (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_name   VARCHAR(50)    NOT NULL,
    run_id          VARCHAR(100)   NOT NULL,
    batch_id        INT            NOT NULL,
    query_name      VARCHAR(100)   NOT NULL,
    result_json     JSON           NOT NULL,
    execution_time  DOUBLE         NOT NULL  COMMENT 'Runtime in seconds',
    batch_size      INT            NOT NULL,
    total_records   INT            NOT NULL,
    num_batches     INT            NOT NULL,
    avg_batch_size  DOUBLE         NOT NULL,
    malformed_count INT            NOT NULL DEFAULT 0,
    executed_at     TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_pipeline (pipeline_name),
    INDEX idx_run (run_id),
    INDEX idx_query (query_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
