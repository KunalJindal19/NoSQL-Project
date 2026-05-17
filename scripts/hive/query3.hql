-- Query 3: Hourly Error Analysis
-- Groups by (log_date, log_hour), computes error counts, rates, distinct error hosts.

INSERT OVERWRITE LOCAL DIRECTORY '${hivevar:OUTPUT_DIR}/query3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    log_date,
    log_hour,
    SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS error_request_count,
    COUNT(*) AS total_request_count,
    SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) / COUNT(*) AS error_rate,
    COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host END) AS distinct_error_hosts
FROM nasa_logs
WHERE host IS NOT NULL
GROUP BY log_date, log_hour
ORDER BY log_date, log_hour;

INSERT OVERWRITE LOCAL DIRECTORY '${hivevar:OUTPUT_DIR}/malformed'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT COUNT(*) AS malformed_count
FROM nasa_logs
WHERE host IS NULL;
