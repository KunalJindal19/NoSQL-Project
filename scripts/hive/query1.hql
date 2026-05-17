-- Query 1: Daily Traffic Summary
-- Groups by (log_date, status_code), computes request_count and total_bytes

INSERT OVERWRITE LOCAL DIRECTORY '${hivevar:OUTPUT_DIR}/query1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    log_date,
    status_code,
    COUNT(*) AS request_count,
    SUM(CASE WHEN bytes_raw = '-' THEN 0 ELSE CAST(bytes_raw AS BIGINT) END) AS total_bytes
FROM nasa_logs
WHERE host IS NOT NULL
GROUP BY log_date, status_code
ORDER BY log_date, status_code;

INSERT OVERWRITE LOCAL DIRECTORY '${hivevar:OUTPUT_DIR}/malformed'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT COUNT(*) AS malformed_count
FROM nasa_logs
WHERE host IS NULL;
