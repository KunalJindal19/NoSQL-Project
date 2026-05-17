-- Query 2: Top Requested Resources
-- Groups by resource_path, computes count, bytes, distinct hosts. Top 20 by count.

INSERT OVERWRITE LOCAL DIRECTORY '${hivevar:OUTPUT_DIR}/query2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    resource_path,
    COUNT(*) AS request_count,
    SUM(CASE WHEN bytes_raw = '-' THEN 0 ELSE CAST(bytes_raw AS BIGINT) END) AS total_bytes,
    COUNT(DISTINCT host) AS distinct_host_count
FROM nasa_logs
WHERE host IS NOT NULL
GROUP BY resource_path
ORDER BY request_count DESC
LIMIT 20;

INSERT OVERWRITE LOCAL DIRECTORY '${hivevar:OUTPUT_DIR}/malformed'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT COUNT(*) AS malformed_count
FROM nasa_logs
WHERE host IS NULL;
