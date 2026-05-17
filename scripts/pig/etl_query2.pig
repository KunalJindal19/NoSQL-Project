-- Parameters: $INPUT (batch file path), $OUTPUT (output directory)
-- Query 2: Top Requested Resources
-- Groups by resource_path, computes count, bytes, distinct hosts. Top 20 by count.

raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

parsed = FOREACH raw GENERATE
    REGEX_EXTRACT(line, '^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"\\s+(\\d{3})\\s+(\\S+)$', 1) AS host,
    REGEX_EXTRACT(line, '^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"\\s+(\\d{3})\\s+(\\S+)$', 5) AS resource_path,
    REGEX_EXTRACT(line, '^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"\\s+(\\d{3})\\s+(\\S+)$', 8) AS bytes_raw;

valid = FILTER parsed BY host IS NOT NULL;
malformed = FILTER parsed BY host IS NULL;

malformed_grp = GROUP malformed ALL;
malformed_count = FOREACH malformed_grp GENERATE COUNT_STAR(malformed) AS cnt;
STORE malformed_count INTO '$OUTPUT/malformed' USING PigStorage('\t');

cleaned = FOREACH valid GENERATE
    host,
    resource_path,
    (long) (bytes_raw == '-' ? '0' : bytes_raw) AS bytes_transferred;

-- CRITICAL: Pig requires nested FOREACH with { } for DISTINCT operations
grouped = GROUP cleaned BY resource_path;
aggregated = FOREACH grouped {
    unique_hosts = DISTINCT cleaned.host;
    GENERATE
        group AS resource_path,
        COUNT(cleaned) AS request_count,
        SUM(cleaned.bytes_transferred) AS total_bytes,
        COUNT(unique_hosts) AS distinct_host_count;
};

-- Sort and limit to top 20
sorted = ORDER aggregated BY request_count DESC;
top20 = LIMIT sorted 20;

STORE top20 INTO '$OUTPUT/results' USING PigStorage('\t');
