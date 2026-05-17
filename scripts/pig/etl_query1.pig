-- Parameters: $INPUT (batch file path), $OUTPUT (output directory)
-- Query 1: Daily Traffic Summary
-- Groups by (log_date, status_code) and computes request_count + total_bytes

-- Load raw log lines
raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

-- Parse using regex
parsed = FOREACH raw GENERATE
    REGEX_EXTRACT(line, '^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"\\s+(\\d{3})\\s+(\\S+)$', 1) AS host,
    REGEX_EXTRACT(line, '^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"\\s+(\\d{3})\\s+(\\S+)$', 2) AS log_date,
    REGEX_EXTRACT(line, '^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"\\s+(\\d{3})\\s+(\\S+)$', 7) AS status_code,
    REGEX_EXTRACT(line, '^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"\\s+(\\d{3})\\s+(\\S+)$', 8) AS bytes_raw;

-- Filter out malformed records (where host is null means regex didn't match)
valid = FILTER parsed BY host IS NOT NULL;
malformed = FILTER parsed BY host IS NULL;

-- Count malformed
malformed_grp = GROUP malformed ALL;
malformed_count = FOREACH malformed_grp GENERATE COUNT_STAR(malformed) AS cnt;
STORE malformed_count INTO '$OUTPUT/malformed' USING PigStorage('\t');

-- Clean bytes: replace '-' with '0', using ternary operator for compatibility
cleaned = FOREACH valid GENERATE
    log_date,
    (int) status_code AS status_code,
    (long) (bytes_raw == '-' ? '0' : bytes_raw) AS bytes_transferred;

-- Query 1: Group by log_date, status_code
grouped = GROUP cleaned BY (log_date, status_code);
results = FOREACH grouped GENERATE
    group.log_date AS log_date,
    group.status_code AS status_code,
    COUNT(cleaned) AS request_count,
    SUM(cleaned.bytes_transferred) AS total_bytes;

STORE results INTO '$OUTPUT/results' USING PigStorage('\t');
