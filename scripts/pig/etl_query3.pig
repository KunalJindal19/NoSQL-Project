-- Parameters: $INPUT (batch file path), $OUTPUT (output directory)
-- Query 3: Hourly Error Analysis
-- Groups by (log_date, log_hour), computes error counts, rates, distinct error hosts.

raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

parsed = FOREACH raw GENERATE
    REGEX_EXTRACT(line, '^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"\\s+(\\d{3})\\s+(\\S+)$', 1) AS host,
    REGEX_EXTRACT(line, '^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"\\s+(\\d{3})\\s+(\\S+)$', 2) AS log_date,
    REGEX_EXTRACT(line, '^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"\\s+(\\d{3})\\s+(\\S+)$', 3) AS log_hour,
    (int) REGEX_EXTRACT(line, '^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]\\s+"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?"\\s+(\\d{3})\\s+(\\S+)$', 7) AS status_code;

valid = FILTER parsed BY host IS NOT NULL;
malformed = FILTER parsed BY host IS NULL;

malformed_grp = GROUP malformed ALL;
malformed_count = FOREACH malformed_grp GENERATE COUNT_STAR(malformed) AS cnt;
STORE malformed_count INTO '$OUTPUT/malformed' USING PigStorage('\t');

-- All records grouped by date+hour
grouped = GROUP valid BY (log_date, log_hour);

results = FOREACH grouped {
    errors = FILTER valid BY status_code >= 400 AND status_code <= 599;
    error_hosts = DISTINCT errors.host;
    GENERATE
        group.log_date AS log_date,
        group.log_hour AS log_hour,
        COUNT(errors) AS error_request_count,
        COUNT(valid) AS total_request_count,
        (double)COUNT(errors) / (double)COUNT(valid) AS error_rate,
        COUNT(error_hosts) AS distinct_error_hosts;
};

STORE results INTO '$OUTPUT/results' USING PigStorage('\t');
