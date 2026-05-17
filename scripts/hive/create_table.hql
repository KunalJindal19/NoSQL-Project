-- Drop and recreate for each batch to point to the batch file location
DROP TABLE IF EXISTS nasa_logs;

CREATE EXTERNAL TABLE nasa_logs (
    host            STRING,
    log_date        STRING,
    log_hour        STRING,
    http_method     STRING,
    resource_path   STRING,
    protocol_version STRING,
    status_code     INT,
    bytes_raw       STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "^(\\S+)\\s+-\\s+-\\s+\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+-\\d{4}\\]\\s+\"(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?\"\\s+(\\d{3})\\s+(\\S+)$"
)
STORED AS TEXTFILE
LOCATION '${hivevar:BATCH_DIR}';
