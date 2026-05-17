CREATE DATABASE IF NOT EXISTS nasa_etl;
USE nasa_etl;

-- External table with RegexSerDe — no preprocessing, raw log file on HDFS
ADD JAR /usr/local/hive/lib/hive-contrib.jar;

DROP TABLE IF EXISTS nasa_logs_raw;

CREATE EXTERNAL TABLE nasa_logs_raw (
    host              STRING,
    raw_timestamp     STRING,
    request           STRING,
    status_code_str   STRING,
    bytes_str         STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+\"([^\"]*)\"\\s+(\\S+)\\s+(\\S+)$"
)
STORED AS TEXTFILE
LOCATION '${INPUT_PATH}';

-- Parsed view: applies field extraction inline
CREATE OR REPLACE VIEW nasa_logs AS
SELECT
    host,
    raw_timestamp                                  AS timestamp,
    from_unixtime(unix_timestamp(
        regexp_replace(raw_timestamp, ' [+-]\\d{4}$', ''),
        'dd/MMM/yyyy:HH:mm:ss'), 'yyyy-MM-dd')     AS log_date,
    CAST(hour(from_unixtime(unix_timestamp(
        regexp_replace(raw_timestamp, ' [+-]\\d{4}$', ''),
        'dd/MMM/yyyy:HH:mm:ss'))) AS INT)           AS log_hour,
    CASE
        WHEN request RLIKE '^(\\S+)\\s+(\\S+)\\s+(\\S+)$'
        THEN regexp_extract(request, '^(\\S+)\\s+', 1)
        ELSE 'UNKNOWN'
    END                                             AS http_method,
    CASE
        WHEN request RLIKE '^(\\S+)\\s+(\\S+)\\s+(\\S+)$'
        THEN regexp_extract(request, '^\\S+\\s+(\\S+)\\s+', 1)
        ELSE request
    END                                             AS resource_path,
    CASE
        WHEN request RLIKE '^(\\S+)\\s+(\\S+)\\s+(\\S+)$'
        THEN regexp_extract(request, '\\s+(\\S+)$', 1)
        ELSE 'UNKNOWN'
    END                                             AS protocol_version,
    CAST(status_code_str AS INT)                    AS status_code,
    CASE
        WHEN bytes_str = '-' THEN 0
        ELSE CAST(bytes_str AS BIGINT)
    END                                             AS bytes_transferred
FROM nasa_logs_raw
WHERE status_code_str RLIKE '^[0-9]+$';
