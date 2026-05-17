-- ============================================================
-- NASA Log Analytics — Apache Pig
-- Input: pre-parsed TSV files (host, log_date, log_hour,
--        http_method, resource_path, protocol_version,
--        status_code, bytes_transferred)
-- ============================================================

data = LOAD '$INPUT' USING PigStorage('\t') AS (
    host:chararray,
    log_date:chararray,
    log_hour:int,
    http_method:chararray,
    resource_path:chararray,
    protocol_version:chararray,
    status_code:int,
    bytes_transferred:long
);

-- ── Q1: Daily Traffic Summary ─────────────────────────────────────────────
q1_grp    = GROUP data BY (log_date, status_code);
q1_result = FOREACH q1_grp GENERATE
    FLATTEN(group)                AS (log_date:chararray, status_code:int),
    COUNT(data)                   AS request_count:long,
    SUM(data.bytes_transferred)   AS total_bytes:long;
q1_sorted = ORDER q1_result BY log_date ASC, status_code ASC;
STORE q1_sorted INTO '$OUTPUT/q1' USING PigStorage('\t');

-- ── Q2: Top 20 Requested Resources ────────────────────────────────────────
q2_grp    = GROUP data BY resource_path;
q2_agg    = FOREACH q2_grp {
    hosts          = data.host;
    distinct_hosts = DISTINCT hosts;
    GENERATE
        group                       AS resource_path:chararray,
        COUNT(data)                 AS request_count:long,
        SUM(data.bytes_transferred) AS total_bytes:long,
        COUNT(distinct_hosts)       AS distinct_host_count:long;
};
STORE q2_agg INTO '$OUTPUT/q2' USING PigStorage('\t');

-- ── Q3: Hourly Error Analysis ──────────────────────────────────────────────
q3_grp    = GROUP data BY (log_date, log_hour);
q3_result = FOREACH q3_grp {
    err_recs           = FILTER data BY status_code >= 400 AND status_code <= 599;
    err_hosts          = err_recs.host;
    distinct_err_hosts = DISTINCT err_hosts;
    total_count        = COUNT(data);
    err_count          = COUNT(err_recs);
    GENERATE
        FLATTEN(group)                      AS (log_date:chararray, log_hour:int),
        err_count                           AS error_request_count:long,
        total_count                         AS total_request_count:long,
        (double)err_count / total_count     AS error_rate:double,
        COUNT(distinct_err_hosts)           AS distinct_error_hosts:long;
};
q3_sorted = ORDER q3_result BY log_date ASC, log_hour ASC;
STORE q3_sorted INTO '$OUTPUT/q3' USING PigStorage('\t');
