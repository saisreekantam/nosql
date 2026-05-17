USE nasa_etl;

-- Q1: Daily Traffic Summary
SELECT
    log_date,
    status_code,
    COUNT(*)             AS request_count,
    SUM(bytes_transferred) AS total_bytes
FROM nasa_logs
WHERE log_date IS NOT NULL
GROUP BY log_date, status_code
ORDER BY log_date, status_code;
