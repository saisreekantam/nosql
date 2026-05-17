USE nasa_etl;

-- Q3: Hourly Error Analysis
SELECT
    log_date,
    log_hour,
    SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS error_request_count,
    COUNT(*)                                                          AS total_request_count,
    SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)
        / COUNT(*)                                                    AS error_rate,
    COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host END)
                                                                      AS distinct_error_hosts
FROM nasa_logs
WHERE log_date IS NOT NULL
GROUP BY log_date, log_hour
ORDER BY log_date, log_hour;
