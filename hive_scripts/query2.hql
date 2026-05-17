USE nasa_etl;

-- Q2: Top 20 Requested Resources
SELECT
    resource_path,
    COUNT(*)               AS request_count,
    SUM(bytes_transferred) AS total_bytes,
    COUNT(DISTINCT host)   AS distinct_host_count
FROM nasa_logs
WHERE resource_path IS NOT NULL
GROUP BY resource_path
ORDER BY request_count DESC
LIMIT 20;
