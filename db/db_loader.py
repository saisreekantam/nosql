import uuid
from datetime import datetime, timezone
import psycopg2
import psycopg2.extras
import config


def get_conn():
    return psycopg2.connect(
        host=config.PG_HOST, port=config.PG_PORT,
        dbname=config.PG_DB, user=config.PG_USER, password=config.PG_PASS,
    )


def save_run_metadata(run_id: str, pipeline: str, total_records: int,
                      malformed: int, batch_size: int, num_batches: int,
                      avg_batch_size: float, runtime_seconds: float):
    sql = """
        INSERT INTO etl_runs
            (run_id, pipeline_name, run_timestamp, total_records,
             malformed_records, batch_size, num_batches, avg_batch_size,
             runtime_seconds)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO NOTHING;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (
            run_id, pipeline, datetime.now(timezone.utc),
            total_records, malformed, batch_size, num_batches,
            avg_batch_size, runtime_seconds,
        ))


def save_q1(run_id: str, pipeline: str, batch_id: int, rows: list):
    """rows: list of dicts with keys log_date, status_code, request_count, total_bytes"""
    sql = """
        INSERT INTO q1_daily_traffic
            (run_id, pipeline_name, batch_id, execution_time,
             log_date, status_code, request_count, total_bytes)
        VALUES %s
    """
    values = [
        (run_id, pipeline, batch_id, datetime.now(timezone.utc),
         r['log_date'], r['status_code'], r['request_count'], r['total_bytes'])
        for r in rows
    ]
    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values)


def save_q2(run_id: str, pipeline: str, batch_id: int, rows: list):
    """rows: list of dicts with keys resource_path, request_count, total_bytes, distinct_host_count"""
    sql = """
        INSERT INTO q2_top_resources
            (run_id, pipeline_name, batch_id, execution_time,
             resource_path, request_count, total_bytes, distinct_host_count)
        VALUES %s
    """
    values = [
        (run_id, pipeline, batch_id, datetime.now(timezone.utc),
         r['resource_path'], r['request_count'], r['total_bytes'],
         r['distinct_host_count'])
        for r in rows
    ]
    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values)


def save_q3(run_id: str, pipeline: str, batch_id: int, rows: list):
    """rows: list of dicts with keys log_date, log_hour, error_request_count,
       total_request_count, error_rate, distinct_error_hosts"""
    sql = """
        INSERT INTO q3_hourly_errors
            (run_id, pipeline_name, batch_id, execution_time,
             log_date, log_hour, error_request_count, total_request_count,
             error_rate, distinct_error_hosts)
        VALUES %s
    """
    values = [
        (run_id, pipeline, batch_id, datetime.now(timezone.utc),
         r['log_date'], r['log_hour'],
         r['error_request_count'], r['total_request_count'],
         r['error_rate'], r['distinct_error_hosts'])
        for r in rows
    ]
    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values)
