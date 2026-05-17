import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import psycopg2
import psycopg2.extras
from tabulate import tabulate
import click
import config


def get_conn():
    return psycopg2.connect(
        host=config.PG_HOST, port=config.PG_PORT,
        dbname=config.PG_DB, user=config.PG_USER, password=config.PG_PASS,
    )


def fetch_run(cursor, run_id: str) -> dict | None:
    cursor.execute(
        "SELECT * FROM etl_runs WHERE run_id = %s", (run_id,)
    )
    row = cursor.fetchone()
    return dict(zip([d[0] for d in cursor.description], row)) if row else None


def fetch_latest_run(cursor, pipeline: str = None) -> dict | None:
    if pipeline:
        cursor.execute(
            "SELECT * FROM etl_runs WHERE pipeline_name = %s "
            "ORDER BY run_timestamp DESC LIMIT 1", (pipeline,)
        )
    else:
        cursor.execute(
            "SELECT * FROM etl_runs ORDER BY run_timestamp DESC LIMIT 1"
        )
    row = cursor.fetchone()
    return dict(zip([d[0] for d in cursor.description], row)) if row else None


def fetch_q1(cursor, run_id: str) -> list:
    cursor.execute(
        "SELECT log_date, status_code, request_count, total_bytes "
        "FROM q1_daily_traffic WHERE run_id = %s "
        "ORDER BY log_date, status_code", (run_id,)
    )
    return cursor.fetchall()


def fetch_q2(cursor, run_id: str) -> list:
    cursor.execute(
        "SELECT resource_path, request_count, total_bytes, distinct_host_count "
        "FROM q2_top_resources WHERE run_id = %s "
        "ORDER BY request_count DESC", (run_id,)
    )
    return cursor.fetchall()


def fetch_q3(cursor, run_id: str) -> list:
    cursor.execute(
        "SELECT log_date, log_hour, error_request_count, total_request_count, "
        "       ROUND(error_rate::numeric, 4) AS error_rate, distinct_error_hosts "
        "FROM q3_hourly_errors WHERE run_id = %s "
        "ORDER BY log_date, log_hour", (run_id,)
    )
    return cursor.fetchall()


def _fetch_query_meta(cursor, table: str, run_id: str) -> dict | None:
    cursor.execute(
        f"SELECT batch_id, execution_time FROM {table} WHERE run_id = %s LIMIT 1",
        (run_id,)
    )
    row = cursor.fetchone()
    return {'batch_id': row[0], 'execution_time': row[1]} if row else None


QUERY_NAMES = {
    'q1': 'Daily Traffic Summary',
    'q2': 'Top Requested Resources',
    'q3': 'Hourly Error Analysis',
}


def print_run_header(run: dict):
    print("\n" + "=" * 72)
    print("  ETL RUN REPORT")
    print("=" * 72)
    print(f"  Pipeline      : {run['pipeline_name']}")
    print(f"  Run ID        : {run['run_id']}")
    print(f"  Timestamp     : {run['run_timestamp']}")
    print(f"  Runtime       : {run['runtime_seconds']:.2f} seconds")
    print(f"  Total Records : {run['total_records']:,}")
    print(f"  Malformed     : {run['malformed_records']:,}")
    print(f"  Batch Size    : {run['batch_size']:,}")
    print(f"  Num Batches   : {run['num_batches']}")
    print(f"  Avg Batch Size: {run['avg_batch_size']:.1f}")
    print(f"  Queries       : Q1={QUERY_NAMES['q1']}  |  Q2={QUERY_NAMES['q2']}  |  Q3={QUERY_NAMES['q3']}")
    print("=" * 72)


@click.command()
@click.option('--run-id', default=None, help="Specific run UUID to report on")
@click.option('--pipeline', default=None,
              type=click.Choice(['mongodb', 'mapreduce', 'hive', 'pig']),
              help="Show latest run for this pipeline")
@click.option('--latest', is_flag=True, default=False,
              help="Show the most recent run regardless of pipeline")
def main(run_id, pipeline, latest):
    """Display ETL results and execution metadata from PostgreSQL."""
    with get_conn() as conn, conn.cursor() as cur:
        if run_id:
            run = fetch_run(cur, run_id)
        elif pipeline or latest:
            run = fetch_latest_run(cur, pipeline)
        else:
            click.echo("Provide --run-id, --pipeline, or --latest")
            sys.exit(1)

        if not run:
            click.echo("No matching run found.")
            sys.exit(1)

        print_run_header(run)
        rid = str(run['run_id'])

        # Each block prints: query name, batch_id, execution_time, then results
        q1_meta = _fetch_query_meta(cur, 'q1_daily_traffic', rid)
        print(f"\n--- Query Name : {QUERY_NAMES['q1']} ---")
        if q1_meta:
            print(f"    Batch ID   : {q1_meta['batch_id']}")
            print(f"    Exec Time  : {q1_meta['execution_time']}")
        q1 = fetch_q1(cur, rid)
        print(tabulate(q1,
            headers=['log_date', 'status_code', 'request_count', 'total_bytes'],
            tablefmt='simple', intfmt=','))

        q2_meta = _fetch_query_meta(cur, 'q2_top_resources', rid)
        print(f"\n--- Query Name : {QUERY_NAMES['q2']} (Top 20) ---")
        if q2_meta:
            print(f"    Batch ID   : {q2_meta['batch_id']}")
            print(f"    Exec Time  : {q2_meta['execution_time']}")
        q2 = fetch_q2(cur, rid)
        print(tabulate(q2,
            headers=['resource_path', 'request_count', 'total_bytes', 'distinct_hosts'],
            tablefmt='simple', intfmt=','))

        q3_meta = _fetch_query_meta(cur, 'q3_hourly_errors', rid)
        print(f"\n--- Query Name : {QUERY_NAMES['q3']} ---")
        if q3_meta:
            print(f"    Batch ID   : {q3_meta['batch_id']}")
            print(f"    Exec Time  : {q3_meta['execution_time']}")
        q3 = fetch_q3(cur, rid)
        print(tabulate(q3,
            headers=['log_date', 'log_hour', 'error_count', 'total_count',
                     'error_rate', 'distinct_error_hosts'],
            tablefmt='simple', intfmt=','))


if __name__ == '__main__':
    main()
