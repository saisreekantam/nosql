import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import psycopg2
import psycopg2.extras
from tabulate import tabulate
import click
import config

PIPELINES = ['mongodb', 'mapreduce', 'hive', 'pig']

QUERY_NAMES = {
    'q1': 'Daily Traffic Summary',
    'q2': 'Top Requested Resources',
    'q3': 'Hourly Error Analysis',
}


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=config.PG_HOST, port=config.PG_PORT,
        dbname=config.PG_DB, user=config.PG_USER, password=config.PG_PASS,
    )


def fetch_run(cursor, run_id: str) -> dict | None:
    cursor.execute("SELECT * FROM etl_runs WHERE run_id = %s", (run_id,))
    row = cursor.fetchone()
    return dict(zip([d[0] for d in cursor.description], row)) if row else None


def fetch_latest_run(cursor, pipeline: str = None) -> dict | None:
    if pipeline:
        cursor.execute(
            "SELECT * FROM etl_runs WHERE pipeline_name = %s "
            "ORDER BY run_timestamp DESC LIMIT 1", (pipeline,)
        )
    else:
        cursor.execute("SELECT * FROM etl_runs ORDER BY run_timestamp DESC LIMIT 1")
    row = cursor.fetchone()
    return dict(zip([d[0] for d in cursor.description], row)) if row else None


def fetch_all_runs(cursor) -> list:
    cursor.execute(
        "SELECT pipeline_name, batch_size, num_batches, avg_batch_size, "
        "       runtime_seconds, total_records, malformed_records, run_id, run_timestamp "
        "FROM etl_runs ORDER BY pipeline_name, batch_size, run_timestamp DESC"
    )
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def fetch_q1(cursor, run_id: str) -> list:
    cursor.execute(
        "SELECT log_date, status_code, request_count, total_bytes "
        "FROM q1_daily_traffic WHERE run_id = %s ORDER BY log_date, status_code",
        (run_id,)
    )
    return cursor.fetchall()


def fetch_q2(cursor, run_id: str) -> list:
    cursor.execute(
        "SELECT resource_path, request_count, total_bytes, distinct_host_count "
        "FROM q2_top_resources WHERE run_id = %s ORDER BY request_count DESC",
        (run_id,)
    )
    return cursor.fetchall()


def fetch_q3(cursor, run_id: str) -> list:
    cursor.execute(
        "SELECT log_date, log_hour, error_request_count, total_request_count, "
        "       ROUND(error_rate::numeric, 4) AS error_rate, distinct_error_hosts "
        "FROM q3_hourly_errors WHERE run_id = %s ORDER BY log_date, log_hour",
        (run_id,)
    )
    return cursor.fetchall()


def _fetch_query_meta(cursor, table: str, run_id: str) -> dict | None:
    cursor.execute(
        f"SELECT batch_id, execution_time FROM {table} WHERE run_id = %s LIMIT 1",
        (run_id,)
    )
    row = cursor.fetchone()
    return {'batch_id': row[0], 'execution_time': row[1]} if row else None


def _q1_row_count(cursor, run_id: str) -> int:
    cursor.execute(
        "SELECT COUNT(*) FROM q1_daily_traffic WHERE run_id = %s", (run_id,)
    )
    return cursor.fetchone()[0]


def _q2_top_resource(cursor, run_id: str) -> tuple:
    cursor.execute(
        "SELECT resource_path, request_count FROM q2_top_resources "
        "WHERE run_id = %s ORDER BY request_count DESC LIMIT 1", (run_id,)
    )
    return cursor.fetchone()


def _q3_row_count(cursor, run_id: str) -> int:
    cursor.execute(
        "SELECT COUNT(*) FROM q3_hourly_errors WHERE run_id = %s", (run_id,)
    )
    return cursor.fetchone()[0]


# ── Single-run report ─────────────────────────────────────────────────────────

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
    print(f"  Queries       : Q1={QUERY_NAMES['q1']}  |  "
          f"Q2={QUERY_NAMES['q2']}  |  Q3={QUERY_NAMES['q3']}")
    print("=" * 72)


def print_single_run(cur, run: dict):
    print_run_header(run)
    rid = str(run['run_id'])

    q1_meta = _fetch_query_meta(cur, 'q1_daily_traffic', rid)
    print(f"\n--- Query Name : {QUERY_NAMES['q1']} ---")
    if q1_meta:
        print(f"    Batch ID   : {q1_meta['batch_id']}")
        print(f"    Exec Time  : {q1_meta['execution_time']}")
    print(tabulate(fetch_q1(cur, rid),
        headers=['log_date', 'status_code', 'request_count', 'total_bytes'],
        tablefmt='simple', intfmt=','))

    q2_meta = _fetch_query_meta(cur, 'q2_top_resources', rid)
    print(f"\n--- Query Name : {QUERY_NAMES['q2']} (Top 20) ---")
    if q2_meta:
        print(f"    Batch ID   : {q2_meta['batch_id']}")
        print(f"    Exec Time  : {q2_meta['execution_time']}")
    print(tabulate(fetch_q2(cur, rid),
        headers=['resource_path', 'request_count', 'total_bytes', 'distinct_hosts'],
        tablefmt='simple', intfmt=','))

    q3_meta = _fetch_query_meta(cur, 'q3_hourly_errors', rid)
    print(f"\n--- Query Name : {QUERY_NAMES['q3']} ---")
    if q3_meta:
        print(f"    Batch ID   : {q3_meta['batch_id']}")
        print(f"    Exec Time  : {q3_meta['execution_time']}")
    print(tabulate(fetch_q3(cur, rid),
        headers=['log_date', 'log_hour', 'error_count', 'total_count',
                 'error_rate', 'distinct_error_hosts'],
        tablefmt='simple', intfmt=','))


# ── Compare report ────────────────────────────────────────────────────────────

def print_compare(cur, batch_size: int = None):
    all_runs = fetch_all_runs(cur)
    if not all_runs:
        print("No runs found in the database.")
        return

    # Determine which batch sizes exist
    available_sizes = sorted(set(r['batch_size'] for r in all_runs))

    print("\n" + "=" * 80)
    print("  PIPELINE COMPARISON REPORT")
    print("=" * 80)

    # ── Section 1: Runtime comparison per batch size ──────────────────────────
    for bs in available_sizes:
        if batch_size and bs != batch_size:
            continue

        # Pick latest run per pipeline for this batch size
        runs_at_bs = {}
        for r in all_runs:
            if r['batch_size'] == bs:
                p = r['pipeline_name']
                if p not in runs_at_bs:   # already sorted DESC so first = latest
                    runs_at_bs[p] = r

        if not runs_at_bs:
            continue

        print(f"\n{'─'*80}")
        print(f"  Batch Size = {bs:,}  |  Pipelines with data: {', '.join(sorted(runs_at_bs))}")
        print(f"{'─'*80}")

        runtime_rows = []
        for p in PIPELINES:
            if p not in runs_at_bs:
                runtime_rows.append([p, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A'])
                continue
            r = runs_at_bs[p]
            runtime_rows.append([
                p,
                f"{r['runtime_seconds']:.2f}s",
                f"{r['total_records']:,}",
                r['num_batches'],
                f"{r['avg_batch_size']:.1f}",
                f"{r['malformed_records']:,}",
                str(r['run_timestamp'])[:19],
            ])

        print(tabulate(runtime_rows,
            headers=['Pipeline', 'Runtime', 'Total Records', 'Batches',
                     'Avg Batch Size', 'Malformed', 'Run Timestamp'],
            tablefmt='simple'))

        # ── Correctness / equivalence check ──────────────────────────────────
        print(f"\n  Correctness Check (batch_size={bs:,}):")
        eq_rows = []
        for p in PIPELINES:
            if p not in runs_at_bs:
                eq_rows.append([p, 'N/A', 'N/A', 'N/A', 'N/A'])
                continue
            rid = str(runs_at_bs[p]['run_id'])
            q1c = _q1_row_count(cur, rid)
            q2top = _q2_top_resource(cur, rid)
            q3c = _q3_row_count(cur, rid)
            top_resource = q2top[0] if q2top else 'N/A'
            top_count = f"{q2top[1]:,}" if q2top else 'N/A'
            eq_rows.append([p, q1c, f"{top_resource} ({top_count})", q3c,
                            runs_at_bs[p]['total_records']])

        print(tabulate(eq_rows,
            headers=['Pipeline', 'Q1 Rows', 'Q2 Top Resource (count)', 'Q3 Rows', 'Total Records'],
            tablefmt='simple'))

        # Match verdict
        q1_counts = [r[1] for r in eq_rows if r[1] != 'N/A']
        q3_counts = [r[3] for r in eq_rows if r[3] != 'N/A']
        q1_match = len(set(q1_counts)) == 1
        q3_match = len(set(q3_counts)) == 1
        print(f"\n  Q1 row count match : {'✓ ALL MATCH' if q1_match else '✗ MISMATCH'} ({q1_counts})")
        print(f"  Q3 row count match : {'✓ ALL MATCH' if q3_match else '✗ MISMATCH'} ({q3_counts})")

    # ── Section 2: Batch size effect per pipeline (if multiple sizes) ─────────
    if len(available_sizes) > 1 and not batch_size:
        print(f"\n{'─'*80}")
        print("  Batch Size Effect on Runtime")
        print(f"{'─'*80}")

        # Group by pipeline → list of (batch_size, runtime, num_batches, avg_batch)
        from collections import defaultdict
        by_pipeline = defaultdict(list)
        seen = set()
        for r in all_runs:
            key = (r['pipeline_name'], r['batch_size'])
            if key not in seen:
                seen.add(key)
                by_pipeline[r['pipeline_name']].append(r)

        for p in PIPELINES:
            if p not in by_pipeline:
                continue
            rows = sorted(by_pipeline[p], key=lambda x: x['batch_size'])
            print(f"\n  {p.upper()}")
            print(tabulate(
                [[r['batch_size'], r['num_batches'], f"{r['avg_batch_size']:.1f}",
                  f"{r['runtime_seconds']:.2f}s"] for r in rows],
                headers=['Batch Size', 'Num Batches', 'Avg Batch Size', 'Runtime'],
                tablefmt='simple'
            ))

    print(f"\n{'='*80}\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

@click.command()
@click.option('--run-id',   default=None, help="Specific run UUID to report on")
@click.option('--pipeline', default=None,
              type=click.Choice(PIPELINES), help="Show latest run for this pipeline")
@click.option('--latest',   is_flag=True, default=False,
              help="Show the most recent run regardless of pipeline")
@click.option('--compare',  is_flag=True, default=False,
              help="Compare all 4 pipelines side-by-side (all batch sizes in DB)")
@click.option('--batch-size', 'batch_size', default=None, type=int,
              help="Filter --compare to a specific batch size")
def main(run_id, pipeline, latest, compare, batch_size):
    """Display ETL results and execution metadata from PostgreSQL.

    \b
    Examples:
      python reporting/report.py --pipeline mongodb
      python reporting/report.py --latest
      python reporting/report.py --run-id <uuid>
      python reporting/report.py --compare
      python reporting/report.py --compare --batch-size 100000
    """
    with get_conn() as conn, conn.cursor() as cur:
        if compare:
            print_compare(cur, batch_size=batch_size)
            return

        if run_id:
            run = fetch_run(cur, run_id)
        elif pipeline or latest:
            run = fetch_latest_run(cur, pipeline)
        else:
            click.echo("Provide --run-id, --pipeline, --latest, or --compare")
            sys.exit(1)

        if not run:
            click.echo("No matching run found.")
            sys.exit(1)

        print_single_run(cur, run)


if __name__ == '__main__':
    main()
