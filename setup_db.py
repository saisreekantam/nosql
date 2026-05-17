#!/usr/bin/env python3
"""
One-shot DB setup script.
Run once with the postgres superuser password to create the nasa_etl
database, a dedicated user, and all required tables.

Usage:
    python setup_db.py --pg-pass <your_postgres_password>
    # or set PG_PASS env var and run:
    PG_PASS=yourpass python setup_db.py
"""
import sys
import os
import click
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

SCHEMA = """
CREATE TABLE IF NOT EXISTS etl_runs (
    run_id            UUID PRIMARY KEY,
    pipeline_name     VARCHAR(20)  NOT NULL,
    run_timestamp     TIMESTAMP    NOT NULL DEFAULT NOW(),
    total_records     INTEGER,
    malformed_records INTEGER,
    batch_size        INTEGER,
    num_batches       INTEGER,
    avg_batch_size    FLOAT,
    runtime_seconds   FLOAT
);

CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id             SERIAL PRIMARY KEY,
    run_id         UUID        REFERENCES etl_runs(run_id),
    pipeline_name  VARCHAR(20),
    batch_id       INTEGER,
    execution_time TIMESTAMP,
    log_date       DATE,
    status_code    INTEGER,
    request_count  BIGINT,
    total_bytes    BIGINT
);

CREATE TABLE IF NOT EXISTS q2_top_resources (
    id                  SERIAL PRIMARY KEY,
    run_id              UUID        REFERENCES etl_runs(run_id),
    pipeline_name       VARCHAR(20),
    batch_id            INTEGER,
    execution_time      TIMESTAMP,
    resource_path       TEXT,
    request_count       BIGINT,
    total_bytes         BIGINT,
    distinct_host_count INTEGER
);

CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    id                   SERIAL PRIMARY KEY,
    run_id               UUID        REFERENCES etl_runs(run_id),
    pipeline_name        VARCHAR(20),
    batch_id             INTEGER,
    execution_time       TIMESTAMP,
    log_date             DATE,
    log_hour             SMALLINT,
    error_request_count  INTEGER,
    total_request_count  INTEGER,
    error_rate           FLOAT,
    distinct_error_hosts INTEGER
);

CREATE INDEX IF NOT EXISTS idx_q1_run    ON q1_daily_traffic(run_id);
CREATE INDEX IF NOT EXISTS idx_q2_run    ON q2_top_resources(run_id);
CREATE INDEX IF NOT EXISTS idx_q3_run    ON q3_hourly_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_runs_pipe ON etl_runs(pipeline_name);
"""


@click.command()
@click.option('--host',    default='localhost',  show_default=True)
@click.option('--port',    default=5432,         show_default=True)
@click.option('--pg-user', default='postgres',   show_default=True,
              help='Superuser to connect with initially')
@click.option('--pg-pass', default=None,
              help='Superuser password (or set PG_PASS env var)')
@click.option('--db-name', default='nasa_etl',   show_default=True,
              help='Database to create/use')
@click.option('--app-user', default='nasa_user', show_default=True,
              help='App-level DB user to create')
@click.option('--app-pass', default='nasa123',   show_default=True,
              help='Password for the app user')
def main(host, port, pg_user, pg_pass, db_name, app_user, app_pass):
    pg_pass = pg_pass or os.getenv('PG_PASS')
    if not pg_pass:
        click.echo("ERROR: provide --pg-pass or set PG_PASS env var", err=True)
        sys.exit(1)

    # ── Step 1: connect as superuser to postgres db ──────────────────────
    click.echo(f"Connecting to postgres@{host}:{port} …")
    conn = psycopg2.connect(
        host=host, port=port, dbname='postgres',
        user=pg_user, password=pg_pass,
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # ── Step 2: create database ───────────────────────────────────────────
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    if cur.fetchone():
        click.echo(f"Database '{db_name}' already exists — skipping create.")
    else:
        cur.execute(f'CREATE DATABASE "{db_name}"')
        click.echo(f"Created database '{db_name}'.")

    # ── Step 3: create app user ───────────────────────────────────────────
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (app_user,))
    if cur.fetchone():
        click.echo(f"User '{app_user}' already exists — skipping create.")
    else:
        cur.execute(
            f"CREATE USER {app_user} WITH PASSWORD %s", (app_pass,)
        )
        click.echo(f"Created user '{app_user}'.")

    cur.execute(f'GRANT ALL PRIVILEGES ON DATABASE "{db_name}" TO {app_user}')
    cur.close()
    conn.close()

    # ── Step 4: connect to nasa_etl and create tables ────────────────────
    click.echo(f"Creating schema in '{db_name}' …")
    conn2 = psycopg2.connect(
        host=host, port=port, dbname=db_name,
        user=pg_user, password=pg_pass,
    )
    conn2.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur2 = conn2.cursor()

    # Grant schema permissions to app user
    cur2.execute(f'GRANT ALL ON SCHEMA public TO {app_user}')

    for stmt in SCHEMA.split(';'):
        stmt = stmt.strip()
        if stmt:
            cur2.execute(stmt)

    # Grant table-level permissions
    cur2.execute(f"""
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {app_user};
        GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {app_user};
    """)

    cur2.close()
    conn2.close()

    click.echo("\nSetup complete!")
    click.echo(f"  DB       : {db_name}")
    click.echo(f"  User     : {app_user}  /  Password: {app_pass}")
    click.echo(f"\nAdd to your environment (or update config.py):")
    click.echo(f"  export PG_DB={db_name}")
    click.echo(f"  export PG_USER={app_user}")
    click.echo(f"  export PG_PASS={app_pass}")
    click.echo(f"\nOr just run:")
    click.echo(f"  PG_USER={app_user} PG_PASS={app_pass} python main.py --pipeline mongodb")


if __name__ == '__main__':
    main()
