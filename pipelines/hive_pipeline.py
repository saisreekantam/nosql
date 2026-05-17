import os
import time
import subprocess
import tempfile  # still used for TemporaryDirectory in run()

import config
from db import db_loader
from parser.log_parser import iter_batches
from pipelines.base_pipeline import BasePipeline

HADOOP_BIN = os.path.join(os.getenv('HADOOP_HOME',
    '/opt/homebrew/Cellar/hadoop/3.5.0/libexec'), 'bin')
JAVA_HOME  = os.getenv('JAVA_HOME',
    '/opt/homebrew/Cellar/openjdk@21/21.0.11/libexec/openjdk.jdk/Contents/Home')
BEELINE_URL = 'jdbc:hive2://'

# NASA log regex for RegexSerDe.
# Backslashes are doubled so Hive's string parser stores single backslashes
# (Hive processes \\ → \, so \\S stored → \S in the Java regex engine).
_NASA_REGEX = (
    r'^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+'
    r'"([^"]*)"\\s+(\\S+)\\s+(\\S+)$'
)


def _beeline(sql: str) -> str:
    """Run SQL via beeline -e (passes SQL directly, avoids -f execution bug in Hive 4.2.0)."""
    env = os.environ.copy()
    env['JAVA_HOME'] = JAVA_HOME
    env['TERM'] = 'dumb'
    result = subprocess.run(
        ['beeline', '--silent=true', '--outputformat=tsv2',
         '-u', BEELINE_URL, '-e', sql],
        capture_output=True, text=True, env=env
    )
    if result.returncode != 0:
        err = result.stderr
        for line in err.splitlines():
            if 'FAILED:' in line or 'ParseException' in line:
                raise RuntimeError(f"Beeline error: {line}")
        raise RuntimeError(f"Beeline error:\n{err[-1000:]}")
    return result.stdout


def _hdfs(args: list):
    env = os.environ.copy()
    env['JAVA_HOME'] = JAVA_HOME
    subprocess.run(
        [os.path.join(HADOOP_BIN, 'hdfs')] + args,
        check=True, capture_output=True, env=env
    )


def _parse_beeline_tsv(raw: str, columns: list, casts: list) -> list:
    rows = []
    for line in raw.splitlines():
        line = line.strip()
        if (not line or 'Hive Session' in line or 'Job running' in line
                or line == '\t'.join(columns)):
            continue
        fields = line.split('\t')
        if len(fields) != len(columns):
            continue
        row = {}
        for col, cast, val in zip(columns, casts, fields):
            v = val.strip()
            try:
                row[col] = cast(v) if v not in ('NULL', '') else None
            except (ValueError, TypeError):
                row[col] = None
        rows.append(row)
    return rows


class HivePipeline(BasePipeline):
    PIPELINE_NAME = "hive"

    def run(self, log_files: list, batch_size: int, run_id: str) -> dict:
        start_time = time.perf_counter()
        total_records = 0
        total_malformed = 0
        num_batches = 0

        # ── Setup table + view ───────────────────────────────────────────────
        # Wipe the HDFS batches dir so stale files from prior runs don't skew counts
        try:
            _hdfs(['dfs', '-rm', '-r', '-f', '/user/nasa_etl/batches'])
        except Exception:
            pass
        _hdfs(['dfs', '-mkdir', '-p', '/user/nasa_etl/batches'])
        self._create_table()

        # ── Load batches to HDFS ─────────────────────────────────────────────
        with tempfile.TemporaryDirectory() as tmpdir:
            for batch_id, batch, malformed in iter_batches(log_files, batch_size):
                if not batch:
                    continue
                batch_file = os.path.join(tmpdir, f"batch_{batch_id}.log")
                with open(batch_file, 'w') as f:
                    for r in batch:
                        f.write(_reconstruct_line(r) + '\n')

                _hdfs(['dfs', '-put', '-f', batch_file,
                       f'/user/nasa_etl/batches/batch_{batch_id}'])
                num_batches     += 1
                total_records   += len(batch)
                total_malformed += malformed
                print(f"  [hive] batch {batch_id}: {len(batch):,} records → HDFS")

        # ── Run HiveQL queries ────────────────────────────────────────────────
        print("  [hive] running HiveQL Q1 …")
        q1_rows = _parse_beeline_tsv(_beeline(_q1_sql()),
            ['log_date', 'status_code', 'request_count', 'total_bytes'],
            [str, int, int, int])

        print("  [hive] running HiveQL Q2 …")
        q2_rows = _parse_beeline_tsv(_beeline(_q2_sql()),
            ['resource_path', 'request_count', 'total_bytes', 'distinct_host_count'],
            [str, int, int, int])

        print("  [hive] running HiveQL Q3 …")
        q3_rows = _parse_beeline_tsv(_beeline(_q3_sql()),
            ['log_date', 'log_hour', 'error_request_count',
             'total_request_count', 'error_rate', 'distinct_error_hosts'],
            [str, int, int, int, float, int])

        avg_batch_size = total_records / num_batches if num_batches else 0
        runtime = time.perf_counter() - start_time

        db_loader.save_run_metadata(
            run_id=run_id, pipeline=self.PIPELINE_NAME,
            total_records=total_records, malformed=total_malformed,
            batch_size=batch_size, num_batches=num_batches,
            avg_batch_size=avg_batch_size, runtime_seconds=runtime,
        )
        db_loader.save_q1(run_id, self.PIPELINE_NAME, num_batches, q1_rows)
        db_loader.save_q2(run_id, self.PIPELINE_NAME, num_batches, q2_rows)
        db_loader.save_q3(run_id, self.PIPELINE_NAME, num_batches, q3_rows)

        print(f"  [hive] done in {runtime:.2f}s "
              f"({total_records:,} records, {num_batches} batches)")
        return {
            'run_id': run_id, 'pipeline': self.PIPELINE_NAME,
            'total_records': total_records, 'malformed': total_malformed,
            'num_batches': num_batches, 'avg_batch_size': avg_batch_size,
            'runtime_seconds': runtime,
        }

    def _create_table(self):
        # All DDL in ONE session so Derby sees consistent state
        _beeline(f"""
CREATE DATABASE IF NOT EXISTS nasa_etl;
DROP VIEW  IF EXISTS nasa_etl.nasa_logs;
DROP TABLE IF EXISTS nasa_etl.nasa_logs_raw;

CREATE EXTERNAL TABLE nasa_etl.nasa_logs_raw (
    host            STRING,
    raw_timestamp   STRING,
    request         STRING,
    status_code_str STRING,
    bytes_str       STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ('input.regex' = '{_NASA_REGEX}')
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/nasa_etl/batches';

CREATE VIEW nasa_etl.nasa_logs AS
SELECT host, SUBSTR(regexp_replace(raw_timestamp, ' [+-][0-9]{{4}}$', ''), 1, 11) AS log_date_raw, CAST(SUBSTR(regexp_replace(raw_timestamp, ' [+-][0-9]{{4}}$', ''), 13, 2) AS INT) AS log_hour, if(length(regexp_extract(request, '^(\\S+) ', 1))>0, regexp_extract(request, '^(\\S+) ', 1), 'UNKNOWN') AS http_method, if(length(regexp_extract(request, '^\\S+ (\\S+) ', 1))>0, regexp_extract(request, '^\\S+ (\\S+) ', 1), request) AS resource_path, if(length(regexp_extract(request, ' (\\S+)$', 1))>0, regexp_extract(request, ' (\\S+)$', 1), 'UNKNOWN') AS protocol_version, CAST(status_code_str AS INT) AS status_code, if(bytes_str = '-', 0, CAST(bytes_str AS BIGINT)) AS bytes_transferred
FROM nasa_etl.nasa_logs_raw WHERE status_code_str RLIKE '^[0-9]+$';
""")


def _q1_sql():
    return """
SELECT log_date_raw       AS log_date,
       status_code,
       COUNT(*)           AS request_count,
       SUM(bytes_transferred) AS total_bytes
FROM nasa_etl.nasa_logs
WHERE log_date_raw IS NOT NULL
GROUP BY log_date_raw, status_code
ORDER BY log_date_raw, status_code;
"""

def _q2_sql():
    return """
SELECT resource_path,
       COUNT(*)               AS request_count,
       SUM(bytes_transferred) AS total_bytes,
       COUNT(DISTINCT host)   AS distinct_host_count
FROM nasa_etl.nasa_logs
WHERE resource_path IS NOT NULL
GROUP BY resource_path
ORDER BY request_count DESC
LIMIT 20;
"""

def _q3_sql():
    return """
SELECT log_date_raw AS log_date,
       log_hour,
       SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)
           AS error_request_count,
       COUNT(*) AS total_request_count,
       SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) / COUNT(*)
           AS error_rate,
       COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host END)
           AS distinct_error_hosts
FROM nasa_etl.nasa_logs
WHERE log_date_raw IS NOT NULL
GROUP BY log_date_raw, log_hour
ORDER BY log_date_raw, log_hour;
"""

def _reconstruct_line(r: dict) -> str:
    req = f"{r['http_method']} {r['resource_path']} {r['protocol_version']}"
    b = str(r['bytes_transferred']) if r['bytes_transferred'] else '-'
    return f"{r['host']} - - [{r['timestamp']}] \"{req}\" {r['status_code']} {b}"
