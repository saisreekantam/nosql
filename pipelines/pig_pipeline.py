import os
import time
import subprocess
import tempfile

import config
from db import db_loader
from parser.log_parser import iter_batches
from pipelines.base_pipeline import BasePipeline

PIG_SCRIPTS  = os.path.join(os.path.dirname(__file__), '..', 'pig_scripts')
HADOOP_HOME  = os.getenv('HADOOP_HOME', '/opt/homebrew/Cellar/hadoop/3.5.0/libexec')
HADOOP_BIN   = os.path.join(HADOOP_HOME, 'bin')
HADOOP_CONF  = os.path.join(HADOOP_HOME, 'etc', 'hadoop')
# Hive/Hadoop need Java 21; Pig 0.17.0 needs Java 17
JAVA_HOME_HADOOP = os.getenv('JAVA_HOME',
    '/opt/homebrew/Cellar/openjdk@21/21.0.11/libexec/openjdk.jdk/Contents/Home')
JAVA_HOME_PIG = os.getenv('PIG_JAVA_HOME',
    '/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home')

HDFS_PIG_INPUT  = '/user/nasa_etl/pig_batches'
HDFS_PIG_OUTPUT = '/user/nasa_etl/pig_output'


def _hdfs(args: list):
    env = os.environ.copy()
    env['JAVA_HOME'] = JAVA_HOME_HADOOP
    subprocess.run(
        [os.path.join(HADOOP_BIN, 'hdfs')] + args,
        check=True, capture_output=True, env=env
    )


class PigPipeline(BasePipeline):
    PIPELINE_NAME = "pig"

    def run(self, log_files: list, batch_size: int, run_id: str) -> dict:
        start_time = time.perf_counter()
        total_records = 0
        total_malformed = 0
        num_batches = 0

        # Wipe HDFS input dir so stale batches from prior runs don't mix in
        try:
            _hdfs(['dfs', '-rm', '-r', '-f', HDFS_PIG_INPUT])
        except Exception:
            pass
        _hdfs(['dfs', '-mkdir', '-p', HDFS_PIG_INPUT])

        # Also wipe any leftover output dir
        try:
            _hdfs(['dfs', '-rm', '-r', '-f', HDFS_PIG_OUTPUT])
        except Exception:
            pass

        with tempfile.TemporaryDirectory() as tmpdir:
            # Parse batches → TSV → upload to HDFS
            for batch_id, batch, malformed in iter_batches(log_files, batch_size):
                if not batch:
                    continue
                local_file = os.path.join(tmpdir, f"batch_{batch_id}.tsv")
                with open(local_file, 'w') as f:
                    for r in batch:
                        f.write(_tsv_line(r) + '\n')
                _hdfs(['dfs', '-put', local_file,
                       f'{HDFS_PIG_INPUT}/batch_{batch_id}.tsv'])
                num_batches     += 1
                total_records   += len(batch)
                total_malformed += malformed
                print(f"  [pig] batch {batch_id}: {len(batch):,} records → HDFS")

            pig_script = os.path.join(PIG_SCRIPTS, 'etl_queries.pig')
            print(f"  [pig] running Pig on Hadoop MapReduce ({num_batches} batches) …")

            env = os.environ.copy()
            env['JAVA_HOME']      = JAVA_HOME_PIG
            env['HADOOP_HOME']    = HADOOP_HOME
            env['HADOOP_CONF_DIR'] = HADOOP_CONF

            result = subprocess.run(
                [
                    'pig',                    # no -x local → real MapReduce
                    '-param', f'INPUT=hdfs://localhost:9000{HDFS_PIG_INPUT}',
                    '-param', f'OUTPUT=hdfs://localhost:9000{HDFS_PIG_OUTPUT}',
                    '-f', pig_script,
                ],
                capture_output=True, text=True, env=env
            )
            if result.returncode != 0:
                raise RuntimeError(f"Pig script failed:\n{result.stderr[-3000:]}")

            # Pull results from HDFS to local temp dir for reading
            local_output = os.path.join(tmpdir, 'output')
            os.makedirs(local_output)
            for q in ('q1', 'q2', 'q3'):
                _hdfs(['dfs', '-get',
                       f'{HDFS_PIG_OUTPUT}/{q}',
                       os.path.join(local_output, q)])

            q1_rows = _read_pig_output(local_output, 'q1',
                ['log_date', 'status_code', 'request_count', 'total_bytes'],
                [str, int, int, int])

            q2_rows = _read_pig_output(local_output, 'q2',
                ['resource_path', 'request_count', 'total_bytes', 'distinct_host_count'],
                [str, int, int, int], top_n=20, sort_key='request_count')

            q3_rows = _read_pig_output(local_output, 'q3',
                ['log_date', 'log_hour', 'error_request_count',
                 'total_request_count', 'error_rate', 'distinct_error_hosts'],
                [str, int, int, int, float, int])

        # Cleanup HDFS output (input kept for inspection; cleaned at next run start)
        try:
            _hdfs(['dfs', '-rm', '-r', '-f', HDFS_PIG_OUTPUT])
        except Exception:
            pass

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
        print(f"  [pig] done in {runtime:.2f}s")

        return {
            'run_id':          run_id,
            'pipeline':        self.PIPELINE_NAME,
            'total_records':   total_records,
            'malformed':       total_malformed,
            'num_batches':     num_batches,
            'avg_batch_size':  avg_batch_size,
            'runtime_seconds': runtime,
        }


def _read_pig_output(output_dir: str, query: str,
                     columns: list, casts: list,
                     top_n: int = None, sort_key: str = None) -> list:
    q_dir = os.path.join(output_dir, query)
    rows = []
    for fname in os.listdir(q_dir):
        if not fname.startswith('part-'):
            continue
        with open(os.path.join(q_dir, fname)) as f:
            for line in f:
                fields = line.rstrip('\n').split('\t')
                if len(fields) != len(columns):
                    continue
                row = {}
                for col, cast, val in zip(columns, casts, fields):
                    try:
                        row[col] = cast(val)
                    except (ValueError, TypeError):
                        row[col] = None
                rows.append(row)
    if sort_key:
        rows.sort(key=lambda r: r.get(sort_key, 0), reverse=True)
    if top_n:
        rows = rows[:top_n]
    return rows


def _tsv_line(r: dict) -> str:
    return '\t'.join([
        r['host'], r['log_date'], str(r['log_hour']),
        r['http_method'], r['resource_path'], r['protocol_version'],
        str(r['status_code']), str(r['bytes_transferred']),
    ])
