import os
import sys
import time
import tempfile
from collections import defaultdict

from db import db_loader
from parser.log_parser import iter_batches
from pipelines.base_pipeline import BasePipeline

MR_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def _run_mr_job(mr_class, input_path: str) -> list:
    return mr_class().run_on_file(input_path)


class MapReducePipeline(BasePipeline):
    PIPELINE_NAME = "mapreduce"

    def run(self, log_files: list, batch_size: int, run_id: str) -> dict:
        sys.path.insert(0, MR_DIR)
        from mapreduce.mr_query1 import MRDailyTraffic
        from mapreduce.mr_query2 import MRTopResources
        from mapreduce.mr_query3 import MRHourlyErrors

        total_records = 0
        total_malformed = 0
        num_batches = 0

        # Q1 accumulator: (log_date, status_code) → {request_count, total_bytes}
        q1_acc = defaultdict(lambda: {'request_count': 0, 'total_bytes': 0})

        # Q2 accumulator: resource_path → {request_count, total_bytes, hosts: set}
        # hosts set is unioned across batches for exact distinct count
        q2_acc = defaultdict(lambda: {'request_count': 0, 'total_bytes': 0, 'hosts': set()})

        # Q3 accumulator: (log_date, log_hour) → {error_count, total_count, error_hosts: set}
        q3_acc = defaultdict(lambda: {
            'error_request_count': 0, 'total_request_count': 0, 'error_hosts': set()
        })

        start_time = time.perf_counter()

        with tempfile.TemporaryDirectory() as tmpdir:
            for batch_id, batch, malformed in iter_batches(log_files, batch_size):
                if not batch:
                    continue

                batch_file = os.path.join(tmpdir, f"batch_{batch_id}.log")
                with open(batch_file, 'w') as f:
                    for r in batch:
                        f.write(_reconstruct_line(r) + '\n')

                num_batches     += 1
                total_records   += len(batch)
                total_malformed += malformed
                print(f"  [mapreduce] batch {batch_id}: {len(batch):,} records …")

                # ── Q1 ──────────────────────────────────────────────────────
                for row in _run_mr_job(MRDailyTraffic, batch_file):
                    k = (row['log_date'], row['status_code'])
                    q1_acc[k]['request_count'] += row['request_count']
                    q1_acc[k]['total_bytes']   += row['total_bytes']

                # ── Q2 ──────────────────────────────────────────────────────
                for row in _run_mr_job(MRTopResources, batch_file):
                    k = row['resource_path']
                    q2_acc[k]['request_count'] += row['request_count']
                    q2_acc[k]['total_bytes']   += row['total_bytes']
                    # Union host sets across batches for exact distinct count
                    q2_acc[k]['hosts'].update(row.get('hosts', []))

                # ── Q3 ──────────────────────────────────────────────────────
                for row in _run_mr_job(MRHourlyErrors, batch_file):
                    k = (row['log_date'], row['log_hour'])
                    q3_acc[k]['error_request_count'] += row['error_request_count']
                    q3_acc[k]['total_request_count'] += row['total_request_count']
                    q3_acc[k]['error_hosts'].update(row.get('error_hosts', []))

        # ── Finalise Q1 ──────────────────────────────────────────────────────
        q1_rows = [
            {
                'log_date':      k[0],
                'status_code':   k[1],
                'request_count': v['request_count'],
                'total_bytes':   v['total_bytes'],
            }
            for k, v in sorted(q1_acc.items())
        ]

        # ── Finalise Q2: top 20 by request_count ─────────────────────────────
        q2_sorted = sorted(q2_acc.items(),
                           key=lambda x: x[1]['request_count'], reverse=True)[:20]
        q2_rows = [
            {
                'resource_path':      k,
                'request_count':      v['request_count'],
                'total_bytes':        v['total_bytes'],
                'distinct_host_count': len(v['hosts']),
            }
            for k, v in q2_sorted
        ]

        # ── Finalise Q3 ──────────────────────────────────────────────────────
        q3_rows = []
        for (log_date, log_hour), v in sorted(q3_acc.items()):
            total = v['total_request_count']
            err   = v['error_request_count']
            q3_rows.append({
                'log_date':             log_date,
                'log_hour':             log_hour,
                'error_request_count':  err,
                'total_request_count':  total,
                'error_rate':           err / total if total > 0 else 0.0,
                'distinct_error_hosts': len(v['error_hosts']),
            })

        avg_batch_size = total_records / num_batches if num_batches else 0
        runtime = time.perf_counter() - start_time

        # etl_runs row must exist before FK-dependent query result rows
        db_loader.save_run_metadata(
            run_id=run_id, pipeline=self.PIPELINE_NAME,
            total_records=total_records, malformed=total_malformed,
            batch_size=batch_size, num_batches=num_batches,
            avg_batch_size=avg_batch_size, runtime_seconds=runtime,
        )
        db_loader.save_q1(run_id, self.PIPELINE_NAME, num_batches, q1_rows)
        db_loader.save_q2(run_id, self.PIPELINE_NAME, num_batches, q2_rows)
        db_loader.save_q3(run_id, self.PIPELINE_NAME, num_batches, q3_rows)

        print(f"  [mapreduce] done in {runtime:.2f}s  "
              f"({total_records:,} records, {num_batches} batches)")

        return {
            'run_id':          run_id,
            'pipeline':        self.PIPELINE_NAME,
            'total_records':   total_records,
            'malformed':       total_malformed,
            'num_batches':     num_batches,
            'avg_batch_size':  avg_batch_size,
            'runtime_seconds': runtime,
        }


def _reconstruct_line(r: dict) -> str:
    req = f"{r['http_method']} {r['resource_path']} {r['protocol_version']}"
    b   = str(r['bytes_transferred']) if r['bytes_transferred'] else '-'
    return f"{r['host']} - - [{r['timestamp']}] \"{req}\" {r['status_code']} {b}"
