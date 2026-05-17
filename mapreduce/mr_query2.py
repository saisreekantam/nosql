"""
MapReduce Job — Query 2: Top 20 Requested Resources
Two-step simulation:
  Step 1: group by (resource_path, host) → per-host totals
  Step 2: group by resource_path → merge hosts, compute distinct count

run_on_file() runs both steps internally.
Output rows: {resource_path, request_count, total_bytes, hosts, distinct_host_count}
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from collections import defaultdict
from mapreduce.mr_runner import MRJob
from parser.log_parser import parse_line


class MRTopResources(MRJob):
    """Single-pass MR that accumulates hosts in reducer memory."""

    def mapper(self, _, line):
        record = parse_line(line)
        if record is None:
            return
        yield record['resource_path'], (record['host'], record['bytes_transferred'])

    def reducer(self, resource_path, values):
        hosts = set()
        total_count = 0
        total_bytes = 0
        for host, b in values:
            hosts.add(host)
            total_count += 1
            total_bytes += b
        yield None, {
            'resource_path':       resource_path,
            'request_count':       total_count,
            'total_bytes':         total_bytes,
            'hosts':               list(hosts),
            'distinct_host_count': len(hosts),
        }
