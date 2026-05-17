"""
MapReduce Job — Query 1: Daily Traffic Summary
Output rows: {log_date, status_code, request_count, total_bytes}
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from mapreduce.mr_runner import MRJob
from parser.log_parser import parse_line


class MRDailyTraffic(MRJob):

    def mapper(self, _, line):
        record = parse_line(line)
        if record is None:
            return
        yield (record['log_date'], record['status_code']), \
              (1, record['bytes_transferred'])

    def reducer(self, key, values):
        request_count, total_bytes = 0, 0
        for count, b in values:
            request_count += count
            total_bytes   += b
        log_date, status_code = key
        yield None, {
            'log_date':      log_date,
            'status_code':   status_code,
            'request_count': request_count,
            'total_bytes':   total_bytes,
        }
