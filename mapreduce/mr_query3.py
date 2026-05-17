"""
MapReduce Job — Query 3: Hourly Error Analysis
Output rows: {log_date, log_hour, error_request_count, total_request_count,
              error_rate, error_hosts, distinct_error_hosts}
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from mapreduce.mr_runner import MRJob
from parser.log_parser import parse_line


class MRHourlyErrors(MRJob):

    def mapper(self, _, line):
        record = parse_line(line)
        if record is None:
            return
        is_error = 400 <= record['status_code'] <= 599
        yield (record['log_date'], record['log_hour']), \
              (is_error, record['host'] if is_error else None)

    def reducer(self, key, values):
        log_date, log_hour = key
        total, errors = 0, 0
        error_hosts = set()
        for is_error, host in values:
            total += 1
            if is_error:
                errors += 1
                if host:
                    error_hosts.add(host)
        yield None, {
            'log_date':             log_date,
            'log_hour':             log_hour,
            'error_request_count':  errors,
            'total_request_count':  total,
            'error_rate':           errors / total if total > 0 else 0.0,
            'error_hosts':          list(error_hosts),
            'distinct_error_hosts': len(error_hosts),
        }
