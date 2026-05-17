#!/usr/bin/env python
"""
Pig Streaming UDF — parses raw NASA log lines.
Emits TSV: host, log_date, log_hour, http_method, resource_path,
           protocol_version, status_code, bytes_transferred
Malformed lines emit: MALFORMED\t\t0\t\t\t\t0\t0
"""
import sys
import os
import re

# Mirror of log_parser.parse_line (no imports to avoid path issues in Pig env)
_LOG_RE = re.compile(
    r'^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"([^"]*)"\s+(\S+)\s+(\S+)$'
)
_REQ_RE = re.compile(r'^(\S+)\s+(\S+)\s+(\S+)$')

MONTH_MAP = {
    'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',
    'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'
}

def parse_ts(raw):
    try:
        # "01/Jul/1995:00:00:01 -0400"
        date_part = raw.split(':')[0]           # "01/Jul/1995"
        time_part = raw.split()[0].split(':')   # ["01/Jul/1995","00","00","01"]
        d, mon, y = date_part.split('/')
        hour = int(time_part[1])
        return f"{y}-{MONTH_MAP.get(mon,'00')}-{d.zfill(2)}", hour
    except Exception:
        return None, None

for line in sys.stdin:
    line = line.rstrip('\n').rstrip('\r')
    m = _LOG_RE.match(line)
    if not m:
        print('MALFORMED\t\t0\t\t\t\t0\t0')
        continue
    host, raw_ts, request, status_raw, bytes_raw = m.groups()
    log_date, log_hour = parse_ts(raw_ts)
    if log_date is None:
        print('MALFORMED\t\t0\t\t\t\t0\t0')
        continue
    try:
        status_code = int(status_raw)
    except ValueError:
        print('MALFORMED\t\t0\t\t\t\t0\t0')
        continue
    bytes_val = 0 if bytes_raw == '-' else int(bytes_raw) if bytes_raw.isdigit() else 0
    rm = _REQ_RE.match(request.strip())
    if rm:
        method, path, proto = rm.groups()
    else:
        method, path, proto = 'UNKNOWN', request.strip() or '/', 'UNKNOWN'
    print(f"{host}\t{log_date}\t{log_hour}\t{method}\t{path}\t{proto}\t{status_code}\t{bytes_val}")
