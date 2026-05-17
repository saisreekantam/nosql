import re
from datetime import datetime
from typing import Optional

# NASA Combined Log Format:
# host - - [DD/Mon/YYYY:HH:MM:SS -ZONE] "METHOD /path HTTP/ver" status bytes
_LOG_RE = re.compile(
    r'^(\S+)'           # host
    r'\s+\S+\s+\S+'     # ident, authuser (ignored)
    r'\s+\[([^\]]+)\]'  # [timestamp]
    r'\s+"([^"]*)"'     # "request"
    r'\s+(\S+)'         # status_code
    r'\s+(\S+)$'        # bytes
)
_REQ_RE = re.compile(r'^(\S+)\s+(\S+)\s+(\S+)$')

MONTH_MAP = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,  'May': 5,  'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
}


def parse_timestamp(raw: str):
    # "01/Jul/1995:00:00:01 -0400"
    try:
        dt = datetime.strptime(raw.strip(), "%d/%b/%Y:%H:%M:%S %z")
        return dt.strftime("%Y-%m-%d"), dt.hour
    except ValueError:
        return None, None


def parse_line(line: str) -> Optional[dict]:
    """
    Returns a parsed record dict, or None if the line is malformed.
    Never raises — caller tracks the None count.
    """
    line = line.rstrip('\n').rstrip('\r')
    m = _LOG_RE.match(line)
    if not m:
        return None

    host, raw_ts, request, status_raw, bytes_raw = m.groups()

    log_date, log_hour = parse_timestamp(raw_ts)
    if log_date is None:
        return None

    # Parse status code
    try:
        status_code = int(status_raw)
    except ValueError:
        return None

    # Parse bytes (- => 0)
    bytes_transferred = 0 if bytes_raw == '-' else None
    if bytes_transferred is None:
        try:
            bytes_transferred = int(bytes_raw)
        except ValueError:
            bytes_transferred = 0

    # Parse request string into method / path / protocol
    rm = _REQ_RE.match(request.strip())
    if rm:
        http_method, resource_path, protocol_version = rm.groups()
    else:
        # Non-standard request string — keep what we have, mark rest as unknown
        http_method = 'UNKNOWN'
        resource_path = request.strip() or '/'
        protocol_version = 'UNKNOWN'

    return {
        'host':             host,
        'timestamp':        raw_ts,
        'log_date':         log_date,
        'log_hour':         log_hour,
        'http_method':      http_method,
        'resource_path':    resource_path,
        'protocol_version': protocol_version,
        'status_code':      status_code,
        'bytes_transferred': bytes_transferred,
    }


def iter_batches(log_files: list, batch_size: int):
    """
    Yields (batch_id, batch, malformed_in_batch) tuples.
    batch is a list of parsed record dicts.
    malformed_in_batch is the raw malformed count for that batch window.
    """
    batch_id = 1
    batch = []
    malformed = 0

    for path in log_files:
        with open(path, 'r', encoding='utf-8', errors='replace') as fh:
            for line in fh:
                if not line.strip():
                    continue
                record = parse_line(line)
                if record is None:
                    malformed += 1
                else:
                    batch.append(record)

                if len(batch) == batch_size:
                    yield batch_id, batch, malformed
                    batch_id += 1
                    batch = []
                    malformed = 0

    if batch:  # final partial batch
        yield batch_id, batch, malformed
