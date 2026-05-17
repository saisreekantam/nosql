# DAS 839 — NoSQL Systems
# Project Report: Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics

**Contributors:**

| Name | Roll Number |
|---|---|
| Sai Venkat Sreekantam | IMT2023501 |
| Revanth | IMT2023118 |
| Aditya | IMT2023114 |

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Dataset Description](#2-dataset-description)
3. [System Architecture](#3-system-architecture)
4. [Parsing Strategy](#4-parsing-strategy)
5. [ETL Workflow](#5-etl-workflow)
6. [Batching Approach](#6-batching-approach)
7. [Analytical Queries](#7-analytical-queries)
8. [Pipeline Implementations](#8-pipeline-implementations)
   - 8.1 [MongoDB](#81-mongodb-pipeline)
   - 8.2 [MapReduce](#82-mapreduce-pipeline)
   - 8.3 [Hive](#83-hive-pipeline)
   - 8.4 [Pig](#84-pig-pipeline)
9. [Relational Database Schema](#9-relational-database-schema)
10. [Reporting Module](#10-reporting-module)
11. [Equivalence Across Pipelines](#11-equivalence-across-pipelines)
12. [Experimental Results](#12-experimental-results)
13. [Runtime Analysis and Comparison](#13-runtime-analysis-and-comparison)
14. [Design Decisions](#14-design-decisions)
15. [Challenges and Solutions](#15-challenges-and-solutions)
16. [How to Run](#16-how-to-run)
17. [Conclusion](#17-conclusion)

---

## 1. Project Overview

The goal of this project is to design and implement a **multi-pipeline ETL and reporting framework** that executes the same analytical workload over the NASA HTTP Web Server Logs using four different execution backends: **MongoDB**, **MapReduce**, **Apache Hive**, and **Apache Pig**.

The framework is built as a single coherent CLI tool. The user selects one of the four pipelines at runtime — the tool reads raw NASA log files, parses them, runs three predefined analytical queries, and stores the aggregated results in a PostgreSQL reporting database. A separate reporting script queries that database and displays the results along with execution metadata.

The core objective is comparative: to study how different data processing paradigms handle the same semi-structured analytics problem and to measure them in terms of runtime, implementation complexity, batching behaviour, and suitability for log analytics.

---

## 2. Dataset Description

**Source:** NASA HTTP Web Server Logs from the Internet Traffic Archive  
**Files:**
- `NASA_access_log_Jul95` — July 1995 access logs
- `NASA_access_log_Aug95` — August 1995 access logs

**Format:** Plain text, one HTTP request per line, Combined Log Format:
```
host - - [DD/Mon/YYYY:HH:MM:SS -ZONE] "METHOD /path HTTP/ver" status_code bytes
```

**Example line:**
```
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
```

**Dataset Statistics (after parsing):**

| Metric | Value |
|---|---|
| Total valid records | 3,461,580 |
| Malformed / unparseable lines | 33 |
| Total bytes transferred | 65,524,307,881 (~61 GB) |
| Date range | July 1 – August 31, 1995 |

No manual cleaning, editing, filtering, or format conversion was done outside the pipeline. The only permitted pre-processing step per the assignment was decompressing the `.gz` files.

---

## 3. System Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    main.py  (CLI — Click)                  │
│   --pipeline {mongodb|mapreduce|hive|pig}                  │
│   --batch-size N   --log-files file1 file2                 │
└───────────────────────┬──────────────────────────────────┘
                        │  importlib dynamic load
           ┌────────────▼────────────┐
           │     BasePipeline        │  (abstract interface)
           │     .run(files, bs, id) │
           └──┬────┬────┬────┬──────┘
              │    │    │    │
    ┌─────────▼┐ ┌─▼──┐ ┌▼────┐ ┌▼────┐
    │ MongoDB  │ │ MR │ │Hive │ │ Pig │
    └─────┬────┘ └──┬─┘ └──┬──┘ └──┬──┘
          │         │      │       │
          └─────────┴──────┴───────┘
                        │
           ┌────────────▼────────────┐
           │   parser/log_parser.py  │  (shared: parse_line + iter_batches)
           └────────────────────────┘
                        │
           ┌────────────▼────────────┐
           │   db/db_loader.py       │  (save_run_metadata / save_q1/q2/q3)
           └────────────────────────┘
                        │
           ┌────────────▼────────────┐
           │   PostgreSQL (nasa_etl) │  (4 tables: etl_runs, q1, q2, q3)
           └────────────────────────┘
                        │
           ┌────────────▼────────────┐
           │  reporting/report.py    │  (tabulated query results + run metadata)
           └────────────────────────┘
```

**Why this architecture?**  
The `BasePipeline` abstract class enforces a common `run()` interface across all four backends. Pipelines are loaded dynamically via `importlib` so the CLI does not import all four backends — only the selected one is loaded, avoiding unnecessary dependency conflicts (e.g., importing Hive dependencies when running MongoDB).

---

## 4. Parsing Strategy

**File:** `parser/log_parser.py`  
**Design principle:** One parser, shared by all four pipelines. Ensures identical field extraction and identical malformed-record handling regardless of backend.

### 4.1 Regex

The parser uses two compiled regex patterns:

```python
# Outer: captures the 5 main fields from Combined Log Format
_LOG_RE = re.compile(
    r'^(\S+)'           # host
    r'\s+\S+\s+\S+'     # ident, authuser (ignored)
    r'\s+\[([^\]]+)\]'  # [timestamp]
    r'\s+"([^"]*)"'     # "request string"
    r'\s+(\S+)'         # status_code
    r'\s+(\S+)$'        # bytes
)

# Inner: splits the request string into method + path + protocol
_REQ_RE = re.compile(r'^(\S+)\s+(\S+)\s+(\S+)$')
```

### 4.2 Field Extraction

| Field | Source | Handling |
|---|---|---|
| `host` | Outer regex group 1 | Taken as-is |
| `log_date` | Timestamp string | Parsed via `datetime.strptime`, output as `YYYY-MM-DD` |
| `log_hour` | Timestamp string | `dt.hour` integer |
| `http_method` | Inner regex group 1 | `UNKNOWN` if request doesn't match 3-part format |
| `resource_path` | Inner regex group 2 | Raw string if inner regex fails |
| `protocol_version` | Inner regex group 3 | `UNKNOWN` if inner regex fails |
| `status_code` | Outer regex group 4 | `int()` — returns `None` on failure |
| `bytes_transferred` | Outer regex group 5 | `0` if value is `-`; `int()` otherwise |

### 4.3 Malformed Record Handling

`parse_line()` returns `None` (never raises) if:
- The line doesn't match `_LOG_RE`
- The timestamp cannot be parsed by `datetime.strptime`
- The status code is not a valid integer

The caller (`iter_batches`) counts every `None` return. The count is stored in the database and shown in the report. It is never silently dropped. Across 3,461,613 total lines, 33 were malformed.

---

## 5. ETL Workflow

Every pipeline follows the same three-phase flow:

```
EXTRACT   → Read raw .log files line-by-line via iter_batches()
              (shared parser handles format normalisation)

TRANSFORM → Execute Q1, Q2, Q3 using the backend's native engine
              (MongoDB aggregation / Python MR / HiveQL / Pig Latin)

LOAD      → Write aggregated results to PostgreSQL via db_loader
              (save_run_metadata first, then save_q1/q2/q3)
```

**Runtime measurement:**  
`time.perf_counter()` is called at the very start of `run()` (before first file read) and at the very end (after final `save_q3()` call). Dataset download time, software installation time, and report rendering time are all excluded.

---

## 6. Batching Approach

**File:** `parser/log_parser.py` — `iter_batches(log_files, batch_size)`

### How it works

- Both log files (Jul95 + Aug95) are treated as **one continuous stream** — no reset between files
- Records are accumulated into a list until `len(batch) == batch_size`
- When the threshold is reached, `(batch_id, batch, malformed_count)` is yielded
- Batch IDs start at **1** and increment sequentially
- The final partial batch (which may have fewer records than `batch_size`) is still yielded as a valid batch

### Key numbers at `batch_size = 100,000`

| Metric | Value |
|---|---|
| Total batches | 35 |
| Full batches (100,000 records) | 34 |
| Final partial batch | 61,580 records |
| Average batch size | 98,902.3 |

Average batch size formula (per assignment specification):
```
avg_batch_size = total_records / num_batches = 3,461,580 / 35 = 98,902.3
```

### Why batch?

Batching prevents loading 3.46M records into memory at once. Each pipeline processes and stores one batch at a time (MongoDB inserts, Hive uploads to HDFS, Pig uploads to HDFS, MapReduce writes temp file). This makes the tool memory-safe for arbitrarily large log files.

---

## 7. Analytical Queries

The same three queries are implemented in all four pipelines with identical semantics:

### Query 1: Daily Traffic Summary

For each `(log_date, status_code)` pair, compute total request count and total bytes transferred.

**Output schema:** `log_date, status_code, request_count, total_bytes`  
**Result rows:** 295

### Query 2: Top 20 Requested Resources

Group by `resource_path`, compute request count, total bytes, and exact distinct host count. Return top 20 by request count.

**Output schema:** `resource_path, request_count, total_bytes, distinct_host_count`  
**Result rows:** 20  
**Top resource:** `/images/NASA-logosmall.gif` — 208,362 requests

### Query 3: Hourly Error Analysis

For each `(log_date, log_hour)`, compute error request count (status 400–599), total request count, error rate, and distinct error host count.

**Output schema:** `log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts`  
**Result rows:** 1,369

---

## 8. Pipeline Implementations

---

### 8.1 MongoDB Pipeline

**File:** `pipelines/mongodb_pipeline.py`  
**Runtime:** 36.65 seconds

#### Architecture

MongoDB is used as both the intermediate store and the query engine. All 3.46M parsed records are bulk-inserted into a MongoDB collection (`nasa_etl.nasa_logs`), then three aggregation pipelines are executed against that collection. Results are returned as Python dicts and written to PostgreSQL.

#### Why MongoDB

MongoDB's native aggregation framework (`$group`, `$sum`, `$addToSet`, `$size`) maps exactly to all three queries without any schema transformations. The `$addToSet` operator gives exact distinct host counts without approximation. `allowDiskUse=True` prevents memory errors on large groupings.

#### Batch Loading

Each batch from `iter_batches()` is converted to a list of documents and inserted via `insert_many(ordered=False)`. `ordered=False` allows MongoDB to continue inserting remaining documents if a single write fails, improving throughput.

#### Queries

**Q1** uses `$group` on `(log_date, status_code)` with `$sum: 1` for count and `$sum: "$bytes_transferred"` for bytes.

**Q2** groups by `resource_path`, uses `$addToSet: "$host"` to accumulate all unique hosts into a set, then `$size: "$hosts"` to count them. Sorted by `request_count DESC`, limited to 20.

**Q3** groups by `(log_date, log_hour)` and uses a `$cond` expression to conditionally count error records and accumulate error hosts: only hosts with `status_code` in `[400, 599]` are added to the set via `$$REMOVE`.

#### Performance Reason

MongoDB is the fastest pipeline at 36.65s because:
1. No JVM startup — native C++ process
2. After `insert_many`, the collection is held in memory; all three aggregations scan RAM, not disk
3. No container scheduling, no HDFS, no shuffle-to-disk
4. The aggregation engine is highly optimised and runs in the same process as the data store

---

### 8.2 MapReduce Pipeline

**File:** `pipelines/mapreduce_pipeline.py`  
**Supporting files:** `mapreduce/mr_runner.py`, `mr_query1.py`, `mr_query2.py`, `mr_query3.py`  
**Runtime:** 92.88 seconds

#### Architecture

A custom lightweight MapReduce runner (`MRJob`) was implemented in pure Python. The framework uses Python's `itertools.groupby` after an in-memory sort to emulate the Map → Sort → Shuffle → Reduce phases. Each query is a subclass of `MRJob` with `mapper()` and `reducer()` methods.

For each batch, a temp TSV file is written, then `MRDailyTraffic`, `MRTopResources`, and `MRHourlyErrors` are each run on it. Results are accumulated into cross-batch dictionaries using `defaultdict`.

#### Why Custom Runner Instead of `mrjob`

`mrjob` was the first choice but it uses Python's `pipes` module which was removed in Python 3.14. A lightweight replacement was written from scratch — it has no external dependencies, handles both tuple and string keys correctly (via `str(key)` for sort), and is clean enough to extend.

#### Cross-Batch Aggregation

For Q2 and Q3, distinct host counts cannot be computed per-batch and then summed — summing per-batch counts would over-count hosts that appear in multiple batches. The solution is to maintain **Python sets** across batches:
- `q2_acc[resource_path]['hosts']` is a `set` that grows via `.update()` across all 35 batches
- `q3_acc[(date, hour)]['error_hosts']` is similarly accumulated
- `len(set)` gives the exact distinct count at the end

For Q1, simple integer sums suffice since `(log_date, status_code)` aggregation is additive.

#### Performance Reason

92.88s is faster than Hive and Pig because:
1. No JVM, no YARN container overhead
2. Everything runs in a single Python process
3. No HDFS I/O — temp files are written to `/tmp` (local SSD)

The limitation is that it's sequential: 35 batches × 3 queries = 105 serial map-reduce passes with no parallelism.

---

### 8.3 Hive Pipeline

**File:** `pipelines/hive_pipeline.py`  
**Runtime:** 195.62 seconds  
**Cluster:** Hadoop 3.5.0 pseudo-distributed + YARN ResourceManager/NodeManager

#### Architecture

The Hive pipeline uses HiveServer2 (accessed via `beeline` CLI) with an EXTERNAL TABLE backed by HDFS. The pipeline:

1. Wipes and recreates `/user/nasa_etl/batches/` on HDFS before each run
2. Creates a `nasa_logs_raw` EXTERNAL TABLE using `RegexSerDe` to parse the raw Combined Log Format directly in Hive
3. Creates a `nasa_logs` VIEW over the raw table that extracts `log_date`, `log_hour`, `http_method`, `resource_path`, `protocol_version`, `status_code`, `bytes_transferred` using Hive built-in string functions
4. Uploads each reconstructed batch file to HDFS via `hdfs dfs -put`
5. Runs Q1, Q2, Q3 as three separate `beeline -e` sessions

#### Why `beeline -e` and not `beeline -f`

A critical bug was discovered in Hive 4.2.0: `beeline -f <sqlfile>` silently echoes the SQL to stdout without actually executing it. No error is raised, and no rows are affected. The fix is to pass SQL directly via `beeline -e "<sql>"` which reliably executes.

#### EXTERNAL TABLE + RegexSerDe

Instead of pre-parsing files into CSV/TSV before uploading, the raw log files are uploaded to HDFS as-is. The `RegexSerDe` uses a Java regex to parse each line at query time. A Hive `VIEW` then applies string manipulation (`SUBSTR`, `regexp_extract`, `CAST`, `IF`) to extract properly typed fields.

This approach was chosen because it demonstrates genuine Hive usage — the parsing happens inside the Hive/MapReduce layer, not in Python preprocessing.

#### HDFS Cleanup

Before each run, `hdfs dfs -rm -r -f /user/nasa_etl/batches` is called. Without this, stale batch files from previous runs remain on HDFS and inflate record counts (e.g., a 5,000-record test batch from debugging inflated counts by 5,000 on first run).

#### All DDL in One Session

All DDL (`CREATE DATABASE`, `DROP TABLE`, `CREATE EXTERNAL TABLE`, `CREATE VIEW`) is sent in a **single `beeline -e` call**. This is required because Derby (Hive's embedded metastore) uses a single-connection model — splitting DDL across two `beeline` sessions can cause the second session to not see the schema created by the first.

#### YARN Integration

Hive submits real MapReduce jobs to the YARN ResourceManager (`mapreduce.framework.name=yarn` in `mapred-site.xml`). Each of the 3 queries becomes one YARN MR job. On a pseudo-distributed single node, this adds JVM container allocation overhead per job but demonstrates authentic distributed execution.

#### Performance Reason

195.62s because:
- 3 separate beeline → YARN MR job cycles (JVM start + RM/NM handshake + task run + teardown per query)
- RegexSerDe re-parses all 3.46M records for every query (3 full scans)
- No MULTI_QUERY optimisation — each HiveQL is its own independent MR job

---

### 8.4 Pig Pipeline

**File:** `pipelines/pig_pipeline.py`  
**Script:** `pig_scripts/etl_queries.pig`  
**Runtime:** 1031.76 seconds (~17 minutes)  
**Cluster:** Hadoop 3.5.0 pseudo-distributed + YARN ResourceManager/NodeManager

#### Architecture

The Pig pipeline uploads pre-parsed TSV batch files to HDFS, then runs a single Pig Latin script that covers all three queries. Pig submits the jobs to YARN as real MapReduce jobs.

The Pig pipeline flow:
1. Wipe and recreate `/user/nasa_etl/pig_batches/` on HDFS
2. For each batch, write a 8-column TSV (host, log_date, log_hour, http_method, resource_path, protocol_version, status_code, bytes_transferred) to a local temp file and upload via `hdfs dfs -put`
3. Run `pig -f etl_queries.pig` with HDFS input/output paths as `-param` arguments
4. Pull results back from HDFS with `hdfs dfs -get`
5. Parse output TSV files and load into PostgreSQL

#### Why Pre-Parsed TSV Instead of Streaming UDF

The original design used a Python streaming UDF to parse the raw log format inside Pig. This was abandoned for two reasons:

1. **Java serialization crash**: `STREAM raw THROUGH ParseLog` with a Python UDF causes a `NullPointerException` (`OperatorKey.mKey null`) in Pig 0.17.0 on Java 17+. This is a known incompatibility — the streaming UDF mechanism relies on internal Pig serialization that was broken with newer JVM versions.
2. **SHIP path issues**: Even when the crash was bypassed, the `SHIP('/absolute/path/udf.py')` parameter did not reliably resolve from within the Pig subprocess's working directory.

The pre-parsed TSV approach is cleaner: Python handles what Python is best at (parsing), and Pig handles what Pig is best at (grouping, aggregating, joining at scale).

#### Pig Latin Script Design

**Q1** — Groups `data` by `(log_date, status_code)`, computes `COUNT` and `SUM(bytes_transferred)`, sorts by date and status code.

**Q2** — Groups by `resource_path`, uses a nested `DISTINCT` block for exact distinct host count:
```pig
q2_agg = FOREACH q2_grp {
    hosts          = data.host;
    distinct_hosts = DISTINCT hosts;
    GENERATE ... COUNT(distinct_hosts) AS distinct_host_count;
};
```
`COUNT(DISTINCT X)` inline is not supported in Pig 0.17.0 — the `DISTINCT` must be a separate named alias inside the nested block.

**Q3** — Uses a nested `FILTER` inside `FOREACH` to isolate error records, then `DISTINCT` on those hosts for exact distinct error host count.

**ORDER BY for Q2** was deliberately removed from the Pig script. `ORDER ... BY ... DESC; LIMIT 20` for Q2 crashes Pig 0.17.0 with a `Job failed` error on single-node YARN. The top-20 selection is handled in Python after reading output files via `sort_key='request_count', top_n=20` in `_read_pig_output()`.

#### Java Version Handling

Hive and Hadoop require Java 21; Pig 0.17.0 requires Java 17. Two separate `JAVA_HOME` constants are maintained:
- `JAVA_HOME_HADOOP` — used for `hdfs` CLI calls (Java 21)
- `JAVA_HOME_PIG` — used for the `pig` subprocess (Java 17)

The Pig subprocess also receives `HADOOP_CONF_DIR` pointing to the Hadoop configuration so it can locate the YARN ResourceManager.

#### Performance Reason

1031.76s (~17 minutes) because Pig submits approximately 5 YARN MR jobs:
- Q1 sort job + Q1 compute job
- Q2 compute job (MULTI_QUERY with Q1 where possible)
- Q3 compute job + Q3 sort job

Each YARN job on a single-node pseudo-distributed cluster incurs 60–120s of container allocation overhead (ResourceManager scheduling → NodeManager JVM launch → task execution → JVM teardown). With 5 jobs: 5 × 3–4 minutes ≈ 15–20 minutes. This matches what students on similar setups observe (~30 minutes on slower hardware).

On a real 10-node cluster this overhead amortizes over true parallel execution and Pig would outperform both Python MapReduce and Hive significantly.

---

## 9. Relational Database Schema

**Database:** PostgreSQL 18.1 (`nasa_etl`)  
**File:** `db/schema.sql`

```sql
CREATE TABLE etl_runs (
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

CREATE TABLE q1_daily_traffic (
    id             SERIAL PRIMARY KEY,
    run_id         UUID REFERENCES etl_runs(run_id),
    pipeline_name  VARCHAR(20),
    batch_id       INTEGER,
    execution_time TIMESTAMP,
    log_date       DATE,
    status_code    INTEGER,
    request_count  BIGINT,
    total_bytes    BIGINT
);

CREATE TABLE q2_top_resources (
    id                  SERIAL PRIMARY KEY,
    run_id              UUID REFERENCES etl_runs(run_id),
    pipeline_name       VARCHAR(20),
    batch_id            INTEGER,
    execution_time      TIMESTAMP,
    resource_path       TEXT,
    request_count       BIGINT,
    total_bytes         BIGINT,
    distinct_host_count INTEGER
);

CREATE TABLE q3_hourly_errors (
    id                   SERIAL PRIMARY KEY,
    run_id               UUID REFERENCES etl_runs(run_id),
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

CREATE INDEX idx_q1_run    ON q1_daily_traffic(run_id);
CREATE INDEX idx_q2_run    ON q2_top_resources(run_id);
CREATE INDEX idx_q3_run    ON q3_hourly_errors(run_id);
CREATE INDEX idx_runs_pipe ON etl_runs(pipeline_name);
```

### Design Decisions

**One result table per query** — chosen over a single `results` table with a `query_id` column because it gives each query a precise schema (correct types, correct column names) and makes the reporting queries simple `SELECT *` joins rather than type-casting pivots.

**UUID for `run_id`** — chosen over `SERIAL` integer IDs because UUIDs are generation-safe across concurrent runs, carry no ordering assumption, and can be passed from the CLI at run time (`--run-id` flag) or auto-generated.

**FK ordering constraint** — `save_run_metadata()` is always called before `save_q1/q2/q3()`. All three result tables have `run_id REFERENCES etl_runs(run_id)`, so the parent row must exist first. Violating this order causes a FK constraint violation.

**`BIGINT` for counts and bytes** — `request_count` and `total_bytes` use `BIGINT` because July+August 1995 already produce 65 billion total bytes. `INTEGER` (max ~2.1 billion) would overflow.

---

## 10. Reporting Module

**File:** `reporting/report.py`  
**Dependency:** `tabulate`, `psycopg2`, `click`

The reporting script is a separate CLI tool that reads from PostgreSQL and displays results. It can be invoked three ways:

```bash
# Show a specific run
python reporting/report.py --run-id <uuid>

# Show latest run for a pipeline
python reporting/report.py --pipeline mongodb

# Show most recent run overall
python reporting/report.py --latest
```

**Output includes:**
- Run metadata: pipeline name, run ID, timestamp, runtime, total records, malformed count, batch size, num batches, avg batch size
- Q1 table: all 295 rows sorted by `log_date, status_code`
- Q2 table: top 20 resources sorted by `request_count DESC`
- Q3 table: all 1,369 rows sorted by `log_date, log_hour`

All tables are rendered using `tabulate` in `simple` format with integer formatting (commas in large numbers).

---

## 11. Equivalence Across Pipelines

Three mechanisms ensure all four pipelines produce identical results:

### 1. Shared Parser
Every pipeline calls the same `iter_batches(log_files, batch_size)` generator. The same regex, same field extraction, same `-`→`0` bytes handling, same malformed-record definition applies to all four. No pipeline does its own parsing.

### 2. Same Query Logic
Each query is defined once semantically and then faithfully translated to the backend's native idiom:

| Query | MongoDB | MapReduce | Hive | Pig |
|---|---|---|---|---|
| Q1 grouping | `$group` | `groupby` dict | `GROUP BY` | `GROUP BY` |
| Q2 distinct hosts | `$addToSet` + `$size` | Python `set` union | `COUNT(DISTINCT host)` | `DISTINCT` + `COUNT` |
| Q3 error filter | `$cond $gte/$lte` | `if 400 <= sc <= 599` | `CASE WHEN BETWEEN` | `FILTER status_code >= 400` |

### 3. Same Output Schema
All four write to the same PostgreSQL tables with the same column names and types. The reporting script sees no difference between a MongoDB run and a Pig run.

### Verified Results

All four pipelines produce **identical** output:

| Metric | Value |
|---|---|
| Total records | 3,461,580 |
| Malformed records | 33 |
| Q1 rows | 295 |
| Q2 rows | 20 |
| Q3 rows | 1,369 |
| Total bytes (Q1 sum) | 65,524,307,881 |
| Top Q2 resource | `/images/NASA-logosmall.gif` — 208,362 requests |

---

## 12. Experimental Results

All runs used `batch_size = 100,000`.

### Run Metadata

| Pipeline | Runtime | Records | Batches | Avg Batch | Malformed |
|---|---|---|---|---|---|
| MongoDB | 36.65s | 3,461,580 | 35 | 98,902.3 | 33 |
| MapReduce | 92.88s | 3,461,580 | 35 | 98,902.3 | 33 |
| Hive | 195.62s | 3,461,580 | 35 | 98,902.3 | 33 |
| Pig | 1031.76s | 3,461,580 | 35 | 98,902.3 | 33 |

### Query 1 Sample (Top 5 rows by date)

| log_date | status_code | request_count | total_bytes |
|---|---|---|---|
| 1995-07-01 | 200 | 31,888 | 702,394,599 |
| 1995-07-01 | 302 | 1,022 | 0 |
| 1995-07-01 | 304 | 2,413 | 0 |
| 1995-07-01 | 403 | 10 | 0 |
| 1995-07-01 | 404 | 314 | 0 |

### Query 2 Sample (Top 5 resources)

| resource_path | request_count | total_bytes | distinct_hosts |
|---|---|---|---|
| /images/NASA-logosmall.gif | 208,362 | 1,533,107,872 | 11,193 |
| /images/KSC-logosmall.gif | 164,278 | 1,025,222,256 | 9,891 |
| /images/MOSAIC-logosmall.gif | 127,907 | 781,740,768 | 7,935 |
| /images/USA-logosmall.gif | 127,112 | 782,477,952 | 7,880 |
| /images/WORLD-logosmall.gif | 124,113 | 760,371,456 | 7,692 |

### Query 3 Sample (Peak error hours)

| log_date | log_hour | error_count | total_count | error_rate | distinct_error_hosts |
|---|---|---|---|---|---|
| 1995-07-13 | 17 | 4,801 | 10,542 | 0.4554 | 985 |
| 1995-07-13 | 18 | 5,012 | 11,029 | 0.4545 | 1,022 |

---

## 13. Runtime Analysis and Comparison

### Why MongoDB is Fastest (36.65s)

- **No JVM** — native C++ process already running
- **In-memory execution** — after `insert_many`, the collection resides in RAM; all three aggregations scan from memory with no disk I/O
- **No framework overhead** — no YARN, no HDFS, no container scheduling
- **In-process aggregation** — data never leaves the MongoDB process during query execution; no serialization boundary

### Why MapReduce is Second (92.88s)

- **No JVM, no YARN** — pure Python in a single process
- **Local disk I/O only** — temp files written to `/tmp` (SSD)
- **Sequential but lightweight** — 35 batches × 3 queries = 105 map-reduce passes, no parallelism
- **Python overhead** — sort + groupby is slower than Java, but absence of framework overhead more than compensates on a single node

### Why Hive is Third (195.62s)

- **3 real YARN MR jobs** — one per query, each with JVM container allocation overhead
- **3 full data scans** — RegexSerDe re-parses all 3.46M records for every query (no caching between queries)
- **JVM startup per session** — each `beeline -e` call starts a new JVM, connects to HiveServer2, compiles the HiveQL
- On a multi-node cluster, Hive's JVM overhead amortizes over true parallelism and Hive would outperform Python MapReduce significantly

### Why Pig is Slowest (1031.76s)

- **~5 real YARN MR jobs** — Q1 + Q2 via MULTI_QUERY, Q3 separate, plus ORDER BY sort jobs
- **Single-node YARN overhead dominates** — each YARN job on a single-node: RM schedules → NM allocates container → JVM starts → task runs → JVM stops. This is 60–120s per job
- **5 jobs × ~3–4 min overhead = ~17 minutes** total
- **Pre-parsed TSV = zero parsing cost** — slowness is entirely YARN scheduling, not computation
- On a 10-node cluster with real parallelism, Pig would likely be the fastest of the four due to MULTI_QUERY optimisation and distributed map tasks

### Summary Table

| Pipeline | Execution Engine | Parallelism | MR Jobs | Key Overhead |
|---|---|---|---|---|
| MongoDB | C++ in-process | None needed | 0 | None |
| MapReduce | Python in-process | None | 0 | Sequential loops |
| Hive | JVM + YARN | Single node | 3 | JVM×3 + RegexSerDe×3 |
| Pig | JVM + YARN | Single node | ~5 | YARN container×5 |

---

## 14. Design Decisions

### Why PostgreSQL over MySQL
PostgreSQL was chosen for native `UUID` type (clean run ID management), `SERIAL` for auto-increment IDs, and better support for analytical queries in the reporting module. Both are permitted by the assignment.

### Why Click for the CLI
Click was chosen over `argparse` for cleaner option definitions, auto-generated `--help`, and `type=click.Choice()` which validates pipeline selection at the CLI boundary before any code executes.

### Why `importlib` Dynamic Loading
Pipelines are loaded dynamically via `importlib.import_module()`. This means importing the MongoDB pipeline does not trigger the `pymongo` import — only the selected pipeline's dependencies are loaded. This avoids import errors when running on a machine where only some backends are installed.

### Why Not Use `mrjob`
`mrjob` requires the `pipes` module which was removed in Python 3.14. A 50-line custom `MRJob` base class was written that has no external dependencies and handles both tuple and string keys correctly.

### Why Pre-Parsed TSV for Pig Instead of Streaming UDF
The streaming UDF approach was abandoned due to a `NullPointerException` in Pig 0.17.0's serialization layer when running on Java 17+. Pre-parsed TSV is cleaner, faster (no parsing overhead inside Pig), and more reliable. The parsing still happens within the pipeline's `run()` method — it is not a separate preprocessing step outside the tool.

### Why YARN Instead of Local Mode
The assignment requires that "core data processing genuinely happen using the selected execution technology." Running Pig and Hive with `mapreduce.framework.name=local` uses the `LocalJobRunner` — a single-threaded Java simulation, not actual MapReduce. YARN was enabled to demonstrate authentic distributed job submission even on a single-node cluster.

---

## 15. Challenges and Solutions

### Challenge 1: Hive `beeline -f` Silent No-Op Bug

**Problem:** In Hive 4.2.0, `beeline -f <sqlfile>` echoes the SQL to stdout but does not execute it. No error is raised. Tables created in the file do not exist after the call.

**Discovery:** `SHOW TABLES` returned 0 rows even in the same session. No `"Executing command:"` log lines appeared in the output.

**Solution:** Switch entirely to `beeline -e "<sql>"` which reliably executes. The `-e` mode shows `"Executing command:"` and `"No rows affected"` confirming execution.

---

### Challenge 2: Stale HDFS Data Inflating Record Counts

**Problem:** A 5,000-record test batch uploaded to HDFS during debugging remained on HDFS. The next Hive run found 3,466,580 records instead of 3,461,580 — exactly 5,000 extra.

**Solution:** Add `hdfs dfs -rm -r -f /user/nasa_etl/batches` at the start of every `run()` call, before uploading new batches.

---

### Challenge 3: Pig Streaming UDF `OperatorKey.mKey null` NPE

**Problem:** `STREAM raw THROUGH ParseLog USING SHIP(...)` in Pig 0.17.0 on Java 17+ throws a `NullPointerException` deep in Pig's internal `OperatorKey` serialization. The UDF process starts, then crashes when Pig tries to wire up the streaming operator.

**Solution:** Replace streaming UDF entirely. Python parser writes pre-parsed TSV files; Pig loads them with `PigStorage('\t')`. This also eliminated the SHIP path resolution issues.

---

### Challenge 4: Pig `COUNT(DISTINCT X)` Syntax Error

**Problem:** `COUNT(DISTINCT hosts)` inline inside `FOREACH` is not supported in Pig 0.17.0.

**Solution:**
```pig
q2_agg = FOREACH q2_grp {
    hosts          = data.host;
    distinct_hosts = DISTINCT hosts;      -- separate alias
    GENERATE COUNT(distinct_hosts) ...;   -- then count
};
```

---

### Challenge 5: Pig Q2 `ORDER BY + LIMIT` Crash

**Problem:** `q2_sorted = ORDER q2_agg BY request_count DESC; LIMIT q2_sorted 20;` causes a YARN `Job failed` error in Pig 0.17.0.

**Solution:** Remove `ORDER BY` and `LIMIT` from the Pig script entirely. After reading the HDFS output files in Python, apply `sort_key='request_count'` and `top_n=20` in `_read_pig_output()`.

---

### Challenge 6: YARN SSH Startup Failure

**Problem:** `start-yarn.sh` on macOS fails because SSH to localhost is not enabled by default. Error: `Connection refused`.

**Solution:** Start ResourceManager and NodeManager manually as background processes:
```bash
yarn resourcemanager > /tmp/yarn-rm.log 2>&1 &
yarn nodemanager    > /tmp/yarn-nm.log 2>&1 &
```

---

### Challenge 7: Pig Running in Local Mode Despite No `-x local` Flag

**Problem:** Pig was completing in ~47 seconds even after removing `-x local`. Investigation showed `mapreduce.framework.name=local` was still set in `mapred-site.xml`, causing Pig to use `LocalJobRunner` regardless of the flag.

**Solution:** Change `mapred-site.xml` to `mapreduce.framework.name=yarn`, configure `yarn-site.xml` with ResourceManager/NodeManager settings and memory limits, and start YARN. Pig now takes ~17 minutes — authentic YARN execution.

---

### Challenge 8: Python 3.14 `mrjob` Incompatibility

**Problem:** `mrjob` imports `pipes` at module load time, and `pipes` was removed in Python 3.14.

**Solution:** Replace `mrjob` with a custom 50-line `MRJob` base class using only `itertools.groupby` and standard Python. All three query classes (`MRDailyTraffic`, `MRTopResources`, `MRHourlyErrors`) implement `mapper()` and `reducer()` against this base class.

---

## 16. How to Run

**Project root:** `/Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/`

All commands below are run from the project root unless stated otherwise.

---

### Prerequisites

#### 1. Install Python dependencies
```bash
cd /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj
pip install -r requirements.txt
```

`requirements.txt` includes: `pymongo`, `psycopg2-binary`, `tabulate`, `click`, `python-dateutil`

#### 2. Set up PostgreSQL database
```bash
# Connect to PostgreSQL and run the schema script
psql -U postgres -f /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/db/schema.sql
```
This creates the `nasa_etl` database and all 4 tables (`etl_runs`, `q1_daily_traffic`, `q2_top_resources`, `q3_hourly_errors`).

Alternatively run the Python setup helper:
```bash
python /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/setup_db.py
```

#### 3. Ensure data files are present
```
/Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/data/NASA_access_log_Jul95
/Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/data/NASA_access_log_Aug95
```

---

### Start Required Services

#### HDFS (required for Hive and Pig)
```bash
# Start NameNode + DataNode
/opt/homebrew/Cellar/hadoop/3.5.0/libexec/sbin/start-dfs.sh

# Verify HDFS is up
/opt/homebrew/Cellar/hadoop/3.5.0/libexec/bin/hdfs dfs -ls /
```

#### YARN (required for Hive and Pig — real MapReduce execution)
```bash
# Start ResourceManager and NodeManager manually (SSH not required)
yarn resourcemanager > /tmp/yarn-rm.log 2>&1 &
yarn nodemanager    > /tmp/yarn-nm.log 2>&1 &

# Verify cluster has 1 node running
yarn node -list
```

#### HiveServer2 (required for Hive)
```bash
hive --service hiveserver2 > /tmp/hiveserver2.log 2>&1 &

# Wait ~10 seconds for HiveServer2 to be ready, then verify:
beeline -u jdbc:hive2:// -e "SHOW DATABASES;"
```

#### MongoDB (required for MongoDB pipeline)
```bash
# MongoDB usually auto-starts; if not:
brew services start mongodb-community

# Verify:
mongosh --eval "db.adminCommand('ping')"
```

---

### Run the ETL Pipelines

**General command format:**
```bash
python /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/main.py \
  --pipeline <mongodb|mapreduce|hive|pig> \
  --batch-size 100000 \
  --log-files /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/data/NASA_access_log_Jul95 \
              /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/data/NASA_access_log_Aug95
```

#### MongoDB Pipeline
```bash
python /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/main.py \
  --pipeline mongodb \
  --batch-size 100000 \
  --log-files /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/data/NASA_access_log_Jul95 \
              /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/data/NASA_access_log_Aug95
```
**Expected runtime:** ~37 seconds  
**Requires:** MongoDB running on `localhost:27017`

#### MapReduce Pipeline
```bash
python /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/main.py \
  --pipeline mapreduce \
  --batch-size 100000 \
  --log-files /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/data/NASA_access_log_Jul95 \
              /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/data/NASA_access_log_Aug95
```
**Expected runtime:** ~93 seconds  
**Requires:** Nothing beyond Python + PostgreSQL

#### Hive Pipeline
```bash
python /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/main.py \
  --pipeline hive \
  --batch-size 100000 \
  --log-files /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/data/NASA_access_log_Jul95 \
              /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/data/NASA_access_log_Aug95
```
**Expected runtime:** ~196 seconds  
**Requires:** HDFS running, YARN running, HiveServer2 running

#### Pig Pipeline
```bash
python /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/main.py \
  --pipeline pig \
  --batch-size 100000 \
  --log-files /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/data/NASA_access_log_Jul95 \
              /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/data/NASA_access_log_Aug95
```
**Expected runtime:** ~17 minutes  
**Requires:** HDFS running, YARN running  
**Note:** Java 17 must be available at `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home` for the Pig subprocess; Java 21 is used for HDFS CLI calls.

---

### View Results (Reporting)

**View latest run for a specific pipeline:**
```bash
python /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/reporting/report.py --pipeline mongodb
python /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/reporting/report.py --pipeline mapreduce
python /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/reporting/report.py --pipeline hive
python /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/reporting/report.py --pipeline pig
```

**View the most recent run regardless of pipeline:**
```bash
python /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/reporting/report.py --latest
```

**View a specific run by UUID:**
```bash
python /Users/sreekantamsaivenkat/Desktop/nosqlfinalprj/reporting/report.py --run-id <uuid>
```

---

### Key File Paths Reference

| File | Purpose |
|---|---|
| `nosqlfinalprj/main.py` | CLI entry point — select and run a pipeline |
| `nosqlfinalprj/config.py` | PostgreSQL / MongoDB / Hadoop connection settings |
| `nosqlfinalprj/parser/log_parser.py` | Shared NASA log parser + `iter_batches()` |
| `nosqlfinalprj/pipelines/mongodb_pipeline.py` | MongoDB ETL implementation |
| `nosqlfinalprj/pipelines/mapreduce_pipeline.py` | MapReduce ETL implementation |
| `nosqlfinalprj/pipelines/hive_pipeline.py` | Hive ETL implementation |
| `nosqlfinalprj/pipelines/pig_pipeline.py` | Pig ETL implementation |
| `nosqlfinalprj/mapreduce/mr_runner.py` | Custom MRJob base class |
| `nosqlfinalprj/mapreduce/mr_query1.py` | MR Q1: Daily Traffic |
| `nosqlfinalprj/mapreduce/mr_query2.py` | MR Q2: Top Resources |
| `nosqlfinalprj/mapreduce/mr_query3.py` | MR Q3: Hourly Errors |
| `nosqlfinalprj/pig_scripts/etl_queries.pig` | Pig Latin script (Q1 + Q2 + Q3) |
| `nosqlfinalprj/db/schema.sql` | PostgreSQL DDL — creates all 4 tables |
| `nosqlfinalprj/db/db_loader.py` | PostgreSQL write helpers |
| `nosqlfinalprj/reporting/report.py` | CLI report viewer |
| `nosqlfinalprj/data/NASA_access_log_Jul95` | Input log file — July 1995 |
| `nosqlfinalprj/data/NASA_access_log_Aug95` | Input log file — August 1995 |
| `/opt/homebrew/Cellar/hadoop/3.5.0/libexec/etc/hadoop/mapred-site.xml` | Hadoop MR config (`framework=yarn`) |
| `/opt/homebrew/Cellar/hadoop/3.5.0/libexec/etc/hadoop/yarn-site.xml` | YARN ResourceManager/NodeManager config |

---

## 17. Conclusion

The project successfully implements a four-pipeline ETL framework where all pipelines produce numerically identical results from the same input. The system demonstrates that fundamentally different processing paradigms — document stores, Python-based MapReduce, SQL-on-Hadoop, and dataflow — can be made interchangeable at the interface level while each retaining its native execution characteristics.

### Key Comparative Observations

| Criterion | Winner | Notes |
|---|---|---|
| Runtime (single node) | MongoDB | Native C++, in-memory, no overhead |
| Implementation simplicity | MongoDB | Aggregation pipeline is concise and expressive |
| SQL familiarity | Hive | HiveQL is closest to standard SQL |
| Distributed scalability | Pig / Hive | YARN + HDFS designed for multi-node scale-out |
| Framework transparency | MapReduce | Explicit map/shuffle/reduce makes data flow visible |
| Parsing flexibility | Hive | RegexSerDe parses raw logs without preprocessing |

### Suitability for Semi-Structured Log Analytics

- **MongoDB** is the best fit for interactive analytics on datasets that fit in RAM — fast, flexible schema, expressive aggregation.
- **MapReduce** is transparent and dependency-free but does not scale horizontally without infrastructure.
- **Hive** is best when the team knows SQL and the cluster has many nodes to amortize JVM overhead.
- **Pig** is best for multi-step dataflow pipelines where MULTI_QUERY optimisation and composable Latin scripts outweigh the setup complexity.

On a single node, MongoDB wins conclusively. On a 10-node cluster processing terabytes, Pig and Hive would reverse the ranking.
