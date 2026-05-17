# DAS 839 — Phase 1 Viva Preparation
## Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics

---

## 1. What is the Project?

The project is a **multi-pipeline ETL and reporting tool** for web server log analytics. The same analytical workload — parsing raw NASA HTTP logs, running three analytical queries, storing results — can be executed using any one of four different execution backends:

- **MongoDB** (document store + aggregation pipeline)
- **MapReduce** (custom Python MR runner)
- **Apache Hive** (HiveQL over HDFS, MapReduce engine)
- **Apache Pig** (Pig Latin scripts, local mode)

The goal is to study how different data processing paradigms solve the same semi-structured analytics problem and to compare them on runtime, batching behaviour, and implementation complexity.

**Dataset:** NASA HTTP Web Server Logs — Kennedy Space Center, Jul 1995 + Aug 1995  
**Size:** ~3.46 million records, ~356 MB uncompressed  
**Format:** ASCII log files, one HTTP request per line

---

## 2. What Phase 1 Requires You to Demonstrate

From the assignment, Phase 1 must cover:

| Requirement | Our Status |
|---|---|
| Overall design of the tool | ✅ Complete — `main.py` CLI orchestrates all 4 pipelines |
| Parsing strategy | ✅ Shared regex parser in `parser/log_parser.py` |
| ETL workflow | ✅ Extract → Transform → Load → Report flow for all pipelines |
| Batching approach | ✅ `iter_batches()` yields fixed-size chunks, batch IDs from 1 |
| Relational DB schema | ✅ PostgreSQL with 4 tables: `etl_runs`, `q1`, `q2`, `q3` |
| Plan for equivalence across pipelines | ✅ All pipelines use the same parser, same queries, same schema |
| Working prototype (1-2 pipelines) | ✅ **All 4 pipelines fully implemented and verified** |

---

## 3. System Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    main.py  (CLI)                        │
│   --pipeline {mongodb|mapreduce|hive|pig}                │
│   --batch-size N   --log-files file1 file2               │
└────────────────────┬────────────────────────────────────┘
                     │  loads pipeline dynamically via importlib
          ┌──────────▼──────────┐
          │   BasePipeline      │  (abstract interface)
          │   .run(files, bs)   │
          └──┬───┬───┬───┬─────┘
             │   │   │   │
    MongoDB  MR  Hive Pig  (each inherits, implements .run())
             │
    ┌────────▼──────────────────────────────────────────┐
    │  parser/log_parser.py  (SHARED across all 4)      │
    │  parse_line() → dict or None                      │
    │  iter_batches() → (batch_id, records, malformed)  │
    └────────────────────────────────────────────────────┘
             │
    ┌────────▼──────────────────────────────────────────┐
    │  db/db_loader.py   (SHARED across all 4)          │
    │  save_run_metadata(), save_q1(), save_q2(), save_q3()│
    └────────────────────────────────────────────────────┘
             │
    ┌────────▼──────────────────────────────────────────┐
    │  PostgreSQL: nasa_etl database                    │
    │  etl_runs | q1_daily_traffic | q2_top_resources   │
    │           | q3_hourly_errors                      │
    └────────────────────────────────────────────────────┘
             │
    ┌────────▼──────────────────────────────────────────┐
    │  reporting/report.py                              │
    │  --pipeline X --latest  (or --run-id UUID)        │
    └────────────────────────────────────────────────────┘
```

**Key design principle:** The parser and DB loader are shared modules. Only the ETL execution core differs per pipeline. This is what ensures equivalence.

---

## 4. Parsing Strategy

**File:** `parser/log_parser.py`

### Log Format (NASA Combined Log Format)
```
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
```

### Regex Used
```python
_LOG_RE = re.compile(
    r'^(\S+)'           # host
    r'\s+\S+\s+\S+'     # ident, authuser (ignored)
    r'\s+\[([^\]]+)\]'  # [timestamp]
    r'\s+"([^"]*)"'     # "request string"
    r'\s+(\S+)'         # status_code
    r'\s+(\S+)$'        # bytes
)
```

### Fields Extracted
| Field | Source | Notes |
|---|---|---|
| `host` | Group 1 | IP or hostname |
| `timestamp` | Group 2 | Raw string kept |
| `log_date` | Parsed from timestamp | `YYYY-MM-DD` format |
| `log_hour` | Parsed from timestamp | Integer 0–23 |
| `http_method` | Split from request | `GET`, `POST`, etc.; `UNKNOWN` if malformed |
| `resource_path` | Split from request | URL path |
| `protocol_version` | Split from request | `HTTP/1.0`, `HTTP/1.1`, etc. |
| `status_code` | Group 4 | Must be integer — invalid ones → malformed |
| `bytes_transferred` | Group 5 | `-` → `0`; non-numeric → `0` |

### Malformed Record Handling
- `parse_line()` returns `None` on any failure — **never raises**
- `iter_batches()` counts Nones as `malformed` and yields the count per batch
- Malformed records are **counted and reported** — not silently dropped (requirement)
- Result: **33 malformed** records out of 3,461,580 total (0.001%)

### Timestamp Parsing
```python
dt = datetime.strptime(raw.strip(), "%d/%b/%Y:%H:%M:%S %z")
log_date = dt.strftime("%Y-%m-%d")   # e.g. "1995-07-01"
log_hour = dt.hour                   # e.g. 0
```

---

## 5. ETL Workflow

For every pipeline, the same logical flow is followed:

```
EXTRACT                TRANSFORM               LOAD
──────────────────     ─────────────────────   ─────────────────────────
Read raw .log file  →  parse_line() per line → Execute Q1, Q2, Q3
in batches of N        regex match             Aggregate results
                       split fields            ↓
                       handle bytes='-'→0      save_run_metadata()  ← first (FK constraint)
                       count malformed         save_q1()
                                               save_q2()
                                               save_q3()
```

The **runtime clock** starts when the tool begins reading files and stops after the last PostgreSQL write — exactly as specified in the assignment.

---

## 6. Batching Approach

**File:** `parser/log_parser.py` → `iter_batches()`

- **Batch size** = number of input log records in one batch (not file size, not lines)
- Batch IDs start at **1** and increment sequentially
- If the final batch has fewer records than `batch_size`, it is still a valid batch
- Malformed records are **counted per batch** but not included in the batch list (they don't count toward batch_size)
- `avg_batch_size = total_records / num_batches`

### Example with batch_size=100,000:
```
Batch 1:  100,000 records  (batches 1–34 are full)
Batch 2:  100,000 records
...
Batch 34: 100,000 records
Batch 35:  61,580 records  ← final partial batch, still valid
──────────────────────────
Total:  3,461,580 records, 35 batches
Avg:    98,902.3 records/batch
```

---

## 7. The Three Mandatory Queries

### Query 1 — Daily Traffic Summary

**What it computes:** For every (date, HTTP status code) combination, count requests and sum bytes.

**SQL equivalent:**
```sql
SELECT log_date, status_code, COUNT(*) AS request_count, SUM(bytes_transferred) AS total_bytes
FROM logs
GROUP BY log_date, status_code
ORDER BY log_date, status_code;
```

**Result:** 295 rows (combinations of date × status code across Jul+Aug 1995)

**Sample output:**
```
log_date     status_code  request_count  total_bytes
1995-07-01   200          58,033         1,617,409,574
1995-07-01   302          2,568          218,465
1995-07-01   304          3,797          0
1995-07-01   404          316            0
```

---

### Query 2 — Top 20 Requested Resources

**What it computes:** Top 20 most-requested URLs, with total bytes and distinct host count.

**SQL equivalent:**
```sql
SELECT resource_path, COUNT(*) AS request_count,
       SUM(bytes_transferred) AS total_bytes,
       COUNT(DISTINCT host) AS distinct_host_count
FROM logs
GROUP BY resource_path
ORDER BY request_count DESC
LIMIT 20;
```

**Result:** 20 rows

**Sample output (top 5):**
```
resource_path                    request_count  total_bytes   distinct_hosts
/images/NASA-logosmall.gif       208,362        131,441,994   81,424
/images/KSC-logosmall.gif        164,813        169,168,020   85,100
/images/MOSAIC-logosmall.gif     127,656        40,500,273    54,836
/images/USA-logosmall.gif        126,820        25,932,114    54,429
/images/WORLD-logosmall.gif      125,676        73,624,119    53,985
```

**Key insight:** Image files dominate because browsers request page assets. The most popular resource had 208K requests from 81K distinct hosts.

---

### Query 3 — Hourly Error Analysis

**What it computes:** For each (date, hour), count 4xx/5xx errors, total requests, error rate, and distinct error-generating hosts.

**SQL equivalent:**
```sql
SELECT log_date, log_hour,
       SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS error_request_count,
       COUNT(*) AS total_request_count,
       SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) / COUNT(*) AS error_rate,
       COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host END) AS distinct_error_hosts
FROM logs
GROUP BY log_date, log_hour
ORDER BY log_date, log_hour;
```

**Result:** 1,369 rows (date × hour combinations with traffic)

**Highest error rate observed:** 1995-08-07 at 02:00 — 11.5% error rate (158 errors / 1,374 requests)

---

## 8. PostgreSQL Schema

**Database:** `nasa_etl`

### Table 1: `etl_runs` (one row per pipeline run)
```sql
run_id            UUID PRIMARY KEY
pipeline_name     VARCHAR(20)        -- 'mongodb', 'mapreduce', 'hive', 'pig'
run_timestamp     TIMESTAMP
total_records     INTEGER
malformed_records INTEGER
batch_size        INTEGER
num_batches       INTEGER
avg_batch_size    FLOAT
runtime_seconds   FLOAT
```

### Tables 2–4: Query Result Tables
All three share the same pattern — they reference `etl_runs` via FK:

```sql
q1_daily_traffic  → (run_id, pipeline_name, batch_id, execution_time, log_date, status_code, request_count, total_bytes)
q2_top_resources  → (run_id, pipeline_name, batch_id, execution_time, resource_path, request_count, total_bytes, distinct_host_count)
q3_hourly_errors  → (run_id, pipeline_name, batch_id, execution_time, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts)
```

### Why this schema?
- `run_id` (UUID) uniquely identifies each pipeline execution — multiple runs of the same pipeline can coexist
- FK from result tables to `etl_runs` enforces referential integrity
- `pipeline_name` + `batch_id` stored in every row satisfies the assignment requirement to include "pipeline name, run identifier, batch identifier, and time of execution"
- Indexes on `run_id` in all three result tables for fast report queries

### Important constraint
`save_run_metadata()` **must be called before** `save_q1/q2/q3()` because of the FK reference. This ordering is enforced in every pipeline.

---

## 9. How Equivalence Across Pipelines is Ensured

This is the most important design decision and the hardest to get right.

### Shared components (identical for all 4 pipelines)
| Component | File | What it does |
|---|---|---|
| Log parser | `parser/log_parser.py` | Identical regex, field extraction, malformed counting |
| Batch iterator | `parser/log_parser.py` → `iter_batches()` | Same batch sizing, same ID scheme |
| DB loader | `db/db_loader.py` | Same SQL inserts to same schema |
| Query logic | Same GROUP BY / aggregation semantics | Verified by comparing outputs |

### What varies per pipeline (only the execution core)
| Pipeline | What differs |
|---|---|
| MongoDB | Inserts parsed dicts into MongoDB, runs 3 aggregation pipelines |
| MapReduce | Runs 3 custom Python MR jobs per batch, accumulates across batches |
| Hive | Writes batches to HDFS, runs HiveQL via beeline, reads TSV output |
| Pig | Writes pre-parsed TSV batches, runs Pig Latin script via subprocess |

### How equivalence is verified
All 4 pipelines produce **identical numerical results**:
- Q1: 295 rows, total requests = 3,461,580, total bytes = 65,524,307,881
- Q2: 20 rows (same top resources in same order)
- Q3: 1,369 rows (same error rates per hour)

---

## 10. Pipeline-Specific Implementation Notes

### MongoDB
- Parsed records are batch-inserted as BSON documents with a `batch_id` field
- Three separate aggregation pipelines with `allowDiskUse=True` run over the full collection
- `$addToSet` + `$size` used for distinct host count in Q2
- `$cond` with `$REMOVE` used for conditional distinct host set in Q3
- All data stays in-memory (MongoDB cache) after first load

### MapReduce
- Custom `MRJob` base class in `mapreduce/mr_runner.py` (replaced broken `mrjob` which fails on Python 3.14 due to removed `pipes` module)
- Processes one batch at a time: 35 batches × 3 jobs = 105 MR executions
- Cross-batch result accumulation uses Python `defaultdict`
- For exact distinct host counts across batches: host sets are **unioned** across batches in the accumulator (not just counted per batch)
- Three MR classes: `MRDailyTraffic` (Q1), `MRTopResources` (Q2), `MRHourlyErrors` (Q3)

### Hive
- Hadoop HDFS running in pseudo-distributed mode (NameNode + DataNode on localhost)
- EXTERNAL TABLE with `RegexSerDe` to parse raw log format directly from HDFS
- A VIEW (`nasa_etl.nasa_logs`) adds computed columns (log_date, log_hour, method, path, etc.)
- Beeline CLI used with `TERM=dumb` flag to avoid jline terminal error
- **Critical fix discovered:** Hive 4.2.0 `beeline -f <file>` silently echoes SQL without executing — all DDL must be passed via `-e` flag
- HDFS batch directory wiped before each run to prevent stale data contamination
- All DDL (DROP TABLE, CREATE TABLE, CREATE VIEW) issued in ONE beeline session because Derby embedded metastore is single-connection

### Pig
- Runs in **real Hadoop MapReduce mode** (no `-x local`) on the same HDFS cluster as Hive
- Uploads pre-parsed TSV files to `hdfs://localhost:9000/user/nasa_etl/pig_batches/`
- Pig reads from HDFS, submits MR jobs, writes output back to HDFS
- Results copied back from HDFS to local temp dir for PostgreSQL loading
- Pig's MULTI_QUERY optimizer combines Q1 and Q2 into a single data scan pass
- `DISTINCT` must be done as a separate nested alias before `COUNT()` — Pig 0.17.0 doesn't support `COUNT(DISTINCT X)` inline
- `ORDER BY + LIMIT` for Q2 top-20 crashes in Pig 0.17.0 — handled: Pig outputs all Q2 rows, Python sorts and takes top 20 after reading

---

## 11. Runtime Results (batch_size = 100,000)

| Pipeline | Runtime | Records | Batches | Malformed |
|---|---|---|---|---|
| MongoDB | **36.65s** | 3,461,580 | 35 | 33 |
| MapReduce | **92.88s** | 3,461,580 | 35 | 33 |
| Hive | **195.62s** | 3,461,580 | 35 | 33 |
| Pig | **1031.76s (~17 min)** | 3,461,580 | 35 | 33 |

All produce identical Q1/Q2/Q3 results.

---

## 12. Why the Runtimes Differ (Key Explanation)

### MongoDB fastest (36.65s)
- Single process, no JVM, no IPC
- Native C++ aggregation engine after data is in memory
- Data loaded once, queried 3 times over same in-memory buffer
- No serialization boundary between load and query

### MapReduce second (92.88s)
- 35 batches × 3 queries = **105 sequential Python MR runs**
- Python's sort+groupby is slower than Java
- Each batch file is read 3 separate times (once per query)
- Cross-batch set unions for distinct hosts add Python overhead

### Hive third (195.62s) — real YARN MapReduce
- **3 separate beeline sessions** = 3× (JVM startup + Derby metastore init + Hive compile + YARN container allocation + MR job + teardown)
- HDFS writes even on localhost involve NameNode RPC calls
- RegexSerDe **re-parses all 3.46M records 3 times** (once per query)
- No MULTI_QUERY optimization — each HiveQL is its own MR job

### Pig slowest (1031.76s / ~17 min) — real YARN MapReduce
- Runs WITHOUT `-x local` — submits actual MapReduce jobs to YARN ResourceManager
- Batch TSV files uploaded to HDFS, Pig reads from `hdfs://localhost:9000`
- Runs ~5 MR jobs (Q1+Q2 combined via MULTI_QUERY, Q3 separate, plus sort jobs)
- **Each YARN job**: RM schedules → NM launches container → JVM starts → task runs → JVM stops
- On single-node laptop: container allocation adds ~60–120s overhead **per MR job**
- 5 jobs × ~3–4 min overhead = ~15–20 minutes total (matches friend's ~30 min on slower laptop)
- Pre-parsed TSV → zero parsing cost in Pig itself; slowness is pure YARN scheduling overhead

---

## 13. Likely Viva Questions and Answers

**Q: Why did you choose PostgreSQL over MySQL?**  
A: Both are accepted per the assignment. PostgreSQL was chosen for its native UUID type (cleaner run ID management), `SERIAL` for auto-increment IDs, and better support for analytical queries in the reporting module.

**Q: What does "batch size" mean in this project?**  
A: The number of input log records (parsed) processed in one batch. Not file size, not line count. Malformed lines are counted but not included toward batch size. The final partial batch (61,580 records) is still a valid batch.

**Q: How do you ensure all pipelines produce the same results?**  
A: The parser (`log_parser.py`) is shared — same regex, same field extraction, same `-`→`0` handling, same malformed definition. The query logic is defined once and translated faithfully to each engine's idiom. Results were verified numerically: all four produce 3,461,580 requests, 65,524,307,881 total bytes, Q1=295 rows, Q2=20 rows, Q3=1,369 rows.

**Q: What is a malformed record and how do you handle it?**  
A: A record that fails the regex match, has an invalid timestamp, or has a non-integer status code. `parse_line()` returns `None` — it never raises. The caller (`iter_batches`) increments a counter. 33 malformed records were found across 3.46M lines. The count is stored in `etl_runs.malformed_records` and shown in the report.

**Q: How does the batching work across two log files (Jul + Aug)?**  
A: `iter_batches()` treats both files as one continuous stream. It opens Jul95 first, fills batches, then when Jul95 is exhausted it continues into Aug95 without resetting the batch. A batch can span the file boundary. The batch counter never resets between files.

**Q: Why is Pig the slowest if it has MULTI_QUERY optimization?**  
A: Pig runs on real YARN — every MapReduce job requires the ResourceManager to schedule containers, NodeManager to launch JVMs, and all the YARN handshake overhead. On a single node that overhead is ~60–120s per job. Pig executes ~5 MR jobs for our 3 queries, so ~5 × overhead = ~17 minutes. MULTI_QUERY helps (Q1+Q2 in one pass instead of two), but can't eliminate the per-job container cost. On a real 10-node cluster, that container cost amortizes over massive parallelism and Pig would beat Python MapReduce easily.

**Q: Why does Hive run in ~3 minutes if it also uses YARN?**  
A: Hive runs only 3 MR jobs (one per query), vs Pig's ~5 (queries + sort jobs). Also, Hive's beeline sessions are sequential and lightweight — each session starts a JVM once and runs one job. Our MapReduce pipeline (92s, pure Python in-process) has zero JVM overhead, which is why it beats both Hive and Pig on a single laptop despite being conceptually "simpler."

**Q: Why does MongoDB beat Pig despite Pig being compiled Java?**  
A: Three reasons: (1) MongoDB's aggregation pipeline is native C++ code. (2) MongoDB has data already in-memory after batch inserts — no disk I/O during queries. (3) Pig's 1031s is dominated by YARN scheduling overhead, not computation — remove YARN and Pig's actual MR work is probably under 60s.

**Q: How does MongoDB handle distinct host count for Q2?**  
A: Uses `$addToSet` to accumulate all unique hosts per resource path into a set, then `$size` to count the set. This gives an exact distinct count, not an approximation.

**Q: What is the ETL flow for the Hive pipeline specifically?**  
1. Wipe and recreate HDFS `/user/nasa_etl/batches/` directory
2. Create external table (RegexSerDe) + view in one beeline `-e` session
3. For each batch: write batch to local temp file → `hdfs dfs -put` to HDFS
4. Run Q1, Q2, Q3 as three separate beeline `-e` sessions, parse TSV output
5. Save results to PostgreSQL

**Q: What was the hardest technical challenge?**  
A: Two stand out. For Hive: discovering that `beeline -f <file>` in Hive 4.2.0 silently echoes SQL without executing it — the fix was switching to `-e`. For Pig: the streaming UDF approach crashed with a Java serialization NPE (`OperatorKey.mKey null`) on newer JVMs — the fix was replacing streaming with pre-parsed TSV input, which also turned out to be faster.

**Q: How is the runtime measured?**  
A: `time.perf_counter()` is called immediately before the first file read and immediately after the last `db_loader.save_q3()` call. Report rendering time is excluded. Dataset download and installation time are excluded.

**Q: What is the interface for selecting a pipeline?**  
A: A Click CLI in `main.py`:
```
python main.py --pipeline mongodb --batch-size 100000 \
    --log-files data/NASA_access_log_Jul95 data/NASA_access_log_Aug95
```
The pipeline is loaded dynamically via `importlib`. A separate report can be viewed via:
```
python reporting/report.py --pipeline mongodb --latest
```

---

## 14. Project File Structure

```
nosqlfinalprj/
├── main.py                        # CLI entry point
├── config.py                      # Shared config (PG, Mongo, paths)
├── parser/
│   └── log_parser.py              # Shared NASA log parser + iter_batches
├── pipelines/
│   ├── base_pipeline.py           # Abstract BasePipeline
│   ├── mongodb_pipeline.py        # MongoDB implementation
│   ├── mapreduce_pipeline.py      # MapReduce implementation
│   ├── hive_pipeline.py           # Hive/HDFS implementation
│   └── pig_pipeline.py            # Pig implementation
├── mapreduce/
│   ├── mr_runner.py               # Custom MRJob base class
│   ├── mr_query1.py               # Q1: Daily Traffic
│   ├── mr_query2.py               # Q2: Top Resources
│   └── mr_query3.py               # Q3: Hourly Errors
├── pig_scripts/
│   ├── etl_queries.pig            # Pig Latin for Q1, Q2, Q3
│   └── udfs/parse_udf.py          # Python streaming UDF (backup)
├── hive_scripts/
│   └── create_table.hql           # DDL reference (inline in pipeline)
├── db/
│   ├── schema.sql                 # PostgreSQL DDL
│   └── db_loader.py               # save_run_metadata, save_q1/q2/q3
└── reporting/
    └── report.py                  # Click CLI report viewer
```

---

## 15. Quick Numbers to Remember

| Fact | Value |
|---|---|
| Total records | 3,461,580 |
| Malformed records | 33 |
| Total batches (batch_size=100K) | 35 |
| Last batch size | 61,580 |
| Avg batch size | 98,902.3 |
| Q1 result rows | 295 |
| Q2 result rows | 20 |
| Q3 result rows | 1,369 |
| Most requested resource | `/images/NASA-logosmall.gif` (208,362 requests) |
| MongoDB runtime | 36.65s |
| MapReduce runtime | 92.88s |
| Hive runtime (YARN MR) | 195.62s |
| Pig runtime (YARN MR on HDFS) | 1031.76s (~17 min) |
| Total bytes transferred | 65,524,307,881 (~61 GB) |
