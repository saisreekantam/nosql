# DAS 839 — Multi-Pipeline ETL and Reporting Framework

**Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics**

**Contributors:** Sai Venkat Sreekantam (IMT2023501) · Revanth (IMT2023118) · Aditya (IMT2023114)

---

## Overview

A single CLI tool that runs the same ETL workload over NASA HTTP Web Server Logs using four swappable backends:

| Pipeline | Technology | Runtime (batch=100K) |
|---|---|---|
| `mongodb` | MongoDB aggregation | ~37s |
| `mapreduce` | Custom Python MapReduce | ~93s |
| `hive` | Apache Hive 4.2 + YARN | ~196s |
| `pig` | Apache Pig 0.17 + YARN | ~17 min |

All four pipelines produce identical results for the three analytical queries and store aggregated output in PostgreSQL.

See **[PROJECT_REPORT.md](PROJECT_REPORT.md)** for the full project report.

---

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/saisreekantam/nosql.git
cd nosql
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp config.example.py config.py
# Edit config.py with your PostgreSQL password and paths
```

### 3. Download the NASA log dataset

The raw data files are not included in the repo (391 MB). Download from:

```
https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
```

Decompress into `data/`:

```bash
mkdir -p data
gunzip -c NASA_access_log_Jul95.gz > data/NASA_access_log_Jul95
gunzip -c NASA_access_log_Aug95.gz > data/NASA_access_log_Aug95
```

### 4. Set up PostgreSQL

```bash
psql -U postgres -f db/schema.sql
# or
python setup_db.py
```

### 5. Start required services (for Hive and Pig only)

```bash
# HDFS
start-dfs.sh

# YARN
yarn resourcemanager > /tmp/yarn-rm.log 2>&1 &
yarn nodemanager    > /tmp/yarn-nm.log 2>&1 &

# HiveServer2
hive --service hiveserver2 > /tmp/hiveserver2.log 2>&1 &
```

---

## Running a Pipeline

```bash
python main.py --pipeline mongodb    --batch-size 100000
python main.py --pipeline mapreduce  --batch-size 100000
python main.py --pipeline hive       --batch-size 100000
python main.py --pipeline pig        --batch-size 100000
```

## Viewing Results

```bash
python reporting/report.py --pipeline mongodb
python reporting/report.py --pipeline mapreduce
python reporting/report.py --pipeline hive
python reporting/report.py --pipeline pig
python reporting/report.py --latest
```

---

## Project Structure

```
nosql/
├── main.py                        # CLI entry point
├── config.example.py              # Config template (copy to config.py)
├── requirements.txt
├── setup_db.py                    # PostgreSQL database initialiser
├── parser/
│   └── log_parser.py              # Shared NASA log parser + iter_batches
├── pipelines/
│   ├── base_pipeline.py
│   ├── mongodb_pipeline.py
│   ├── mapreduce_pipeline.py
│   ├── hive_pipeline.py
│   └── pig_pipeline.py
├── mapreduce/
│   ├── mr_runner.py               # Custom MRJob base class
│   ├── mr_query1.py
│   ├── mr_query2.py
│   └── mr_query3.py
├── pig_scripts/
│   └── etl_queries.pig
├── hive_scripts/
│   └── create_table.hql
├── db/
│   ├── schema.sql
│   └── db_loader.py
├── reporting/
│   └── report.py
├── PROJECT_REPORT.md              # Full project report
└── VIVA_PHASE1.md                 # Phase 1 viva preparation notes
```
