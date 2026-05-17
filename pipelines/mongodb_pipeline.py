import time
from datetime import datetime, timezone

from pymongo import MongoClient, DESCENDING
from pymongo.errors import BulkWriteError

import config
from db import db_loader
from parser.log_parser import iter_batches
from pipelines.base_pipeline import BasePipeline


class MongoDBPipeline(BasePipeline):
    PIPELINE_NAME = "mongodb"

    def run(self, log_files: list, batch_size: int, run_id: str) -> dict:
        client = MongoClient(config.MONGO_URI)
        db = client[config.MONGO_DB]
        col = db[config.MONGO_COLLECTION]

        # Drop previous run data for this collection to keep things clean per run
        col.drop()

        total_records = 0
        total_malformed = 0
        num_batches = 0

        start_time = time.perf_counter()

        # ── EXTRACT + LOAD phase ────────────────────────────────────────────
        for batch_id, batch, malformed in iter_batches(log_files, batch_size):
            if not batch:
                continue
            docs = []
            for r in batch:
                docs.append({
                    'batch_id':          batch_id,
                    'host':              r['host'],
                    'timestamp':         r['timestamp'],
                    'log_date':          r['log_date'],
                    'log_hour':          r['log_hour'],
                    'http_method':       r['http_method'],
                    'resource_path':     r['resource_path'],
                    'protocol_version':  r['protocol_version'],
                    'status_code':       r['status_code'],
                    'bytes_transferred': r['bytes_transferred'],
                })
            col.insert_many(docs, ordered=False)
            total_records   += len(batch)
            total_malformed += malformed
            num_batches     += 1
            print(f"  [mongodb] batch {batch_id}: {len(batch):,} records loaded "
                  f"({malformed} malformed)")

        avg_batch_size = total_records / num_batches if num_batches else 0

        # ── TRANSFORM + QUERY phase ─────────────────────────────────────────
        print(f"  [mongodb] running aggregations on {total_records:,} documents …")

        q1_rows = self._query1(col)
        q2_rows = self._query2(col)
        q3_rows = self._query3(col)

        runtime = time.perf_counter() - start_time
        avg_batch_size = total_records / num_batches if num_batches else 0

        # ── LOAD results into PostgreSQL ─────────────────────────────────────
        # etl_runs must be inserted first (FK constraint), then query results
        db_loader.save_run_metadata(
            run_id          = run_id,
            pipeline        = self.PIPELINE_NAME,
            total_records   = total_records,
            malformed       = total_malformed,
            batch_size      = batch_size,
            num_batches     = num_batches,
            avg_batch_size  = avg_batch_size,
            runtime_seconds = runtime,
        )
        final_batch_id = num_batches
        db_loader.save_q1(run_id, self.PIPELINE_NAME, final_batch_id, q1_rows)
        db_loader.save_q2(run_id, self.PIPELINE_NAME, final_batch_id, q2_rows)
        db_loader.save_q3(run_id, self.PIPELINE_NAME, final_batch_id, q3_rows)

        client.close()
        print(f"  [mongodb] done in {runtime:.2f}s  "
              f"({total_records:,} records, {num_batches} batches, "
              f"{total_malformed} malformed)")

        return {
            'run_id':          run_id,
            'pipeline':        self.PIPELINE_NAME,
            'total_records':   total_records,
            'malformed':       total_malformed,
            'num_batches':     num_batches,
            'avg_batch_size':  avg_batch_size,
            'runtime_seconds': runtime,
        }

    # ── Q1: Daily Traffic Summary ──────────────────────────────────────────
    def _query1(self, col) -> list:
        pipeline = [
            {"$group": {
                "_id": {
                    "log_date":   "$log_date",
                    "status_code": "$status_code",
                },
                "request_count": {"$sum": 1},
                "total_bytes":   {"$sum": "$bytes_transferred"},
            }},
            {"$project": {
                "_id":           0,
                "log_date":      "$_id.log_date",
                "status_code":   "$_id.status_code",
                "request_count": 1,
                "total_bytes":   1,
            }},
            {"$sort": {"log_date": 1, "status_code": 1}},
        ]
        return list(col.aggregate(pipeline, allowDiskUse=True))

    # ── Q2: Top 20 Requested Resources ────────────────────────────────────
    def _query2(self, col) -> list:
        pipeline = [
            {"$group": {
                "_id": "$resource_path",
                "request_count": {"$sum": 1},
                "total_bytes":   {"$sum": "$bytes_transferred"},
                "hosts":         {"$addToSet": "$host"},
            }},
            {"$project": {
                "_id":                0,
                "resource_path":      "$_id",
                "request_count":      1,
                "total_bytes":        1,
                "distinct_host_count": {"$size": "$hosts"},
            }},
            {"$sort": {"request_count": DESCENDING}},
            {"$limit": 20},
        ]
        return list(col.aggregate(pipeline, allowDiskUse=True))

    # ── Q3: Hourly Error Analysis ──────────────────────────────────────────
    def _query3(self, col) -> list:
        pipeline = [
            {"$group": {
                "_id": {
                    "log_date": "$log_date",
                    "log_hour": "$log_hour",
                },
                "total_request_count": {"$sum": 1},
                "error_request_count": {
                    "$sum": {
                        "$cond": [
                            {"$and": [
                                {"$gte": ["$status_code", 400]},
                                {"$lte": ["$status_code", 599]},
                            ]},
                            1, 0
                        ]
                    }
                },
                # Collect error hosts using a conditional push
                "error_hosts": {
                    "$addToSet": {
                        "$cond": [
                            {"$and": [
                                {"$gte": ["$status_code", 400]},
                                {"$lte": ["$status_code", 599]},
                            ]},
                            "$host",
                            "$$REMOVE"
                        ]
                    }
                },
            }},
            {"$project": {
                "_id":                  0,
                "log_date":             "$_id.log_date",
                "log_hour":             "$_id.log_hour",
                "error_request_count":  1,
                "total_request_count":  1,
                "error_rate": {
                    "$cond": [
                        {"$gt": ["$total_request_count", 0]},
                        {"$divide": ["$error_request_count", "$total_request_count"]},
                        0.0
                    ]
                },
                "distinct_error_hosts": {"$size": "$error_hosts"},
            }},
            {"$sort": {"log_date": 1, "log_hour": 1}},
        ]
        return list(col.aggregate(pipeline, allowDiskUse=True))
