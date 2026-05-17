#!/usr/bin/env python3
"""
Multi-Pipeline ETL Framework for NASA Log Analytics
Usage:
    python main.py --pipeline mongodb    --batch-size 50000
    python main.py --pipeline mapreduce  --batch-size 50000
    python main.py --pipeline hive       --batch-size 50000
    python main.py --pipeline pig        --batch-size 50000
"""
import sys
import uuid
import click

import config


PIPELINES = {
    'mongodb':    'pipelines.mongodb_pipeline.MongoDBPipeline',
    'mapreduce':  'pipelines.mapreduce_pipeline.MapReducePipeline',
    'hive':       'pipelines.hive_pipeline.HivePipeline',
    'pig':        'pipelines.pig_pipeline.PigPipeline',
}


def load_pipeline(name: str):
    module_path, class_name = PIPELINES[name].rsplit('.', 1)
    import importlib
    mod = importlib.import_module(module_path)
    return getattr(mod, class_name)()


@click.command()
@click.option(
    '--pipeline', '-p',
    required=True,
    type=click.Choice(list(PIPELINES.keys())),
    help="Execution backend to use",
)
@click.option(
    '--batch-size', '-b',
    default=config.DEFAULT_BATCH_SIZE,
    show_default=True,
    help="Number of log records per batch",
)
@click.option(
    '--log-files', '-f',
    multiple=True,
    default=config.LOG_FILES,
    help="Raw log file paths (can specify multiple times)",
)
@click.option(
    '--run-id',
    default=None,
    help="Optional run UUID (auto-generated if omitted)",
)
def main(pipeline, batch_size, log_files, run_id):
    """Execute the NASA log ETL pipeline and store results in PostgreSQL."""
    run_id = run_id or str(uuid.uuid4())
    log_files = list(log_files)

    click.echo(f"\nStarting ETL run")
    click.echo(f"  Pipeline   : {pipeline}")
    click.echo(f"  Batch size : {batch_size:,}")
    click.echo(f"  Log files  : {log_files}")
    click.echo(f"  Run ID     : {run_id}\n")

    pipe = load_pipeline(pipeline)
    result = pipe.run(log_files, batch_size, run_id)

    click.echo(f"\nRun complete.")
    click.echo(f"  Runtime        : {result['runtime_seconds']:.2f}s")
    click.echo(f"  Total records  : {result['total_records']:,}")
    click.echo(f"  Malformed      : {result['malformed']:,}")
    click.echo(f"  Batches        : {result['num_batches']}")
    click.echo(f"  Avg batch size : {result['avg_batch_size']:.1f}")
    click.echo(f"\nTo view report:")
    click.echo(f"  python reporting/report.py --run-id {run_id}")


if __name__ == '__main__':
    main()
