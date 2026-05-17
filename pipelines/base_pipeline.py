from abc import ABC, abstractmethod


class BasePipeline(ABC):
    """
    Every pipeline must implement run() and return a RunResult.
    The contract: same log_files + batch_size → semantically identical
    Q1/Q2/Q3 results regardless of which subclass is used.
    """

    @abstractmethod
    def run(self, log_files: list, batch_size: int, run_id: str) -> dict:
        """
        Execute the full ETL:
          1. Read raw log files in batches of batch_size records
          2. Parse with the shared parser
          3. Execute Q1, Q2, Q3 using the pipeline's native engine
          4. Write results + metadata to PostgreSQL via db_loader

        Returns a dict:
          {
            'run_id':          str,
            'pipeline':        str,
            'total_records':   int,
            'malformed':       int,
            'num_batches':     int,
            'avg_batch_size':  float,
            'runtime_seconds': float,
          }
        """
