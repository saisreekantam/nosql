"""
Lightweight MapReduce runner — no external dependencies.
Works on Python 3.14+ (replaces mrjob which requires the removed `pipes` module).
Sort key and groupby key both use str(key) for consistency across tuple/string keys.
"""
from abc import ABC, abstractmethod
from itertools import groupby


class MRJob(ABC):

    @abstractmethod
    def mapper(self, key, value):
        """Yield (out_key, out_value) pairs."""

    @abstractmethod
    def reducer(self, key, values):
        """Yield (out_key, out_value) pairs."""

    def run_on_lines(self, lines) -> list:
        # Map phase
        mapped = []
        for line in lines:
            line = line.rstrip('\n').rstrip('\r')
            if not line:
                continue
            for k, v in self.mapper(None, line):
                mapped.append((k, v))

        # Sort + group by str(key) so tuples and strings are handled uniformly
        mapped.sort(key=lambda x: str(x[0]))

        results = []
        for _, group in groupby(mapped, key=lambda x: str(x[0])):
            items = list(group)
            key = items[0][0]
            values = (v for _, v in items)
            for _, out_val in self.reducer(key, values):
                if out_val is not None:
                    results.append(out_val)
        return results

    def run_on_file(self, path: str) -> list:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            return self.run_on_lines(f)
