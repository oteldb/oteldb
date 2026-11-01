import tempfile
import unittest
from pathlib import Path

from analyze import metrics


class MetricsTest(unittest.TestCase):
    def test_aggregate_only_traces(self):
        sample = """# TYPE storage_flush_bytes_total counter
storage_flush_bytes_total{signal="trace",tenant="a"} 1.25e6
storage_flush_bytes_total{tenant="b",signal="trace"} 750000
storage_flush_bytes_total{signal="metric"} 99000000
go_memstats_heap_alloc_bytes 123456
unparseable
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metrics.txt"
            path.write_text(sample)
            self.assertEqual(
                metrics(path),
                {
                    "storage_flush_bytes_total": 2000000,
                    "go_memstats_heap_alloc_bytes": 123456,
                },
            )


if __name__ == "__main__":
    unittest.main()
