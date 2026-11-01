import json
from pathlib import Path
import runpy
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


HERE = Path(__file__).resolve().parent


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    perf_counter = monotonic

    def time(self):
        return 1700000000 + self.now

    def sleep(self, seconds):
        self.now += seconds


class DashboardSoakTest(unittest.TestCase):
    def setUp(self):
        self.soak = runpy.run_path(str(HERE / "profile_metrics.py"))["dashboard_soak"]
        self.harness = self.soak.__globals__["harness"]
        self.clock = Clock()
        self.args = SimpleNamespace(seed=1, nodes=10000, jobs=4, query_budget=60,
                                    soak_seconds=45, refresh_interval=15)
        self.queries = [{"expr": 'up{instance="$node"}', "instant": True}] * 6

    def test_refreshes_preserve_cache_modes_and_advance_random_time(self):
        query = lambda server, expr, end, instant, nocache: {
            "server": server, "query": expr, "end": end, "nocache": nocache,
            "ms": 1, "ok": True, "empty": False}
        capture = Mock()
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(self.soak.__globals__, {"time": self.clock}), \
                 patch.dict(self.harness, {"dashboard": lambda _: self.queries}), \
                 patch.dict(self.harness["load_dashboard"].__globals__, {"query": query, "time": self.clock}):
                summary = self.soak(self.args, Path(tmp), ["oteldb", "victoriametrics"], capture)
            rows = [json.loads(line) for line in (Path(tmp) / "soak-dashboards.jsonl").read_text().splitlines()]
            self.assertEqual(json.loads((Path(tmp) / "soak-summary.json").read_text()), summary)
        loads = summary["loads"]
        self.assertEqual(len(loads), 12)
        self.assertEqual(capture.call_count, 2)
        self.assertEqual(summary["elapsed_s"], 45)
        self.assertTrue(all(row["complete"] and row["queries"] == 6 for row in loads))
        random = [row for row in loads if row["server"] == "oteldb" and row["mode"] == "random"]
        cached = [row for row in loads if row["server"] == "oteldb" and row["mode"] == "cached"]
        self.assertEqual(len({row["host"] for row in random}), 3)
        self.assertEqual(len({row["end"] for row in random}), 3)
        self.assertEqual({row["host"] for row in cached}, {"host-0"})
        self.assertEqual(len({row["end"] for row in cached}), 1)
        self.assertEqual([row["warmup"] for row in cached], [True, True, False])
        self.assertTrue(all(row["nocache"] == (row["mode"] == "random" and row["server"] == "victoriametrics")
                            for row in rows if row["kind"] == "query"))

    def test_worker_timeout_error_propagates(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(self.soak.__globals__, {"time": self.clock}), \
                 patch.dict(self.harness, {"dashboard": lambda _: self.queries,
                                          "load_dashboard": Mock(side_effect=TimeoutError("worker failed"))}):
                with self.assertRaisesRegex(TimeoutError, "worker failed"):
                    self.soak(self.args, Path(tmp), ["oteldb"], Mock())

    def test_completion_between_timeout_and_done_check(self):
        pending = Mock()
        pending.result.side_effect = [TimeoutError(), []]
        pending.done.return_value = True
        runner = Mock()
        runner.__enter__ = Mock(return_value=runner)
        runner.__exit__ = Mock(return_value=False)
        runner.submit.return_value = pending
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(self.soak.__globals__, {"ThreadPoolExecutor": Mock(return_value=runner)}):
                summary = self.soak(self.args, Path(tmp), ["oteldb"], Mock())
        self.assertEqual(summary["loads"], [])
        self.assertEqual(pending.result.call_count, 2)


if __name__ == "__main__":
    unittest.main()
