import json
from pathlib import Path
import runpy
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


HERE = Path(__file__).resolve().parent


class DashboardLoadsTest(unittest.TestCase):
    def setUp(self):
        self.harness = runpy.run_path(str(HERE / "run.py"))
        self.load = self.harness["dashboard_loads"]
        self.globals = self.load.__globals__
        self.queries = [{"expr": 'up{instance="$node"}', "instant": True} for _ in range(6)]
        self.args = SimpleNamespace(seed=1, runs=2, jobs=4, query_budget=60)

    def test_modes_and_complete_dashboard(self):
        def query(server, expression, end, instant, nocache):
            return {"server": server, "query": expression, "ms": 2.0,
                    "ok": True, "empty": False, "nocache": nocache}

        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(self.globals, {"dashboard": lambda _: self.queries, "query": query}):
                loads = self.load("victoriametrics", 10000, 100, self.args, Path(tmp))
            rows = [json.loads(line) for line in (Path(tmp) / "dashboard-loads-victoriametrics.jsonl").read_text().splitlines()]
        self.assertEqual(len(loads), 4)
        self.assertTrue(all(row["complete"] and row["queries"] == 6 for row in loads))
        self.assertTrue(all(row["errors"] == 0 and row["max_query_ms"] == 2 for row in loads))
        queries = [row for row in rows if row["kind"] == "query"]
        self.assertEqual(len(queries), 36)
        self.assertEqual(sum(row["warmup"] for row in queries), 12)
        self.assertTrue(all(row["nocache"] == (row["mode"] == "random") for row in queries))
        self.assertTrue(all(row["host"] == "host-0" for row in queries if row["mode"] == "cached"))

    def test_budget_marks_partial_dashboard_incomplete(self):
        ticks = iter([0, 0, 0, 100, 100, 100])
        clock = SimpleNamespace(monotonic=lambda: next(ticks), perf_counter=lambda: 0)
        query = lambda *args: {"ok": True, "empty": False, "ms": 1}
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(self.globals, {"dashboard": lambda _: self.queries, "query": query, "time": clock}):
                loads = self.load("oteldb", 10000, 100, self.args, Path(tmp))
        self.assertEqual(len(loads), 1)
        self.assertEqual(loads[0]["queries"], 4)
        self.assertFalse(loads[0]["complete"])


if __name__ == "__main__":
    unittest.main()
