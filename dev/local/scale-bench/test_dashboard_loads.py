from concurrent.futures import ThreadPoolExecutor
from itertools import chain, repeat
import json
from pathlib import Path
import runpy
import tempfile
from threading import Event
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
        self.args = SimpleNamespace(seed=1, runs=2, jobs=4, query_budget=60, dashboard_dispatch="waves")

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

    def test_rolling_dispatch_refills_slot_and_preserves_query_order(self):
        load = self.harness["load_dashboard"]
        third_started = Event()
        queries = [{"expr": str(i), "instant": True} for i in range(3)]

        def query(server, expression, end, instant, nocache):
            if expression == "0":
                self.assertTrue(third_started.wait(timeout=5))
            elif expression == "2":
                third_started.set()
            return {"ok": True, "empty": False, "ms": 1}

        args = SimpleNamespace(jobs=2, dashboard_dispatch="rolling")
        with patch.dict(self.globals, {"query": query}), ThreadPoolExecutor(max_workers=2) as pool:
            rows, dashboard = load("oteldb", "host-0", 100, "random", 0, False,
                                   queries, pool, args, float("inf"))
        self.assertTrue(dashboard["complete"])
        self.assertEqual(dashboard["dispatch"], "rolling")
        self.assertEqual([row["query_index"] for row in rows], [0, 1, 2])

    def test_rolling_dispatch_drains_outstanding_requests_after_deadline(self):
        rolling = self.harness["rolling_dashboard_rows"]
        ticks = chain((0, 0), repeat(2))
        clock = SimpleNamespace(monotonic=lambda: next(ticks))

        def query(index):
            return {"query_index": index}

        with patch.dict(self.globals, {"time": clock}), ThreadPoolExecutor(max_workers=2) as pool:
            rows = rolling(query, 6, pool, 2, 1)
        self.assertEqual([row["query_index"] for row in rows], [0, 1])


if __name__ == "__main__":
    unittest.main()
