"""Optional soak monitoring keeps the measured dashboard workload unchanged."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import runpy
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from test_dashboard_soak import Clock, ClockEvent


HERE = Path(__file__).resolve().parent


class HostCountMonitorTest(unittest.TestCase):
    def test_soak_summary_records_monitor_workload(self):
        for mode in ["on", "off"]:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                profile = runpy.run_path(str(HERE / "profile_metrics.py"))
                soak = profile["dashboard_soak"]
                clock = Clock()
                args = SimpleNamespace(seed=1, nodes=10000, jobs=4, query_budget=60,
                                       soak_seconds=15, refresh_interval=15, soak_host_count=mode)
                queries = [{"expr": 'up{instance="$node"}', "instant": True}] * 242
                query = Mock(return_value={"ok": True, "empty": False, "ms": 1})
                capture = Mock()
                with patch.dict(soak.__globals__, {"time": clock}), \
                     patch.dict(profile["harness"], {"dashboard": lambda _: queries}), \
                     patch.dict(profile["harness"]["load_dashboard"].__globals__,
                                {"query": query, "time": clock}):
                    summary = soak(args, Path(tmp), ["oteldb"], capture, ClockEvent(clock))
                self.assertEqual(summary["host_count_monitor"], profile["host_count_workload"](mode))
                self.assertEqual([load["queries"] for load in summary["loads"]], [242, 242])
                self.assertTrue(summary["loads"][1]["warmup"])
                self.assertEqual(query.call_count, 484)
                self.assertEqual(capture.call_count, 2)

    def test_probe_records_timing_results_and_exceptions(self):
        profile = runpy.run_path(str(HERE / "profile_metrics.py"))
        probe = profile["host_count_probe"]
        clock = Clock()
        for response in [{"ok": True, "value": "10000", "ms": 125},
                         {"ok": False, "error": "timeout", "ms": 125},
                         RuntimeError("canceled")]:
            with self.subTest(response=response), tempfile.TemporaryDirectory() as tmp:
                def query(*args):
                    clock.sleep(.125)
                    if isinstance(response, BaseException):
                        raise response
                    return response

                query = Mock(side_effect=query)
                with patch.dict(probe.__globals__, {"time": clock}), \
                     patch.dict(profile["harness"], {"query": query}):
                    if isinstance(response, BaseException):
                        with self.assertRaisesRegex(RuntimeError, "canceled"):
                            probe(Path(tmp), 60, "soak")
                    else:
                        self.assertEqual(probe(Path(tmp), 60, "soak"), response)
                saved = json.loads((Path(tmp) / "monitor-host-count.jsonl").read_text())
                self.assertEqual(saved["phase"], "soak")
                self.assertEqual(saved["elapsed"], 60)
                self.assertEqual(saved["ended_unix"] - saved["started_unix"], .125)
                self.assertEqual(saved["wall_ms"], 125)
                self.assertEqual(query.call_args.args,
                                 ("oteldb", profile["HOST_COUNT_QUERY"], saved["started_unix"]))
                if isinstance(response, BaseException):
                    self.assertEqual(saved["result"], {"ok": False, "error": "canceled",
                                                       "exception": "RuntimeError"})
                else:
                    self.assertEqual(saved["result"], response)

    def test_default_and_on_preserve_counts_off_only_removes_soak_counts(self):
        outcomes = {}
        for mode in [None, "on", "off"]:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                profile = runpy.run_path(str(HERE / "profile_metrics.py"))
                namespace = profile["main"].__globals__
                harness = profile["harness"]
                clock = Clock()
                clock.strftime = namespace["time"].strftime
                clock.gmtime = namespace["time"].gmtime
                directory = Path(tmp)
                binary = directory / "binary"
                binary.write_bytes(b"fake binary")
                source_config = directory / "config.yml"
                source_config.write_text("storage: {}\n")
                config = {"services": {"oteldb": {"ports": [], "volumes": [
                    {"target": "/etc/oteldb.yml", "source": "unused"}]},
                    "vmagent": {"command": ["-remoteWrite.url=http://oteldb:19291/api/v1/write"]}}}
                calls = []
                checks = []
                captures = []
                dashboard_rows = []
                queries = [{"expr": 'up{instance="$node"}', "instant": True}] * 242

                def check_output(command, **kwargs):
                    if "--format" in command:
                        return json.dumps(config).encode()
                    if command[:2] == ["docker", "inspect"]:
                        checks.append(command)
                        return json.dumps([{"State": {"Running": True, "OOMKilled": False},
                                            "HostConfig": {"NanoCpus": 8_000_000_000,
                                                           "Memory": 16 << 30,
                                                           "MemorySwap": 16 << 30}}]).encode()
                    return b"[]"

                def run(command, **kwargs):
                    calls.append(command)
                    return SimpleNamespace(stdout="owned-db\n" if "ps" in command else "", returncode=0)

                def soak(args, target, servers, capture, stop):
                    self.assertEqual(args.jobs, 4)
                    self.assertEqual(servers, ["oteldb"])
                    capture()
                    captures.append(None)
                    with ThreadPoolExecutor(args.jobs) as pool:
                        rows, load = harness["load_dashboard"](
                            "oteldb", "host-1", 1700000000, "random", 0, False,
                            queries, pool, args, float("inf"))
                    self.assertEqual(load["queries"], 242)
                    dashboard_rows.extend(rows)
                    capture()
                    captures.append(None)

                arguments = ["profile_metrics.py", "--image", "fake-image", "--binary", str(binary),
                             "--config", str(source_config), "--nodes", "8", "--seconds", "0",
                             "--queries", "--live-queries", "--soak-seconds", "15"]
                if mode is not None:
                    arguments.extend(["--soak-host-count", mode])
                query = Mock(return_value={"value": "8", "ok": True, "empty": False, "ms": 1})
                fetch = Mock(return_value=True)
                with patch.object(sys, "argv", arguments), \
                     patch.object(namespace["subprocess"], "check_output", side_effect=check_output), \
                     patch.object(namespace["subprocess"], "run", side_effect=run), \
                     patch.dict(namespace, {"HERE": directory, "time": clock, "fetch": fetch,
                                           "dashboard_soak": soak}), \
                     patch.dict(harness, {"SERVERS": ["oteldb"], "query": query,
                                           "dashboard": Mock(return_value=queries), "benchmark": Mock()}), \
                     patch.dict(harness["load_dashboard"].__globals__, {"query": query}):
                    profile["main"]()
                target = next((directory / "results").iterdir())
                options = json.loads((target / "options.json").read_text())
                self.assertEqual(options["soak_host_count"], mode or "on")
                metadata = json.loads((target / "monitoring-workload.json").read_text())
                self.assertEqual(metadata["soak_enabled"], mode != "off")
                self.assertTrue(metadata["resource_captures_enabled"])
                self.assertTrue(metadata["readiness_queries_unchanged"])
                records = [json.loads(row) for row in
                           (target / "monitor-host-count.jsonl").read_text().splitlines()]
                self.assertEqual([r["phase"] for r in records if r["phase"] != "soak"],
                                 ["ingestion", "queries"])
                self.assertEqual(sum(r["phase"] == "soak" for r in records), 0 if mode == "off" else 2)
                readiness = [call for call in query.call_args_list if call.kwargs.get("nocache")]
                self.assertEqual(len(readiness), 4, "initial and final readiness gates must run")
                self.assertEqual(len(dashboard_rows), 242)
                self.assertEqual(len(captures), 2)
                self.assertTrue(json.loads((target / "soak-freshness.json").read_text())["servers"]["oteldb"]["ready"])
                outcomes[mode] = (len(checks), fetch.call_count,
                                  sum(c[:2] == ["docker", "exec"] for c in calls),
                                  len((target / "resources.jsonl").read_text().splitlines()))
        self.assertEqual(outcomes[None], outcomes["on"])
        self.assertEqual(outcomes["on"], outcomes["off"])


if __name__ == "__main__":
    unittest.main()
