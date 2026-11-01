"""Fail-fast and cancellation tests using fake Docker responses and bounded thread gates."""
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
import json
from pathlib import Path
import runpy
import tempfile
import threading
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from test_dashboard_soak import Clock


HERE = Path(__file__).resolve().parent


class ProfileFailFastTest(unittest.TestCase):
    def setUp(self):
        self.profile = runpy.run_path(str(HERE / "profile_metrics.py"))
        self.soak = self.profile["dashboard_soak"]
        self.globals = self.soak.__globals__
        self.harness = self.globals["harness"]

    def test_container_death_saves_state_and_stops(self):
        for running, oom in [(False, False), (False, True), (True, True)]:
            with self.subTest(running=running, oom=oom), tempfile.TemporaryDirectory() as tmp:
                stop = threading.Event()
                state = {"Running": running, "OOMKilled": oom, "ExitCode": 137}
                with patch.object(self.globals["subprocess"], "check_output",
                                  return_value=json.dumps([{"State": state}]).encode()) as inspect:
                    with self.assertRaisesRegex(RuntimeError, "exited during soak"):
                        self.profile["check_container"]("owned", Path(tmp), stop, "soak")
                self.assertTrue(stop.is_set())
                saved = json.loads((Path(tmp) / "early-exit.json").read_text())
                self.assertEqual(saved["OOMKilled"], oom)
                self.assertEqual(saved["phase"], "soak")
                self.assertEqual(inspect.call_args.args[0], ["docker", "inspect", "owned"])

    def test_capture_failure_stops_before_executor_exit(self):
        for during in ["queries", "refresh delay"]:
            for failure in [RuntimeError("container died"), KeyboardInterrupt()]:
                with self.subTest(during=during, failure=type(failure).__name__):
                    self.check_capture_failure(during, failure)

    def check_capture_failure(self, during, failure):
        arrived = threading.Event()
        calls = []
        clock = Clock()
        case = self

        class WaitingEvent(threading.Event):
            def wait(self, timeout=None):
                if during == "refresh delay":
                    arrived.set()
                return super().wait(timeout)

        stop = WaitingEvent()

        def query(*args):
            calls.append(args)
            if during == "queries":
                arrived.set()
                case.assertTrue(stop.wait(2), "capture must stop in-flight requests before joining")
            return {"ok": True, "empty": False, "ms": 1}

        class Pending:
            def __init__(self, future):
                self.future = future

            def result(self, timeout=None):
                case.assertTrue(arrived.wait(2), "refresh did not reach the cancellation gate")
                raise FutureTimeout()

            def done(self):
                return self.future.done()

        class Runner:
            def __enter__(self):
                self.inner = ThreadPoolExecutor(1)
                self.inner.__enter__()
                return self

            def submit(self, work):
                return Pending(self.inner.submit(work))

            def __exit__(self, *args):
                case.assertTrue(stop.is_set(), "executor must not wait while the refresh is unstopped")
                return self.inner.__exit__(*args)

        def executor(workers):
            return Runner() if workers == 1 else ThreadPoolExecutor(workers)

        args = SimpleNamespace(seed=1, nodes=10000, jobs=4, query_budget=3600,
                               soak_seconds=3600, refresh_interval=900)
        capture = Mock(side_effect=[None, failure])
        queries = [{"expr": "up", "instant": True}] * 242
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(self.globals, {"time": clock, "ThreadPoolExecutor": executor}), \
                 patch.dict(self.harness, {"dashboard": lambda _: queries}), \
                 patch.dict(self.harness["load_dashboard"].__globals__, {"query": query, "time": clock}):
                with self.assertRaises(type(failure)):
                    self.soak(args, Path(tmp), ["oteldb"], capture, stop)
            summary = json.loads((Path(tmp) / "soak-summary.json").read_text())
            self.assertTrue(summary["stopped"])
        if during == "queries":
            self.assertLessEqual(len(calls), args.jobs, "no new HTTP wave may start after cancellation")
        else:
            self.assertEqual(len(calls), 2 * len(queries), "the refresh delay must be interrupted")

    def test_queued_queries_and_profile_fetches_skip_io_when_stopped(self):
        stop = threading.Event()
        stop.set()
        query = Mock()
        namespace = self.harness["load_dashboard"].__globals__
        with patch.dict(namespace, {"query": query}):
            with self.profile["cancellation_hooks"](stop):
                with self.assertRaises(self.profile["BenchmarkStopped"]):
                    self.harness["query"]("oteldb", "up", 0)
        query.assert_not_called()
        with patch.dict(self.harness, {"get": Mock()}) as harness:
            self.assertFalse(self.profile["fetch"]("unused", Path("unused"), stop=stop))
            harness["get"].assert_not_called()

    def test_owned_otelbench_is_terminated_on_cancellation(self):
        stop = threading.Event()
        process = Mock()
        process.poll.return_value = None

        def wait(timeout=None):
            if not stop.is_set():
                stop.set()
                raise self.globals["subprocess"].TimeoutExpired("owned-otelbench", timeout)
            return -15

        process.wait.side_effect = wait
        with patch.object(self.globals["subprocess"], "Popen", return_value=process):
            with self.assertRaises(self.profile["BenchmarkStopped"]):
                self.profile["StoppableSubprocess"](stop).run(["owned-otelbench"], timeout=3600)
        process.terminate.assert_called_once()
        process.kill.assert_not_called()

    def test_soak_death_attempts_owned_cleanup_and_keep_data_is_explicit(self):
        for keep_data in [False, True]:
            with self.subTest(keep_data=keep_data), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                binary = directory / "binary"
                binary.write_bytes(b"fake benchmark binary")
                config_file = directory / "config.yml"
                config_file.write_text("storage: {}\n")
                config = {"services": {"oteldb": {"ports": [], "volumes": [
                    {"target": "/etc/oteldb.yml", "source": "unused"}]},
                    "vmagent": {"command": ["-remoteWrite.url=http://oteldb:19291/api/v1/write"]}}}
                dead = False
                calls = []

                def check_output(command, **kwargs):
                    if "--format" in command:
                        return json.dumps(config).encode()
                    if command[:2] == ["docker", "inspect"]:
                        return json.dumps([{"State": {"Running": not dead, "OOMKilled": dead,
                                                       "ExitCode": 137 if dead else 0},
                                            "HostConfig": {"NanoCpus": 8_000_000_000,
                                                           "Memory": 16 << 30, "MemorySwap": 16 << 30}}]).encode()
                    return b"[]"

                def run(command, **kwargs):
                    calls.append(command)
                    if "logs" in command:
                        kwargs["stdout"].write("owned logs\n")
                    return SimpleNamespace(stdout="owned-db\n" if "ps" in command else "", returncode=0)

                def die_during_soak(args, target, servers, capture, stop):
                    nonlocal dead
                    dead = True
                    capture()

                arguments = ["profile_metrics.py", "--image", "fake-image", "--binary", str(binary),
                             "--config", str(config_file), "--nodes", "8", "--seconds", "0",
                             "--queries", "--live-queries", "--soak-seconds", "15"]
                if keep_data:
                    arguments.append("--keep-data")
                query = Mock(return_value={"value": "8", "ok": True, "empty": False})
                namespace = self.harness["load_dashboard"].__globals__
                with patch.object(sys, "argv", arguments), \
                     patch.object(self.globals["subprocess"], "check_output", side_effect=check_output), \
                     patch.object(self.globals["subprocess"], "run", side_effect=run), \
                     patch.dict(self.globals, {"HERE": directory, "fetch": Mock(return_value=True),
                                               "dashboard_soak": die_during_soak}), \
                     patch.dict(self.harness, {"SERVERS": ["oteldb"], "query": query,
                                               "dashboard": Mock(return_value=[]), "benchmark": Mock()}), \
                     patch.dict(namespace, {"query": query}):
                    with self.assertRaisesRegex(RuntimeError, "exited during soak"):
                        self.profile["main"]()
                down = [call for call in calls if "down" in call]
                self.assertEqual(len(down), 0 if keep_data else 1)
                if down:
                    self.assertEqual(down[0][-2:], ["down", "--volumes"])
                    self.assertEqual(down[0][:3], ["docker", "compose", "-p"])
                self.assertTrue(any("stop" in call for call in calls))
                results = list((directory / "results").iterdir())
                self.assertEqual(len(results), 1)
                state = json.loads((results[0] / "early-exit.json").read_text())
                self.assertTrue(state["OOMKilled"])
                self.assertEqual(state["phase"], "soak")


if __name__ == "__main__":
    unittest.main()
