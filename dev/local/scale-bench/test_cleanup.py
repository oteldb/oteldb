"""Hermetic checks for owned benchmark cleanup after diagnostic failures."""
from pathlib import Path
import runpy
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch


finish_run = runpy.run_path(str(Path(__file__).with_name("profile_metrics.py")))["finish_run"]


class CleanupTest(unittest.TestCase):
    def check_cleanup(self, keep_data, failure=None):
        prefix = ["docker", "compose", "-p", "owned-profile", "-f", "/owned/compose.json"]
        calls = []
        log_calls = inspect_calls = 0

        def dc(*args, **kwargs):
            nonlocal log_calls
            calls.append(prefix + list(args))
            stage = args[0]
            if stage == "logs":
                log_calls += 1
                stage = "logs" if log_calls == 1 else "shutdown-logs"
            if stage == failure:
                raise RuntimeError("injected " + stage)
            if args[0] == "logs":
                kwargs["stdout"].write("full owned logs\n")
            return SimpleNamespace(stdout="owned-db owned-agent\n" if args[0] == "ps" else "")

        def inspect(args):
            nonlocal inspect_calls
            inspect_calls += 1
            self.assertEqual(args, ["docker", "inspect", "owned-db", "owned-agent"])
            stage = "inspect" if inspect_calls == 1 else "stopped-inspect"
            if stage == failure:
                raise RuntimeError("injected " + stage)
            return b"[]\n"

        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with patch.object(finish_run.__globals__["subprocess"], "check_output", side_effect=inspect):
                if failure:
                    with self.assertRaisesRegex(RuntimeError, "injected " + failure):
                        finish_run(dc, directory, keep_data)
                else:
                    finish_run(dc, directory, keep_data)
            self.assertIn(prefix + ["stop", "-t", "30"], calls)
            self.assertTrue(all(call[:len(prefix)] == prefix for call in calls))
            down = [call for call in calls if call[len(prefix)] == "down"]
            self.assertEqual(down, [] if keep_data else [prefix + ["down", "--volumes"]])
            if not failure:
                self.assertEqual((directory / "shutdown.log").read_text(), "full owned logs\n")

    def test_default_removes_only_owned_project(self):
        self.check_cleanup(False)

    def test_keep_data_stops_but_retains_project(self):
        self.check_cleanup(True)

    def test_default_cleanup_survives_failures(self):
        for failure in ["ps", "inspect", "logs", "stop", "stopped-inspect", "shutdown-logs"]:
            with self.subTest(failure=failure):
                self.check_cleanup(False, failure)

    def test_keep_data_retains_project_on_failure(self):
        for failure in ["inspect", "logs", "stop"]:
            with self.subTest(failure=failure):
                self.check_cleanup(True, failure)


if __name__ == "__main__":
    unittest.main()
