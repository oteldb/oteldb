"""Hermetic checks for retained-fixture draining."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("drain", Path(__file__).with_name("drain.py"))
drain = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(drain)


class DrainTest(unittest.TestCase):
    def test_completion_and_unavailable_server(self):
        for unavailable in [False, True]:
            with self.subTest(unavailable=unavailable), tempfile.TemporaryDirectory() as directory:
                fixture = Path(directory) / "run" / "1000"
                fixture.mkdir(parents=True)
                (fixture / "summary.json").write_text(json.dumps({"nodes": 1000, "query_end": 123}))
                original = "scrape_configs: [original]\n"
                (fixture / "scrape.yml").write_text(original)
                harness = dict(drain.harness)
                if unavailable:
                    def get(_url):
                        raise OSError("connection refused")
                else:
                    def get(_url):
                        return b'vmagent_remotewrite_pending_data_bytes{url="1"} 0\n'
                harness.update(get=get, query=lambda *_args, **_kwargs: {"value": "6"})
                with patch.object(drain, "harness", harness), \
                        patch.object(drain.subprocess, "run") as run, \
                        patch("sys.argv", ["drain.py", str(fixture)]), \
                        contextlib.redirect_stdout(io.StringIO()):
                    if unavailable:
                        with self.assertRaises(SystemExit):
                            drain.main()
                    else:
                        drain.main()
                result = json.loads((fixture / "drain/summary.json").read_text())
                self.assertEqual(result["complete"], not unavailable)
                self.assertEqual(result["error"], "connection refused" if unavailable else None)
                self.assertEqual(result["query_end"], 123)
                self.assertEqual((fixture / "drain/original-scrape.yml").read_text(), original)
                self.assertIn("scrape_configs: []", (fixture / "scrape.yml").read_text())
                self.assertEqual(run.call_args_list[0].args[0][-2:], ["start", "vmagent"])
                self.assertEqual(run.call_args_list[-1].args[0][-4:], ["stop", "-t", "20", "vmagent"])


if __name__ == "__main__":
    unittest.main()
