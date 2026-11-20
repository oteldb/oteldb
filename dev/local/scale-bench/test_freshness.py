from pathlib import Path
import runpy
import unittest
from unittest.mock import Mock, patch


HERE = Path(__file__).resolve().parent


class FreshnessTest(unittest.TestCase):
    def test_freshness_requires_all_hosts_and_history(self):
        freshness = runpy.run_path(str(HERE / "profile_metrics.py"))["freshness"]
        harness = freshness.__globals__["harness"]
        for hosts, samples, ready in [(10000, 8, True), (9999, 8, False),
                                      (10000, 5, False), (None, 8, False),
                                      (10000, None, False)]:
            with self.subTest(hosts=hosts, samples=samples):
                responses = [{"value": str(value)} if value is not None else {"ok": False}
                             for value in (hosts, samples)]
                query = Mock(side_effect=responses)
                with patch.dict(harness, {"SERVERS": ["oteldb"], "query": query}):
                    result = freshness(10000, 1700000000)
                self.assertEqual(result["end"], 1700000000)
                self.assertEqual(result["servers"]["oteldb"], {
                    "hosts": responses[0], "history": responses[1], "ready": ready})
                self.assertEqual(query.call_count, 2)
                for call in query.call_args_list:
                    self.assertEqual(call.args[0], "oteldb")
                    self.assertEqual(call.args[2], 1700000000)
                    self.assertTrue(call.kwargs["nocache"])


if __name__ == "__main__":
    unittest.main()
