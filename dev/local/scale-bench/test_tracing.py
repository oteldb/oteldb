import copy
import runpy
import unittest
from pathlib import Path

configure_tracing = runpy.run_path(str(Path(__file__).with_name("profile_metrics.py")))["configure_tracing"]


class TracingTest(unittest.TestCase):
    def test_disabled_preserves_config(self):
        config = {"services": {"oteldb": {"environment": {"OTEL_TRACES_EXPORTER": "none"}}}}
        original = copy.deepcopy(config)
        configure_tracing(config, None, None, 1, "test")
        self.assertEqual(original, config)

    def test_tracing_retains_database_limits_and_networks(self):
        for networks in [None, {"default": None, "extra": None}, ["default", "extra"]]:
            with self.subTest(networks=networks):
                database = {"environment": {"OTEL_METRICS_EXPORTER": "prometheus"},
                            "cpus": 8, "mem_limit": "16g", "memswap_limit": "16g"}
                if networks is not None:
                    database["networks"] = copy.deepcopy(networks)
                config = {"services": {"oteldb": database}, "networks": {"default": {}}}
                configure_tracing(config, "http://jaeger:4317", "collector", .1, "test")
                self.assertEqual(8, database["cpus"])
                self.assertEqual("16g", database["mem_limit"])
                self.assertEqual("16g", database["memswap_limit"])
                self.assertIn("default", database["networks"])
                if networks is not None:
                    self.assertIn("extra", database["networks"])
                self.assertIn("trace-collector", database["networks"])
                self.assertEqual({"external": True, "name": "collector"}, config["networks"]["trace-collector"])
                self.assertEqual("prometheus", database["environment"]["OTEL_METRICS_EXPORTER"])
                self.assertEqual("otlp", database["environment"]["OTEL_TRACES_EXPORTER"])
                self.assertEqual("0.1", database["environment"]["OTEL_TRACES_SAMPLER_ARG"])


if __name__ == "__main__":
    unittest.main()
