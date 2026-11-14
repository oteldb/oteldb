import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("enable_count_grid", Path(__file__).with_name("enable_count_grid.py"))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CountGridOverlayTest(unittest.TestCase):
    def test_unknown_writer_rejected_before_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            overlay = root / "input.json"
            overlay.write_text(json.dumps({"Replace": {}}))
            output = root / "out.json"
            with self.assertRaisesRegex(ValueError, "writer contract changed"):
                MODULE.rewrite(overlay, root / "app", root / "storage", output)
            self.assertFalse(output.exists())

    def test_patch_preserves_original_and_remaps_roots(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            replacements = {}
            contracts = {}
            for name in ["writable_heads_test.go", "head_dispatch_test.go"]:
                source = root / name
                source.write_text("package engine\n")
                contracts[name] = hashlib.sha256(source.read_bytes()).hexdigest()
                replacements["/old/storage/engine/zz_" + name] = str(source)
            engine = root / "engine_runtime.go"
            engine.write_text("package engine\n")
            replacements["/old/storage/engine/zz_benchmark_runtime.go"] = str(engine)
            backend = root / "backend_runtime.go"
            original = '''package storagebackend
import (
 "context"
)
type benchmarkAdapter struct {}
func (a *benchmarkAdapter) write(ctx context.Context, md sigmetric.Metrics) error {
 return a.group.WriteMetrics(ctx,md)
}
func (s benchmarkSource) Fetcher() fetch.Fetcher {
 return s.adapter.store.Fetcher(s.adapter.keys...)
}
'''
            backend.write_text(original)
            replacements["/old/app/internal/storagebackend/zz_benchmark_runtime.go"] = str(backend)
            overlay = root / "input.json"
            overlay.write_text(json.dumps({"Replace": replacements}))
            output = root / "out.json"
            with patch.object(MODULE, "CONTRACTS", contracts):
                MODULE.rewrite(overlay, root / "app", root / "storage", output)
            self.assertEqual(original, backend.read_text())
            mapped = json.loads(output.read_text())["Replace"]
            path = mapped[str(root / "app/internal/storagebackend/zz_benchmark_runtime.go")]
            body = Path(path).read_text()
            fixture = mapped[str(root / "app/integration/prome2e/zz_head_count_grid_test.go")]
            self.assertTrue(Path(fixture).is_file())
            self.assertIn("BenchmarkHeadCountProbe", Path(fixture).read_text())
            self.assertIn("fetch.MergeDisjoint", body)
            self.assertIn("countPushdownLease", body)
            self.assertIn("countSafety.invalidate(ctx)", body)


if __name__ == "__main__":
    unittest.main()
