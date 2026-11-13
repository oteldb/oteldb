#!/usr/bin/env python3
"""Add exact count routing to the recorded, policy-free eight-head benchmark overlay."""
import argparse
import hashlib
import json
import subprocess
from pathlib import Path

CONTRACTS = {
    "writable_heads_test.go": "a9edce3aef3822cefad621f83fc3e692aa92caaeede8266d5ed38f467824c1bc",
    "head_dispatch_test.go": "4da17940b1a90a3269ff7ec13c2be731302c2fdc4f109ae25581de3a9ad27b17",
}


def rewrite(overlay, app, storage, output):
    original = json.loads(overlay.read_text())["Replace"]
    sources = {Path(path).name: Path(path) for path in original.values()}
    for name, expected in CONTRACTS.items():
        if name not in sources or hashlib.sha256(sources[name].read_bytes()).hexdigest() != expected:
            raise ValueError("writer contract changed; re-audit canonical ID routing and absence of overflow")
    backend_suffix = "/internal/storagebackend/zz_benchmark_runtime.go"
    engine_suffix = "/engine/zz_benchmark_runtime.go"
    old_app = next(path.removesuffix(backend_suffix) for path in original if path.endswith(backend_suffix))
    old_storage = next(path.removesuffix(engine_suffix) for path in original if path.endswith(engine_suffix))
    destination = output.parent / "count-grid-sources"
    destination.mkdir(parents=True, exist_ok=True)
    replacements = {}
    for virtual, actual in original.items():
        source = Path(actual)
        body = source.read_text()
        if virtual.endswith(backend_suffix):
            body = body.replace('"context"', '\"context\"\n\t\"github.com/prometheus/prometheus/model/value\"', 1)
            body = body.replace("type benchmarkAdapter struct {", "type benchmarkAdapter struct {\n countSafety countPushdownSafety", 1)
            needle = "func (a *benchmarkAdapter) write(ctx context.Context, md sigmetric.Metrics) error {"
            assert needle in body
            body = body.replace(needle, needle + """
 for ri:=range md.Resources {for si:=range md.Resources[ri].Scopes {for mi:=range md.Resources[ri].Scopes[si].Metrics {
  for _,point:=range md.Resources[ri].Scopes[si].Metrics[mi].Points {if value.IsStaleNaN(point.Value) {if err:=a.countSafety.invalidate(ctx);err!=nil {return err}}}
 }}}
""", 1)
            needle = "return s.adapter.store.Fetcher(s.adapter.keys...)"
            assert needle in body
            body = body.replace(needle, """children:=make([]fetch.Fetcher,len(s.adapter.keys))
 for i,key:=range s.adapter.keys {children[i]=s.adapter.store.Fetcher(key)}
 return fetch.MergeDisjoint(s.adapter.countSafety.valid,children...)""", 1)
            body += """
// The pinned writer contract uses canonical-ID partitioning and AppendLimits{}, with no remapping.
func (s benchmarkSource) countPushdownLease() (safe bool,release func()) {return s.adapter.countSafety.lease()}
"""
        target = destination / source.name
        target.write_text(body)
        subprocess.run(["gofmt", "-w", str(target)], check=True)
        if virtual.startswith(old_app + "/"):
            virtual = str(app / Path(virtual).relative_to(old_app))
        elif virtual.startswith(old_storage + "/"):
            virtual = str(storage / Path(virtual).relative_to(old_storage))
        else:
            raise ValueError("unexpected overlay source root")
        replacements[virtual] = str(target)
    # Keep benchmark-only Go fixtures in this harness PR, even when --app is another checkout.
    fixture = Path(__file__).resolve().parents[3] / "integration/prome2e/head_count_grid_test.go"
    replacements[str(app / "integration/prome2e/zz_head_count_grid_test.go")] = str(fixture)
    output.write_text(json.dumps({"Replace": replacements}, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overlay", type=Path, required=True)
    parser.add_argument("--app", type=Path, required=True)
    parser.add_argument("--storage", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rewrite(args.overlay.resolve(), args.app.resolve(), args.storage.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
