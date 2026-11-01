#!/usr/bin/env python3
"""Snapshot a stopped scale-benchmark database for query-only Go benchmarks."""

import argparse
import datetime
import json
from pathlib import Path
import re
import subprocess


def targets_from_rows(rows):
    rows = sorted(
        (row for row in rows if row.get("kind") == "query"
         and row["server"] == "oteldb" and row["mode"] == "random"
         and row["trial"] == 0), key=lambda row: row["query_index"])
    if len(rows) != 242:
        raise ValueError("a completed 242-target dashboard load is required")
    targets = []
    for row in rows:
        query, replaced = re.subn(r'instance="host-[0-9]+"', 'instance="host-0"', row["query"])
        if not replaced:
            raise ValueError(f"target {row['query_index']} has no concrete instance")
        targets.append({"query": query, "instant": row["instant"], "query_index": row["query_index"]})
    return targets


def snapshot(run, output):
    recorded = json.loads((run / "container.json").read_text())
    recorded = recorded[0] if isinstance(recorded, list) else recorded
    container = recorded["Name"].lstrip("/")
    current = json.loads(subprocess.check_output(["docker", "inspect", container], text=True))[0]
    if current["State"]["Running"]:
        raise ValueError("stop the benchmark before taking a fixture snapshot")
    volume = next(mount for mount in current["Mounts"] if mount["Destination"] == "/data")
    source = Path(volume["Source"]) / "parts"
    if not source.is_dir():
        raise ValueError("the native-file parts directory must be locally accessible")
    options = json.loads((run / "options.json").read_text())
    if options["nodes"] != 10000:
        raise ValueError("this reproducer requires a 10k-host run")
    latest = max(run.glob("*-storage.json"), key=lambda path: float(path.name.split("-")[0]))
    series = json.loads(latest.read_text())["engine"]["tenants"][0]["total_series"]
    with (run / "soak-dashboards.jsonl").open() as data:
        targets = targets_from_rows(json.loads(line) for line in data)
    finished = datetime.datetime.fromisoformat(current["State"]["FinishedAt"].replace("Z", "+00:00"))
    end = int((finished.timestamp() - 120) // 15 * 15)
    output.mkdir(parents=True, exist_ok=False)
    # Require a COW copy: do not silently duplicate tens of GiB on an unsupported filesystem.
    subprocess.run(["cp", "-a", "--reflink=always", str(source), str(output / "parts")], check=True)
    manifest = {
        "version": 1, "source_run": run.name, "source_container": container,
        "source_volume": volume.get("Name"), "snapshot": "cp -a --reflink=always",
        "parts_dir": "parts", "hosts": 10000, "expected_series": series,
        "end_unix": end, "end_utc": datetime.datetime.fromtimestamp(end, datetime.timezone.utc).isoformat(),
        "targets_file": "targets.json", "read_cache_bytes": 896 * 2**20,
        "decode_cache_bytes": 448 * 2**20, "decode_memory_bytes": 5824 * 2**20,
        "max_query_bytes": 3584 * 2**20,
        "notes": "Persisted historical samples only; no WAL replay or background writes.",
    }
    (output / "targets.json").write_text(json.dumps(targets, indent=2) + "\n")
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(output / "manifest.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    snapshot(args.run.resolve(), args.output.expanduser().resolve())
