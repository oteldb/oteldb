#!/usr/bin/env python3
"""Summarize sampled memory and storage counters from a trace benchmark run."""

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess


def metrics(path):
    result = {}
    for line in path.read_text().splitlines():
        if line.startswith("#"):
            continue
        match = re.match(r"([^\s{]+)(?:\{([^}]*)\})?\s+([^\s]+)", line)
        if not match:
            continue
        name, labels, value = match.groups()
        if labels and "signal=" in labels and 'signal="trace"' not in labels:
            continue
        result[name] = result.get(name, 0) + float(value)
    return result


def summarize(directory):
    sender = [
        json.loads(line)
        for line in (directory / "sender.jsonl").read_text().splitlines()
    ]
    rows = [
        json.loads(line)
        for line in (directory / "resources.jsonl").read_text().splitlines()
    ]
    samples = []
    for row in rows:
        status = row.get("/proc/1/status", "")
        rss = next(
            (
                int(line.split()[1]) * 1024
                for line in status.splitlines()
                if line.startswith("VmRSS:")
            ),
            0,
        )
        path = directory / f"{row['label']}.metrics"
        values = metrics(path) if path.exists() else {}
        samples.append(
            {
                "label": row["label"],
                "elapsed": row["elapsed"],
                "rss_bytes": rss,
                "cgroup_bytes": int(row.get("/sys/fs/cgroup/memory.current", "0")),
                "heap_alloc_bytes": values.get("go_memstats_heap_alloc_bytes", 0),
                "allocated_bytes_total": values.get("go_memstats_alloc_bytes_total", 0),
                "head_bytes": values.get("storage_head_bytes", 0),
                "identity_bytes": values.get("storage_head_identity_bytes", 0),
                "flush_bytes": values.get("storage_flush_bytes_total", 0),
                "merge_bytes": values.get("storage_merge_bytes_out_total", 0),
                "merge_seconds": values.get("storage_merge_duration_seconds_sum", 0),
            }
        )
    final = samples[-1]
    baseline = samples[0]
    ratio = (
        final["merge_bytes"] / final["flush_bytes"] if final["flush_bytes"] else None
    )
    inspect = json.loads((directory / "container.json").read_text())[0]
    stopped_path = directory / "stopped-container.json"
    stopped = (
        json.loads(stopped_path.read_text())[0]["State"]
        if stopped_path.exists()
        else None
    )
    image = json.loads((directory / "image.json").read_text())[0]
    result = {
        "workload": json.loads((directory / "workload.json").read_text()),
        "sender": sender[-1],
        "image": {"id": image["Id"], "labels": image["Config"].get("Labels", {})},
        "limits": {
            k: inspect["HostConfig"].get(k)
            for k in ["NanoCpus", "Memory", "MemorySwap"]
        },
        "baseline": baseline,
        "final": final,
        "peak_sampled_rss_bytes": max(row["rss_bytes"] for row in samples),
        "cgroup_peak_bytes": max(
            int(row.get("/sys/fs/cgroup/memory.peak", "0")) for row in rows
        ),
        "peak_sampled_heap_alloc_bytes": max(
            row["heap_alloc_bytes"] for row in samples
        ),
        "peak_sampled_head_bytes": max(row["head_bytes"] for row in samples),
        "peak_sampled_identity_bytes": max(row["identity_bytes"] for row in samples),
        "merge_rewrite_factor": ratio,
        "allocation_bytes_per_payload_byte": (
            final["allocated_bytes_total"] - baseline["allocated_bytes_total"]
        )
        / sender[-1]["payload_bytes"],
        "stopped_state": stopped,
        "samples": samples,
    }
    binary = Path(__file__).resolve().parent.parent / "scale-bench/bin/oteldb"
    if binary.exists() and shutil.which("go"):
        profiles = {}
        for label in ["baseline", "steady", "loaded", "loaded-gc", "drained-gc"]:
            profile = directory / f"{label}.heap.pb.gz"
            if profile.exists():
                output = subprocess.check_output(
                    [
                        "go",
                        "tool",
                        "pprof",
                        "-top",
                        "-unit=bytes",
                        "-nodecount=12",
                        str(binary),
                        str(profile),
                    ],
                    text=True,
                )
                (directory / f"{label}.heap.bytes.txt").write_text(output)
                total = re.search(r"of ([\d.]+)B total", output)
                profiles[label] = int(float(total.group(1))) if total else None
        result["profile_inuse_bytes"] = profiles
    (directory / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: value
                for key, value in result.items()
                if key not in ["samples", "image"]
            },
            indent=2,
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    summarize(args.directory)


if __name__ == "__main__":
    main()
