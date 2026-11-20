"""Summarize write-pressure runs, preserving truncated/OOM status; print JSON."""
import json
from pathlib import Path
import re
import sys


def metric(path, name, where=None):
    total = 0.0
    for line in path.read_text().splitlines():
        if re.match(re.escape(name) + r"(?:\{|\s)", line) and (where is None or where in line):
            total += float(line.rsplit(" ", 1)[1])
    return total


def fields(text):
    return {k.rstrip(":"): float(v) for k, v in (line.split() for line in text.splitlines())}


def summarize(path):
    db = path / "loaded-final-oteldb.metrics"
    complete = db.exists()
    target = None
    if not complete:
        db = sorted(path.glob("[0-9]*-oteldb.metrics"))[-1]
        target = int(db.name.split("-")[0])
    agent = path / db.name.replace("oteldb", "vmagent")
    rows = [json.loads(line) for line in (path / "resources.jsonl").read_text().splitlines()]
    if target is None:
        target = rows[-2]["elapsed"]
    required = ["/sys/fs/cgroup/cpu.stat", "/proc/1/io", "/sys/fs/cgroup/memory.peak",
                "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.stat", "/sys/fs/cgroup/memory.events"]
    rows = [r for r in rows if all(key in r for key in required)]
    row = min(rows, key=lambda value: abs(value["elapsed"] - target))
    elapsed = row["elapsed"]
    cpu = fields(row["/sys/fs/cgroup/cpu.stat"])
    io = fields(row["/proc/1/io"])
    accepted = metric(db, "storage_ingest_accepted_total")
    appends = metric(db, "storage_wal_appends_total")
    container = json.loads((path / "container.json").read_text())[0]
    limits = container["HostConfig"]
    assert limits["Memory"] == 16 << 30
    assert limits["MemorySwap"] == limits["Memory"]
    assert limits["NanoCpus"] == 8_000_000_000
    working = [int(r["/sys/fs/cgroup/memory.current"]) - fields(r["/sys/fs/cgroup/memory.stat"])["inactive_file"]
               for r in rows if r["elapsed"] <= elapsed]
    stopped = path / "stopped-containers.json"
    state = None
    if stopped.exists():
        state = next(c["State"] for c in json.loads(stopped.read_text())
                     if c["Config"]["Labels"].get("com.docker.compose.service") == "oteldb")
    return {
        "directory": str(path), "image": container["Image"], "elapsed_s": elapsed,
        "metrics_file": db.name, "completed_load": complete,
        "stopped_state": state,
        "offered_points": metric(agent, "vmagent_remotewrite_rows_pushed_after_relabel_total"),
        "accepted_points": accepted, "accepted_points_per_s": accepted / elapsed,
        "WAL_appends": appends, "points_per_WAL_append": accepted / appends if appends else None,
        "pending_bytes": metric(agent, "vmagent_remotewrite_pending_data_bytes"),
        "write_syscalls": io["syscw"], "process_wchar": io["wchar"],
        "mean_CPU_cores": cpu["usage_usec"] / 1e6 / elapsed,
        "throttled_s": cpu["throttled_usec"] / 1e6,
        "memory_peak_bytes": int(row["/sys/fs/cgroup/memory.peak"]),
        "sampled_peak_working_set_bytes": max(working),
        "memory_events": fields(row["/sys/fs/cgroup/memory.events"]),
        "backend_write_bytes": metric(db, "storage_backend_bytes_total", 'op="write"'),
        "flush_count": metric(db, "storage_flush_total"),
        "flush_s": metric(db, "storage_flush_duration_seconds_sum"),
        "merge_count": metric(db, "storage_merge_total"),
        "merge_s": metric(db, "storage_merge_duration_seconds_sum"),
    }


print(json.dumps({Path(arg).name: summarize(Path(arg)) for arg in sys.argv[1:]}, indent=2))
