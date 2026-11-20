#!/usr/bin/env python3
"""Capture write-only metrics, CPU profiles, and a runtime trace under live fan-out."""

import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request

HERE = Path(__file__).resolve().parent


def fetch(url, path, timeout=10):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response, path.open("wb") as output:
            while chunk := response.read(1 << 20):
                output.write(chunk)
        return True
    except OSError as exc:
        path.with_suffix(path.suffix + ".error").write_text(str(exc))
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=10000)
    parser.add_argument("--seconds", type=int, default=300)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--image", default=os.environ.get("OTELDB_BENCH_IMAGE", "local/oteldb:scale-compact"))
    args = parser.parse_args()
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    directory = HERE / "results" / ("write-" + stamp)
    directory.mkdir()
    project = "odbwrite-" + stamp.lower()
    env = dict(os.environ, BENCH_RUN_DIR=str(directory), OTELDB_BENCH_IMAGE=args.image)
    scrape = ["global:", "  scrape_interval: 15s", "  scrape_timeout: 10s", "scrape_configs:",
              "  - job_name: node_exporter", "    static_configs:"]
    scrape.extend(f"      - targets: ['node-cache:9100']\n        labels: {{instance: 'host-{i}'}}"
                  for i in range(args.nodes))
    (directory / "scrape.yml").write_text("\n".join(scrape) + "\n")
    original = ["docker", "compose", "-p", project, "-f", str(HERE / "docker-compose.yml")]
    cfg = json.loads(subprocess.check_output(original + ["config", "--format", "json"], env=env))
    del cfg["services"]["victoriametrics"]
    agent = cfg["services"]["vmagent"]
    agent["depends_on"].pop("victoriametrics", None)
    agent["command"] = [x for x in agent["command"] if "victoriametrics:8428" not in x]
    database = cfg["services"]["oteldb"]
    database["environment"].update(OTEL_METRICS_EXPORTER="prometheus", OTEL_LOG_LEVEL="debug",
                                   OTEL_EXPORTER_PROMETHEUS_HOST="0.0.0.0", OTEL_EXPORTER_PROMETHEUS_PORT="9464")
    database["ports"].append({"target": 9464, "published": "19464", "host_ip": "127.0.0.1", "protocol": "tcp"})
    (directory / "compose.json").write_text(json.dumps(cfg, indent=2))
    compose = ["docker", "compose", "-p", project, "-f", str(directory / "compose.json")]

    def dc(*cmd, **kwargs):
        return subprocess.run(compose + list(cmd), env=env, text=True, check=True, **kwargs)

    def capture(container, label):
        paths = ["/proc/1/io", "/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/memory.stat",
                 "/sys/fs/cgroup/memory.events", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.peak",
                 "/sys/fs/cgroup/io.stat", "/sys/fs/cgroup/cpu.pressure", "/sys/fs/cgroup/io.pressure",
                 "/sys/fs/cgroup/memory.pressure"]
        row = {"time": time.time(), "elapsed": time.monotonic() - begin}
        for path in paths:
            r = subprocess.run(["docker", "exec", container, "cat", path], text=True, capture_output=True)
            if r.returncode == 0:
                row[path] = r.stdout
        with (directory / "resources.jsonl").open("a") as output:
            output.write(json.dumps(row) + "\n")
        fetch("http://127.0.0.1:19464/metrics", directory / f"{label}-oteldb.metrics", 5)
        fetch("http://127.0.0.1:18429/metrics", directory / f"{label}-vmagent.metrics", 5)

    print(f"Results: {directory}", flush=True)
    print("Only oteldb receives writes; database limits remain 8 vCPU / 16 GiB / no swap.", flush=True)
    futures = []
    try:
        with (directory / "compose.log").open("w") as output:
            dc("up", "-d", stdout=output, stderr=output)
        container = dc("ps", "-q", "oteldb", capture_output=True).stdout.strip()
        (directory / "container.json").write_bytes(subprocess.check_output(["docker", "inspect", container]))
        begin = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            profiled = False
            early_heap = False
            while time.monotonic() - begin < args.seconds:
                elapsed = int(time.monotonic() - begin)
                capture(container, f"{elapsed:04d}")
                print(f"Captured {elapsed}s", flush=True)
                if elapsed >= 90 and not early_heap:
                    futures.append(pool.submit(fetch, "http://127.0.0.1:19010/debug/pprof/heap",
                                               directory / "early.heap.pb.gz", 15))
                    early_heap = True
                if elapsed >= 180 and not profiled:
                    futures.append(pool.submit(fetch, "http://127.0.0.1:19010/debug/pprof/profile?seconds=30",
                                               directory / "steady.cpu.pb.gz", 45))
                    futures.append(pool.submit(fetch, "http://127.0.0.1:19010/debug/pprof/trace?seconds=5",
                                               directory / "steady.trace", 20))
                    futures.append(pool.submit(fetch, "http://127.0.0.1:19010/debug/pprof/heap",
                                               directory / "steady.heap.pb.gz", 15))
                    profiled = True
                time.sleep(min(15, max(0, args.seconds - (time.monotonic() - begin))))
            fetch("http://127.0.0.1:19010/debug/pprof/heap", directory / "loaded.heap.pb.gz")
            fetch("http://127.0.0.1:19010/debug/pprof/goroutine?debug=2", directory / "loaded.goroutines.txt")
            capture(container, "loaded-final")
            dc("stop", "-t", "15", "vmagent", capture_output=True)
            fetch("http://127.0.0.1:19010/debug/pprof/profile?seconds=15", directory / "draining.cpu.pb.gz", 30)
            capture(container, "draining-final")
            for future in futures:
                future.result()
    finally:
        with (directory / "containers.log").open("w") as output:
            dc("logs", "--no-color", stdout=output, stderr=output)
        if args.keep:
            dc("stop", "-t", "30", capture_output=True)
            ids = dc("ps", "-aq", capture_output=True).stdout.split()
            if ids:
                (directory / "stopped-containers.json").write_bytes(
                    subprocess.check_output(["docker", "inspect", *ids]))
            with (directory / "shutdown.log").open("w") as output:
                dc("logs", "--no-color", "--tail", "100", stdout=output, stderr=output)
        else:
            dc("down", "-v", "--remove-orphans", capture_output=True)
    print(f"Complete: {directory}", flush=True)


if __name__ == "__main__":
    main()
