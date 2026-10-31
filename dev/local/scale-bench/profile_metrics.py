#!/usr/bin/env python3
"""Capture bounded ingest profiles on the paired node-exporter benchmark."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import time

HERE = Path(__file__).resolve().parent
harness = runpy.run_path(str(HERE / "run.py"))


def fetch(url, destination, timeout=15):
    try:
        destination.write_bytes(harness["get"](url, timeout))
        return True
    except OSError as exc:
        destination.with_suffix(destination.suffix + ".error").write_text(str(exc))
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=10000)
    parser.add_argument("--seconds", type=int, default=360)
    parser.add_argument("--image", required=True)
    parser.add_argument("--binary", default=str(HERE / "bin/oteldb"), help="matching image binary to retain")
    parser.add_argument("--trace-seconds", type=int, default=0, help="execution trace duration at 120s and 300s")
    args = parser.parse_args()
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    directory = HERE / "results" / ("profile-" + stamp)
    directory.mkdir(parents=True)
    project = "odbprofile-" + stamp.lower()
    env = dict(os.environ, BENCH_RUN_DIR=str(directory), OTELDB_BENCH_IMAGE=args.image)
    scrape = ["global:", "  scrape_interval: 15s", "  scrape_timeout: 10s", "scrape_configs:",
              "  - job_name: node_exporter", "    static_configs:"]
    scrape.extend(f"      - targets: ['node-cache:9100']\n        labels: {{instance: 'host-{i}'}}"
                  for i in range(args.nodes))
    (directory / "scrape.yml").write_text("\n".join(scrape) + "\n")
    original = ["docker", "compose", "-p", project, "-f", str(HERE / "docker-compose.yml")]
    config = json.loads(subprocess.check_output(original + ["config", "--format", "json"], env=env))
    database = config["services"]["oteldb"]
    source_config = HERE.parent / "embedded-bench/oteldb.yml"
    (directory / "oteldb.yml").write_text(source_config.read_text() + "\nadmin:\n  bind: 0.0.0.0:8090\n")
    for volume in database["volumes"]:
        if volume["target"] == "/etc/oteldb.yml":
            volume["source"] = str(directory / "oteldb.yml")
    database["ports"].append({"target": 8090, "published": "19809", "host_ip": "127.0.0.1", "protocol": "tcp"})
    (directory / "compose.json").write_text(json.dumps(config, indent=2) + "\n")
    (directory / "options.json").write_text(json.dumps(vars(args), indent=2) + "\n")
    shutil.copyfile(args.binary, directory / "oteldb")
    (directory / "buildinfo.txt").write_bytes(subprocess.check_output(["go", "version", "-m", str(directory / "oteldb")]))
    (directory / "image.json").write_bytes(subprocess.check_output(["docker", "image", "inspect", args.image]))
    compose = ["docker", "compose", "-p", project, "-f", str(directory / "compose.json")]

    def dc(*cmd, **kwargs):
        return subprocess.run(compose + list(cmd), env=env, text=True, check=True, **kwargs)

    def capture(elapsed, container):
        label = f"{elapsed:06.1f}"
        for server, port in [("oteldb", 19464), ("vmagent", 18429), ("victoriametrics", 18428)]:
            fetch(f"http://127.0.0.1:{port}/metrics", directory / f"{label}-{server}.metrics", 5)
        fetch("http://127.0.0.1:19809/api/v1/storage", directory / f"{label}-storage.json", 5)
        paths = ["/proc/1/io", "/proc/1/status", "/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/memory.stat",
                 "/sys/fs/cgroup/memory.events", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.peak",
                 "/sys/fs/cgroup/cpu.pressure", "/sys/fs/cgroup/io.pressure"]
        command = 'for p in ' + ' '.join(paths) + '; do echo "PATH=$p"; cat "$p"; done'
        result = subprocess.run(["docker", "exec", container, "sh", "-c", command], text=True, capture_output=True)
        with (directory / "resources.jsonl").open("a") as output:
            output.write(json.dumps({"elapsed": elapsed, "time": time.time(), "raw": result.stdout}) + "\n")
        return harness["query"]("oteldb", 'count(node_uname_info{job="node_exporter"})', time.time())

    print(f"Results: {directory}", flush=True)
    try:
        with (directory / "compose.log").open("w") as output:
            dc("up", "-d", stdout=output, stderr=output)
        container = dc("ps", "-q", "oteldb", capture_output=True).stdout.strip()
        initial = subprocess.check_output(["docker", "inspect", container])
        (directory / "container.json").write_bytes(initial)
        limits = json.loads(initial)[0]["HostConfig"]
        assert limits["NanoCpus"] == 8_000_000_000
        assert limits["Memory"] == limits["MemorySwap"] == 16 << 30
        start = time.monotonic()
        taken = set()
        with ThreadPoolExecutor(4) as pool:
            futures = []
            while time.monotonic() - start < args.seconds:
                elapsed = time.monotonic() - start
                row = capture(elapsed, container)
                print(f"{elapsed:.0f}s: hosts={row.get('value', row.get('error'))}", flush=True)
                for threshold in [60, 120, 180, 240, 300]:
                    if elapsed >= threshold and threshold not in taken:
                        taken.add(threshold)
                        futures.append(pool.submit(fetch, "http://127.0.0.1:19010/debug/pprof/heap",
                                                   directory / f"{threshold}-heap.pb.gz"))
                        futures.append(pool.submit(fetch, "http://127.0.0.1:19010/debug/pprof/goroutine?debug=2",
                                                   directory / f"{threshold}-goroutines.txt"))
                        if threshold in [120, 300] and args.trace_seconds > 0:
                            futures.append(pool.submit(fetch,
                                f"http://127.0.0.1:19010/debug/pprof/trace?seconds={args.trace_seconds}",
                                directory / f"{threshold}.trace", args.trace_seconds + 20))
                        if threshold in [60, 240]:
                            for kind in ["profile", "allocs"]:
                                futures.append(pool.submit(fetch,
                                    f"http://127.0.0.1:19010/debug/pprof/{kind}?seconds=30",
                                    directory / f"{threshold}-{kind}.pb.gz", 45))
                state = json.loads(subprocess.check_output(["docker", "inspect", container]))[0]["State"]
                if not state["Running"]:
                    (directory / "early-exit.json").write_text(json.dumps(state, indent=2) + "\n")
                    break
                time.sleep(min(15, max(0, args.seconds - (time.monotonic() - start))))
            capture(time.monotonic() - start, container)
            dc("stop", "-t", "20", "vmagent", capture_output=True)
            time.sleep(15)
            fetch("http://127.0.0.1:19010/debug/pprof/heap?gc=1", directory / "quiescent-gc.heap.pb.gz", 30)
            capture(time.monotonic() - start, container)
            for future in futures:
                future.result()
    finally:
        ids = dc("ps", "-aq", capture_output=True).stdout.split()
        if ids:
            (directory / "completed-containers.json").write_bytes(subprocess.check_output(["docker", "inspect", *ids]))
        with (directory / "containers.log").open("w") as output:
            dc("logs", "--no-color", stdout=output, stderr=output)
        dc("stop", "-t", "30", capture_output=True)
        if ids:
            (directory / "stopped-containers.json").write_bytes(subprocess.check_output(["docker", "inspect", *ids]))
        with (directory / "shutdown.log").open("w") as output:
            dc("logs", "--no-color", "--tail", "100", stdout=output, stderr=output)
    print(f"Stopped benchmark containers; retained volumes and profiles in {directory}", flush=True)


if __name__ == "__main__":
    main()
