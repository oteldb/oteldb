#!/usr/bin/env python3
"""Measure trace-only native file storage at a controlled OTLP protobuf byte rate."""

import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request

HERE = Path(__file__).resolve().parent


def fetch(url, path, timeout=10):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            path.write_bytes(response.read())
        return True
    except OSError as exc:
        path.with_suffix(path.suffix + ".error").write_text(str(exc))
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--image",
        default=os.environ.get("OTELDB_BENCH_IMAGE", "local/oteldb:trace-bench"),
    )
    parser.add_argument("--seconds", type=int, default=300)
    parser.add_argument("--rate", type=int, default=1_000_000)
    parser.add_argument("--payload", type=int, default=512)
    parser.add_argument("--services", type=int, default=16)
    parser.add_argument("--age", default="0s")
    args = parser.parse_args()
    if args.seconds <= 0 or args.rate <= 0 or args.payload < 0 or args.services <= 0:
        parser.error(
            "duration, rate and services must be positive; payload must be nonnegative"
        )
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    directory = HERE / "results" / stamp
    directory.mkdir(parents=True)
    project = "odbtrace-" + stamp.lower()
    cfg = {
        "services": {
            "oteldb": {
                "image": args.image,
                "pull_policy": "never",
                "command": ["--embedded", "--config=/etc/oteldb.yml"],
                "volumes": [f"{HERE / 'oteldb.yml'}:/etc/oteldb.yml:ro", "data:/data"],
                "environment": {
                    "GOMAXPROCS": "8",
                    "GOMEMLIMIT": "14GiB",
                    "PPROF_ADDR": "0.0.0.0:9010",
                    "OTEL_LOG_LEVEL": "info",
                    "OTEL_METRICS_EXPORTER": "prometheus",
                    "OTEL_EXPORTER_PROMETHEUS_HOST": "0.0.0.0",
                    "OTEL_EXPORTER_PROMETHEUS_PORT": "9464",
                    "OTEL_LOGS_EXPORTER": "none",
                    "OTEL_TRACES_EXPORTER": "none",
                },
                "cpus": 8,
                "mem_limit": "16g",
                "memswap_limit": "16g",
                "ports": [
                    "127.0.0.1:24318:4318",
                    "127.0.0.1:24910:9010",
                    "127.0.0.1:24133:13133",
                    "127.0.0.1:24964:9464",
                    "127.0.0.1:24090:8090",
                ],
            }
        },
        "volumes": {"data": {}},
    }
    (directory / "compose.json").write_text(json.dumps(cfg, indent=2))
    (directory / "workload.json").write_text(json.dumps(vars(args), indent=2))
    compose = [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(directory / "compose.json"),
    ]

    def dc(*cmd, **kwargs):
        return subprocess.run(compose + list(cmd), check=True, **kwargs)

    begin = time.monotonic()
    container = None
    sender = None
    sender_output = None

    def capture(label):
        row = {"elapsed": time.monotonic() - begin, "time": time.time(), "label": label}
        paths = [
            "/proc/1/status",
            "/proc/1/smaps_rollup",
            "/sys/fs/cgroup/memory.stat",
            "/sys/fs/cgroup/memory.events",
            "/sys/fs/cgroup/memory.current",
            "/sys/fs/cgroup/memory.peak",
            "/sys/fs/cgroup/cpu.stat",
            "/sys/fs/cgroup/io.stat",
        ]
        for path in paths:
            result = subprocess.run(
                ["docker", "exec", container, "cat", path],
                text=True,
                capture_output=True,
            )
            if result.returncode == 0:
                row[path] = result.stdout
        with (directory / "resources.jsonl").open("a") as output:
            output.write(json.dumps(row) + "\n")
        fetch("http://127.0.0.1:24964/metrics", directory / f"{label}.metrics")
        fetch(
            "http://127.0.0.1:24090/api/v1/storage", directory / f"{label}.storage.json"
        )
        return row

    print(f"Results: {directory}", flush=True)
    try:
        with (directory / "compose.log").open("w") as output:
            dc("up", "-d", stdout=output, stderr=output)
        container = dc(
            "ps", "-q", "oteldb", capture_output=True, text=True
        ).stdout.strip()
        (directory / "container.json").write_bytes(
            subprocess.check_output(["docker", "inspect", container])
        )
        (directory / "image.json").write_bytes(
            subprocess.check_output(["docker", "image", "inspect", args.image])
        )
        subprocess.run(
            [
                "docker",
                "cp",
                f"{container}:/usr/local/bin/oteldb",
                str(directory / "oteldb"),
            ],
            check=True,
            capture_output=True,
        )
        ready = False
        for _ in range(60):
            if fetch(
                "http://127.0.0.1:24133/readiness", directory / "readiness.txt", 2
            ):
                ready = True
                break
            time.sleep(1)
        if not ready:
            raise RuntimeError("server did not become ready")
        fetch(
            "http://127.0.0.1:24910/debug/pprof/heap?gc=1",
            directory / "baseline.heap.pb.gz",
        )
        capture("baseline")
        sender_output = (directory / "sender.jsonl").open("w")
        generator = HERE.parent / "scale-bench/bin/trace-loadgen"
        sender = subprocess.Popen(
            [
                str(generator),
                "-bytes-per-second",
                str(args.rate),
                "-duration",
                f"{args.seconds}s",
                "-payload",
                str(args.payload),
                "-services",
                str(args.services),
                "-age",
                args.age,
            ],
            stdout=sender_output,
            stderr=subprocess.STDOUT,
            env=dict(os.environ, GOMAXPROCS="2"),
        )
        begin = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            futures = []
            profiled = False
            while sender.poll() is None:
                elapsed = time.monotonic() - begin
                label = f"{int(elapsed):04d}"
                row = capture(label)
                rss = next(
                    (
                        line.split()[1]
                        for line in row.get("/proc/1/status", "").splitlines()
                        if line.startswith("VmRSS:")
                    ),
                    "unknown",
                )
                print(f"{label}s RSS={rss} KiB", flush=True)
                if elapsed >= min(120, args.seconds / 2) and not profiled:
                    futures.append(
                        pool.submit(
                            fetch,
                            "http://127.0.0.1:24910/debug/pprof/profile?seconds=20",
                            directory / "steady.cpu.pb.gz",
                            35,
                        )
                    )
                    futures.append(
                        pool.submit(
                            fetch,
                            "http://127.0.0.1:24910/debug/pprof/heap",
                            directory / "steady.heap.pb.gz",
                        )
                    )
                    profiled = True
                if elapsed > args.seconds + 30:
                    raise RuntimeError("sender exceeded its duration budget")
                time.sleep(5)
            if sender.returncode != 0:
                raise RuntimeError(f"sender exited {sender.returncode}")
            sender_output.close()
            sender_output = None
            capture("loaded")
            fetch(
                "http://127.0.0.1:24910/debug/pprof/heap",
                directory / "loaded.heap.pb.gz",
            )
            fetch(
                "http://127.0.0.1:24910/debug/pprof/heap?gc=1",
                directory / "loaded-gc.heap.pb.gz",
            )
            for step in range(3):
                time.sleep(15)
                capture(f"drain-{(step + 1) * 15}")
            fetch(
                "http://127.0.0.1:24910/debug/pprof/heap?gc=1",
                directory / "drained-gc.heap.pb.gz",
            )
            for future in futures:
                if not future.result():
                    raise RuntimeError("profile capture failed")
        subprocess.run(
            [
                "docker",
                "cp",
                f"{container}:/data/parts/default/traces/bucket-index.bin",
                str(directory / "traces-index.bin"),
            ],
            check=False,
            capture_output=True,
        )
    finally:
        if sender is not None and sender.poll() is None:
            sender.terminate()
            sender.wait(timeout=20)
        if sender_output is not None:
            sender_output.close()
        with (directory / "server.log").open("w") as output:
            dc("logs", "--no-color", "oteldb", stdout=output, stderr=output)
        if container:
            dc("stop", "-t", "45", "oteldb", capture_output=True)
            (directory / "stopped-container.json").write_bytes(
                subprocess.check_output(["docker", "inspect", container])
            )
            with (directory / "shutdown.log").open("w") as output:
                dc(
                    "logs",
                    "--no-color",
                    "--tail",
                    "100",
                    "oteldb",
                    stdout=output,
                    stderr=output,
                )
    print(f"Complete; stopped container and volume retained: {directory}", flush=True)


if __name__ == "__main__":
    main()
