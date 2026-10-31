#!/usr/bin/env python3
"""Capture bounded ingest profiles on the paired node-exporter benchmark."""

import argparse
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
import json
import os
from pathlib import Path
import random
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


def dashboard_soak(args, directory, servers, capture):
    """Refresh full dashboards while the caller samples the live write queue and resources."""
    queries = harness["dashboard"](True)
    hosts = list(range(1, args.nodes))
    random.Random(args.seed + args.nodes).shuffle(hosts)
    started = time.monotonic()
    started_unix = time.time()
    deadline = started + args.soak_seconds
    cached_end = int((time.time() - 30) // 15 * 15)

    def refresh():
        measured = []
        trial = 0
        with (directory / "soak-dashboards.jsonl").open("w") as output:
            with ThreadPoolExecutor(args.jobs) as clients:
                while time.monotonic() < deadline:
                    scheduled = started + trial * args.refresh_interval
                    lateness = max(0, time.monotonic() - scheduled)
                    end = int((time.time() - 30) // 15 * 15)
                    for server in servers:
                        for mode in ["random", "cached"]:
                            if time.monotonic() >= deadline:
                                break
                            host = f"host-{hosts[trial % len(hosts)]}" if mode == "random" else "host-0"
                            query_end = end if mode == "random" else cached_end
                            rows, load = harness["load_dashboard"](
                                server, host, query_end, mode, trial, mode == "cached" and trial < 2,
                                queries, clients, args, min(deadline, time.monotonic() + args.query_budget))
                            load.update(elapsed=time.monotonic() - started, lateness_s=lateness,
                                        end=query_end)
                            for row in rows:
                                output.write(json.dumps(row) + "\n")
                            output.write(json.dumps(load) + "\n")
                            output.flush()
                            measured.append(load)
                            print(f"  soak {server} {mode}/{trial}: {load['queries']}/{len(queries)} "
                                  f"queries, {load['wall_ms']:.1f}ms", flush=True)
                    trial += 1
                    delay = min(deadline, started + trial * args.refresh_interval) - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
        return measured

    capture()
    with ThreadPoolExecutor(1) as runner:
        pending = runner.submit(refresh)
        while True:
            try:
                loads = pending.result(timeout=15)
                break
            except FutureTimeout:
                if pending.done():
                    raise
                capture()
    capture()
    summary = {"started_unix": started_unix, "ended_unix": time.time(),
               "elapsed_s": time.monotonic() - started,
               "refresh_interval_s": args.refresh_interval, "loads": loads}
    (directory / "soak-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=10000)
    parser.add_argument("--seconds", type=int, default=360)
    parser.add_argument("--image", required=True)
    parser.add_argument("--subnet", help="explicit Docker network subnet when automatic pools are exhausted")
    parser.add_argument("--config", help="benchmark application config; defaults to embedded-bench/oteldb.yml")
    parser.add_argument("--native-remote-write-url", default="http://oteldb:19291/api/v1/write",
                        help="native target URL inside the Compose network; defaults to the Collector receiver")
    parser.add_argument("--binary", default=str(HERE / "bin/oteldb"), help="matching image binary to retain")
    parser.add_argument("--trace-seconds", type=int, default=0, help="execution trace duration at 120s and 300s")
    parser.add_argument("--queries", action="store_true", help="run dashboard queries after capture for servers passing freshness")
    parser.add_argument("--live-queries", action="store_true", help="keep ingestion running during dashboard queries")
    parser.add_argument("--dashboard-loads", action="store_true", help="also measure complete-dashboard wall time")
    parser.add_argument("--query-profile-seconds", type=int, default=0, help="capture CPU/allocation profiles during live dashboard measurement")
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--seed", type=int, default=20261027)
    parser.add_argument("--query-budget", type=int, default=120)
    parser.add_argument("--otelbench", action="store_true", help="also run the complete dashboard for ready servers")
    parser.add_argument("--soak-seconds", type=int, default=0, help="sustained dashboard refresh duration after the query phase")
    parser.add_argument("--refresh-interval", type=float, default=15, help="seconds between sustained dashboard refreshes")
    args = parser.parse_args()
    if args.live_queries and not args.queries:
        parser.error("--live-queries requires --queries")
    if args.soak_seconds < 0 or args.refresh_interval <= 0:
        parser.error("soak duration must be non-negative and refresh interval must be positive")
    if args.soak_seconds and (not args.live_queries or args.nodes < 2):
        parser.error("--soak-seconds requires --live-queries and at least two nodes")
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
    if args.subnet:
        config["networks"]["default"]["ipam"] = {"config": [{"subnet": args.subnet}]}
    database = config["services"]["oteldb"]
    agent = config["services"]["vmagent"]
    agent["command"] = [f"-remoteWrite.url={args.native_remote_write_url}"
                        if item.startswith("-remoteWrite.url=http://oteldb:") else item
                        for item in agent["command"]]
    source_config = Path(args.config) if args.config else HERE.parent / "embedded-bench/oteldb.yml"
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
            end = int((time.time() - 30) // 15 * 15)
            readiness = {}
            history = []
            for server in harness["SERVERS"]:
                hosts = harness["query"](server, 'count(node_uname_info{job="node_exporter"})', end, nocache=True)
                samples = harness["query"](server,
                    'min(count_over_time(node_uname_info{job="node_exporter"}[120s]))', end, nocache=True)
                history.append(samples)
                readiness[server] = {"hosts": hosts, "history": samples,
                    "ready": harness["number"](hosts) == args.nodes and harness["number"](samples) >= 6}
            (directory / "freshness-final.json").write_text(json.dumps(history, indent=2) + "\n")
            (directory / "gate-summary.json").write_text(json.dumps(readiness, indent=2) + "\n")
            if not args.live_queries:
                dc("stop", "-t", "20", "vmagent", capture_output=True)
                time.sleep(15)
                fetch("http://127.0.0.1:19010/debug/pprof/heap?gc=1", directory / "quiescent-gc.heap.pb.gz", 30)
                capture(time.monotonic() - start, container)
            for future in futures:
                future.result()
            if args.queries:
                query_start = time.time()
                query_profiles = []
                if args.query_profile_seconds > 0:
                    for kind in ["profile", "allocs"]:
                        query_profiles.append(pool.submit(fetch,
                            f"http://127.0.0.1:19010/debug/pprof/{kind}?seconds={args.query_profile_seconds}",
                            directory / f"query-{kind}.pb.gz", args.query_profile_seconds + 20))
                queries = harness["dashboard"]()
                (directory / "queries.json").write_text(json.dumps(queries, indent=2) + "\n")
                with (directory / "requests.jsonl").open("w") as output:
                    for server, gate in readiness.items():
                        if gate["ready"]:
                            harness["benchmark"](server, args.nodes, end, queries, args, output)
                        else:
                            print(f"Skipping {server} dashboard: freshness gate failed", flush=True)
                if args.dashboard_loads:
                    outcomes = {}
                    for server, gate in readiness.items():
                        if gate["ready"]:
                            outcomes[server] = harness["dashboard_loads"](server, args.nodes, end, args, directory)
                    (directory / "dashboard-loads.json").write_text(json.dumps(outcomes, indent=2) + "\n")
                if args.otelbench:
                    outcomes = {}
                    for server, gate in readiness.items():
                        if gate["ready"]:
                            outcomes[server] = harness["otelbench_dashboard"](server, args.nodes, end, args, directory)
                    (directory / "otelbench.json").write_text(json.dumps(outcomes, indent=2) + "\n")
                (directory / "query-phase.json").write_text(json.dumps({
                    "start": query_start, "end": time.time(), "live_ingestion": args.live_queries,
                }, indent=2) + "\n")
                if args.live_queries:
                    capture(time.monotonic() - start, container)
                for future in query_profiles:
                    future.result()
                if args.soak_seconds:
                    dashboard_soak(args, directory,
                        [server for server, gate in readiness.items() if gate["ready"]],
                        lambda: capture(time.monotonic() - start, container))
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
