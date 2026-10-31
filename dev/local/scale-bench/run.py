#!/usr/bin/env python3
"""Compare native file oteldb and VictoriaMetrics with live node-exporter fan-out."""

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
from pathlib import Path
import random
import statistics
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
SERVERS = {"oteldb": "http://127.0.0.1:19090", "victoriametrics": "http://127.0.0.1:18428"}
PANELS = {"CPU Busy", "Sys Load (5m avg)", "RAM Used", "CPU Cores", "Uptime",
          "CPU Basic", "Memory Basic", "Network Traffic Basic", "Disk Space Used Basic"}


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def command(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def get(url, timeout=15):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read()


def query(server, expr, end, instant=True, nocache=False, timeout=12):
    params = {"query": expr, "timeout": "10s"}
    if instant:
        params["time"] = end
    else:
        params.update(start=end - 60, end=end, step=15)
    if nocache:
        params["nocache"] = 1
    url = SERVERS[server] + ("/api/v1/query" if instant else "/api/v1/query_range")
    start = time.perf_counter()
    row = {"server": server, "query": expr, "instant": instant, "end": end, "nocache": nocache}
    try:
        data = json.loads(get(url + "?" + urllib.parse.urlencode(params), timeout))
        if data.get("status") != "success":
            raise ValueError(str(data)[:1000])
        results = data.get("data", {}).get("result", [])
        row.update(ok=True, series=len(results), empty=not results,
                   points=sum(len(r.get("values", [])) or int("value" in r) for r in results),
                   warnings=data.get("warnings", []))
        if instant and len(results) == 1:
            row["value"] = results[0].get("value", [None, None])[1]
        row["result_sha256"] = hashlib.sha256(json.dumps(results, sort_keys=True).encode()).hexdigest()
    except (OSError, ValueError) as exc:
        detail = exc.read().decode(errors="replace")[:1000] if isinstance(exc, urllib.error.HTTPError) else str(exc)
        row.update(ok=False, error=detail)
    row["ms"] = (time.perf_counter() - start) * 1000
    return row


def number(row):
    try:
        return float(row.get("value", "nan"))
    except (ValueError, TypeError):
        return math.nan


def dashboard(all_panels=False):
    source = ROOT / "dev/local/ch-bench-read/grafana/dashboards/node-exporter.json"
    result = []

    def visit(panels):
        for panel in panels:
            visit(panel.get("panels", []))
            if not all_panels and panel.get("title") not in PANELS:
                continue
            for target in panel.get("targets", []):
                expr = target.get("expr")
                if not expr or target.get("hide"):
                    continue
                result.append({"panel": panel["title"], "expr": expr,
                               "instant": bool(target.get("instant"))})

    visit(json.loads(source.read_text())["panels"])
    return result


def interpolate(expr, host):
    for name, value in {"__rate_interval": "1m", "node": host, "job": "node_exporter",
                        "diskdevices": "[a-z]+|nvme[0-9]+n[0-9]+|mmcblk[0-9]+"}.items():
        expr = expr.replace("$" + name, value)
    if "$" in expr:
        raise ValueError("unexpanded dashboard variable: " + expr)
    return expr


def benchmark(server, nodes, end, queries, args, output):
    rng = random.Random(args.seed + nodes)
    hosts = [f"host-{n}" for n in rng.sample(range(1, nodes), args.runs)]
    rows = []
    deadline = time.monotonic() + args.query_budget
    for mode in ["random", "cached"]:
        jobs = []
        for trial in range(args.runs):
            host = hosts[trial] if mode == "random" else "host-0"
            for index, item in enumerate(queries):
                jobs.append((trial, host, index, item))

        def execute(job, warmup=False):
            trial, host, index, item = job
            row = query(server, interpolate(item["expr"], host), end,
                        item["instant"], mode == "random" and server == "victoriametrics")
            row.update(mode=mode, trial=trial, host=host, panel=item["panel"],
                       query_index=index, warmup=warmup)
            return row

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
            if mode == "cached":
                for _ in range(2):
                    if time.monotonic() >= deadline:
                        break
                    for row in pool.map(lambda job: execute(job, True), jobs[:len(queries)]):
                        rows.append(row)
                        output.write(json.dumps(row) + "\n")
                    output.flush()
            # Bound queued work so a failed server does not leave minutes of pending timeouts.
            for offset in range(0, len(jobs), args.jobs):
                if time.monotonic() >= deadline:
                    break
                for row in pool.map(execute, jobs[offset:offset + args.jobs]):
                    rows.append(row)
                    output.write(json.dumps(row) + "\n")
                output.flush()
        measured = [r for r in rows if r["mode"] == mode and not r["warmup"]]
        print(f"  {server} {mode}: {len(measured)}/{len(jobs)} requests, "
              f"{sum(not r['ok'] for r in measured)} errors, "
              f"{sum(r.get('empty', False) for r in measured)} empty", flush=True)
    return rows


def dashboard_expression(item, host):
    expr = item["expr"].replace("group_left(chip_name) node_hwmon_chip_names",
        'group_left(chip_name) node_hwmon_chip_names{instance="$node",job="$job"}')
    return interpolate(expr, host)


def load_dashboard(server, host, end, mode, trial, warmup, queries, pool, args, deadline):
    rows = []

    def execute(index):
        item = queries[index]
        row = query(server, dashboard_expression(item, host), end, item["instant"],
                    mode == "random" and server == "victoriametrics")
        row.update(kind="query", mode=mode, trial=trial, host=host,
                   query_index=index, warmup=warmup)
        return row

    started = time.perf_counter()
    for offset in range(0, len(queries), args.jobs):
        if time.monotonic() >= deadline:
            break
        rows.extend(pool.map(execute, range(offset, min(offset + args.jobs, len(queries)))))
    load = {"kind": "load", "server": server, "mode": mode, "host": host,
            "trial": trial, "warmup": warmup,
            "wall_ms": (time.perf_counter() - started) * 1000,
            "queries": len(rows), "expected_queries": len(queries),
            "complete": len(rows) == len(queries),
            "errors": sum(not row["ok"] for row in rows),
            "empty": sum(row.get("empty", False) for row in rows),
            "max_query_ms": max((row["ms"] for row in rows), default=None)}
    return rows, load


def dashboard_loads(server, nodes, end, args, target):
    """Measure a complete dashboard's wall time with bounded concurrent requests."""
    queries = dashboard(True)
    rng = random.Random(args.seed + nodes)
    hosts = [f"host-{n}" for n in rng.sample(range(1, nodes), args.runs)]
    deadline = time.monotonic() + args.query_budget
    measured = []
    with (target / f"dashboard-loads-{server}.jsonl").open("w") as output:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
            for mode in ["random", "cached"]:
                trials = range(args.runs) if mode == "random" else range(-2, args.runs)
                for trial in trials:
                    if time.monotonic() >= deadline:
                        break
                    host = hosts[trial] if mode == "random" else "host-0"
                    rows, load = load_dashboard(server, host, end, mode, trial, trial < 0,
                                                queries, pool, args, deadline)
                    for row in rows:
                        output.write(json.dumps(row) + "\n")
                    output.write(json.dumps(load) + "\n")
                    output.flush()
                    if trial >= 0:
                        measured.append(load)
                        print(f"  {server} full dashboard {mode}/{trial}: "
                              f"{load['queries']}/{len(queries)} queries, {load['wall_ms']:.1f}ms", flush=True)
    return measured


def otelbench_dashboard(server, nodes, end, args, target):
    """Flatten collapsed rows for otelbench, which reads only top-level panels."""
    binary = HERE / "bin/otelbench"
    if not binary.exists():
        raise FileNotFoundError("build otelbench with build.sh before using --otelbench")
    queries = dashboard(True)
    rng = random.Random(args.seed + nodes)
    hosts = [f"host-{n}" for n in rng.sample(range(1, nodes), args.runs)]
    outcomes = {}
    for mode in ["random", "cached"]:
        panels = []
        for trial in range(args.runs if mode == "random" else 1):
            host = hosts[trial] if mode == "random" else "host-0"
            for item in queries:
                # The dashboard's bare sensor-name join spans every cloned host otherwise.
                panels.append({"title": f"{item['panel']} / {host}", "targets": [{
                    "datasource": {"type": "prometheus"}, "expr": dashboard_expression(item, host)}]})
        source = target / f"otelbench-{mode}.json"
        save(source, {"panels": panels})
        # dashboard bench applies its global time range to every target, including stat panels.
        # It has no nocache flag; random-host query strings differ on every trial.
        cmd = [str(binary), "dashboard", "bench", "--input", str(source),
               "--output", str(target / f"otelbench-{server}-{mode}.yml"),
               "--prom-addr", SERVERS[server], "--start", str(end - 60), "--end", str(end),
               "--step", "15s", "--count", str(args.runs if mode == "cached" else 1),
               "--warmup", "2" if mode == "cached" else "0"]
        with (target / f"otelbench-{server}-{mode}.log").open("w") as output:
            try:
                result = subprocess.run(cmd, text=True, stdout=output, stderr=output,
                                        timeout=args.query_budget, check=False)
                outcomes[mode] = {"exit_code": result.returncode, "panels": len(queries)}
            except subprocess.TimeoutExpired:
                outcomes[mode] = {"timeout": True, "panels": len(queries)}
        print(f"  otelbench full dashboard {server} {mode}: {outcomes[mode]}", flush=True)
    return outcomes


def stage(nodes, args, run_dir):
    target = run_dir / str(nodes)
    target.mkdir()
    scrape = ["global:", "  scrape_interval: 15s", "  scrape_timeout: 10s", "scrape_configs:",
              "  - job_name: node_exporter", "    static_configs:"]
    scrape.extend(f"      - targets: ['node-cache:9100']\n        labels: {{instance: 'host-{i}'}}"
                  for i in range(nodes))
    (target / "scrape.yml").write_text("\n".join(scrape) + "\n")
    env = dict(os.environ, BENCH_RUN_DIR=str(target))
    compose = ["docker", "compose", "-p", f"odbscale-{run_dir.name.lower()}-{nodes}",
               "-f", str(HERE / "docker-compose.yml")]

    def dc(*cmd, **kwargs):
        return command(compose + list(cmd), env=env, **kwargs)

    summary = {"nodes": nodes, "gate": {}, "rows": [], "started": time.time()}
    stop = threading.Event()

    def monitor():
        with (target / "resources.jsonl").open("w") as stream:
            while not stop.is_set():
                try:
                    ids = dc("ps", "-aq", capture_output=True).stdout.split()
                    if ids:
                        inspected = command(["docker", "inspect", *ids], capture_output=True)
                        for item in json.loads(inspected.stdout):
                            stream.write(json.dumps({"time": time.time(), "name": item["Name"],
                                "state": item["State"], "limits": {k: item["HostConfig"][k]
                                for k in ["Memory", "MemorySwap", "NanoCpus"]}}) + "\n")
                        stats = command(["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids],
                                        capture_output=True)
                        for line in stats.stdout.splitlines():
                            stream.write(json.dumps({"time": time.time(), "stats": json.loads(line)}) + "\n")
                        stream.flush()
                    stamp = str(time.time_ns())
                    for name, url in {"oteldb": "http://127.0.0.1:19464/metrics",
                                      "vmagent": "http://127.0.0.1:18429/metrics"}.items():
                        try:
                            (target / f"{stamp}-{name}.metrics").write_bytes(get(url, timeout=5))
                        except OSError:
                            pass
                except subprocess.CalledProcessError:
                    pass
                stop.wait(10)

    thread = threading.Thread(target=monitor, daemon=True)
    try:
        with (target / "compose.log").open("w") as log:
            dc("up", "-d", stdout=log, stderr=log)
        thread.start()
        begin = time.monotonic()
        ready = set()
        failed = set()
        gate_expr = 'count(node_uname_info{job="node_exporter"})'
        with (target / "gates.jsonl").open("w") as output:
            while time.monotonic() - begin < args.ingest_budget:
                elapsed = time.monotonic() - begin
                for server in SERVERS:
                    if server in failed:
                        continue
                    row = query(server, gate_expr, time.time(), nocache=True)
                    row.update(elapsed=elapsed, check="hosts")
                    output.write(json.dumps(row) + "\n")
                    summary["gate"][server] = row
                    if elapsed >= args.prewarm and number(row) == nodes:
                        history = query(server, 'min(count_over_time(node_uname_info{job="node_exporter"}[120s]))',
                                        time.time() - 30, nocache=True)
                        history.update(elapsed=elapsed, check="history")
                        output.write(json.dumps(history) + "\n")
                        summary["gate"][server]["history_min_samples"] = history.get("value")
                        if number(history) >= 6:
                            ready.add(server)
                    print(f"  {nodes} nodes / {elapsed:.0f}s: {server} hosts={row.get('value', row.get('error'))}", flush=True)
                output.flush()
                ids = dc("ps", "-aq", capture_output=True).stdout.split()
                if ids:
                    for item in json.loads(command(["docker", "inspect", *ids], capture_output=True).stdout):
                        service = item["Config"]["Labels"]["com.docker.compose.service"]
                        if item["State"]["Status"] == "exited":
                            summary.setdefault("exited", {})[service] = item["State"]
                            if service in SERVERS:
                                failed.add(service)
                                ready.discard(service)
                            elif service in ["vmagent", "node-cache", "node-exporter"]:
                                failed.update(SERVERS)
                                ready.clear()
                if len(ready | failed) == len(SERVERS):
                    break
                time.sleep(15)
        summary["ready"] = sorted(ready)
        end = int((time.time() - 30) // 15 * 15)
        summary["query_end"] = end
        queries = dashboard(args.all_panels)
        save(target / "queries.json", queries)
        save(target / "gate-summary.json", summary)
        try:
            (target / "vm-before.metrics").write_bytes(get(SERVERS["victoriametrics"] + "/metrics"))
        except OSError:
            pass
        with (target / "requests.jsonl").open("w") as output:
            for server in SERVERS:
                if server in ready:
                    summary["rows"].extend(benchmark(server, nodes, end, queries, args, output))
        if args.otelbench:
            for server in SERVERS:
                if server in ready:
                    summary.setdefault("otelbench", {})[server] = otelbench_dashboard(server, nodes, end, args, target)
        for name, url in {"oteldb": "http://127.0.0.1:19464/metrics",
                          "vmagent": "http://127.0.0.1:18429/metrics",
                          "victoriametrics": "http://127.0.0.1:18428/metrics"}.items():
            try:
                (target / f"{name}.metrics").write_bytes(get(url))
            except OSError:
                pass
        # Freeze ingest before checking physical storage and clean-restart persistence.
        dc("stop", "-t", "20", "vmagent", capture_output=True)
        for server in SERVERS:
            if server not in ready:
                continue
            usage = dc("exec", "-T", server, "du", "-sk", "/data", capture_output=True)
            (target / f"{server}-disk.txt").write_text(usage.stdout)
        if nodes == args.nodes[0] and "oteldb" in ready:
            dc("restart", "-t", "30", "oteldb", capture_output=True)
            for _ in range(30):
                proof = query("oteldb", gate_expr, end)
                if number(proof) == nodes:
                    break
                time.sleep(1)
            summary["restart_proof"] = proof
    finally:
        stop.set()
        if thread.is_alive():
            thread.join(timeout=20)
        with (target / "containers.log").open("w") as log:
            dc("logs", "--no-color", "--tail", "200", stdout=log, stderr=log)
        save(target / "summary.json", summary)
        if not args.keep:
            dc("down", "-v", "--remove-orphans", capture_output=True)
    return summary


def report(summaries, directory, args):
    lines = ["# Node-exporter scaling benchmark", "", f"Source: `{args.revision}`. Each server: 8 vCPU, 16 GiB hard cap, "
             "14 GiB Go soft limit, swap disabled. Local Docker volumes; oteldb native file backend.", "",
             "Live node_exporter → nginx (2s cache) → vmagent (15s scrape) → both servers. "
             "Each scale uses fresh volumes. Random mode uses distinct seeded hosts and VictoriaMetrics `nocache=1`. "
             "Cached mode warms host-0 twice, then repeats identical requests. This does not flush disk/OS/column caches. "
             "VictoriaMetrics cacheTimestampOffset=15s; queries end at least 30s in the past.", "",
             f"Readiness requires all hosts and ≥6 samples/host over a 120s historical window after {args.prewarm}s ingest. "
             f"Range requests cover 60s at 15s step, rate window 1m; {args.runs} trials/query, {args.jobs} clients. "
             "Empty results and errors are excluded from latency statistics and counted separately. "
             "Short live runs measure initial scaling, not long-retention steady state.", "",
             "| Nodes | Server | Gate | Mode | Requests | Errors | Empty | p50 ms | p95 ms |", "|---:|---|---|---|---:|---:|---:|---:|---:|"]
    for summary in summaries:
        for server in SERVERS:
            for mode in ["random", "cached"]:
                rows = [r for r in summary["rows"] if r["server"] == server and r["mode"] == mode and not r["warmup"]]
                ms = sorted(r["ms"] for r in rows if r["ok"] and not r["empty"])
                gate = "pass" if server in summary.get("ready", []) else "FAIL"
                p50 = f"{statistics.median(ms):.2f}" if ms else "—"
                p95 = f"{ms[math.ceil(len(ms)*.95)-1]:.2f}" if ms else "—"
                lines.append(f"| {summary['nodes']:,} | {server} | {gate} | {mode} | {len(rows)} | "
                             f"{sum(not r['ok'] for r in rows)} | {sum(r.get('empty', False) for r in rows)} | {p50} | {p95} |")
    lines.extend(["", "## Coverage and failures", ""])
    for summary in summaries:
        lines.append(f"### {summary['nodes']:,} hosts")
        lines.append("")
        for server in SERVERS:
            gate = summary.get("gate", {}).get(server, {})
            state = summary.get("exited", {}).get(server)
            detail = (f"OOMKilled={state['OOMKilled']}, exit={state['ExitCode']}, "
                      f"started={state['StartedAt']}, stopped={state['FinishedAt']}") if state else "no observed process exit"
            lines.append(f"- {server}: {'ready' if server in summary.get('ready', []) else 'NOT ready'}; "
                         f"last host-count response={gate.get('value', gate.get('error', 'none'))}; {detail}.")
        if "restart_proof" in summary:
            proof = summary["restart_proof"]
            lines.append(f"- Oteldb clean restart: observed {proof.get('value', proof.get('error'))} hosts.")
        lines.append("")
    lines.extend(["## Per-query measurements", "",
                  "All times in milliseconds. Query indices match each scale's `queries.json`.", "",
                  "| Nodes | Server | Mode | Query | Panel | Nonempty OK | p50 | p95 |",
                  "|---:|---|---|---:|---|---:|---:|---:|"])
    for summary in summaries:
        groups = {}
        for row in summary["rows"]:
            if row["warmup"]:
                continue
            key = (row["server"], row["mode"], row["query_index"], row["panel"])
            groups.setdefault(key, [])
            if row["ok"] and not row["empty"]:
                groups[key].append(row["ms"])
        for (server, mode, index, panel), timings in sorted(groups.items()):
            timings.sort()
            p50 = f"{statistics.median(timings):.2f}" if timings else "—"
            p95 = f"{timings[math.ceil(len(timings)*.95)-1]:.2f}" if timings else "—"
            lines.append(f"| {summary['nodes']:,} | {server} | {mode} | {index} | {panel} | "
                         f"{len(timings)} | {p50} | {p95} |")
    lines.extend(["", "Raw request timings, query text, selected hosts, empties, errors, resource samples, "
                  "readiness checks, container limits/states, logs, and server metrics are in each scale's directory.", ""])
    (directory / "REPORT.md").write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, nargs="+", default=[1000, 10000, 25000])
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--prewarm", type=int, default=180)
    parser.add_argument("--ingest-budget", type=int, default=360)
    parser.add_argument("--query-budget", type=int, default=240)
    parser.add_argument("--seed", type=int, default=20261027)
    parser.add_argument("--all-panels", action="store_true")
    parser.add_argument("--otelbench", action="store_true", help="also run all 242 queries through otelbench dashboard bench")
    parser.add_argument("--keep", action="store_true", help="retain stopped ingest, server containers, and data for profiling")
    args = parser.parse_args()
    if min(args.nodes) <= args.runs or min(args.nodes) < 2 or args.runs < 1 or args.jobs < 1:
        parser.error("nodes must exceed positive runs; jobs must be positive")
    args.revision = command(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True).stdout.strip()
    directory = HERE / "results" / time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    directory.mkdir(parents=True)
    save(directory / "config.json", vars(args))
    command(["docker", "image", "inspect", os.environ.get("OTELDB_BENCH_IMAGE", "local/oteldb:scale-bench"), "victoriametrics/victoria-metrics:v1.141.0",
             "victoriametrics/vmagent:v1.141.0", "nginx:1.27-alpine", "prom/node-exporter:v1.8.2"],
            stdout=(directory / "images.json").open("w"))
    summaries = []
    print(f"Results: {directory}", flush=True)
    for nodes in args.nodes:
        print(f"Starting {nodes} hosts", flush=True)
        summaries.append(stage(nodes, args, directory))
        report(summaries, directory, args)
    print(f"Report: {directory / 'REPORT.md'}", flush=True)


if __name__ == "__main__":
    main()
