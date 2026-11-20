#!/usr/bin/env python3
"""Profile reads on a retained, stopped-ingest scale-bench fixture."""

import argparse
import concurrent.futures
import http.client
import hashlib
import json
from pathlib import Path
import random
import runpy
import statistics
import threading
import time
import urllib.parse
import urllib.request

HERE = Path(__file__).resolve().parent
harness = runpy.run_path(str(HERE / "run.py"))
PORTS = {"oteldb": 19090, "victoriametrics": 18428}


def request(conn, expr, end, instant, nocache):
    params = {"query": expr, "timeout": "10s"}
    if instant:
        params["time"] = end
    else:
        params.update(start=end - 60, end=end, step=15)
    if nocache:
        params["nocache"] = 1
    path = ("/api/v1/query" if instant else "/api/v1/query_range")
    start = time.perf_counter()
    conn.request("GET", path + "?" + urllib.parse.urlencode(params))
    response = conn.getresponse()
    data = json.loads(response.read())
    elapsed = (time.perf_counter() - start) * 1000
    if response.status != 200 or data.get("status") != "success" or not data["data"]["result"]:
        raise ValueError(str(data)[:500])
    result = sorted(data["data"]["result"], key=lambda item: json.dumps(item["metric"], sort_keys=True))
    digest = hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()
    return elapsed, len(result), digest


def profile(server, queries, end, output, seconds, nodes):
    stop = threading.Event()
    counts = []

    def load(worker):
        rng = random.Random(100 + worker)
        conn = http.client.HTTPConnection("127.0.0.1", PORTS[server], timeout=15)
        n = 0
        try:
            while not stop.is_set():
                item = queries[n % len(queries)]
                expr = harness["interpolate"](item["expr"], f"host-{rng.randrange(nodes)}")
                request(conn, expr, end, item["instant"], server == "victoriametrics")
                n += 1
        finally:
            conn.close()
            counts.append(n)

    profiler = 19010 if server == "oteldb" else PORTS[server]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(load, i) for i in range(4)]
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{profiler}/debug/pprof/profile?seconds={seconds}",
                                        timeout=seconds + 20) as response:
                (output / f"read-{server}.cpu.pb.gz").write_bytes(response.read())
        finally:
            stop.set()
        for future in futures:
            future.result()
    with urllib.request.urlopen(f"http://127.0.0.1:{profiler}/debug/pprof/heap", timeout=15) as response:
        (output / f"read-{server}.heap.pb.gz").write_bytes(response.read())
    print(f"{server} profiled {sum(counts)} requests over {seconds}s", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument("--control-port", type=int)
    parser.add_argument("--profile-seconds", type=int, default=25)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--profile-servers", nargs="*", default=list(PORTS), choices=list(PORTS))
    args = parser.parse_args()
    if args.control_port:
        PORTS["oteldb-before"] = args.control_port
    summary = json.loads((args.fixture / "summary.json").read_text())
    end = summary["query_end"]
    output = args.output or args.fixture / "diagnosis"
    output.mkdir(exist_ok=True)
    queries = harness["dashboard"]()
    conns = {s: http.client.HTTPConnection("127.0.0.1", p, timeout=15) for s, p in PORTS.items()}
    groups = {}
    rng = random.Random(20261027)
    hosts = rng.sample(range(1, summary["nodes"]), args.runs)
    with (output / "latencies.jsonl").open("w") as stream:
        for mode in ["random", "cached", "repeated-no-response-cache"]:
            for index, item in enumerate(queries):
                for trial in range(-5, args.runs):
                    host = f"host-{hosts[trial]}" if mode == "random" and trial >= 0 else "host-0"
                    expr = harness["interpolate"](item["expr"], host)
                    order = list(PORTS)
                    rng.shuffle(order)
                    for server in order:
                        elapsed, n, digest = request(conns[server], expr, end, item["instant"],
                                             server == "victoriametrics" and mode != "cached")
                        row = {"server": server, "query_index": index, "panel": item["panel"],
                               "mode": mode, "trial": trial, "ms": elapsed, "series": n,
                               "host": host, "query": expr, "end": end, "result_sha256": digest}
                        stream.write(json.dumps(row) + "\n")
                        if trial >= 0:
                            groups.setdefault((server, mode, index), []).append(elapsed)
            stream.flush()
            values = [v for (s, m, _), vs in groups.items() if m == mode and s == "oteldb" for v in vs]
            print(f"idle ingest, keepalive, {mode}: oteldb pooled p50={statistics.median(values):.3f}ms", flush=True)
    for conn in conns.values():
        conn.close()
    table = []
    for (server, mode, index), values in groups.items():
        values.sort()
        table.append({"server": server, "mode": mode, "query_index": index,
                      "panel": queries[index]["panel"], "p50_ms": statistics.median(values),
                      "p95_ms": values[int(len(values)*.95)], "runs": len(values)})
    (output / "latency-summary.json").write_text(json.dumps(table, indent=2) + "\n")
    for server in args.profile_servers:
        profile(server, queries, end, output, args.profile_seconds, summary["nodes"])


if __name__ == "__main__":
    main()
