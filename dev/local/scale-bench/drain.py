#!/usr/bin/env python3
"""Drain a retained fixture before comparing reads without ongoing scraping."""

import argparse
import json
import os
from pathlib import Path
import runpy
import subprocess
import time

HERE = Path(__file__).resolve().parent
harness = runpy.run_path(str(HERE / "run.py"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--seconds", type=int, default=360)
    args = parser.parse_args()
    fixture = args.fixture.resolve()
    summary = json.loads((fixture / "summary.json").read_text())
    output = fixture / "drain"
    output.mkdir()
    scrape = fixture / "scrape.yml"
    (output / "original-scrape.yml").write_bytes(scrape.read_bytes())
    scrape.write_text("global:\n  scrape_interval: 15s\nscrape_configs: []\n")
    env = dict(os.environ, BENCH_RUN_DIR=str(fixture))
    compose = ["docker", "compose", "-p", f"odbscale-{fixture.parent.name.lower()}-{summary['nodes']}",
               "-f", str(HERE / "docker-compose.yml")]
    subprocess.run(compose + ["start", "vmagent"], env=env, check=True)
    start = time.monotonic()
    complete = False
    failure = None
    try:
        with (output / "progress.jsonl").open("w") as log:
            while time.monotonic() - start < args.seconds:
                elapsed = time.monotonic() - start
                metrics = harness["get"]("http://127.0.0.1:18429/metrics").decode()
                (output / f"{elapsed:06.1f}-vmagent.metrics").write_text(metrics)
                (output / f"{elapsed:06.1f}-oteldb.metrics").write_bytes(
                    harness["get"]("http://127.0.0.1:19464/metrics"))
                pending = sum(float(line.rsplit(" ", 1)[1]) for line in metrics.splitlines()
                              if line.startswith("vmagent_remotewrite_pending_data_bytes{"))
                gates = {server: harness["query"](server,
                         'min(count_over_time(node_uname_info{job="node_exporter"}[120s]))',
                         summary["query_end"], nocache=True) for server in harness["SERVERS"]}
                complete = pending == 0 and all(harness["number"](row) >= 6 for row in gates.values())
                row = {"elapsed": elapsed, "pending_bytes": pending, "history": gates, "complete": complete}
                log.write(json.dumps(row) + "\n")
                log.flush()
                print(f"{elapsed:.0f}s: pending={pending:.0f}, history="
                      f"{ {s: r.get('value', r.get('error')) for s, r in gates.items()} }", flush=True)
                if complete:
                    break
                time.sleep(10)
    except OSError as exc:
        failure = str(exc)
        print(f"Drain failed: {failure}", flush=True)
    finally:
        subprocess.run(compose + ["stop", "-t", "20", "vmagent"], env=env, check=True)
    (output / "summary.json").write_text(json.dumps({"complete": complete,
        "error": failure, "elapsed": time.monotonic() - start,
        "query_end": summary["query_end"]}, indent=2) + "\n")
    if not complete:
        raise SystemExit("fixture did not finish draining; do not score its queries")


if __name__ == "__main__":
    main()
