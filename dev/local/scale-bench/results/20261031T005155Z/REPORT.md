# Node-exporter scaling benchmark

Source: `b5666f1b06706e23a182bb5c679d02562de52393`. Each server: 8 vCPU, 16 GiB hard cap, 14 GiB Go soft limit, swap disabled. Local Docker volumes; oteldb native file backend.

Live node_exporter → nginx (2s cache) → vmagent (15s scrape) → both servers. Each scale uses fresh volumes. Random mode uses distinct seeded hosts and VictoriaMetrics `nocache=1`. Cached mode warms host-0 twice, then repeats identical requests. This does not flush disk/OS/column caches. VictoriaMetrics cacheTimestampOffset=15s; queries end at least 30s in the past.

Readiness requires all hosts and ≥6 samples/host over a 120s historical window after 180s ingest. Range requests cover 60s at 15s step, rate window 1m; 10 trials/query, 4 clients. Empty results and errors are excluded from latency statistics and counted separately. Short live runs measure initial scaling, not long-retention steady state.

| Nodes | Server | Gate | Mode | Requests | Errors | Empty | p50 ms | p95 ms |
|---:|---|---|---|---:|---:|---:|---:|---:|
| 25,000 | oteldb | FAIL | random | 0 | 0 | 0 | — | — |
| 25,000 | oteldb | FAIL | cached | 0 | 0 | 0 | — | — |
| 25,000 | victoriametrics | FAIL | random | 0 | 0 | 0 | — | — |
| 25,000 | victoriametrics | FAIL | cached | 0 | 0 | 0 | — | — |

## Coverage and failures

### 25,000 hosts

- oteldb: NOT ready; last host-count response=<urlopen error [Errno 111] Connection refused>; OOMKilled=True, exit=137, started=2026-10-31T00:51:55.988831992Z, stopped=2026-10-31T00:54:45.643989346Z.
- victoriametrics: NOT ready; last host-count response=25000; no observed process exit.

## Per-query measurements

All times in milliseconds. Query indices match each scale's `queries.json`.

| Nodes | Server | Mode | Query | Panel | Nonempty OK | p50 | p95 |
|---:|---|---|---:|---|---:|---:|---:|

Raw request timings, query text, selected hosts, empties, errors, resource samples, readiness checks, container limits/states, logs, and server metrics are in each scale's directory.
