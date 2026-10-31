# Node-exporter scaling benchmark

Source: `b5666f1b06706e23a182bb5c679d02562de52393`. Each server: 8 vCPU, 16 GiB hard cap, 14 GiB Go soft limit, swap disabled. Local Docker volumes; oteldb native file backend.

Live node_exporter → nginx (2s cache) → vmagent (15s scrape) → both servers. Each scale uses fresh volumes. Random mode uses distinct seeded hosts and VictoriaMetrics `nocache=1`. Cached mode warms host-0 twice, then repeats identical requests. This does not flush disk/OS/column caches. VictoriaMetrics cacheTimestampOffset=15s; queries end at least 30s in the past.

Readiness requires all hosts and ≥6 samples/host over a 120s historical window after 180s ingest. Range requests cover 60s at 15s step, rate window 1m; 10 trials/query, 4 clients. Empty results and errors are excluded from latency statistics and counted separately. Short live runs measure initial scaling, not long-retention steady state.

| Nodes | Server | Gate | Mode | Requests | Errors | Empty | p50 ms | p95 ms |
|---:|---|---|---|---:|---:|---:|---:|---:|
| 10,000 | oteldb | FAIL | random | 0 | 0 | 0 | — | — |
| 10,000 | oteldb | FAIL | cached | 0 | 0 | 0 | — | — |
| 10,000 | victoriametrics | pass | random | 190 | 0 | 0 | 2.67 | 7.75 |
| 10,000 | victoriametrics | pass | cached | 190 | 0 | 0 | 1.02 | 1.92 |

## Coverage and failures

### 10,000 hosts

- oteldb: NOT ready; last host-count response=10000; no observed process exit.
- victoriametrics: ready; last host-count response=10000; no observed process exit.

## Per-query measurements

All times in milliseconds. Query indices match each scale's `queries.json`.

| Nodes | Server | Mode | Query | Panel | Nonempty OK | p50 | p95 |
|---:|---|---|---:|---|---:|---:|---:|
| 10,000 | victoriametrics | cached | 0 | CPU Busy | 10 | 2.05 | 2.82 |
| 10,000 | victoriametrics | cached | 1 | Sys Load (5m avg) | 10 | 1.41 | 2.19 |
| 10,000 | victoriametrics | cached | 2 | RAM Used | 10 | 0.96 | 1.48 |
| 10,000 | victoriametrics | cached | 3 | CPU Cores | 10 | 0.76 | 1.21 |
| 10,000 | victoriametrics | cached | 4 | Uptime | 10 | 1.09 | 2.13 |
| 10,000 | victoriametrics | cached | 5 | CPU Basic | 10 | 1.12 | 1.31 |
| 10,000 | victoriametrics | cached | 6 | CPU Basic | 10 | 1.06 | 1.30 |
| 10,000 | victoriametrics | cached | 7 | CPU Basic | 10 | 1.10 | 1.25 |
| 10,000 | victoriametrics | cached | 8 | CPU Basic | 10 | 1.07 | 1.43 |
| 10,000 | victoriametrics | cached | 9 | CPU Basic | 10 | 1.04 | 1.23 |
| 10,000 | victoriametrics | cached | 10 | CPU Basic | 10 | 1.05 | 1.25 |
| 10,000 | victoriametrics | cached | 11 | Memory Basic | 10 | 0.88 | 1.10 |
| 10,000 | victoriametrics | cached | 12 | Memory Basic | 10 | 0.99 | 1.40 |
| 10,000 | victoriametrics | cached | 13 | Memory Basic | 10 | 1.00 | 1.14 |
| 10,000 | victoriametrics | cached | 14 | Memory Basic | 10 | 0.99 | 1.25 |
| 10,000 | victoriametrics | cached | 15 | Memory Basic | 10 | 1.02 | 1.20 |
| 10,000 | victoriametrics | cached | 16 | Network Traffic Basic | 10 | 0.97 | 1.16 |
| 10,000 | victoriametrics | cached | 17 | Network Traffic Basic | 10 | 0.90 | 1.09 |
| 10,000 | victoriametrics | cached | 18 | Disk Space Used Basic | 10 | 0.92 | 1.08 |
| 10,000 | victoriametrics | random | 0 | CPU Busy | 10 | 7.24 | 11.91 |
| 10,000 | victoriametrics | random | 1 | Sys Load (5m avg) | 10 | 6.04 | 10.76 |
| 10,000 | victoriametrics | random | 2 | RAM Used | 10 | 3.84 | 8.54 |
| 10,000 | victoriametrics | random | 3 | CPU Cores | 10 | 2.06 | 6.44 |
| 10,000 | victoriametrics | random | 4 | Uptime | 10 | 3.86 | 6.05 |
| 10,000 | victoriametrics | random | 5 | CPU Basic | 10 | 2.19 | 4.14 |
| 10,000 | victoriametrics | random | 6 | CPU Basic | 10 | 2.12 | 4.18 |
| 10,000 | victoriametrics | random | 7 | CPU Basic | 10 | 2.05 | 3.15 |
| 10,000 | victoriametrics | random | 8 | CPU Basic | 10 | 2.37 | 3.38 |
| 10,000 | victoriametrics | random | 9 | CPU Basic | 10 | 2.15 | 7.40 |
| 10,000 | victoriametrics | random | 10 | CPU Basic | 10 | 1.81 | 2.85 |
| 10,000 | victoriametrics | random | 11 | Memory Basic | 10 | 1.28 | 1.92 |
| 10,000 | victoriametrics | random | 12 | Memory Basic | 10 | 6.14 | 9.71 |
| 10,000 | victoriametrics | random | 13 | Memory Basic | 10 | 4.97 | 7.79 |
| 10,000 | victoriametrics | random | 14 | Memory Basic | 10 | 2.14 | 4.19 |
| 10,000 | victoriametrics | random | 15 | Memory Basic | 10 | 3.72 | 4.75 |
| 10,000 | victoriametrics | random | 16 | Network Traffic Basic | 10 | 2.85 | 6.15 |
| 10,000 | victoriametrics | random | 17 | Network Traffic Basic | 10 | 2.93 | 4.22 |
| 10,000 | victoriametrics | random | 18 | Disk Space Used Basic | 10 | 4.30 | 9.88 |

Raw request timings, query text, selected hosts, empties, errors, resource samples, readiness checks, container limits/states, logs, and server metrics are in each scale's directory.
