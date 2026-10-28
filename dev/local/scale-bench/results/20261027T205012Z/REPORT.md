# Node-exporter scaling benchmark

Source: `e5de6b841f45e167ec484be59c86cfecdfbbd7ef`. Each server: 8 vCPU, 16 GiB hard cap, 14 GiB Go soft limit, swap disabled. Local Docker volumes; oteldb native file backend.

Live node_exporter → nginx (2s cache) → vmagent (15s scrape) → both servers. Each scale uses fresh volumes. Random mode uses distinct seeded hosts and VictoriaMetrics `nocache=1`. Cached mode warms host-0 twice, then repeats identical requests. This does not flush disk/OS/column caches. VictoriaMetrics cacheTimestampOffset=15s; queries end at least 30s in the past.

Readiness requires all hosts and ≥6 samples/host over a 120s historical window after 180s ingest. Range requests cover 60s at 15s step, rate window 1m; 10 trials/query, 4 clients. Empty results and errors are excluded from latency statistics and counted separately. Short live runs measure initial scaling, not long-retention steady state.

| Nodes | Server | Gate | Mode | Requests | Errors | Empty | p50 ms | p95 ms |
|---:|---|---|---|---:|---:|---:|---:|---:|
| 1,000 | oteldb | pass | random | 190 | 0 | 0 | 2.03 | 21.83 |
| 1,000 | oteldb | pass | cached | 190 | 0 | 0 | 1.29 | 3.39 |
| 1,000 | victoriametrics | pass | random | 190 | 0 | 0 | 1.48 | 2.75 |
| 1,000 | victoriametrics | pass | cached | 190 | 0 | 0 | 1.00 | 1.43 |

## Coverage and failures

### 1,000 hosts

- oteldb: ready; last host-count response=1000; no observed process exit.
- victoriametrics: ready; last host-count response=1000; no observed process exit.
- Oteldb clean restart: observed 1000 hosts.

## Per-query measurements

All times in milliseconds. Query indices match each scale's `queries.json`.

| Nodes | Server | Mode | Query | Panel | Nonempty OK | p50 | p95 |
|---:|---|---|---:|---|---:|---:|---:|
| 1,000 | oteldb | cached | 0 | CPU Busy | 10 | 2.29 | 3.60 |
| 1,000 | oteldb | cached | 1 | Sys Load (5m avg) | 10 | 1.93 | 3.43 |
| 1,000 | oteldb | cached | 2 | RAM Used | 10 | 0.75 | 2.11 |
| 1,000 | oteldb | cached | 3 | CPU Cores | 10 | 1.47 | 2.02 |
| 1,000 | oteldb | cached | 4 | Uptime | 10 | 1.01 | 9.18 |
| 1,000 | oteldb | cached | 5 | CPU Basic | 10 | 2.24 | 8.99 |
| 1,000 | oteldb | cached | 6 | CPU Basic | 10 | 2.44 | 8.88 |
| 1,000 | oteldb | cached | 7 | CPU Basic | 10 | 2.58 | 8.49 |
| 1,000 | oteldb | cached | 8 | CPU Basic | 10 | 2.68 | 8.38 |
| 1,000 | oteldb | cached | 9 | CPU Basic | 10 | 2.63 | 8.72 |
| 1,000 | oteldb | cached | 10 | CPU Basic | 10 | 2.35 | 2.93 |
| 1,000 | oteldb | cached | 11 | Memory Basic | 10 | 0.84 | 1.07 |
| 1,000 | oteldb | cached | 12 | Memory Basic | 10 | 1.13 | 1.30 |
| 1,000 | oteldb | cached | 13 | Memory Basic | 10 | 1.08 | 1.25 |
| 1,000 | oteldb | cached | 14 | Memory Basic | 10 | 0.92 | 1.25 |
| 1,000 | oteldb | cached | 15 | Memory Basic | 10 | 1.06 | 1.27 |
| 1,000 | oteldb | cached | 16 | Network Traffic Basic | 10 | 1.01 | 1.25 |
| 1,000 | oteldb | cached | 17 | Network Traffic Basic | 10 | 0.89 | 1.13 |
| 1,000 | oteldb | cached | 18 | Disk Space Used Basic | 10 | 1.10 | 1.18 |
| 1,000 | oteldb | random | 0 | CPU Busy | 10 | 21.75 | 25.03 |
| 1,000 | oteldb | random | 1 | Sys Load (5m avg) | 10 | 20.11 | 25.07 |
| 1,000 | oteldb | random | 2 | RAM Used | 10 | 2.06 | 3.18 |
| 1,000 | oteldb | random | 3 | CPU Cores | 10 | 2.13 | 22.91 |
| 1,000 | oteldb | random | 4 | Uptime | 10 | 1.52 | 2.65 |
| 1,000 | oteldb | random | 5 | CPU Basic | 10 | 2.57 | 3.35 |
| 1,000 | oteldb | random | 6 | CPU Basic | 10 | 2.57 | 3.14 |
| 1,000 | oteldb | random | 7 | CPU Basic | 10 | 2.73 | 3.04 |
| 1,000 | oteldb | random | 8 | CPU Basic | 10 | 2.65 | 3.16 |
| 1,000 | oteldb | random | 9 | CPU Basic | 10 | 2.62 | 2.99 |
| 1,000 | oteldb | random | 10 | CPU Basic | 10 | 2.35 | 2.58 |
| 1,000 | oteldb | random | 11 | Memory Basic | 10 | 0.88 | 1.07 |
| 1,000 | oteldb | random | 12 | Memory Basic | 10 | 1.63 | 1.87 |
| 1,000 | oteldb | random | 13 | Memory Basic | 10 | 1.53 | 1.69 |
| 1,000 | oteldb | random | 14 | Memory Basic | 10 | 1.07 | 1.40 |
| 1,000 | oteldb | random | 15 | Memory Basic | 10 | 1.34 | 1.87 |
| 1,000 | oteldb | random | 16 | Network Traffic Basic | 10 | 1.31 | 1.72 |
| 1,000 | oteldb | random | 17 | Network Traffic Basic | 10 | 1.47 | 5.79 |
| 1,000 | oteldb | random | 18 | Disk Space Used Basic | 10 | 1.88 | 5.60 |
| 1,000 | victoriametrics | cached | 0 | CPU Busy | 10 | 1.40 | 1.88 |
| 1,000 | victoriametrics | cached | 1 | Sys Load (5m avg) | 10 | 1.19 | 1.35 |
| 1,000 | victoriametrics | cached | 2 | RAM Used | 10 | 0.88 | 1.12 |
| 1,000 | victoriametrics | cached | 3 | CPU Cores | 10 | 0.95 | 1.21 |
| 1,000 | victoriametrics | cached | 4 | Uptime | 10 | 1.07 | 1.47 |
| 1,000 | victoriametrics | cached | 5 | CPU Basic | 10 | 1.11 | 1.42 |
| 1,000 | victoriametrics | cached | 6 | CPU Basic | 10 | 0.89 | 1.54 |
| 1,000 | victoriametrics | cached | 7 | CPU Basic | 10 | 0.93 | 1.78 |
| 1,000 | victoriametrics | cached | 8 | CPU Basic | 10 | 1.01 | 1.31 |
| 1,000 | victoriametrics | cached | 9 | CPU Basic | 10 | 0.93 | 1.61 |
| 1,000 | victoriametrics | cached | 10 | CPU Basic | 10 | 0.98 | 1.35 |
| 1,000 | victoriametrics | cached | 11 | Memory Basic | 10 | 0.97 | 1.30 |
| 1,000 | victoriametrics | cached | 12 | Memory Basic | 10 | 1.02 | 1.32 |
| 1,000 | victoriametrics | cached | 13 | Memory Basic | 10 | 1.11 | 1.33 |
| 1,000 | victoriametrics | cached | 14 | Memory Basic | 10 | 0.98 | 1.44 |
| 1,000 | victoriametrics | cached | 15 | Memory Basic | 10 | 0.96 | 1.26 |
| 1,000 | victoriametrics | cached | 16 | Network Traffic Basic | 10 | 0.89 | 1.20 |
| 1,000 | victoriametrics | cached | 17 | Network Traffic Basic | 10 | 0.77 | 1.15 |
| 1,000 | victoriametrics | cached | 18 | Disk Space Used Basic | 10 | 0.99 | 1.35 |
| 1,000 | victoriametrics | random | 0 | CPU Busy | 10 | 2.57 | 3.64 |
| 1,000 | victoriametrics | random | 1 | Sys Load (5m avg) | 10 | 1.93 | 3.44 |
| 1,000 | victoriametrics | random | 2 | RAM Used | 10 | 1.49 | 2.36 |
| 1,000 | victoriametrics | random | 3 | CPU Cores | 10 | 1.63 | 3.12 |
| 1,000 | victoriametrics | random | 4 | Uptime | 10 | 1.57 | 2.01 |
| 1,000 | victoriametrics | random | 5 | CPU Basic | 10 | 1.75 | 2.08 |
| 1,000 | victoriametrics | random | 6 | CPU Basic | 10 | 1.65 | 2.03 |
| 1,000 | victoriametrics | random | 7 | CPU Basic | 10 | 1.63 | 1.89 |
| 1,000 | victoriametrics | random | 8 | CPU Basic | 10 | 1.62 | 2.92 |
| 1,000 | victoriametrics | random | 9 | CPU Basic | 10 | 1.69 | 3.06 |
| 1,000 | victoriametrics | random | 10 | CPU Basic | 10 | 1.32 | 1.91 |
| 1,000 | victoriametrics | random | 11 | Memory Basic | 10 | 0.77 | 0.95 |
| 1,000 | victoriametrics | random | 12 | Memory Basic | 10 | 1.28 | 3.02 |
| 1,000 | victoriametrics | random | 13 | Memory Basic | 10 | 1.13 | 3.28 |
| 1,000 | victoriametrics | random | 14 | Memory Basic | 10 | 0.96 | 1.65 |
| 1,000 | victoriametrics | random | 15 | Memory Basic | 10 | 1.32 | 2.43 |
| 1,000 | victoriametrics | random | 16 | Network Traffic Basic | 10 | 1.24 | 1.63 |
| 1,000 | victoriametrics | random | 17 | Network Traffic Basic | 10 | 1.02 | 1.48 |
| 1,000 | victoriametrics | random | 18 | Disk Space Used Basic | 10 | 1.35 | 2.34 |

Raw request timings, query text, selected hosts, empties, errors, resource samples, readiness checks, container limits/states, logs, and server metrics are in each scale's directory.
