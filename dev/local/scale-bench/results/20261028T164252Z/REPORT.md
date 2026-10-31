# Node-exporter scaling benchmark

Source: `e5de6b841f45e167ec484be59c86cfecdfbbd7ef`. Each server: 8 vCPU, 16 GiB hard cap, 14 GiB Go soft limit, swap disabled. Local Docker volumes; oteldb native file backend.

Live node_exporter → nginx (2s cache) → vmagent (15s scrape) → both servers. Each scale uses fresh volumes. Random mode uses distinct seeded hosts and VictoriaMetrics `nocache=1`. Cached mode warms host-0 twice, then repeats identical requests. This does not flush disk/OS/column caches. VictoriaMetrics cacheTimestampOffset=15s; queries end at least 30s in the past.

Readiness requires all hosts and ≥6 samples/host over a 120s historical window after 180s ingest. Range requests cover 60s at 15s step, rate window 1m; 10 trials/query, 4 clients. Empty results and errors are excluded from latency statistics and counted separately. Short live runs measure initial scaling, not long-retention steady state.

| Nodes | Server | Gate | Mode | Requests | Errors | Empty | p50 ms | p95 ms |
|---:|---|---|---|---:|---:|---:|---:|---:|
| 1,000 | oteldb | pass | random | 190 | 0 | 0 | 2.11 | 22.36 |
| 1,000 | oteldb | pass | cached | 190 | 0 | 0 | 1.26 | 2.90 |
| 1,000 | victoriametrics | pass | random | 190 | 0 | 0 | 1.40 | 2.89 |
| 1,000 | victoriametrics | pass | cached | 190 | 0 | 0 | 0.87 | 1.22 |

## Coverage and failures

### 1,000 hosts

- oteldb: ready; last host-count response=1000; no observed process exit.
- victoriametrics: ready; last host-count response=1000; no observed process exit.
- Oteldb clean restart: observed 1000 hosts.

## Per-query measurements

All times in milliseconds. Query indices match each scale's `queries.json`.

| Nodes | Server | Mode | Query | Panel | Nonempty OK | p50 | p95 |
|---:|---|---|---:|---|---:|---:|---:|
| 1,000 | oteldb | cached | 0 | CPU Busy | 10 | 2.34 | 2.90 |
| 1,000 | oteldb | cached | 1 | Sys Load (5m avg) | 10 | 2.12 | 2.61 |
| 1,000 | oteldb | cached | 2 | RAM Used | 10 | 0.85 | 1.05 |
| 1,000 | oteldb | cached | 3 | CPU Cores | 10 | 1.51 | 1.76 |
| 1,000 | oteldb | cached | 4 | Uptime | 10 | 1.02 | 1.16 |
| 1,000 | oteldb | cached | 5 | CPU Basic | 10 | 2.29 | 3.23 |
| 1,000 | oteldb | cached | 6 | CPU Basic | 10 | 2.46 | 3.52 |
| 1,000 | oteldb | cached | 7 | CPU Basic | 10 | 2.38 | 3.50 |
| 1,000 | oteldb | cached | 8 | CPU Basic | 10 | 2.55 | 3.29 |
| 1,000 | oteldb | cached | 9 | CPU Basic | 10 | 2.75 | 3.04 |
| 1,000 | oteldb | cached | 10 | CPU Basic | 10 | 2.24 | 2.56 |
| 1,000 | oteldb | cached | 11 | Memory Basic | 10 | 0.82 | 1.14 |
| 1,000 | oteldb | cached | 12 | Memory Basic | 10 | 1.17 | 1.30 |
| 1,000 | oteldb | cached | 13 | Memory Basic | 10 | 1.08 | 1.41 |
| 1,000 | oteldb | cached | 14 | Memory Basic | 10 | 0.84 | 1.19 |
| 1,000 | oteldb | cached | 15 | Memory Basic | 10 | 1.09 | 1.30 |
| 1,000 | oteldb | cached | 16 | Network Traffic Basic | 10 | 0.94 | 1.12 |
| 1,000 | oteldb | cached | 17 | Network Traffic Basic | 10 | 1.03 | 1.13 |
| 1,000 | oteldb | cached | 18 | Disk Space Used Basic | 10 | 1.04 | 1.23 |
| 1,000 | oteldb | random | 0 | CPU Busy | 10 | 22.32 | 30.23 |
| 1,000 | oteldb | random | 1 | Sys Load (5m avg) | 10 | 20.90 | 23.35 |
| 1,000 | oteldb | random | 2 | RAM Used | 10 | 1.74 | 8.33 |
| 1,000 | oteldb | random | 3 | CPU Cores | 10 | 2.20 | 23.15 |
| 1,000 | oteldb | random | 4 | Uptime | 10 | 1.45 | 14.15 |
| 1,000 | oteldb | random | 5 | CPU Basic | 10 | 2.99 | 8.57 |
| 1,000 | oteldb | random | 6 | CPU Basic | 10 | 2.75 | 8.22 |
| 1,000 | oteldb | random | 7 | CPU Basic | 10 | 2.85 | 8.58 |
| 1,000 | oteldb | random | 8 | CPU Basic | 10 | 2.87 | 8.27 |
| 1,000 | oteldb | random | 9 | CPU Basic | 10 | 2.96 | 7.19 |
| 1,000 | oteldb | random | 10 | CPU Basic | 10 | 2.52 | 2.89 |
| 1,000 | oteldb | random | 11 | Memory Basic | 10 | 0.92 | 1.22 |
| 1,000 | oteldb | random | 12 | Memory Basic | 10 | 1.72 | 11.43 |
| 1,000 | oteldb | random | 13 | Memory Basic | 10 | 1.56 | 7.72 |
| 1,000 | oteldb | random | 14 | Memory Basic | 10 | 0.99 | 7.30 |
| 1,000 | oteldb | random | 15 | Memory Basic | 10 | 1.56 | 7.86 |
| 1,000 | oteldb | random | 16 | Network Traffic Basic | 10 | 1.57 | 7.63 |
| 1,000 | oteldb | random | 17 | Network Traffic Basic | 10 | 1.45 | 7.53 |
| 1,000 | oteldb | random | 18 | Disk Space Used Basic | 10 | 1.91 | 11.61 |
| 1,000 | victoriametrics | cached | 0 | CPU Busy | 10 | 1.35 | 1.53 |
| 1,000 | victoriametrics | cached | 1 | Sys Load (5m avg) | 10 | 1.09 | 1.24 |
| 1,000 | victoriametrics | cached | 2 | RAM Used | 10 | 0.81 | 1.20 |
| 1,000 | victoriametrics | cached | 3 | CPU Cores | 10 | 0.80 | 1.04 |
| 1,000 | victoriametrics | cached | 4 | Uptime | 10 | 0.88 | 1.31 |
| 1,000 | victoriametrics | cached | 5 | CPU Basic | 10 | 0.80 | 1.10 |
| 1,000 | victoriametrics | cached | 6 | CPU Basic | 10 | 0.92 | 1.16 |
| 1,000 | victoriametrics | cached | 7 | CPU Basic | 10 | 0.85 | 1.09 |
| 1,000 | victoriametrics | cached | 8 | CPU Basic | 10 | 0.84 | 1.22 |
| 1,000 | victoriametrics | cached | 9 | CPU Basic | 10 | 0.83 | 1.15 |
| 1,000 | victoriametrics | cached | 10 | CPU Basic | 10 | 0.90 | 1.15 |
| 1,000 | victoriametrics | cached | 11 | Memory Basic | 10 | 0.76 | 1.14 |
| 1,000 | victoriametrics | cached | 12 | Memory Basic | 10 | 1.03 | 1.20 |
| 1,000 | victoriametrics | cached | 13 | Memory Basic | 10 | 0.83 | 1.37 |
| 1,000 | victoriametrics | cached | 14 | Memory Basic | 10 | 0.93 | 1.19 |
| 1,000 | victoriametrics | cached | 15 | Memory Basic | 10 | 0.84 | 1.12 |
| 1,000 | victoriametrics | cached | 16 | Network Traffic Basic | 10 | 0.88 | 1.04 |
| 1,000 | victoriametrics | cached | 17 | Network Traffic Basic | 10 | 0.77 | 0.91 |
| 1,000 | victoriametrics | cached | 18 | Disk Space Used Basic | 10 | 0.80 | 0.91 |
| 1,000 | victoriametrics | random | 0 | CPU Busy | 10 | 2.62 | 6.14 |
| 1,000 | victoriametrics | random | 1 | Sys Load (5m avg) | 10 | 1.73 | 5.72 |
| 1,000 | victoriametrics | random | 2 | RAM Used | 10 | 1.25 | 5.13 |
| 1,000 | victoriametrics | random | 3 | CPU Cores | 10 | 1.49 | 5.44 |
| 1,000 | victoriametrics | random | 4 | Uptime | 10 | 1.25 | 2.01 |
| 1,000 | victoriametrics | random | 5 | CPU Basic | 10 | 1.58 | 2.19 |
| 1,000 | victoriametrics | random | 6 | CPU Basic | 10 | 1.68 | 2.44 |
| 1,000 | victoriametrics | random | 7 | CPU Basic | 10 | 1.65 | 2.17 |
| 1,000 | victoriametrics | random | 8 | CPU Basic | 10 | 1.46 | 2.83 |
| 1,000 | victoriametrics | random | 9 | CPU Basic | 10 | 1.68 | 2.86 |
| 1,000 | victoriametrics | random | 10 | CPU Basic | 10 | 1.39 | 1.75 |
| 1,000 | victoriametrics | random | 11 | Memory Basic | 10 | 0.83 | 1.06 |
| 1,000 | victoriametrics | random | 12 | Memory Basic | 10 | 1.38 | 2.13 |
| 1,000 | victoriametrics | random | 13 | Memory Basic | 10 | 1.13 | 2.02 |
| 1,000 | victoriametrics | random | 14 | Memory Basic | 10 | 0.86 | 1.40 |
| 1,000 | victoriametrics | random | 15 | Memory Basic | 10 | 1.38 | 1.52 |
| 1,000 | victoriametrics | random | 16 | Network Traffic Basic | 10 | 1.22 | 1.58 |
| 1,000 | victoriametrics | random | 17 | Network Traffic Basic | 10 | 1.21 | 2.01 |
| 1,000 | victoriametrics | random | 18 | Disk Space Used Basic | 10 | 1.46 | 3.70 |

Raw request timings, query text, selected hosts, empties, errors, resource samples, readiness checks, container limits/states, logs, and server metrics are in each scale's directory.
