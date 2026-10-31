# Node-exporter scaling benchmark

Source: `b5666f1b06706e23a182bb5c679d02562de52393`. Each server: 8 vCPU, 16 GiB hard cap, 14 GiB Go soft limit, swap disabled. Local Docker volumes; oteldb native file backend.

Live node_exporter → nginx (2s cache) → vmagent (15s scrape) → both servers. Each scale uses fresh volumes. Random mode uses distinct seeded hosts and VictoriaMetrics `nocache=1`. Cached mode warms host-0 twice, then repeats identical requests. This does not flush disk/OS/column caches. VictoriaMetrics cacheTimestampOffset=15s; queries end at least 30s in the past.

Readiness requires all hosts and ≥6 samples/host over a 120s historical window after 180s ingest. Range requests cover 60s at 15s step, rate window 1m; 10 trials/query, 4 clients. Empty results and errors are excluded from latency statistics and counted separately. Short live runs measure initial scaling, not long-retention steady state.

| Nodes | Server | Gate | Mode | Requests | Errors | Empty | p50 ms | p95 ms |
|---:|---|---|---|---:|---:|---:|---:|---:|
| 1,000 | oteldb | pass | random | 190 | 0 | 0 | 1.90 | 23.47 |
| 1,000 | oteldb | pass | cached | 190 | 0 | 0 | 1.55 | 5.21 |
| 1,000 | victoriametrics | pass | random | 190 | 0 | 0 | 1.37 | 2.77 |
| 1,000 | victoriametrics | pass | cached | 190 | 0 | 0 | 0.87 | 1.19 |

## Coverage and failures

### 1,000 hosts

- oteldb: ready; last host-count response=1000; no observed process exit.
- victoriametrics: ready; last host-count response=1000; no observed process exit.
- Oteldb clean restart: observed 1000 hosts.

## Per-query measurements

All times in milliseconds. Query indices match each scale's `queries.json`.

| Nodes | Server | Mode | Query | Panel | Nonempty OK | p50 | p95 |
|---:|---|---|---:|---|---:|---:|---:|
| 1,000 | oteldb | cached | 0 | CPU Busy | 10 | 2.78 | 8.25 |
| 1,000 | oteldb | cached | 1 | Sys Load (5m avg) | 10 | 2.46 | 2.86 |
| 1,000 | oteldb | cached | 2 | RAM Used | 10 | 0.97 | 1.16 |
| 1,000 | oteldb | cached | 3 | CPU Cores | 10 | 1.59 | 1.66 |
| 1,000 | oteldb | cached | 4 | Uptime | 10 | 0.98 | 4.83 |
| 1,000 | oteldb | cached | 5 | CPU Basic | 10 | 2.54 | 6.41 |
| 1,000 | oteldb | cached | 6 | CPU Basic | 10 | 2.81 | 6.71 |
| 1,000 | oteldb | cached | 7 | CPU Basic | 10 | 2.97 | 6.47 |
| 1,000 | oteldb | cached | 8 | CPU Basic | 10 | 3.10 | 8.53 |
| 1,000 | oteldb | cached | 9 | CPU Basic | 10 | 2.66 | 8.44 |
| 1,000 | oteldb | cached | 10 | CPU Basic | 10 | 2.24 | 8.37 |
| 1,000 | oteldb | cached | 11 | Memory Basic | 10 | 0.88 | 2.42 |
| 1,000 | oteldb | cached | 12 | Memory Basic | 10 | 1.09 | 5.21 |
| 1,000 | oteldb | cached | 13 | Memory Basic | 10 | 1.14 | 4.98 |
| 1,000 | oteldb | cached | 14 | Memory Basic | 10 | 0.94 | 4.82 |
| 1,000 | oteldb | cached | 15 | Memory Basic | 10 | 1.13 | 4.79 |
| 1,000 | oteldb | cached | 16 | Network Traffic Basic | 10 | 1.06 | 6.03 |
| 1,000 | oteldb | cached | 17 | Network Traffic Basic | 10 | 0.97 | 3.85 |
| 1,000 | oteldb | cached | 18 | Disk Space Used Basic | 10 | 1.10 | 6.02 |
| 1,000 | oteldb | random | 0 | CPU Busy | 10 | 22.75 | 26.05 |
| 1,000 | oteldb | random | 1 | Sys Load (5m avg) | 10 | 22.03 | 25.83 |
| 1,000 | oteldb | random | 2 | RAM Used | 10 | 1.69 | 1.85 |
| 1,000 | oteldb | random | 3 | CPU Cores | 10 | 2.30 | 24.02 |
| 1,000 | oteldb | random | 4 | Uptime | 10 | 1.49 | 4.67 |
| 1,000 | oteldb | random | 5 | CPU Basic | 10 | 2.53 | 7.43 |
| 1,000 | oteldb | random | 6 | CPU Basic | 10 | 2.84 | 7.23 |
| 1,000 | oteldb | random | 7 | CPU Basic | 10 | 2.69 | 7.03 |
| 1,000 | oteldb | random | 8 | CPU Basic | 10 | 2.85 | 3.41 |
| 1,000 | oteldb | random | 9 | CPU Basic | 10 | 2.98 | 3.60 |
| 1,000 | oteldb | random | 10 | CPU Basic | 10 | 2.44 | 2.81 |
| 1,000 | oteldb | random | 11 | Memory Basic | 10 | 0.92 | 1.12 |
| 1,000 | oteldb | random | 12 | Memory Basic | 10 | 1.63 | 2.02 |
| 1,000 | oteldb | random | 13 | Memory Basic | 10 | 1.47 | 1.82 |
| 1,000 | oteldb | random | 14 | Memory Basic | 10 | 1.14 | 1.68 |
| 1,000 | oteldb | random | 15 | Memory Basic | 10 | 1.40 | 1.84 |
| 1,000 | oteldb | random | 16 | Network Traffic Basic | 10 | 1.45 | 1.74 |
| 1,000 | oteldb | random | 17 | Network Traffic Basic | 10 | 1.43 | 2.70 |
| 1,000 | oteldb | random | 18 | Disk Space Used Basic | 10 | 1.74 | 3.94 |
| 1,000 | victoriametrics | cached | 0 | CPU Busy | 10 | 1.19 | 1.80 |
| 1,000 | victoriametrics | cached | 1 | Sys Load (5m avg) | 10 | 1.06 | 1.74 |
| 1,000 | victoriametrics | cached | 2 | RAM Used | 10 | 0.82 | 1.00 |
| 1,000 | victoriametrics | cached | 3 | CPU Cores | 10 | 0.84 | 0.97 |
| 1,000 | victoriametrics | cached | 4 | Uptime | 10 | 0.84 | 1.01 |
| 1,000 | victoriametrics | cached | 5 | CPU Basic | 10 | 0.87 | 1.09 |
| 1,000 | victoriametrics | cached | 6 | CPU Basic | 10 | 0.88 | 1.06 |
| 1,000 | victoriametrics | cached | 7 | CPU Basic | 10 | 0.95 | 1.15 |
| 1,000 | victoriametrics | cached | 8 | CPU Basic | 10 | 0.82 | 1.03 |
| 1,000 | victoriametrics | cached | 9 | CPU Basic | 10 | 0.84 | 1.06 |
| 1,000 | victoriametrics | cached | 10 | CPU Basic | 10 | 0.89 | 1.03 |
| 1,000 | victoriametrics | cached | 11 | Memory Basic | 10 | 0.83 | 1.05 |
| 1,000 | victoriametrics | cached | 12 | Memory Basic | 10 | 0.86 | 1.08 |
| 1,000 | victoriametrics | cached | 13 | Memory Basic | 10 | 0.87 | 1.07 |
| 1,000 | victoriametrics | cached | 14 | Memory Basic | 10 | 0.84 | 1.07 |
| 1,000 | victoriametrics | cached | 15 | Memory Basic | 10 | 0.88 | 1.07 |
| 1,000 | victoriametrics | cached | 16 | Network Traffic Basic | 10 | 0.83 | 1.09 |
| 1,000 | victoriametrics | cached | 17 | Network Traffic Basic | 10 | 0.79 | 1.31 |
| 1,000 | victoriametrics | cached | 18 | Disk Space Used Basic | 10 | 0.77 | 1.47 |
| 1,000 | victoriametrics | random | 0 | CPU Busy | 10 | 2.52 | 5.32 |
| 1,000 | victoriametrics | random | 1 | Sys Load (5m avg) | 10 | 1.78 | 4.81 |
| 1,000 | victoriametrics | random | 2 | RAM Used | 10 | 1.14 | 3.62 |
| 1,000 | victoriametrics | random | 3 | CPU Cores | 10 | 1.38 | 4.18 |
| 1,000 | victoriametrics | random | 4 | Uptime | 10 | 1.56 | 2.29 |
| 1,000 | victoriametrics | random | 5 | CPU Basic | 10 | 1.68 | 2.73 |
| 1,000 | victoriametrics | random | 6 | CPU Basic | 10 | 1.70 | 2.71 |
| 1,000 | victoriametrics | random | 7 | CPU Basic | 10 | 1.70 | 2.43 |
| 1,000 | victoriametrics | random | 8 | CPU Basic | 10 | 1.44 | 3.04 |
| 1,000 | victoriametrics | random | 9 | CPU Basic | 10 | 1.69 | 3.29 |
| 1,000 | victoriametrics | random | 10 | CPU Basic | 10 | 1.38 | 2.07 |
| 1,000 | victoriametrics | random | 11 | Memory Basic | 10 | 0.85 | 1.02 |
| 1,000 | victoriametrics | random | 12 | Memory Basic | 10 | 1.34 | 2.32 |
| 1,000 | victoriametrics | random | 13 | Memory Basic | 10 | 1.11 | 2.10 |
| 1,000 | victoriametrics | random | 14 | Memory Basic | 10 | 0.95 | 1.26 |
| 1,000 | victoriametrics | random | 15 | Memory Basic | 10 | 1.25 | 1.66 |
| 1,000 | victoriametrics | random | 16 | Network Traffic Basic | 10 | 1.20 | 1.62 |
| 1,000 | victoriametrics | random | 17 | Network Traffic Basic | 10 | 1.08 | 1.63 |
| 1,000 | victoriametrics | random | 18 | Disk Space Used Basic | 10 | 1.27 | 2.45 |

Raw request timings, query text, selected hosts, empties, errors, resource samples, readiness checks, container limits/states, logs, and server metrics are in each scale's directory.
