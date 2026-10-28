# Node-exporter scaling benchmark

Source: `e5de6b841f45e167ec484be59c86cfecdfbbd7ef`. Each server: 8 vCPU, 16 GiB hard cap, 14 GiB Go soft limit, swap disabled. Local Docker volumes; oteldb native file backend.

Live node_exporter → nginx (2s cache) → vmagent (15s scrape) → both servers. Each scale uses fresh volumes. Random mode uses distinct seeded hosts and VictoriaMetrics `nocache=1`. Cached mode warms host-0 twice, then repeats identical requests. This does not flush disk/OS/column caches. VictoriaMetrics cacheTimestampOffset=15s; queries end at least 30s in the past.

Readiness requires all hosts and ≥6 samples/host over a 120s historical window after 180s ingest. Range requests cover 60s at 15s step, rate window 1m; 10 trials/query, 4 clients. Empty results and errors are excluded from latency statistics and counted separately. Short live runs measure initial scaling, not long-retention steady state.

| Nodes | Server | Gate | Mode | Requests | Errors | Empty | p50 ms | p95 ms |
|---:|---|---|---|---:|---:|---:|---:|---:|
| 1,000 | oteldb | pass | random | 190 | 0 | 0 | 2.26 | 21.46 |
| 1,000 | oteldb | pass | cached | 190 | 0 | 0 | 1.36 | 3.64 |
| 1,000 | victoriametrics | pass | random | 190 | 0 | 0 | 1.43 | 2.57 |
| 1,000 | victoriametrics | pass | cached | 190 | 0 | 0 | 0.91 | 1.28 |

## Coverage and failures

### 1,000 hosts

- oteldb: ready; last host-count response=1000; no observed process exit.
- victoriametrics: ready; last host-count response=1000; no observed process exit.
- Oteldb clean restart: observed 1000 hosts.

## Per-query measurements

All times in milliseconds. Query indices match each scale's `queries.json`.

| Nodes | Server | Mode | Query | Panel | Nonempty OK | p50 | p95 |
|---:|---|---|---:|---|---:|---:|---:|
| 1,000 | oteldb | cached | 0 | CPU Busy | 10 | 2.53 | 2.84 |
| 1,000 | oteldb | cached | 1 | Sys Load (5m avg) | 10 | 2.21 | 2.89 |
| 1,000 | oteldb | cached | 2 | RAM Used | 10 | 0.97 | 1.17 |
| 1,000 | oteldb | cached | 3 | CPU Cores | 10 | 1.48 | 2.41 |
| 1,000 | oteldb | cached | 4 | Uptime | 10 | 0.94 | 1.29 |
| 1,000 | oteldb | cached | 5 | CPU Basic | 10 | 2.46 | 8.90 |
| 1,000 | oteldb | cached | 6 | CPU Basic | 10 | 2.59 | 8.64 |
| 1,000 | oteldb | cached | 7 | CPU Basic | 10 | 2.71 | 8.68 |
| 1,000 | oteldb | cached | 8 | CPU Basic | 10 | 2.87 | 7.91 |
| 1,000 | oteldb | cached | 9 | CPU Basic | 10 | 2.98 | 5.88 |
| 1,000 | oteldb | cached | 10 | CPU Basic | 10 | 2.64 | 5.65 |
| 1,000 | oteldb | cached | 11 | Memory Basic | 10 | 0.99 | 3.12 |
| 1,000 | oteldb | cached | 12 | Memory Basic | 10 | 1.23 | 3.26 |
| 1,000 | oteldb | cached | 13 | Memory Basic | 10 | 1.11 | 1.42 |
| 1,000 | oteldb | cached | 14 | Memory Basic | 10 | 0.95 | 1.21 |
| 1,000 | oteldb | cached | 15 | Memory Basic | 10 | 1.07 | 1.36 |
| 1,000 | oteldb | cached | 16 | Network Traffic Basic | 10 | 1.03 | 1.22 |
| 1,000 | oteldb | cached | 17 | Network Traffic Basic | 10 | 1.00 | 1.20 |
| 1,000 | oteldb | cached | 18 | Disk Space Used Basic | 10 | 1.10 | 1.30 |
| 1,000 | oteldb | random | 0 | CPU Busy | 10 | 21.83 | 25.33 |
| 1,000 | oteldb | random | 1 | Sys Load (5m avg) | 10 | 20.75 | 23.57 |
| 1,000 | oteldb | random | 2 | RAM Used | 10 | 1.87 | 4.39 |
| 1,000 | oteldb | random | 3 | CPU Cores | 10 | 2.35 | 21.63 |
| 1,000 | oteldb | random | 4 | Uptime | 10 | 1.66 | 6.05 |
| 1,000 | oteldb | random | 5 | CPU Basic | 10 | 2.60 | 4.83 |
| 1,000 | oteldb | random | 6 | CPU Basic | 10 | 2.79 | 5.27 |
| 1,000 | oteldb | random | 7 | CPU Basic | 10 | 2.88 | 4.89 |
| 1,000 | oteldb | random | 8 | CPU Basic | 10 | 2.72 | 5.04 |
| 1,000 | oteldb | random | 9 | CPU Basic | 10 | 2.85 | 3.57 |
| 1,000 | oteldb | random | 10 | CPU Basic | 10 | 2.73 | 5.25 |
| 1,000 | oteldb | random | 11 | Memory Basic | 10 | 0.94 | 3.96 |
| 1,000 | oteldb | random | 12 | Memory Basic | 10 | 1.69 | 4.89 |
| 1,000 | oteldb | random | 13 | Memory Basic | 10 | 1.52 | 4.51 |
| 1,000 | oteldb | random | 14 | Memory Basic | 10 | 1.17 | 2.18 |
| 1,000 | oteldb | random | 15 | Memory Basic | 10 | 1.35 | 4.18 |
| 1,000 | oteldb | random | 16 | Network Traffic Basic | 10 | 1.37 | 8.68 |
| 1,000 | oteldb | random | 17 | Network Traffic Basic | 10 | 1.51 | 8.39 |
| 1,000 | oteldb | random | 18 | Disk Space Used Basic | 10 | 1.84 | 8.87 |
| 1,000 | victoriametrics | cached | 0 | CPU Busy | 10 | 1.29 | 1.77 |
| 1,000 | victoriametrics | cached | 1 | Sys Load (5m avg) | 10 | 1.03 | 1.28 |
| 1,000 | victoriametrics | cached | 2 | RAM Used | 10 | 0.82 | 0.96 |
| 1,000 | victoriametrics | cached | 3 | CPU Cores | 10 | 0.74 | 1.01 |
| 1,000 | victoriametrics | cached | 4 | Uptime | 10 | 0.98 | 1.94 |
| 1,000 | victoriametrics | cached | 5 | CPU Basic | 10 | 0.96 | 1.83 |
| 1,000 | victoriametrics | cached | 6 | CPU Basic | 10 | 0.86 | 1.70 |
| 1,000 | victoriametrics | cached | 7 | CPU Basic | 10 | 0.97 | 1.69 |
| 1,000 | victoriametrics | cached | 8 | CPU Basic | 10 | 0.86 | 1.26 |
| 1,000 | victoriametrics | cached | 9 | CPU Basic | 10 | 0.82 | 1.20 |
| 1,000 | victoriametrics | cached | 10 | CPU Basic | 10 | 0.94 | 1.23 |
| 1,000 | victoriametrics | cached | 11 | Memory Basic | 10 | 0.80 | 0.91 |
| 1,000 | victoriametrics | cached | 12 | Memory Basic | 10 | 0.93 | 1.06 |
| 1,000 | victoriametrics | cached | 13 | Memory Basic | 10 | 0.91 | 1.08 |
| 1,000 | victoriametrics | cached | 14 | Memory Basic | 10 | 0.79 | 1.07 |
| 1,000 | victoriametrics | cached | 15 | Memory Basic | 10 | 0.88 | 1.11 |
| 1,000 | victoriametrics | cached | 16 | Network Traffic Basic | 10 | 0.94 | 1.11 |
| 1,000 | victoriametrics | cached | 17 | Network Traffic Basic | 10 | 0.77 | 0.99 |
| 1,000 | victoriametrics | cached | 18 | Disk Space Used Basic | 10 | 0.86 | 1.12 |
| 1,000 | victoriametrics | random | 0 | CPU Busy | 10 | 2.59 | 3.40 |
| 1,000 | victoriametrics | random | 1 | Sys Load (5m avg) | 10 | 1.84 | 3.00 |
| 1,000 | victoriametrics | random | 2 | RAM Used | 10 | 1.16 | 2.30 |
| 1,000 | victoriametrics | random | 3 | CPU Cores | 10 | 1.51 | 2.99 |
| 1,000 | victoriametrics | random | 4 | Uptime | 10 | 1.32 | 1.87 |
| 1,000 | victoriametrics | random | 5 | CPU Basic | 10 | 1.73 | 2.14 |
| 1,000 | victoriametrics | random | 6 | CPU Basic | 10 | 1.72 | 2.00 |
| 1,000 | victoriametrics | random | 7 | CPU Basic | 10 | 1.63 | 2.19 |
| 1,000 | victoriametrics | random | 8 | CPU Basic | 10 | 1.54 | 2.69 |
| 1,000 | victoriametrics | random | 9 | CPU Basic | 10 | 1.75 | 3.27 |
| 1,000 | victoriametrics | random | 10 | CPU Basic | 10 | 1.45 | 1.78 |
| 1,000 | victoriametrics | random | 11 | Memory Basic | 10 | 0.74 | 1.57 |
| 1,000 | victoriametrics | random | 12 | Memory Basic | 10 | 1.32 | 2.32 |
| 1,000 | victoriametrics | random | 13 | Memory Basic | 10 | 1.20 | 2.37 |
| 1,000 | victoriametrics | random | 14 | Memory Basic | 10 | 0.93 | 1.40 |
| 1,000 | victoriametrics | random | 15 | Memory Basic | 10 | 1.29 | 1.83 |
| 1,000 | victoriametrics | random | 16 | Network Traffic Basic | 10 | 1.12 | 1.32 |
| 1,000 | victoriametrics | random | 17 | Network Traffic Basic | 10 | 1.15 | 1.41 |
| 1,000 | victoriametrics | random | 18 | Disk Space Used Basic | 10 | 1.39 | 2.35 |

Raw request timings, query text, selected hosts, empties, errors, resource samples, readiness checks, container limits/states, logs, and server metrics are in each scale's directory.
