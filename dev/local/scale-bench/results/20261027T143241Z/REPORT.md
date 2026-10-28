# Node-exporter scaling benchmark

Source: `e5de6b841f45e167ec484be59c86cfecdfbbd7ef`. Each server: 8 vCPU, 16 GiB hard cap, 14 GiB Go soft limit, swap disabled. Local Docker volumes; oteldb native file backend.

Live node_exporter → nginx (2s cache) → vmagent (15s scrape) → both servers. Each scale uses fresh volumes. Random mode uses distinct seeded hosts and VictoriaMetrics `nocache=1`. Cached mode warms host-0 twice, then repeats identical requests. This does not flush disk/OS/column caches. VictoriaMetrics cacheTimestampOffset=15s; queries end at least 30s in the past.

Readiness requires all hosts and ≥6 samples/host over a 120s historical window after 180s ingest. Range requests cover 60s at 15s step, rate window 1m; 10 trials/query, 4 clients. Empty results and errors are excluded from latency statistics and counted separately. Short live runs measure initial scaling, not long-retention steady state.

| Nodes | Server | Gate | Mode | Requests | Errors | Empty | p50 ms | p95 ms |
|---:|---|---|---|---:|---:|---:|---:|---:|
| 1,000 | oteldb | pass | random | 190 | 0 | 0 | 1.94 | 21.97 |
| 1,000 | oteldb | pass | cached | 190 | 0 | 0 | 1.70 | 3.72 |
| 1,000 | victoriametrics | pass | random | 190 | 0 | 0 | 1.47 | 2.96 |
| 1,000 | victoriametrics | pass | cached | 190 | 0 | 0 | 0.95 | 1.32 |
| 10,000 | oteldb | FAIL | random | 0 | 0 | 0 | — | — |
| 10,000 | oteldb | FAIL | cached | 0 | 0 | 0 | — | — |
| 10,000 | victoriametrics | pass | random | 190 | 0 | 0 | 2.57 | 7.29 |
| 10,000 | victoriametrics | pass | cached | 190 | 0 | 0 | 0.99 | 1.77 |
| 25,000 | oteldb | FAIL | random | 0 | 0 | 0 | — | — |
| 25,000 | oteldb | FAIL | cached | 0 | 0 | 0 | — | — |
| 25,000 | victoriametrics | FAIL | random | 0 | 0 | 0 | — | — |
| 25,000 | victoriametrics | FAIL | cached | 0 | 0 | 0 | — | — |

## Coverage and failures

### 1,000 hosts

- oteldb: ready; last host-count response=1000; no observed process exit.
- victoriametrics: ready; last host-count response=1000; no observed process exit.
- Oteldb clean restart: observed 1000 hosts.

### 10,000 hosts

- oteldb: NOT ready; last host-count response=10000; no observed process exit.
- victoriametrics: ready; last host-count response=10000; no observed process exit.

### 25,000 hosts

- oteldb: NOT ready; last host-count response=14502; no observed process exit.
- victoriametrics: NOT ready; last host-count response=25000; no observed process exit.

## Per-query measurements

All times in milliseconds. Query indices match each scale's `queries.json`.

| Nodes | Server | Mode | Query | Panel | Nonempty OK | p50 | p95 |
|---:|---|---|---:|---|---:|---:|---:|
| 1,000 | oteldb | cached | 0 | CPU Busy | 10 | 2.80 | 3.79 |
| 1,000 | oteldb | cached | 1 | Sys Load (5m avg) | 10 | 2.30 | 2.72 |
| 1,000 | oteldb | cached | 2 | RAM Used | 10 | 1.00 | 1.97 |
| 1,000 | oteldb | cached | 3 | CPU Cores | 10 | 1.63 | 2.73 |
| 1,000 | oteldb | cached | 4 | Uptime | 10 | 1.21 | 2.27 |
| 1,000 | oteldb | cached | 5 | CPU Basic | 10 | 2.52 | 3.23 |
| 1,000 | oteldb | cached | 6 | CPU Basic | 10 | 2.76 | 4.32 |
| 1,000 | oteldb | cached | 7 | CPU Basic | 10 | 3.13 | 4.38 |
| 1,000 | oteldb | cached | 8 | CPU Basic | 10 | 3.01 | 4.06 |
| 1,000 | oteldb | cached | 9 | CPU Basic | 10 | 2.74 | 7.39 |
| 1,000 | oteldb | cached | 10 | CPU Basic | 10 | 2.84 | 8.83 |
| 1,000 | oteldb | cached | 11 | Memory Basic | 10 | 0.88 | 4.38 |
| 1,000 | oteldb | cached | 12 | Memory Basic | 10 | 1.40 | 5.29 |
| 1,000 | oteldb | cached | 13 | Memory Basic | 10 | 1.11 | 1.67 |
| 1,000 | oteldb | cached | 14 | Memory Basic | 10 | 0.98 | 1.61 |
| 1,000 | oteldb | cached | 15 | Memory Basic | 10 | 1.37 | 1.69 |
| 1,000 | oteldb | cached | 16 | Network Traffic Basic | 10 | 1.23 | 1.45 |
| 1,000 | oteldb | cached | 17 | Network Traffic Basic | 10 | 0.90 | 2.24 |
| 1,000 | oteldb | cached | 18 | Disk Space Used Basic | 10 | 1.29 | 2.38 |
| 1,000 | oteldb | random | 0 | CPU Busy | 10 | 21.68 | 23.50 |
| 1,000 | oteldb | random | 1 | Sys Load (5m avg) | 10 | 21.03 | 23.38 |
| 1,000 | oteldb | random | 2 | RAM Used | 10 | 1.69 | 2.71 |
| 1,000 | oteldb | random | 3 | CPU Cores | 10 | 1.98 | 22.01 |
| 1,000 | oteldb | random | 4 | Uptime | 10 | 1.57 | 2.73 |
| 1,000 | oteldb | random | 5 | CPU Basic | 10 | 2.64 | 3.28 |
| 1,000 | oteldb | random | 6 | CPU Basic | 10 | 2.94 | 3.43 |
| 1,000 | oteldb | random | 7 | CPU Basic | 10 | 2.69 | 3.52 |
| 1,000 | oteldb | random | 8 | CPU Basic | 10 | 2.75 | 3.32 |
| 1,000 | oteldb | random | 9 | CPU Basic | 10 | 3.16 | 3.35 |
| 1,000 | oteldb | random | 10 | CPU Basic | 10 | 2.61 | 3.56 |
| 1,000 | oteldb | random | 11 | Memory Basic | 10 | 0.96 | 2.69 |
| 1,000 | oteldb | random | 12 | Memory Basic | 10 | 1.69 | 3.85 |
| 1,000 | oteldb | random | 13 | Memory Basic | 10 | 1.59 | 1.79 |
| 1,000 | oteldb | random | 14 | Memory Basic | 10 | 1.13 | 1.28 |
| 1,000 | oteldb | random | 15 | Memory Basic | 10 | 1.46 | 1.95 |
| 1,000 | oteldb | random | 16 | Network Traffic Basic | 10 | 1.47 | 1.81 |
| 1,000 | oteldb | random | 17 | Network Traffic Basic | 10 | 1.34 | 1.98 |
| 1,000 | oteldb | random | 18 | Disk Space Used Basic | 10 | 1.76 | 3.34 |
| 1,000 | victoriametrics | cached | 0 | CPU Busy | 10 | 1.34 | 1.82 |
| 1,000 | victoriametrics | cached | 1 | Sys Load (5m avg) | 10 | 1.26 | 1.37 |
| 1,000 | victoriametrics | cached | 2 | RAM Used | 10 | 0.86 | 1.12 |
| 1,000 | victoriametrics | cached | 3 | CPU Cores | 10 | 0.87 | 1.10 |
| 1,000 | victoriametrics | cached | 4 | Uptime | 10 | 1.01 | 1.29 |
| 1,000 | victoriametrics | cached | 5 | CPU Basic | 10 | 1.03 | 1.22 |
| 1,000 | victoriametrics | cached | 6 | CPU Basic | 10 | 0.88 | 1.36 |
| 1,000 | victoriametrics | cached | 7 | CPU Basic | 10 | 1.01 | 1.20 |
| 1,000 | victoriametrics | cached | 8 | CPU Basic | 10 | 0.90 | 1.22 |
| 1,000 | victoriametrics | cached | 9 | CPU Basic | 10 | 0.90 | 1.00 |
| 1,000 | victoriametrics | cached | 10 | CPU Basic | 10 | 0.94 | 1.06 |
| 1,000 | victoriametrics | cached | 11 | Memory Basic | 10 | 0.87 | 1.05 |
| 1,000 | victoriametrics | cached | 12 | Memory Basic | 10 | 1.03 | 1.25 |
| 1,000 | victoriametrics | cached | 13 | Memory Basic | 10 | 0.91 | 1.11 |
| 1,000 | victoriametrics | cached | 14 | Memory Basic | 10 | 0.87 | 1.17 |
| 1,000 | victoriametrics | cached | 15 | Memory Basic | 10 | 0.99 | 1.05 |
| 1,000 | victoriametrics | cached | 16 | Network Traffic Basic | 10 | 0.92 | 1.11 |
| 1,000 | victoriametrics | cached | 17 | Network Traffic Basic | 10 | 0.70 | 0.98 |
| 1,000 | victoriametrics | cached | 18 | Disk Space Used Basic | 10 | 0.91 | 1.43 |
| 1,000 | victoriametrics | random | 0 | CPU Busy | 10 | 2.78 | 5.47 |
| 1,000 | victoriametrics | random | 1 | Sys Load (5m avg) | 10 | 1.97 | 4.70 |
| 1,000 | victoriametrics | random | 2 | RAM Used | 10 | 1.15 | 4.45 |
| 1,000 | victoriametrics | random | 3 | CPU Cores | 10 | 1.65 | 4.57 |
| 1,000 | victoriametrics | random | 4 | Uptime | 10 | 1.38 | 1.96 |
| 1,000 | victoriametrics | random | 5 | CPU Basic | 10 | 1.64 | 2.28 |
| 1,000 | victoriametrics | random | 6 | CPU Basic | 10 | 1.63 | 3.16 |
| 1,000 | victoriametrics | random | 7 | CPU Basic | 10 | 1.54 | 2.44 |
| 1,000 | victoriametrics | random | 8 | CPU Basic | 10 | 1.47 | 2.75 |
| 1,000 | victoriametrics | random | 9 | CPU Basic | 10 | 1.72 | 3.31 |
| 1,000 | victoriametrics | random | 10 | CPU Basic | 10 | 1.53 | 1.95 |
| 1,000 | victoriametrics | random | 11 | Memory Basic | 10 | 0.85 | 1.19 |
| 1,000 | victoriametrics | random | 12 | Memory Basic | 10 | 1.49 | 2.43 |
| 1,000 | victoriametrics | random | 13 | Memory Basic | 10 | 1.23 | 2.23 |
| 1,000 | victoriametrics | random | 14 | Memory Basic | 10 | 1.00 | 1.22 |
| 1,000 | victoriametrics | random | 15 | Memory Basic | 10 | 1.35 | 1.58 |
| 1,000 | victoriametrics | random | 16 | Network Traffic Basic | 10 | 1.21 | 1.49 |
| 1,000 | victoriametrics | random | 17 | Network Traffic Basic | 10 | 1.28 | 1.54 |
| 1,000 | victoriametrics | random | 18 | Disk Space Used Basic | 10 | 1.42 | 2.06 |
| 10,000 | victoriametrics | cached | 0 | CPU Busy | 10 | 1.86 | 2.10 |
| 10,000 | victoriametrics | cached | 1 | Sys Load (5m avg) | 10 | 1.43 | 1.80 |
| 10,000 | victoriametrics | cached | 2 | RAM Used | 10 | 0.95 | 1.48 |
| 10,000 | victoriametrics | cached | 3 | CPU Cores | 10 | 0.87 | 1.57 |
| 10,000 | victoriametrics | cached | 4 | Uptime | 10 | 1.02 | 1.78 |
| 10,000 | victoriametrics | cached | 5 | CPU Basic | 10 | 1.00 | 1.37 |
| 10,000 | victoriametrics | cached | 6 | CPU Basic | 10 | 0.98 | 1.31 |
| 10,000 | victoriametrics | cached | 7 | CPU Basic | 10 | 0.92 | 1.37 |
| 10,000 | victoriametrics | cached | 8 | CPU Basic | 10 | 1.04 | 1.31 |
| 10,000 | victoriametrics | cached | 9 | CPU Basic | 10 | 0.89 | 1.17 |
| 10,000 | victoriametrics | cached | 10 | CPU Basic | 10 | 1.07 | 1.23 |
| 10,000 | victoriametrics | cached | 11 | Memory Basic | 10 | 0.94 | 1.28 |
| 10,000 | victoriametrics | cached | 12 | Memory Basic | 10 | 1.05 | 1.18 |
| 10,000 | victoriametrics | cached | 13 | Memory Basic | 10 | 0.98 | 1.16 |
| 10,000 | victoriametrics | cached | 14 | Memory Basic | 10 | 0.91 | 1.12 |
| 10,000 | victoriametrics | cached | 15 | Memory Basic | 10 | 0.93 | 1.28 |
| 10,000 | victoriametrics | cached | 16 | Network Traffic Basic | 10 | 0.97 | 1.19 |
| 10,000 | victoriametrics | cached | 17 | Network Traffic Basic | 10 | 0.84 | 1.01 |
| 10,000 | victoriametrics | cached | 18 | Disk Space Used Basic | 10 | 0.88 | 1.11 |
| 10,000 | victoriametrics | random | 0 | CPU Busy | 10 | 6.60 | 9.21 |
| 10,000 | victoriametrics | random | 1 | Sys Load (5m avg) | 10 | 5.13 | 8.31 |
| 10,000 | victoriametrics | random | 2 | RAM Used | 10 | 3.86 | 7.32 |
| 10,000 | victoriametrics | random | 3 | CPU Cores | 10 | 1.90 | 8.14 |
| 10,000 | victoriametrics | random | 4 | Uptime | 10 | 3.52 | 6.20 |
| 10,000 | victoriametrics | random | 5 | CPU Basic | 10 | 2.23 | 4.46 |
| 10,000 | victoriametrics | random | 6 | CPU Basic | 10 | 1.97 | 4.48 |
| 10,000 | victoriametrics | random | 7 | CPU Basic | 10 | 2.15 | 4.48 |
| 10,000 | victoriametrics | random | 8 | CPU Basic | 10 | 2.25 | 4.23 |
| 10,000 | victoriametrics | random | 9 | CPU Basic | 10 | 2.17 | 7.71 |
| 10,000 | victoriametrics | random | 10 | CPU Basic | 10 | 1.72 | 2.57 |
| 10,000 | victoriametrics | random | 11 | Memory Basic | 10 | 1.20 | 1.54 |
| 10,000 | victoriametrics | random | 12 | Memory Basic | 10 | 4.68 | 8.64 |
| 10,000 | victoriametrics | random | 13 | Memory Basic | 10 | 4.34 | 7.10 |
| 10,000 | victoriametrics | random | 14 | Memory Basic | 10 | 1.49 | 3.38 |
| 10,000 | victoriametrics | random | 15 | Memory Basic | 10 | 3.22 | 4.59 |
| 10,000 | victoriametrics | random | 16 | Network Traffic Basic | 10 | 2.83 | 4.44 |
| 10,000 | victoriametrics | random | 17 | Network Traffic Basic | 10 | 2.96 | 4.29 |
| 10,000 | victoriametrics | random | 18 | Disk Space Used Basic | 10 | 4.03 | 9.52 |

Raw request timings, query text, selected hosts, empties, errors, resource samples, readiness checks, container limits/states, logs, and server metrics are in each scale's directory.
