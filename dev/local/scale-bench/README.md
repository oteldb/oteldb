# Node-exporter scaling benchmark

Compare single-node oteldb's native `file` backend with single-node VictoriaMetrics.
Both receive identical live samples from vmagent. Nginx caches one real node-exporter's
response for 2s; vmagent assigns each scrape target a distinct `instance="host-N"`.

```sh
bash dev/local/scale-bench/build.sh
python dev/local/scale-bench/run.py
```

Requires Go, Docker Compose, and Python 3. Builds the current checkout and `otelbench`;
no source changes or local module replacements are needed. Runtime images are pinned
in Compose. Set `OTELDB_BENCH_IMAGE` to override the local oteldb image tag.

To measure a storage worktree while preserving the application's pinned `go.mod`:

```sh
BENCH_STORAGE_DIR=/absolute/path/to/storage-worktree \
  OTELDB_BENCH_IMAGE=local/oteldb:scale-compact bash dev/local/scale-bench/build.sh
OTELDB_BENCH_IMAGE=local/oteldb:scale-compact python dev/local/scale-bench/run.py
```

The build uses a temporary modfile under `bin/` and records the storage base revision
and source diff hash on the image. `diagnose.py --control-port 19092` can include an
unchanged server in the paired fixed-dataset comparison and records result hashes.

Default scales: 1k, 10k, 25k hosts. Each server gets an enforced 8-vCPU
quota, 16-GiB memory limit, no swap, and `GOMEMLIMIT=14GiB`. These are quotas on a
shared host, not dedicated cores. The load generator has a separate 8-vCPU/12-GiB
budget; its failure is reported separately from server failures.

Every scale starts with fresh Docker volumes. Oteldb uses the existing
`embedded-bench/oteldb.yml`: `storage.backend=file`, persistent WAL and compressed
parts, 10s flush cadence. No S3, ClickHouse, or ephemeral storage engine is involved.
The runner deletes only its own Compose containers and volumes after saving evidence.

## Measurement

The default suite takes 19 visible queries directly from the checked-in node-exporter
dashboard: CPU, load, memory, uptime, networking, and filesystem panels. Instant
and range requests follow the dashboard target settings. `--all-panels` runs all
242 visible targets, including metrics the exporter's default collectors may omit.

Each scale ingests for at least 180s at a 15s scrape interval. Readiness requires all
requested hosts and at least six samples per host in the historical 120s window.
Unready servers receive no latency score. The ingest gate has a 360s deadline.
Queries cover a fixed 60s range at a 15s step, with 1m rate windows. They end at least
30s before measurement; ingestion continues during queries.

- Random: ten distinct seeded hosts, one request/query/host, no warmup.
  VictoriaMetrics additionally receives `nocache=1` to bypass its response cache.
- Cached: two warmup requests/query for host-0, followed by ten identical requests.
  The request timestamps and expressions stay fixed, and response caching is enabled.

VictoriaMetrics uses `search.cacheTimestampOffset=15s` so a short live run can exercise
the response cache. Random requests still benefit from OS, index, and decoded-column
caches; changing hosts does not guarantee cold disk reads. Oteldb's caches are storage
caches, so repeated requests must not be described as proven full-response cache hits.

Four concurrent clients drive each server in turn. A 240s per-server query budget
bounds stalled requests; incomplete counts, errors, and empty results remain visible.
Use `--query-budget` to extend it for the full dashboard. Pooled latency percentiles
describe this query mix; inspect raw timings by query before drawing conclusions.

```sh
python dev/local/scale-bench/run.py --nodes 1000 --all-panels --query-budget 900
python dev/local/scale-bench/run.py --nodes 1000 --otelbench
python dev/local/scale-bench/run.py --nodes 10000 25000
```

`--otelbench` flattens collapsed rows and scopes the two bare `node_hwmon_chip_names`
joins to the selected host. Without that adaptation, cloned chip names produce a
many-to-many join error. Otelbench applies a range to every target, including stat panels.

Results appear under `results/<UTC timestamp>/REPORT.md`. Each scale also saves
request timings/expressions/hosts/result counts, readiness checks, container states
and limits, sampled memory/CPU, server metrics, storage sizes, and recent logs.
The first scale checks oteldb persistence across a clean restart. These short runs
test initial cardinality and dashboard reads, not long-retention steady state.

`--keep` retains containers and volumes after stopping vmagent, for investigation.
`diagnose.py <results/timestamp/1000>` runs paired keepalive queries with ingestion
stopped, including repeated queries with VictoriaMetrics' response cache disabled,
and captures query-only CPU/heap profiles. The retained servers must still be running.

If a server fails the live freshness gate, keep that failure in the report. To measure
reads separately, `drain.py <results/timestamp/10000>` disables new scrapes, restarts
vmagent to deliver its persistent queue, and checks the original historical window.
Only run `diagnose.py` after draining succeeds. This is a stopped-ingest comparison;
it does not establish sustainable write capacity. Original scrape config and drain
progress are retained under the fixture's `drain/` directory.

Periodic `*-oteldb.metrics` and `*-vmagent.metrics` snapshots record accepted points,
offered points, and remote-write queues alongside the resource samples.

VictoriaMetrics cache controls are documented in its
[single-node documentation](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#rollup-result-cache).

## Native batching follow-up

[CONCURRENCY_DESIGN.md](CONCURRENCY_DESIGN.md) is the separately requested Astra design.
To repeat write-only measurement with an explicit image:

```sh
python3 dev/local/scale-bench/write_pressure.py --image local/oteldb:scale-batched --nodes 10000 --seconds 300 --keep
```

From the application benchmark worktree, rebuild the corrected storage worktree with:

```sh
BENCH_STORAGE_DIR="$PWD/../concurrent-storage" \
  OTELDB_BENCH_IMAGE=local/oteldb:scale-concurrent bash dev/local/scale-bench/build.sh
OTELDB_BENCH_IMAGE=local/oteldb:scale-concurrent python3 dev/local/scale-bench/run.py --nodes 1000 --otelbench
```

Keep generated reports, counter summaries, profiles, validation output, raw benchmark
samples and benchstat output local under the ignored `results/` directory. Do not
force-add them to git, documentation or testdata. Put the relevant results and
limitations in PR descriptions or performance commit bodies. Version benchmark code,
tests and reusable methodology only.
Use `summarize_write.py <run-directory>...` to derive counters from retained raw runs.

`profile_metrics.py --queries --live-queries` keeps vmagent running during read measurement.
Without `--live-queries`, its query results describe stopped ingestion. The query-phase
timestamps and final counters distinguish these modes; neither short profile proves steady state.
Add `--dashboard-loads` to measure wall time for all 242 targets with the configured bounded
client count. Individual query timings, complete/incomplete loads, warmups and empty/error
counts are retained separately. `--subnet` supplies an unused explicit Docker subnet when
retained fixtures have exhausted the automatic pools.

For sustained read/write qualification, add `--soak-seconds 1800 --refresh-interval 15` to
`profile_metrics.py --queries --live-queries --dashboard-loads`. After the initial query phase,
it refreshes all 242 targets for each ready server in random-host and repeated-host modes with
the configured bounded client count. Random hosts advance with the query time; repeated queries
keep the same host and timestamp, with the first two refreshes marked as warmups. Resource and
queue sampling continues throughout. `soak-dashboards.jsonl` records every query and complete or
partial load, including refresh lateness; `soak-summary.json` records the phase times and loads.
`soak-freshness.json` repeats the all-host and two-minute sample-history gate after the soak,
while ingestion is still running, and records its evaluation timestamp.
Completion of the script is not an SLO pass: check all loads, individual timings, freshness,
accepted versus offered points, queue trends, OOM events and exit state.
For an image with direct native ingestion enabled, use
`--native-remote-write-url http://oteldb:9090/api/v1/write` and a config containing
`remote_write.enabled: true`. Record the ingestion path with the result; the default remains
the Collector/pdata receiver.

The current baseline uses published storage `v0.50.0`, including the merged stack:
[compact indexes #791](https://github.com/oteldb/storage/pull/791),
[metric batching #792](https://github.com/oteldb/storage/pull/792), and
[bounded concurrency #793](https://github.com/oteldb/storage/pull/793).
Record exact image/source hashes with results in the PR or commit body.

`build_shutdown_probe.py` builds a separate
signal-dump image via an overlay because the normal profiler closes on SIGTERM; it changes
no production source or shutdown policy.
