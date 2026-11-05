# Trace write-pressure benchmark

Run trace-only OTLP HTTP ingestion against native file storage with a persistent WAL.
The server receives an enforced 8-vCPU quota and 16-GiB memory limit, with no swap,
`GOMAXPROCS=8`, `GOMEMLIMIT=14GiB`, and a 10-second flush interval.

```sh
OTELDB_BENCH_IMAGE=local/oteldb:trace-bench bash dev/local/scale-bench/build.sh
go build -o dev/local/scale-bench/bin/trace-loadgen ./dev/local/trace-bench/loadgen
python3 dev/local/trace-bench/run.py --rate 1000000 --seconds 300
python3 dev/local/trace-bench/analyze.py dev/local/trace-bench/results/<timestamp>
```

The shared build script accepts `BENCH_STORAGE_DIR=/absolute/storage/worktree`
to measure a storage change without changing the application's dependency pin.
It records the storage revision and source diff hash on the image.

The rate is **uncompressed protobuf request-body bytes**, excluding HTTP headers.
It is distinct from encoded storage bytes. The sender checks HTTP status and OTLP
partial-success responses and reports actual bytes/s, spans, failures and rejections.
It runs outside the server's resource quota with `GOMAXPROCS=2`. The default shape
has 16 stable resource/scope identities, eight attributes per span, four spans per
trace and a 512-byte unique hexadecimal attribute. IDs and values are synthetic
and seeded; timestamps are fresh. `--payload`, `--services` and `--age` vary shape
and timestamp age while retaining the same byte rate.

Each run starts with a fresh named Docker volume and isolated Compose project.
Only local loopback ports are exposed (24090, 24133, 24318, 24910 and 24964).
Sampling every five seconds records process RSS, cgroup memory/page cache, CPU,
storage counters and admin head/part state. Heap and CPU profiles are captured
during ingestion; final heap profiles include forced-GC snapshots both immediately
after ingestion and after 45 seconds of draining.

The runner stops its server and retains the container and volume for investigation.
Use its generated `compose.json` with the recorded project name to restart or clean
up that run. It does not stop other containers or change any cluster deployments.
Raw profiles, samples and volumes are excluded from git.

Compare resident heap (`inuse_space`) with cumulative allocation (`alloc_space`)
and RSS separately. A process RSS snapshot or merge-traffic share cannot establish
retained RAM per signal. `summary.json` reports sampled maxima rather than claiming
an exact RSS peak. The cgroup samples include `memory.peak` for the container peak,
which also includes file cache. Short runs measure initial compaction behavior;
they do not establish long-retention steady state or reproduce another stand's
unreported span shape, history and configuration.
