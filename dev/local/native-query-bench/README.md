# Native query-only fixture

`BenchmarkNativeDashboard10K` opens persisted native FILE parts read-only, verifies
10k hosts and eight samples per host over 120 seconds, then runs the 242 dashboard
targets directly through the production Scarecrow/native scanner wiring.
There is no ingestion, WAL replay, HTTP encoding or background maintenance.

Prepare a fixture from a **stopped**, completed 10k scale benchmark:

```sh
python3 dev/local/native-query-bench/snapshot.py \
  --run /path/to/scale-bench/results/profile-RUN \
  --output "$HOME/.local/share/oteldb/fixtures/native-10k"
```

The snapshot requires a local reflink-capable filesystem, such as Btrfs.
The parts are a separate COW copy. The fixed end precedes shutdown by two minutes;
the benchmark refuses a fixture that fails its series/host/history checks.
Keep fixtures outside worktrees and tracked testdata.

```sh
GOMAXPROCS=8 GOMEMLIMIT=14GiB \
OTELDB_NATIVE_QUERY_FIXTURE="$HOME/.local/share/oteldb/fixtures/native-10k" \
go test ./internal/storagebackend -run '^$' \
  -bench '^BenchmarkNativeDashboard10K$/range=60s/random' \
  -benchtime=30s -benchmem -timeout=30m
```

Each operation is a whole dashboard with four clients. Cases cover 60s and 600s
windows at 15s steps, using random hosts or repeated `host-0`, all at the same
historical timestamp. Random hosts do not imply cold OS or storage caches.
`OTELDB_NATIVE_QUERY_TARGET=126` restricts execution to one dashboard target.

For dense query-only profiles, preserve a test executable and set the profile directory:

```sh
CGO_ENABLED=0 go test -c -o /tmp/native-query-bench.test ./internal/storagebackend
GOMAXPROCS=8 GOMEMLIMIT=14GiB \
OTELDB_NATIVE_QUERY_FIXTURE="$HOME/.local/share/oteldb/fixtures/native-10k" \
OTELDB_NATIVE_QUERY_PROFILE_DIR=/tmp/native-query-profiles \
/tmp/native-query-bench.test -test.run '^$' \
  -test.bench '^BenchmarkNativeDashboard10K$/range=60s/random' \
  -test.benchtime=30s -test.benchmem -test.timeout=30m -test.v
```

CPU profiling starts **after fixture loading, validation and warmup**. Each case
gets its own profile directory. Allocation profiles bracket only the query phase:

```sh
go tool pprof /tmp/native-query-bench.test PROFILE_DIR/cpu.pb.gz
go tool pprof -alloc_space -base PROFILE_DIR/allocs-before.pb.gz \
  /tmp/native-query-bench.test PROFILE_DIR/allocs-after.pb.gz
```

Standard `-cpuprofile` includes initialization; use the profile-directory option to
exclude the large identity-index load. Compare before/after against the same
snapshot and fixed window. Query-only results do not establish live-ingestion capacity.

To enforce the original CPU/memory limits and filesystem read-only access, run the
static test executable in Docker:

```sh
fixture="$HOME/.local/share/oteldb/fixtures/native-10k"
profiles=/tmp/native-query-profiles
mkdir -p "$profiles"
docker run --rm --network none --read-only --cpus 8 --memory 16g --memory-swap 16g \
  --mount "type=bind,source=$fixture,destination=/fixture,readonly" \
  --mount type=bind,source=/tmp/native-query-bench.test,destination=/bench.test,readonly \
  --mount "type=bind,source=$profiles,destination=/profiles" \
  -e GOMAXPROCS=8 -e GOMEMLIMIT=14GiB \
  -e OTELDB_NATIVE_QUERY_FIXTURE=/fixture \
  -e OTELDB_NATIVE_QUERY_PROFILE_DIR=/profiles \
  alpine:3.22 /bench.test -test.run '^$' \
  -test.bench '^BenchmarkNativeDashboard10K$/range=60s/random' \
  -test.benchtime=30s -test.benchmem -test.timeout=30m -test.v
```
