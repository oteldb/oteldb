# Read benchmarks

To run benchmarks, use `run.sh` script.

# Comparing changes

To set queries file, use `BENCH_QUERIES` environment variable.
To set number of benchmark runs, use `BENCH_RUNS` environment variable.

Checkout base commit or `git stash push` current changes.

### Run benchmark on base

```console
$ cd dev/local/ch-bench-read
$ BENCH_QUERIES=bench-series.yml ./run.sh
```

Then save result in benchstat format:

```console
$ go run github.com/oteldb/oteldb/cmd/otelbench promql analyze -f 'benchstat' -i report.yml | tee bench.old.txt
```

### Run benchmark on changes

```console
$ BENCH_QUERIES=bench-series.yml ./run.sh
$ go run github.com/oteldb/oteldb/cmd/otelbench promql analyze -f 'benchstat' -i report.yml | tee bench.new.txt
```

### Compare results

```console
$ benchstat old=bench.old.txt new=bench.new.txt
```
