#!/bin/bash

echo ">> Benchmark"
start_time="2024-11-20T00:00:00Z"
end_time="2024-12-01T00:00:00Z"

queries_file="${BENCH_QUERIES:-testdata/logql.yml}"
benchmark_runs="${BENCH_RUNS:-15}"

OTEL_EXPORTER_OTLP_INSECURE="true" go run github.com/oteldb/oteldb/cmd/otelbench logql bench \
    --start "$start_time" \
    --end "$end_time" \
    -i "$queries_file" \
    -o report.yml \
    --trace \
    --allow-empty=false \
    --count "$benchmark_runs"

echo ">> Analyze"
go run github.com/oteldb/oteldb/cmd/otelbench logql analyze \
    --input report.yml \
    --format benchstat >benchstat.report.txt

echo ">> Benchstat"
benchstat benchstat.report.txt
