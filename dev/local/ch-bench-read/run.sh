#!/bin/bash

clean_up() {
    ARG=$?
    echo ">> Stopping"
    docker compose down -v
    exit $ARG
}
trap clean_up EXIT

set -e

echo ">> Setup oteldb locally"
docker compose up -d --remove-orphans --build --force-recreate

echo ">> Upload benchmark data"
wget -N https://storage.yandexcloud.net/faster-public/oteldb/req.rwq
go run github.com/oteldb/oteldb/cmd/otelbench promrw replay -i req.rwq

queries_file="${BENCH_QUERIES:-testdata/node-exporter-selected.promql.yml}"

echo ">> Warmup"
go run github.com/oteldb/oteldb/cmd/otelbench promql bench \
    -i "$queries_file" \
    --warmup 10

benchmark_runs="${BENCH_RUNS:-15}"

echo ">> Benchmark"
OTEL_EXPORTER_OTLP_INSECURE="true" go run github.com/oteldb/oteldb/cmd/otelbench promql bench \
    -i "$queries_file" \
    -o report.yml \
    --trace \
    --allow-empty=false \
    --count "$benchmark_runs" \
    --warmup 5

echo ">> Done"
