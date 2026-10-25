#!/bin/bash
# PromQL compliance over native storage and the default Scarecrow engine.

clean_up () {
    ARG=$?
    echo ">> Stopping"
    docker compose -f docker-compose.embedded.yml down -v
    exit $ARG
}
trap clean_up EXIT

set -e

docker compose -f docker-compose.embedded.yml up -d --remove-orphans --build --force-recreate

go run ./cmd/compliance-wait -wait 10s

echo ">> Testing oteldb implementation (embedded storage + scarecrow engine)"
RANGE="1m"
END="1m"
go run github.com/oteldb/oteldb/cmd/promql-compliance-tester \
  -end "${END}" -range "${RANGE}" \
  -query-repeats=3 \
  -config-file promql-test-queries.yml -config-file test-oteldb.yml \
  -output-format json > result.oteldb.embedded.json || true

go run ./cmd/compliance-verify result.oteldb.embedded.json
