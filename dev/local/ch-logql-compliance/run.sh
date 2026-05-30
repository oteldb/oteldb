#!/bin/bash

clean_up() {
  ARG=$?
  echo ">> Stopping"
  docker compose down -v
  exit $ARG
}
trap clean_up EXIT

set -e

docker compose up -d --remove-orphans --build --force-recreate

echo ">> Testing oteldb implementation"
RANGE="1m"
END="5s"
go run github.com/oteldb/oteldb/cmd/logql-compliance-tester \
  -end "${END}" -range "${RANGE}" \
  -config-file logql-test-queries.yml -config-file test-oteldb.yml \
  -output-format json -output-file result.oteldb.json \
  -print-failed || true
