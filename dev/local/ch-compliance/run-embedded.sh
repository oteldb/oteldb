#!/bin/bash
# Same as run.sh, but against oteldb on the embedded storage engine
# (docker-compose.embedded.yml) with internal/scarecrow layered on top
# (docker-compose.embedded.scarecrow.yml, mounting oteldb.embedded.yml), instead of the
# ClickHouse/fork-engine combo run.sh exercises. The base compose file stays scarecrow-free
# because .github/workflows/storage-compliance.yml runs it directly, gated at 100%.

clean_up () {
    ARG=$?
    echo ">> Stopping"
    docker compose -f docker-compose.embedded.yml -f docker-compose.embedded.scarecrow.yml down -v
    exit $ARG
}
trap clean_up EXIT

set -e

docker compose -f docker-compose.embedded.yml -f docker-compose.embedded.scarecrow.yml up -d --remove-orphans --build --force-recreate

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
