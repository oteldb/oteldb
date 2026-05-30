#!/bin/bash

set -e

echo ">> Setup oteldb locally"
docker compose up -d --remove-orphans --build

DUMP_DIR="${DUMP_DIR:-/tmp/slowdump}"

echo ">> Upload benchmark data"
go run github.com/oteldb/oteldb/cmd/otelbench dump restore \
    --truncate \
    --database default \
    --input "${DUMP_DIR}"
