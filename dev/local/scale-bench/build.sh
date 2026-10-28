#!/usr/bin/env bash
set -euo pipefail
bench_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "$bench_dir/../../.." && pwd)"
mkdir -p "$bench_dir/bin"
cd "$repo_dir"
mod_args=()
storage_revision=""
storage_diff=""
if [[ -n "${BENCH_STORAGE_DIR:-}" ]]; then
  cp go.mod "$bench_dir/bin/bench.mod"
  cp go.sum "$bench_dir/bin/bench.sum"
  go mod edit -modfile="$bench_dir/bin/bench.mod" -replace="github.com/oteldb/storage=$BENCH_STORAGE_DIR"
  mod_args=("-modfile=$bench_dir/bin/bench.mod" "-mod=mod")
  storage_revision="$(git -C "$BENCH_STORAGE_DIR" rev-parse HEAD)"
  storage_diff="$(git -C "$BENCH_STORAGE_DIR" diff HEAD | sha256sum | cut -d ' ' -f 1)"
fi
CGO_ENABLED=0 GOOS=linux go build "${mod_args[@]}" -trimpath -buildvcs=false -o "$bench_dir/bin/oteldb" ./cmd/oteldb
CGO_ENABLED=0 GOOS=linux go build "${mod_args[@]}" -trimpath -buildvcs=false -o "$bench_dir/bin/otelbench" ./cmd/otelbench
docker build --build-arg "REVISION=$(git rev-parse HEAD)" \
  --build-arg "STORAGE_REVISION=$storage_revision" --build-arg "STORAGE_DIFF=$storage_diff" \
  -f "$bench_dir/Dockerfile" -t "${OTELDB_BENCH_IMAGE:-local/oteldb:scale-bench}" "$bench_dir/bin"
