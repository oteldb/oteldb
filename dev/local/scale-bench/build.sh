#!/usr/bin/env bash
set -euo pipefail
bench_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "$bench_dir/../../.." && pwd)"
cd "$repo_dir"

require_clean_source() {
  local source_dir="$1"
  local dirty
  dirty="$(git -C "$source_dir" status --porcelain=v1 --untracked-files=all -- . ':(exclude).worktrees/**')"
  if [[ -n "$dirty" ]]; then
    printf 'Refusing benchmark build: commit source changes in %s first.\n%s\n' "$source_dir" "$dirty" >&2
    printf 'Ignored benchmark outputs are allowed; prototype builds need a separate provenance-aware builder.\n' >&2
    exit 1
  fi
}

require_clean_source "$repo_dir"
if [[ -n "${BENCH_STORAGE_DIR:-}" ]]; then
  require_clean_source "$BENCH_STORAGE_DIR"
fi
mkdir -p "$bench_dir/bin"
mod_args=()
storage_revision="$(go list -m -f '{{.Version}}' github.com/oteldb/storage)"
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
