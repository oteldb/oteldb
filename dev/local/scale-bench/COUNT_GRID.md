# Count grids over the experimental metric lanes

`enable_count_grid.py` layers exact count routing onto the recorded eight-head overlay.
It is benchmark tooling, not released writable-head integration. Its writer hashes pin
canonical-ID partitioning and `AppendLimits{}`; unknown writer code is rejected for review.

Use the storage branch containing `fetch.MergeDisjoint` and the single-window count bridge:

```sh
python3 dev/local/scale-bench/enable_count_grid.py \
  --overlay /path/to/recorded/overlay.json \
  --app "$PWD" --storage /path/to/storage \
  --output /path/to/results/count-overlay.json
```

Build with that overlay and a temporary modfile replacing `github.com/oteldb/storage`.
The original overlay is preserved. Guarded per-lane fetchers retain query budgets,
retention and read-gap behavior. Generic `fetch.Merge` retains ID deduplication and
never sums counts from potentially overlapping children. Instant counts use one-step
presence grids; aggregate bucket-width rules are unchanged.

Before publishing a stale marker, the adapter stops granting native pushdown leases
and waits for existing leases to drain. New queries immediately use ordinary sample
evaluation; canceled writes can stop waiting. Invalidation is sticky and deliberately
conservative. Disabling only CountGrid would be insufficient: older aggregate/count
fallbacks also assume marker-free input. The first stale write may wait for an active
query, but new queries do not wait behind it.

Verify the complete adapter with the generated overlay and modfile:

```sh
E2E=1 go test -race -tags=headbench -modfile=/path/to/bench.mod \
  -overlay=/path/to/results/count-overlay.json ./integration/prome2e \
  -run '^TestHeadCountGridAdapter$'
```

`BenchmarkHeadCountProbe` uses 10k target hosts plus 15 unrelated metrics across eight
file-backed lanes (160k series), then measures the actual instant count expression.
Setup and flushes are excluded. Compare the same fixture with the original and enhanced
overlays; keep measurements and profiles in ignored results, not this source directory.
This fixture does not establish sustained live ingestion capacity.
