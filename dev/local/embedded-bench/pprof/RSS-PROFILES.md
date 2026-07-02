# RSS profiles — oteldb under the 1 GiB cap (vs VictoriaMetrics)

Captured against `oteldb:head-7a2c0815` (file backend, GOMEMLIMIT=1GiB, 100
synthetic hosts ⇒ ~142.8k series) on the `/src/oteldb/benchmark` stack.
Symbolicate with the host-built `oteldb` binary of the same commit.

- `rss-heap-rest.pb.gz` — inuse_space, ingest at rest (post-GC). RSS 585 MiB,
  live heap 394 MiB. Dominated by per-series labels (Attributes.Intern +
  slices.Clone[[]uint8] ≈ 201 MiB) and indexes.
- `rss-heap-load.pb.gz` — inuse_space, under 4× concurrent heavy queries. RSS
  3.7 GiB, live heap 3.07 GB. **77% is `engine.growLen` (decoded ts+value
  columns) in `engine/stream.go` via `mergeSeries`.**
- `rss-allocs-load.pb.gz` — alloc_space over 15 s under the same load
  (~10 GB/15 s churn: growLen + chunk.resize).
- `rss-memstats-{rest,load}.txt` — runtime.MemStats at each point.

```bash
go tool pprof -top -inuse_space ./oteldb rss-heap-load.pb.gz
```
