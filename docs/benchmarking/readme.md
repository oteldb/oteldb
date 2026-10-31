# Benchmarking

## Ingestion

- [Metric consumer reuse measurements](metrics-consumer-reuse.md).
- [Attribute copying, batch arenas and identity cache research](attribute-ingestion-design.md).
- [Bounded WAL grouping and sealed metric heads: design proposal](wal-and-head-design.md).

### Prometheus remote write

See [ch-bench][ch-bench] for details.

[ch-bench]: ../../dev/local/ch-bench/README.md

## Data retrieval

```bash
kubectl -n clickhouse port-forward svc/chi-db-cluster-0-0 9000:9000
clickhouse-client
```

```sql
SELECT * FROM faster.logs INTO OUTFILE '/tmp/dump.bin' FORMAT Native;
```
