# PromQL Compatibility testing

https://github.com/prometheus/compliance/tree/main/promql#promql-compliance-tester

## Running

To build and install:
```
go install github.com/oteldb/oteldb/cmd/promql-compliance-tester
```

To run with targets in docker-compose:
```console
promql-compliance-tester -config-file promql-test-queries.yml -config-file test-oteldb.yml
```

> [!WARNING]  
> Results will be false-positive until enough data (~1h) is gathered.
> To fix that, change `-end` and `-range` flags.

For example, running for a couple of seconds is enough to get a relatively good result:
```console
promql-compliance-tester -end 2m -range 1m -config-file promql-test-queries.yml -config-file test-oteldb.yml
```

This will configure tester to check range from T-2m to T-1m.

## Notes

This check was disabled as being broken on latest prometheus reference:
```yaml
# label_replace fails when there would be duplicated identical output label sets.
# !!! HACK: This was disabled because current prometheus reference implementation does not fail !!!
- query: 'label_replace(demo_num_cpus, "instance", "", "", "")'
  should_fail: true
```
Was producing an error:
```
FATA[0000] Error running comparison: expected reference API query "label_replace(demo_num_cpus, \"instance\", \"\", \"\", \"\")" to fail, but succeeded  source="main.go:137
```

## Results

Scarecrow serves both backends. With classic histogram quantiles, the ClickHouse HTTP harness
passes **537 / 537 (100%)**, including three repeats per query. The frozen evaluation window is
pinned inside collected data; a quantile probe returns 27 series and 81 samples from each API,
including finite values. These are compatibility results, not production latency measurements.

The standalone verifier defaults to a 99.5% minimum; storage CI requires 100%. The broader upstream
corpus still has unsupported features; see [the engine's coverage report](../../../docs/promql-engine.md#81-where-the-corpus-stands).

Historical result from the retired Thanos fork:
```
Total: 547 / 548 (99.82%) passed, 0 unsupported
```
