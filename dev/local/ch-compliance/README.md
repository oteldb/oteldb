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

Scarecrow is the default engine for both backends. The ClickHouse HTTP harness measured
525 / 537 (97.77%) on a frozen dataset using the CI procedure (stop demo generators, restart
Prometheus). Twelve `histogram_quantile` cases fail with an explicit unsupported-expression
error. This is below the unchanged compatibility gate; see [#1420](https://github.com/oteldb/oteldb/issues/1420).
The ordinary live-data run measured 523 / 537, with two additional volatile-gauge differences.
These results verify compatibility, not production latency.

Historical result from the retired Thanos fork:
```
Total: 547 / 548 (99.82%) passed, 0 unsupported
```

We are getting (99.6, 100] percent and current target is compatibility not less than 99.6%.