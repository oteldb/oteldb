# Collector 0.163 compatibility

The dependency update uses Collector core 1.69/0.163 and Prometheus 0.315.
Scarecrow remains the primary query engine; there is no Thanos engine dependency.

The embedded collector keeps the application's logger, name, fields, and level.
It does not rebuild logging from Collector telemetry configuration. The removed
`LoggerSettings.ZapOptions` field does not affect application logging.
Prometheus client queries now return separate informational annotations as well
as warnings. The compliance runner accepts both; it continues comparing values.
Scarecrow propagates ordinary NaN samples through `mad_over_time`, matching
Prometheus 3.14 and later. Results recover after the sample leaves the window.
The function remains experimental and requires an explicitly enabled parser;
this update does not enable it on the default HTTP API.

## Agent configuration and output

These upstream changes affect `odbagent` components when configured:

- Stanza receivers supply a default instrumentation scope name and version when
  one is absent (`pkg.stanza.addDefaultScopeName`, beta). This can change log
  stream identity. Disable that gate to retain empty scopes during migration.
- Kubernetes attributes use singular `k8s.pod.label.*` convention names. Disable
  `processor.k8sattributes.DontEmitV0K8sConventions` to emit both conventions.
  `deployment_name_from_replicaset` is removed; remove the configuration key.
  Kubernetes processor internal metrics also use the new format by default.
- Kubelet CPU usage is calculated between scrapes and is absent on the first
  scrape (`receiver.kubeletstats.cpuUsageScrapeBased`, beta).
- Prometheus exporters preserve leading/consecutive underscores by default
  (`pkg.translator.prometheus.PermissiveLabelSanitization`, beta). Disabling the
  gate restores earlier sanitization.
- The Prometheus receiver's `IgnoreScopeInfoMetric` gate is stable. Scope
  attributes come from `otel_scope_*` labels; `otel_scope_info` is an ordinary
  metric. Malformed classic histograms are dropped. Invalid exemplar IDs remain
  attributes instead of being padded or truncated.
- Failover conditions reject empty `error.contains` entries and only react to
  matching downstream errors. Kafka deprecated TLS/authentication settings and
  `group_rebalance_strategy` are removed; use the current equivalents.
- HTTP keepalive settings use the nested `keepalive` block. Deprecated flat
  settings still work individually, but combining both forms is an error.

The local Prometheus remote-write receiver uses this repository's converter.
Changes to the upstream remote-write receiver do not update that converter.
The xhash hash format changes upstream; the local use is a test/benchmark helper,
not storage identity or persisted state.

The original dependency group retains the stable-transition modules
`countconnector`, `routingconnector`, `signaltometricsconnector`, `sumconnector`,
`attributesprocessor`, `filterprocessor`, `transformprocessor`, and
`hostmetricsreceiver` at 0.159. Their major-version migration is separate. In
particular, the new OTTL and signal-to-metrics defaults do not apply to those
retained versions.

See the [Collector core changelog](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.163.0/CHANGELOG.md),
[contrib changelog](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.163.0/CHANGELOG.md),
and [Prometheus changelog](https://github.com/prometheus/prometheus/blob/v0.315.0/CHANGELOG.md).
