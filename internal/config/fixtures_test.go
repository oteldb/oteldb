package config_test

// differentialFixtures are hand-written configs covering the spellings the real files use and the
// edges the two decoders could plausibly disagree on. They double as the fuzzer's seed corpus.
var differentialFixtures = []struct {
	name string
	data string
}{
	{"Empty", "{}\n"},
	{"EmptyBlocks", "prometheus: {}\nloki: {}\ncluster: {}\n"},
	{
		"Listeners",
		`prometheus:
  bind: ":9090"
loki:
  bind: 0.0.0.0:3100
tempo:
  bind: ":3200"
pyroscope:
  bind: ":4040"
admin:
  bind: ":8090"
health_check:
  bind: ":13133"
`,
	},
	{
		// "-" disables an API in odbselect. It is an ordinary string to both decoders, and that is
		// exactly what has to stay true.
		"DisabledBind",
		"prometheus:\n  bind: \"-\"\nloki:\n  bind: '-'\ntempo:\n  bind: \"-\"\npyroscope:\n  bind: \"-\"\n",
	},
	{
		"HumanBytes",
		`prometheus:
  cache:
    max_bytes: 100MiB
    safety_lag: 30s
loki:
  max_sample_result_bytes: 256MiB
`,
	},
	{
		"NumericBytes",
		"prometheus:\n  cache:\n    max_bytes: 268435456\nloki:\n  max_sample_result_bytes: 1024\n",
	},
	{
		// YAML 1.1 underscore separators, as dev/local/embedded-bench/oteldb-s3.yml writes them.
		"UnderscoreInteger",
		"prometheus:\n  max_samples: 10_000_000\n  max_timeseries: 1_000_000\n",
	},
	{
		"Durations",
		`prometheus:
  timeout: 5m
  lookback_delta: 1h30m
loki:
  lookback_delta: 0s
cluster:
  dial_timeout: 1500ms
`,
	},
	{
		// A *bool distinguishes "unset" from "explicitly false", which is what its default hangs on.
		"NegativeOffsetTrue",
		"prometheus:\n  enable_negative_offset: true\n",
	},
	{"NegativeOffsetFalse", "prometheus:\n  enable_negative_offset: false\n"},
	{"NegativeOffsetAbsent", "prometheus:\n  max_samples: 1\n"},
	{
		"Booleans",
		`prometheus:
  enable_at_modifier: true
  enable_per_step_stats: false
  enable_scarecrow_engine: true
  disable_rate_offloading: true
  disable_metric_offloading: false
loki:
  drilldown_enabled: true
`,
	},
	{
		// A list nested in a list element, which is where a descriptor is easiest to get wrong.
		"AuthTokens",
		`auth:
  - type: bearertoken
    tokens:
      - token: secret
      - token_file: /run/secrets/token
`,
	},
	{
		"AuthUsers",
		`prometheus:
  auth:
    - type: basicauth
      users:
        - user: alice
          password: hunter2
        - user: bob
          password_file: /run/secrets/bob
`,
	},
	{
		"AuthMixed",
		`auth:
  - type: none
  - type: bearertoken
    tokens:
      - token: a
  - type: basicauth
    users:
      - user: u
        password: p
`,
	},
	{"AuthEmptyList", "auth: []\n"},
	{
		"Cluster",
		`cluster:
  etcd: ["http://etcd:2379", "http://etcd2:2379"]
  root: /oteldb
  rf: 2
  shards_per_tenant: 8
  dial_timeout: 5s
`,
	},
	{
		"Anchors",
		`x-listener: &listener
  bind: ":9090"
prometheus:
  <<: *listener
  max_samples: 10
`,
	},
	{
		"QuotedNumbers",
		"prometheus:\n  bind: \"9090\"\n  max_samples: 10\n",
	},
	{
		"Nulls",
		"prometheus:\n  bind: null\n  max_samples: ~\nauth: null\n",
	},
	{
		"NegativeAndZero",
		"prometheus:\n  max_samples: 0\n  max_timeseries: -1\ncluster:\n  rf: 0\n",
	},
	{
		// The engine block is pointer-heavy on purpose: an unset cache size means "fit it to the
		// machine", which is not zero, so nil has to survive the descriptor.
		"StoragePointers",
		`storage:
  backend: s3
  read_cache_bytes: 2GiB
  decode_cache_bytes: 0
  aggregate_stats: false
  s3:
    bucket: telemetry
    prefix: oteldb/
    force_path_style: true
`,
	},
	{"StorageAbsentSections", "storage:\n  backend: file\n  dir: /var/lib/oteldb\n"},
	{
		// An empty section is a section: "s3: {}" allocates, and no s3 key at all does not.
		"StorageEmptySections",
		"storage:\n  s3: {}\n  cluster: {}\n  policy: {}\n",
	},
	{
		"StorageNullSections",
		"storage:\n  s3: null\n  policy: ~\n  aggregate_stats: null\n",
	},
	{
		"StoragePolicy",
		`storage:
  policy:
    precision:
      - after: 168h
        bits: 12
    downsample:
      - after: 720h
        interval: 5m
        agg: avg
    recompress:
      after: 2160h
      level: 12
    retention:
      max_age: 8760h
      max_bytes: 10TiB
    limits:
      ingest_bytes_per_second: 50MiB
      max_series: 1000000
`,
	},
	{
		"StorageCluster",
		`storage:
  cluster:
    etcd: ["http://etcd:2379"]
    id: oteldb-0
    zone: eu-central-1a
    port: 7946
    rf: 3
    private_backend: true
`,
	},
	{
		// The collector block is carried verbatim, so the spelling of every scalar in it has to
		// come out the way the plain decoder read it.
		"Collector",
		`otelcol:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: 0.0.0.0:4317
          max_recv_msg_size_mib: 512
        http:
          endpoint: "0.0.0.0:4318"
    prometheusremotewrite: {}
  exporters:
    oteldbexporter:
      dsn: clickhouse://localhost:9000
  service:
    pipelines:
      metrics:
        receivers: [otlp, prometheusremotewrite]
        exporters: [oteldbexporter]
    telemetry: null
`,
	},
	{"CollectorEmpty", "otelcol: {}\n"},
	{"CollectorNull", "otelcol: null\n"},
	{
		"CollectorScalarSpellings",
		"otelcol:\n  a: 512\n  b: \"512\"\n  c: true\n  d: \"true\"\n  e: 1.5\n  f: 0x10\n  g: 010\n  h: 1_000\n",
	},
	{
		"LogLevelAndBytes",
		"storage:\n  merge_memory_bytes: 1GiB\n  decode_memory_bytes: 512MiB\n",
	},
}
