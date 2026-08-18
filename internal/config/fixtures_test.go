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
}
