package main

import (
	"reflect"
	"testing"

	fyaml "github.com/go-faster/figureout/source/yaml"
	"github.com/go-faster/yaml"
	"github.com/stretchr/testify/require"
)

// oteldbFixtures are the shapes an oteldb config is written in, weighted towards what only the
// root has: the named scalars, the pointer-heavy engine block and the collector passthrough.
var oteldbFixtures = []struct {
	name string
	data string
}{
	{"Empty", "{}\n"},
	{
		"Clickhouse",
		`dsn: clickhouse://user:pass@localhost:9000/oteldb
ttl: 720h
cluster: main
replicated: true
ch_log_level: debug
max_result_rows: 100000
max_result_bytes: 256MiB
max_execution_time: 30s
`,
	},
	{
		"Backends",
		`metrics_backend: storage
traces_backend: clickhouse
logs_backend: storage
profiles_backend: storage
storage:
  backend: s3
  wal_dir: /var/lib/oteldb/wal
  read_cache_bytes: 2GiB
  aggregate_stats: false
  s3:
    bucket: telemetry
    region: eu-central-1
    force_path_style: true
`,
	},
	{
		"StorageSections",
		`storage:
  policy:
    retention:
      max_age: 8760h
    limits:
      max_series: 1_000_000
  cluster:
    etcd: ["http://etcd:2379"]
    private_backend: true
`,
	},
	{"StorageEmptySections", "storage:\n  s3: {}\n  cluster: {}\n"},
	{"StorageNullSections", "storage:\n  s3: null\n  aggregate_stats: null\n"},
	{
		"APIs",
		`prometheus:
  bind: ":9090"
  enable_negative_offset: true
loki:
  bind: ":3100"
tempo:
  bind: ":3200"
pyroscope:
  bind: "-"
health_check:
  bind: ":13133"
admin:
  bind: ":8090"
auth:
  - type: bearertoken
    tokens:
      - token: secret
collector_signals:
  metrics: true
  logs: false
`,
	},
	{
		"Collector",
		`dsn: clickhouse://localhost:9000
otelcol:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: 0.0.0.0:4317
          max_recv_msg_size_mib: 512
  service:
    pipelines:
      logs:
        receivers: [otlp]
        exporters: [oteldbexporter]
`,
	},
	{"CollectorEmpty", "otelcol: {}\n"},
}

// TestConfigDecodesAsBefore requires the descriptor to resolve the same value the plain decoder
// produced, defaults included, for every shape an oteldb config is written in.
func TestConfigDecodesAsBefore(t *testing.T) {
	t.Parallel()

	d, err := descriptor()
	require.NoError(t, err)

	for _, tt := range oteldbFixtures {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var old Config
			require.NoError(t, yaml.Unmarshal([]byte(tt.data), &old))
			old.setDefaults()

			next, _, err := d.Resolve(fyaml.Bytes([]byte(tt.data)))
			require.NoError(t, err)
			next.setDefaults()

			require.Equal(t, nilEmpty(old), nilEmpty(next))
		})
	}
}

// TestConfigRejectsUnknownKey is oteldb#1285 at the binary that reads the config: a typo is a
// startup error naming the path rather than a setting that looks applied and is not.
func TestConfigRejectsUnknownKey(t *testing.T) {
	t.Parallel()

	d, err := descriptor()
	require.NoError(t, err)

	for _, tt := range []struct {
		name string
		data string
		want string
	}{
		{"Root", "max_result_rowz: 10\n", "max_result_rowz"},
		{"Nested", "storage:\n  backendd: s3\n", "storage.backendd"},
		{"OptionalSection", "storage:\n  s3:\n    buckett: x\n", "storage.s3.buckett"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := d.Resolve(fyaml.Bytes([]byte(tt.data), fyaml.DisallowUnknownFields()))
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.want)
		})
	}
}

// TestConfigPassesTheCollectorThrough is the other half: the collector's own keys are the
// collector's business, so strictness stops at the block rather than at the file.
func TestConfigPassesTheCollectorThrough(t *testing.T) {
	t.Parallel()

	d, err := descriptor()
	require.NoError(t, err)

	cfg, _, err := d.Resolve(fyaml.Bytes(
		[]byte("otelcol:\n  whatever_this_build_grew:\n    max_recv_msg_size_mib: 512\n"),
		fyaml.DisallowUnknownFields(),
	))
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"whatever_this_build_grew": map[string]any{"max_recv_msg_size_mib": 512},
	}, cfg.Collector)
}

// TestConfigRejectsAnUnknownLogLevel is go-faster/figureout#36 at the field that motivated it:
// bound as the int8 underneath it, "1" resolves to warn and the file means something other than
// what it says.
func TestConfigRejectsAnUnknownLogLevel(t *testing.T) {
	t.Parallel()

	d, err := descriptor()
	require.NoError(t, err)

	_, _, err = d.Resolve(fyaml.Bytes([]byte("ch_log_level: 1\n")))
	require.Error(t, err)
	require.Contains(t, err.Error(), "ch_log_level")

	cfg, _, err := d.Resolve(fyaml.Bytes([]byte("ch_log_level: error\n")))
	require.NoError(t, err)
	require.Equal(t, "error", cfg.CHLogLevel.String())
}

// nilEmpty erases the one difference the two decoders are allowed to have: an absent collection is
// nil under plain unmarshalling and empty under figureout, which resolves a collection nobody
// configured to an empty value so it encodes as [] rather than null.
func nilEmpty(cfg Config) Config {
	normalize(reflect.ValueOf(&cfg).Elem())

	return cfg
}

func normalize(v reflect.Value) {
	switch v.Kind() {
	case reflect.Slice:
		if v.Len() == 0 {
			v.Set(reflect.Zero(v.Type()))
			return
		}
		for i := range v.Len() {
			normalize(v.Index(i))
		}
	case reflect.Map:
		if v.Len() == 0 {
			v.Set(reflect.Zero(v.Type()))
		}
	case reflect.Struct:
		for _, f := range v.Fields() {
			if f.CanSet() {
				normalize(f)
			}
		}
	case reflect.Pointer:
		if !v.IsNil() {
			normalize(v.Elem())
		}
	default:
	}
}
