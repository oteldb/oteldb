package main

import (
	"reflect"
	"testing"

	fyaml "github.com/go-faster/figureout/source/yaml"
	"github.com/go-faster/yaml"
	"github.com/stretchr/testify/require"
)

// odbingestFixtures are the shapes an odbingest config is written in.
var odbingestFixtures = []struct {
	name string
	data string
}{
	{"Empty", "{}\n"},
	{"Minimal", "cluster:\n  etcd: [\"127.0.0.1:2379\"]\n"},
	{
		"Full",
		`cluster:
  etcd: ["etcd:2379"]
  root: /oteldb
  rf: 2
  shards_per_tenant: 8
  dial_timeout: 5s
prometheus_remote_write:
  bind: ":19291"
  path: /api/v1/write
  time_threshold: 24h
  max_body_bytes: 64MiB
  max_decoded_bytes: 256MiB
  read_header_timeout: 5s
  shutdown_timeout: 15s
otlp:
  grpc_bind: "-"
  max_body_bytes: 67108864
  max_decoded_bytes: 268435456
tenant:
  default: default
  header: X-Scope-OrgID
  resource_attributes: ["service.namespace"]
  require: true
`,
	},
}

// TestConfigDecodesAsBefore requires the descriptor to resolve the same value the plain decoder
// produced, for every shape an odbingest config is written in.
func TestConfigDecodesAsBefore(t *testing.T) {
	t.Parallel()

	d, err := descriptor()
	require.NoError(t, err)

	for _, tt := range odbingestFixtures {
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

// TestConfigRejectsUnknownKey is oteldb#1285 at the binary that reads the config.
func TestConfigRejectsUnknownKey(t *testing.T) {
	t.Parallel()

	d, err := descriptor()
	require.NoError(t, err)

	_, _, err = d.Resolve(fyaml.Bytes(
		[]byte("otlp:\n  grpc_bnid: \":4317\"\n"), fyaml.DisallowUnknownFields()))
	require.Error(t, err)
	require.Contains(t, err.Error(), "otlp.grpc_bnid")
}

// nilEmpty treats an absent collection and an empty one as the same value: figureout resolves a
// collection nobody configured to an empty one rather than to nil, and every consumer here tests
// it with len().
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
