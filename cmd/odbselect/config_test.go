package main

import (
	"reflect"
	"testing"

	fyaml "github.com/go-faster/figureout/source/yaml"
	"github.com/go-faster/yaml"
	"github.com/stretchr/testify/require"
)

// odbselectFixtures are the shapes an odbselect config is written in.
var odbselectFixtures = []struct {
	name string
	data string
}{
	{"Empty", "{}\n"},
	{"Minimal", "cluster:\n  etcd: [\"127.0.0.1:2379\"]\n"},
	{
		"DisabledAPIs",
		`cluster:
  etcd: ["etcd:2379"]
  rf: 2
  shards_per_tenant: 8
  dial_timeout: 5s
prometheus:
  bind: ":9090"
  max_samples: 10_000_000
loki:
  bind: "-"
tempo:
  bind: "-"
pyroscope:
  bind: "-"
health:
  bind: ":13133"
shutdown_timeout: 45s
`,
	},
	{
		"Auth",
		`cluster:
  etcd: ["etcd:2379"]
prometheus:
  bind: ":9090"
  auth:
    - type: bearertoken
      tokens:
        - token: secret
        - token_file: /run/secrets/token
loki:
  auth:
    - type: basicauth
      users:
        - user: alice
          password: hunter2
`,
	},
}

// TestConfigDecodesAsBefore requires the descriptor to resolve the same value the plain decoder
// produced, for every shape an odbselect config is written in.
func TestConfigDecodesAsBefore(t *testing.T) {
	t.Parallel()

	d, err := descriptor()
	require.NoError(t, err)

	for _, tt := range odbselectFixtures {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var old Config
			require.NoError(t, yaml.Unmarshal([]byte(tt.data), &old))
			old.setDefaults()

			next, _, err := d.Resolve(fyaml.Bytes([]byte(tt.data)))
			require.NoError(t, err)
			next.setDefaults()

			requireSameConfig(t, old, next)
		})
	}
}

// TestConfigRejectsUnknownKey is oteldb#1285 at the binary that reads the config.
func TestConfigRejectsUnknownKey(t *testing.T) {
	t.Parallel()

	d, err := descriptor()
	require.NoError(t, err)

	_, _, err = d.Resolve(fyaml.Bytes([]byte("cluster:\n  shards_per_tennant: 8\n"), fyaml.DisallowUnknownFields()))
	require.Error(t, err)
	require.Contains(t, err.Error(), "cluster.shards_per_tennant")
}

// requireSameConfig compares two configs, treating an absent collection and an empty one as the
// same value: figureout resolves a collection nobody configured to an empty one rather than to nil,
// and every consumer here tests it with len().
func requireSameConfig(tb testing.TB, old, next Config) {
	tb.Helper()

	require.Equal(tb, nilEmpty(old), nilEmpty(next))
}

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
