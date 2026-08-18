package config_test

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/go-faster/figureout"
	fyaml "github.com/go-faster/figureout/source/yaml"
	"github.com/go-faster/yaml"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/config"
)

// diffConfig composes every block the package ships, so one fixture exercises all of them.
type diffConfig struct {
	Prometheus  config.Prometheus  `json:"prometheus" yaml:"prometheus"`
	Loki        config.Loki        `json:"loki" yaml:"loki"`
	Tempo       config.Tempo       `json:"tempo" yaml:"tempo"`
	Pyroscope   config.Pyroscope   `json:"pyroscope" yaml:"pyroscope"`
	Admin       config.Admin       `json:"admin" yaml:"admin"`
	HealthCheck config.HealthCheck `json:"health_check" yaml:"health_check"`
	Cluster     config.Cluster     `json:"cluster" yaml:"cluster"`
	Auth        []config.Auth      `json:"auth" yaml:"auth"`
}

// diffBlocks are the top-level keys diffConfig claims.
var diffBlocks = []string{
	"prometheus", "loki", "tempo", "pyroscope", "admin", "health_check", "cluster", "auth",
}

func describeDiff(c *diffConfig, s *figureout.Schema[diffConfig]) {
	figureout.Group(s, "prometheus", func(s *figureout.Schema[diffConfig]) {
		config.DescribePrometheus(s, &c.Prometheus)
	})
	figureout.Group(s, "loki", func(s *figureout.Schema[diffConfig]) {
		config.DescribeLoki(s, &c.Loki)
	})
	figureout.Group(s, "tempo", func(s *figureout.Schema[diffConfig]) {
		config.DescribeTempo(s, &c.Tempo)
	})
	figureout.Group(s, "pyroscope", func(s *figureout.Schema[diffConfig]) {
		config.DescribePyroscope(s, &c.Pyroscope)
	})
	figureout.Group(s, "admin", func(s *figureout.Schema[diffConfig]) {
		config.DescribeAdmin(s, &c.Admin)
	})
	figureout.Group(s, "health_check", func(s *figureout.Schema[diffConfig]) {
		config.DescribeHealthCheck(s, &c.HealthCheck)
	})
	figureout.Group(s, "cluster", func(s *figureout.Schema[diffConfig]) {
		config.DescribeCluster(s, &c.Cluster)
	})
	figureout.ListOf(s, &c.Auth, "auth", config.DescribeAuth)
}

func diffDescriptor(tb testing.TB) *figureout.Descriptor[diffConfig] {
	tb.Helper()

	d, err := config.Descriptor(describeDiff)
	require.NoError(tb, err)

	return d
}

// decodeBoth reads data with the plain yaml decoder and with the descriptor.
func decodeBoth(d *figureout.Descriptor[diffConfig], data []byte) (old, next diffConfig, oldErr, nextErr error) {
	oldErr = yaml.Unmarshal(data, &old)
	next, _, nextErr = d.Resolve(fyaml.Bytes(data))

	return old, next, oldErr, nextErr
}

// normalize erases the one difference the two decoders are allowed to have: an absent collection
// is nil under plain unmarshalling and empty under figureout, which resolves a collection nobody
// configured to an empty value so it encodes as [] rather than null. Every consumer in this repo
// tests such a field with len(), so the two are interchangeable — but they are not equal, and the
// harness names the difference it ignores rather than comparing loosely.
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

func normalized[C any](cfg C) C {
	normalize(reflect.ValueOf(&cfg).Elem())
	return cfg
}

// requireSameDecode requires both decoders to agree on data, whether they accept or reject it.
func requireSameDecode(tb testing.TB, d *figureout.Descriptor[diffConfig], data []byte) {
	tb.Helper()

	old, next, oldErr, nextErr := decodeBoth(d, data)
	require.NoError(tb, oldErr, "the plain decoder must accept the fixture")
	require.NoError(tb, nextErr, "the descriptor must accept the fixture")
	require.Equal(tb, normalized(old), normalized(next))
}

func TestDifferentialFixtures(t *testing.T) {
	t.Parallel()

	d := diffDescriptor(t)
	for _, tt := range differentialFixtures {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			requireSameDecode(t, d, []byte(tt.data))
		})
	}
}

// TestDifferentialTreeFixtures decodes the config files checked into the tree.
//
// They are the shapes actually deployed, so a difference here is one an operator would hit. Only
// the blocks this package owns are compared: a fixture's other keys belong to a root defined by a
// binary, so they are dropped before decoding — at the node level, which keeps every scalar
// spelled the way the file spells it.
func TestDifferentialTreeFixtures(t *testing.T) {
	t.Parallel()

	d := diffDescriptor(t)
	for _, path := range treeFixtures(t) {
		t.Run(filepath.Base(filepath.Dir(path))+"/"+filepath.Base(path), func(t *testing.T) {
			t.Parallel()

			data, err := os.ReadFile(path)
			require.NoError(t, err)

			blocks, ok := knownBlocks(t, data)
			if !ok {
				t.Skip("no block this package owns")
			}
			requireSameDecode(t, d, blocks)
		})
	}
}

// knownBlocks re-emits the top-level keys diffConfig claims, preserving their original nodes.
func knownBlocks(tb testing.TB, data []byte) ([]byte, bool) {
	tb.Helper()

	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, false
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, false
	}

	root := doc.Content[0]
	out := &yaml.Node{Kind: yaml.MappingNode, Tag: root.Tag}
	for _, node := range []*yaml.Node{root, mapping(root, "config")} {
		if node == nil {
			continue
		}
		for i := 0; i+1 < len(node.Content); i += 2 {
			if key := node.Content[i]; slices.Contains(diffBlocks, key.Value) {
				out.Content = append(out.Content, key, node.Content[i+1])
			}
		}
	}
	if len(out.Content) == 0 {
		return nil, false
	}

	blocks, err := yaml.Marshal(out)
	require.NoError(tb, err)

	return blocks, true
}

// mapping returns the mapping node at key, such as a Helm values file's config block.
func mapping(node *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key && node.Content[i+1].Kind == yaml.MappingNode {
			return node.Content[i+1]
		}
	}

	return nil
}

func treeFixtures(tb testing.TB) []string {
	tb.Helper()

	root := filepath.Join("..", "..")
	var out []string
	for _, dir := range []string{"dev", ".k8s", "helm"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			switch filepath.Ext(path) {
			case ".yml", ".yaml":
				out = append(out, path)
			}
			return nil
		})
		require.NoError(tb, err)
	}
	require.NotEmpty(tb, out)

	return out
}
