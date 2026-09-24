package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	fyaml "github.com/go-faster/figureout/source/yaml"
	"github.com/go-faster/yaml"
	"github.com/stretchr/testify/require"
)

// TestConfigResolvesTheTreesOwnFiles is the acceptance bar for the migration: every oteldb config
// checked into this repository has to resolve through the descriptor, with unknown keys rejected,
// to exactly the value the plain decoder produced.
//
// Hand-written fixtures test the shapes the descriptor was written for; these test the shapes it
// was not.
func TestConfigResolvesTheTreesOwnFiles(t *testing.T) {
	t.Parallel()

	d, err := descriptor()
	require.NoError(t, err)

	files := configFiles(t)
	require.NotEmpty(t, files, "the tree ships oteldb configs; finding none means the walk is wrong")

	for _, path := range files {
		t.Run(filepath.ToSlash(path), func(t *testing.T) {
			t.Parallel()

			data, err := os.ReadFile(path)
			require.NoError(t, err)

			var old Config
			require.NoError(t, yaml.Unmarshal(data, &old))
			old.setDefaults()

			next, _, err := d.Resolve(fyaml.Bytes(data, fyaml.DisallowUnknownFields()))
			require.NoError(t, err)
			next.setDefaults()

			require.Equal(t, nilEmpty(old), nilEmpty(next))
		})
	}
}

// configFiles finds the oteldb configs in the repository, by the name the binary reads them under.
func configFiles(tb testing.TB) []string {
	tb.Helper()

	var out []string
	root := filepath.Join("..", "..")
	require.NoError(tb, filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err //nolint:wrapcheck // walk errors are returned to the walker.
		}
		name := d.Name()
		if strings.HasPrefix(name, "oteldb") && (strings.HasSuffix(name, ".yml") || strings.HasSuffix(name, ".yaml")) {
			out = append(out, path)
		}

		return nil
	}))

	return out
}
