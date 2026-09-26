package storagebackup

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/storagebackend"
)

// TestEngineLayout pins that odbbackup (read-only) and odbrestore (read-write) resolve the same
// parts and WAL directories as a node configured with the same storage block.
func TestEngineLayout(t *testing.T) {
	dir := t.TempDir()
	wal := filepath.Join(t.TempDir(), "wal")

	configPath := func(t *testing.T, body string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "oteldb.yml")
		require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
		return p
	}

	for _, tt := range []struct {
		name string
		cfg  EngineConfig
		node storagebackend.Config
	}{
		{
			name: "Dir",
			cfg:  EngineConfig{Dir: dir},
			node: storagebackend.Config{Backend: "file", Dir: dir},
		},
		{
			name: "ConfigDefault",
			cfg:  EngineConfig{Path: configPath(t, "storage:\n  backend: file\n  dir: "+dir+"\n")},
			node: storagebackend.Config{Backend: "file", Dir: dir},
		},
		{
			name: "ConfigWALDir",
			cfg: EngineConfig{Path: configPath(t,
				"storage:\n  backend: file\n  dir: "+dir+"\n  wal_dir: "+wal+"\n")},
			node: storagebackend.Config{Backend: "file", Dir: dir, WALDir: wal},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			want, err := tt.node.Layout()
			require.NoError(t, err)

			for _, readOnly := range []bool{true, false} {
				cfg := tt.cfg
				cfg.ReadOnly = readOnly
				scfg, err := storageConfig(cfg)
				require.NoError(t, err)
				require.Equal(t, readOnly, scfg.ReadOnly)

				got, err := scfg.Layout()
				require.NoError(t, err)
				require.Equal(t, want, got, "read-only: %v", readOnly)
			}
		})
	}
}
