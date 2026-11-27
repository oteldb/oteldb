package storagebackend

import (
	"testing"

	"github.com/go-faster/yaml"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"
)

func TestWALSyncModeConfig(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		yaml string
		want storage.WALSyncMode
		err  string
	}{
		{name: "default", yaml: "backend: file", want: storage.WALSyncNone},
		{name: "none", yaml: "wal_sync: none", want: storage.WALSyncNone},
		{name: "always", yaml: "wal_sync: always", want: storage.WALSyncAlways},
		{name: "interval", yaml: "wal_sync: interval", want: storage.WALSyncInterval},
		{name: "off", yaml: "wal_sync: off", want: storage.WALNone},
		{name: "invalid", yaml: "wal_sync: unknown", err: "wal_sync"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cfg Config
			require.NoError(t, yaml.Unmarshal([]byte(tc.yaml), &cfg))
			err := cfg.validate()
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, storeOptions(cfg, cacheSettings{}).WALSync)
		})
	}
}
