package storagebackend

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/go-faster/sdk/app"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestConfigLayout(t *testing.T) {
	dir := t.TempDir()
	other := t.TempDir()

	for _, tt := range []struct {
		name string
		cfg  Config
		want Layout
		err  string
	}{
		{name: "Memory", cfg: Config{Backend: "memory", Dir: dir, WALDir: other}},
		{name: "Unset", cfg: Config{}},
		{
			name: "FileDefault",
			cfg:  Config{Backend: "file", Dir: dir},
			want: Layout{Parts: filepath.Join(dir, "parts"), WAL: filepath.Join(dir, "wal")},
		},
		{
			name: "FileOverride",
			cfg:  Config{Backend: "file", Dir: dir, WALDir: filepath.Join(other, "wal")},
			want: Layout{Parts: filepath.Join(dir, "parts"), WAL: filepath.Join(other, "wal")},
		},
		{
			name: "FileOverrideInsideDir",
			cfg:  Config{Backend: "file", Dir: dir, WALDir: filepath.Join(dir, "journal")},
			want: Layout{Parts: filepath.Join(dir, "parts"), WAL: filepath.Join(dir, "journal")},
		},
		{name: "FileNoDir", cfg: Config{Backend: "file"}, err: "storage.dir is required"},
		{name: "S3NoWAL", cfg: Config{Backend: "s3", Dir: dir}},
		{name: "S3WAL", cfg: Config{Backend: "s3", WALDir: other}, want: Layout{WAL: other}},
		{name: "Unknown", cfg: Config{Backend: "tape"}, err: `unknown storage backend "tape"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.cfg.Layout()
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestConfigLayoutRefusesOverlappingWAL(t *testing.T) {
	dir := t.TempDir()
	parts := filepath.Join(dir, "parts")

	for _, tt := range []struct {
		name string
		wal  string
	}{
		{name: "Equal", wal: parts},
		{name: "Inside", wal: filepath.Join(parts, "wal")},
		{name: "Unclean", wal: filepath.Join(dir, "x", "..", "parts", "wal")},
		{name: "Contains", wal: dir},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{Backend: "file", Dir: dir, WALDir: tt.wal}
			_, err := cfg.Layout()
			require.ErrorContains(t, err, "overlaps the parts directory")
		})
	}

	t.Run("Relative", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		cfg := Config{Backend: "file", Dir: dir, WALDir: filepath.Join("parts", "wal")}
		_, err := cfg.Layout()
		require.ErrorContains(t, err, "overlaps the parts directory")
	})

	t.Run("Symlink", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("symlinks need privileges on Windows")
		}
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))
		link := filepath.Join(t.TempDir(), "link")
		require.NoError(t, os.Symlink(filepath.Join(dir, "parts"), link))

		cfg := Config{Backend: "file", Dir: dir, WALDir: filepath.Join(link, "wal")}
		_, err := cfg.Layout()
		require.ErrorContains(t, err, "overlaps the parts directory")
	})
}

func TestConfigLayoutRefusesPrePartsDir(t *testing.T) {
	mkdirs := func(t *testing.T, root string, names ...string) {
		t.Helper()
		for _, name := range names {
			require.NoError(t, os.MkdirAll(filepath.Join(root, name), 0o750))
		}
	}

	for _, tt := range []struct {
		name  string
		dirs  []string
		files []string
		wal   string
		err   bool
	}{
		{name: "Absent"},
		{name: "Empty", dirs: []string{""}},
		{name: "Migrated", dirs: []string{"parts/default/metrics", "wal"}},
		{name: "WALOnly", dirs: []string{"wal", "lost+found", ".snapshot"}},
		{name: "CustomWAL", dirs: []string{"journal"}, wal: "journal"},
		{name: "StrayFile", dirs: []string{""}, files: []string{"README"}},
		{name: "Tenant", dirs: []string{"default/metrics", "wal"}, err: true},
		{name: "TenantOnly", dirs: []string{"default/logs"}, err: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "data")
			mkdirs(t, dir, tt.dirs...)
			for _, name := range tt.files {
				require.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0o600))
			}
			cfg := Config{Backend: "file", Dir: dir}
			if tt.wal != "" {
				cfg.WALDir = filepath.Join(dir, tt.wal)
			}

			_, err := cfg.Layout()
			if !tt.err {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "holds data outside")
			require.ErrorContains(t, err, filepath.Join(dir, "parts"))
		})
	}
}

func TestOpenRefusesPrePartsDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "default", "metrics"), 0o750))

	_, _, err := Open(t.Context(), Config{Backend: "file", Dir: dir}, zap.NewNop(), &app.Telemetry{})
	require.ErrorContains(t, err, "holds data outside")

	_, statErr := os.Stat(filepath.Join(dir, "parts"))
	require.ErrorIs(t, statErr, os.ErrNotExist, "a refused open must not create the parts directory")
}

func TestOpenReadOnlyWarnsAboutResolvedWAL(t *testing.T) {
	for _, tt := range []struct {
		name   string
		walDir func(dir string) string
	}{
		{name: "Default", walDir: func(string) string { return "" }},
		{name: "Override", walDir: func(dir string) string { return filepath.Join(dir, "elsewhere") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := Config{Backend: "file", Dir: dir, WALDir: tt.walDir(dir), ReadOnly: true}
			layout, err := cfg.Layout()
			require.NoError(t, err)
			require.NoError(t, os.MkdirAll(layout.WAL, 0o750))
			require.NoError(t, os.WriteFile(filepath.Join(layout.WAL, "segment"), []byte("x"), 0o600))

			core, logs := observer.New(zap.WarnLevel)
			_, stop, err := Open(t.Context(), cfg, zap.New(core), &app.Telemetry{})
			require.NoError(t, err)
			require.NoError(t, stop(t.Context()))

			warned := logs.FilterMessageSnippet("write-ahead log is not replayed").All()
			require.Len(t, warned, 1)
			require.Equal(t, layout.WAL, warned[0].ContextMap()["wal_dir"])
		})
	}
}
