package storagebackup_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/oteldb/storage"
	backendfile "github.com/oteldb/storage/backend/file"

	"github.com/oteldb/oteldb/internal/storagebackup"
)

// openFileStore opens a durable engine over dir, shaped like a node's: parts in dir, WAL beside
// them, no maintenance loop so nothing happens on a timer.
func openFileStore(tb testing.TB, dir string) *storage.Storage {
	tb.Helper()

	fb, err := backendfile.New(dir)
	require.NoError(tb, err)

	store, err := storage.Open(tb.Context(), storage.Options{},
		storage.WithBackend(fb),
		storage.WithWALDir(filepath.Join(dir, "wal")),
		storage.WithFlushInterval(-1),
	)
	require.NoError(tb, err)
	return store
}

// liveNodeDir builds a data directory in the state a running node's is in: some data flushed into
// parts, and more of it still only in the write-ahead log, because the second store is abandoned
// rather than closed — exactly what a copy taken from a live node holds.
func liveNodeDir(tb testing.TB) string {
	tb.Helper()

	dir := tb.TempDir()

	flushed := openFileStore(tb, dir)
	writeSample(tb, flushed)
	require.NoError(tb, flushed.Close(context.WithoutCancel(tb.Context())))

	unflushed := openFileStore(tb, dir)
	writeSample(tb, unflushed)

	return dir
}

// treeState fingerprints every file under dir, so any write — a new part, a rewritten bucket index,
// a WAL checkpoint — shows up as a difference.
func treeState(tb testing.TB, dir string) map[string]string {
	tb.Helper()

	out := map[string]string{}
	require.NoError(tb, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		out[filepath.ToSlash(rel)] = hex.EncodeToString(sum[:])
		return nil
	}))
	return out
}

// TestBackupDoesNotWriteToDataDir pins that backing up a data directory does not modify it. Opening
// it read-write recovers the WAL, flushes a head into a new part and checkpoints the segments away
// — which, pointed at a running node, is a second writer discarding segments the node still owns.
func TestBackupDoesNotWriteToDataDir(t *testing.T) {
	t.Parallel()

	lg := zaptest.NewLogger(t)
	dir := liveNodeDir(t)
	before := treeState(t, dir)
	require.NotEmpty(t, before)

	back, stop, err := storagebackup.OpenEngine(t.Context(), storagebackup.EngineConfig{
		Dir:      dir,
		ReadOnly: true,
	}, lg)
	require.NoError(t, err)

	store := back.Store()
	require.NotNil(t, store)

	stats, err := storagebackup.NewBackup(store, lg, backupOptions()).Create(t.Context(), t.TempDir())
	require.NoError(t, err)
	require.Positive(t, stats.Rows, "the backup must have read the flushed parts")
	require.NoError(t, stop(context.WithoutCancel(t.Context())))

	require.Equal(t, before, treeState(t, dir), "a backup must not write to the data directory")
}
