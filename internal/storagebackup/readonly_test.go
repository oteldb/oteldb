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
//
// It holds for a directory whose parts are all referenced. It does not hold for one carrying
// orphans; see [TestBackupSweepsOrphanedObjects] for the write this cannot suppress.
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

// TestBackupSweepsOrphanedObjects pins the one write a read-only backup cannot suppress: opening an
// engine deletes the part objects its bucket index does not name, and both load modes
// storage.Storage can select do that — the mode that sweeps nothing is internal to the engine
// (oteldb/storage#583).
//
// It is not data loss: an orphan is by definition unreferenced, so consecutive backups of the same
// directory return identical rows. But a copy diverges from its original the first time it is
// backed up, which is why this is pinned rather than left to be discovered — and why
// [TestBackupDoesNotWriteToDataDir] passes only because its directory happens to have none.
//
// When #583 lands, this is the test that should fail, and its assertion inverts.
func TestBackupSweepsOrphanedObjects(t *testing.T) {
	t.Parallel()

	lg := zaptest.NewLogger(t)
	dir := liveNodeDir(t)

	// An object no bucket index names, which is what a failed flush leaves behind. A long-running
	// node accumulates them, since this sweep is the only thing that reclaims them.
	orphan := filepath.Join(dir, "default", "logs", "01M1FGJ51CVRFV96490N2AD7AH", "c", "0")
	require.NoError(t, os.MkdirAll(filepath.Dir(orphan), 0o750))
	require.NoError(t, os.WriteFile(orphan, []byte("unreferenced"), 0o600))

	before := treeState(t, dir)

	back, stop, err := storagebackup.OpenEngine(t.Context(), storagebackup.EngineConfig{
		Dir:      dir,
		ReadOnly: true,
	}, lg)
	require.NoError(t, err)

	stats, err := storagebackup.NewBackup(back.Store(), lg, backupOptions()).Create(t.Context(), t.TempDir())
	require.NoError(t, err)
	require.Positive(t, stats.Rows, "the committed data is still backed up")
	require.NoError(t, stop(context.WithoutCancel(t.Context())))

	rel := filepath.ToSlash(filepath.Join("default", "logs", "01M1FGJ51CVRFV96490N2AD7AH", "c", "0"))
	after := treeState(t, dir)

	if _, kept := after[rel]; kept {
		t.Skip("the open no longer sweeps: storage exposes a read-only load, so invert this test")
	}

	delete(before, rel)
	require.Equal(t, before, after, "the sweep takes the orphan and nothing else")
}
