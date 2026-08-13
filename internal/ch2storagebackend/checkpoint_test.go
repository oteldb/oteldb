package ch2storagebackend

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func day(s string) time.Time {
	t, err := time.ParseInLocation(time.DateOnly, s, time.UTC)
	if err != nil {
		panic(err)
	}
	return t
}

func ts(s string) time.Time {
	t, err := time.ParseInLocation(time.DateTime, s, time.UTC)
	if err != nil {
		panic(err)
	}
	return t
}

func tempCheckpoint(t *testing.T) (c *Checkpoint, path string) {
	t.Helper()

	path = filepath.Join(t.TempDir(), "checkpoint.jsonl")
	c, err := OpenCheckpoint(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = c.Close()
	})
	return c, path
}

func TestCheckpointMarkAndDone(t *testing.T) {
	c, _ := tempCheckpoint(t)

	assert.False(t, c.Done(SignalMetrics, day("2026-08-05")))
	require.NoError(t, c.Mark(SignalMetrics, day("2026-08-05"), 2_740_237_809))

	assert.True(t, c.Done(SignalMetrics, day("2026-08-05")))
	assert.False(t, c.Done(SignalMetrics, day("2026-08-06")), "a different day")
	assert.False(t, c.Done(SignalLogs, day("2026-08-05")), "a different signal, same day")
}

func TestCheckpointResumes(t *testing.T) {
	c, path := tempCheckpoint(t)
	require.NoError(t, c.Mark(SignalMetrics, day("2026-08-05"), 1))
	require.NoError(t, c.Mark(SignalLogs, day("2026-08-06"), 2))
	require.NoError(t, c.Close())

	reopened, err := OpenCheckpoint(path)
	require.NoError(t, err)
	defer func() {
		_ = reopened.Close()
	}()

	assert.True(t, reopened.Done(SignalMetrics, day("2026-08-05")))
	assert.True(t, reopened.Done(SignalLogs, day("2026-08-06")))
	assert.False(t, reopened.Done(SignalLogs, day("2026-08-05")))

	// Re-marking an already-done day is idempotent from the reader's side.
	require.NoError(t, reopened.Mark(SignalMetrics, day("2026-08-05"), 1))
	assert.True(t, reopened.Done(SignalMetrics, day("2026-08-05")))
}

func TestCheckpointNormalizesDayToUTC(t *testing.T) {
	c, _ := tempCheckpoint(t)

	// Same instant, different location: the journal keys on the UTC day, so a caller passing a
	// zoned time must not produce a second, unmatched entry.
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)

	require.NoError(t, c.Mark(SignalTraces, day("2026-08-05"), 1))
	assert.True(t, c.Done(SignalTraces, day("2026-08-05").In(tokyo)))
}

func TestCheckpointTornTrailingLine(t *testing.T) {
	c, path := tempCheckpoint(t)
	require.NoError(t, c.Mark(SignalMetrics, day("2026-08-05"), 1))
	require.NoError(t, c.Mark(SignalMetrics, day("2026-08-06"), 2))
	require.NoError(t, c.Close())

	// Simulate a crash mid-append: truncate the last line to a partial record.
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw[:len(raw)-12], 0o644))

	reopened, err := OpenCheckpoint(path)
	require.NoError(t, err, "a torn trailing line is an interrupted append, not a corruption")
	defer func() {
		_ = reopened.Close()
	}()

	assert.True(t, reopened.Done(SignalMetrics, day("2026-08-05")), "the complete line survives")
	assert.False(t, reopened.Done(SignalMetrics, day("2026-08-06")), "the torn day is re-migrated")
}

func TestCheckpointCorruptInteriorLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{not json}\n{\"signal\":\"logs\"}\n"), 0o644))

	_, err := OpenCheckpoint(path)
	require.Error(t, err, "a malformed line that is not the last one is a real corruption")
}

func TestCheckpointMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "checkpoint.jsonl")
	_, err := OpenCheckpoint(path)
	require.Error(t, err, "a missing parent directory is reported, not silently ignored")
}

func TestNilCheckpointIsInert(t *testing.T) {
	var c *Checkpoint

	assert.False(t, c.Done(SignalMetrics, day("2026-08-05")))
	assert.NoError(t, c.Mark(SignalMetrics, day("2026-08-05"), 1))
	assert.NoError(t, c.Close())
}
