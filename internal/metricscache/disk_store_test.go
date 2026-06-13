package metricscache

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiskStore_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := NewDiskStore(dir)
	require.NoError(t, err)

	key := Key{Hash: [16]byte{1}, Step: 60_000, Fn: "rate"}

	// Store an entry.
	e := NewEntry()
	e.append([]int64{1000, 2000, 3000}, []float64{1.1, 2.2, 3.3}, 4000)
	e.markFetched(500, 4000)
	s.Set(key, e)

	// Retrieve it.
	got, ok := s.Get(key)
	require.True(t, ok)

	minTS1, maxTS1 := e.Watermarks()
	minTS2, maxTS2 := got.Watermarks()
	require.Equal(t, minTS1, minTS2)
	require.Equal(t, maxTS1, maxTS2)

	ts1, vals1 := e.slice(minTS1, maxTS1)
	ts2, vals2 := got.slice(minTS2, maxTS2)
	require.Equal(t, ts1, ts2)
	require.Equal(t, vals1, vals2)

	require.Equal(t, 1, s.Size())
}

func TestDiskStore_Miss(t *testing.T) {
	dir := t.TempDir()
	s, err := NewDiskStore(dir)
	require.NoError(t, err)

	_, ok := s.Get(Key{Hash: [16]byte{99}})
	require.False(t, ok)
}

func TestDiskStore_EmptyEntry(t *testing.T) {
	dir := t.TempDir()
	s, err := NewDiskStore(dir)
	require.NoError(t, err)

	key := Key{Hash: [16]byte{2}}
	e := NewEntry()
	e.markFetched(1000, 5000)
	s.Set(key, e)

	got, ok := s.Get(key)
	require.True(t, ok)

	_, maxTS := got.Watermarks()
	require.Equal(t, int64(5000), maxTS)
	require.Equal(t, 0, got.Len())
}

func TestDiskStore_CorruptJSON(t *testing.T) {
	dir := t.TempDir()
	s, err := NewDiskStore(dir)
	require.NoError(t, err)

	key := Key{Hash: [16]byte{5}, Step: 60_000, Fn: "rate"}
	e := NewEntry()
	e.append([]int64{1000, 2000}, []float64{1.0, 2.0}, 3000)
	s.Set(key, e)
	require.Equal(t, 1, s.Size())

	// Corrupt the .json file.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, de := range entries {
		if filepath.Ext(de.Name()) == ".json" {
			require.NoError(t, os.WriteFile(filepath.Join(dir, de.Name()), []byte("not json"), 0o600))
		}
	}

	// Get should return a miss and clean up the entry.
	_, ok := s.Get(key)
	require.False(t, ok)
	require.Equal(t, 0, s.Size())

	// Both files should be gone.
	remaining, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, remaining)
}

func TestDiskStore_MissingBin(t *testing.T) {
	dir := t.TempDir()
	s, err := NewDiskStore(dir)
	require.NoError(t, err)

	key := Key{Hash: [16]byte{6}, Step: 60_000, Fn: "rate"}
	e := NewEntry()
	e.append([]int64{1000, 2000}, []float64{1.0, 2.0}, 3000)
	s.Set(key, e)
	require.Equal(t, 1, s.Size())

	// Remove the .bin file.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, de := range entries {
		if filepath.Ext(de.Name()) == ".bin" {
			require.NoError(t, os.Remove(filepath.Join(dir, de.Name())))
		}
	}

	// Reload: loadIndex should detect missing .bin and delete .json.
	s2, err := NewDiskStore(dir)
	require.NoError(t, err)
	require.Equal(t, 0, s2.Size())

	remaining, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, remaining)
}

func TestDiskStore_CorruptBin(t *testing.T) {
	dir := t.TempDir()
	s, err := NewDiskStore(dir)
	require.NoError(t, err)

	key := Key{Hash: [16]byte{7}, Step: 60_000, Fn: "rate"}
	e := NewEntry()
	e.append([]int64{1000, 2000}, []float64{1.0, 2.0}, 3000)
	s.Set(key, e)
	require.Equal(t, 1, s.Size())

	// Corrupt the .bin file.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, de := range entries {
		if filepath.Ext(de.Name()) == ".bin" {
			require.NoError(t, os.WriteFile(filepath.Join(dir, de.Name()), []byte("not lz4"), 0o600))
		}
	}

	// Get should return a miss and clean up both files.
	_, ok := s.Get(key)
	require.False(t, ok)
	require.Equal(t, 0, s.Size())

	remaining, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, remaining)
}

func TestDiskStore_IndexRebuild(t *testing.T) {
	dir := t.TempDir()

	// Write entries to disk.
	s1, err := NewDiskStore(dir)
	require.NoError(t, err)

	keys := []Key{
		{Hash: [16]byte{1}, Step: 0, Fn: ""},
		{Hash: [16]byte{2}, Step: 60_000, Fn: "rate"},
		{Hash: [16]byte{3}, Step: 300_000, Fn: "sum"},
	}
	for i, k := range keys {
		e := NewEntry()
		e.append([]int64{int64(i+1) * 1000}, []float64{float64(i)}, int64(i+1)*2000)
		s1.Set(k, e)
	}
	require.Equal(t, 3, s1.Size())

	// Reload from disk.
	s2, err := NewDiskStore(dir)
	require.NoError(t, err)
	require.Equal(t, 3, s2.Size())

	for _, k := range keys {
		_, ok := s2.Get(k)
		require.True(t, ok, "key %v missing after reload", k)
	}
}
