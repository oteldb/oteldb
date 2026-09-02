package storagebackup

import (
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func testChunks() map[string]Chunk {
	return map[string]Chunk{
		"Empty": {},
		"IdentityOnly": {
			Series: signal.Series{
				Resource: signal.Resource{
					SchemaURL:  []byte("https://schema"),
					Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("k"), Value: signal.StringValue([]byte("v"))}),
				},
				Scope: signal.Scope{Name: []byte("scope"), Version: []byte("v1")},
			},
		},
		"Samples": {
			Series:     signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("a"), Value: signal.IntValue(-3)})}},
			Timestamps: []int64{-1, 0, 1 << 40},
			Values:     []float64{0, -1.5, 1e308},
		},
		"Columns": {
			Timestamps: []int64{1, 2},
			Columns: []fetch.NamedColumn{
				{Name: "severity", Int64: []int64{9, 17}},
				// A nil element is not an empty one: an unset trace id must not come back as "".
				{Name: "trace_id", Bytes: [][]byte{nil, []byte("x")}},
				{Name: "ratio", Float64: []float64{0.25, 0.5}},
				{Name: "absent"},
			},
		},
	}
}

func TestChunkRoundTrip(t *testing.T) {
	t.Parallel()

	for name, c := range testChunks() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := decodeChunk(appendChunk(nil, &c))
			require.NoError(t, err)
			require.True(t, c.Series.Equal(got.Series))
			require.Equal(t, c.Timestamps, got.Timestamps)
			require.Equal(t, c.Values, got.Values)
			require.Len(t, got.Columns, len(c.Columns))
			for i := range c.Columns {
				require.Equal(t, c.Columns[i], got.Columns[i])
			}
		})
	}
}

func TestChunkTruncated(t *testing.T) {
	t.Parallel()

	c := testChunks()["Columns"]
	full := appendChunk(nil, &c)
	for i := range len(full) {
		_, err := decodeChunk(full[:i])
		require.Error(t, err, "a %d-byte prefix must not decode", i)
	}
}

func TestChunkFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	rel := "log/default/2024-01-02" + fileExt
	header := FileHeader{Version: FormatVersion, Signal: "log", Tenant: "default", Day: "2024-01-02"}

	w, err := createChunkWriter(dir, rel, header, 0)
	require.NoError(t, err)

	want := testChunks()
	names := []string{"Columns", "IdentityOnly", "Samples"}
	for _, name := range names {
		c := want[name]
		require.NoError(t, w.Write(&c))
	}
	require.NoError(t, w.Close())

	// The file is published only on Close, so a name that exists is a complete file.
	require.NoFileExists(t, filepath.Join(dir, filepath.FromSlash(rel))+".tmp")

	r, got, err := openChunkReader(filepath.Join(dir, filepath.FromSlash(rel)))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, r.Close())
	}()
	require.Equal(t, header, got)

	for _, name := range names {
		c, err := r.Next()
		require.NoError(t, err)
		require.Equal(t, want[name].Timestamps, c.Timestamps)
	}
	_, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestChunkFileAbort(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	rel := "log/default/2024-01-02" + fileExt

	w, err := createChunkWriter(dir, rel, FileHeader{Version: FormatVersion, Signal: "log"}, 0)
	require.NoError(t, err)
	c := testChunks()["Samples"]
	require.NoError(t, w.Write(&c))
	w.Abort()

	entries, err := os.ReadDir(filepath.Join(dir, "log", "default"))
	require.NoError(t, err)
	require.Empty(t, entries, "an aborted day leaves nothing behind for a resumed run to trust")
}

func TestOpenChunkReaderRejects(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	name := filepath.Join(dir, "bogus"+fileExt)
	require.NoError(t, os.WriteFile(name, []byte("not zstd"), 0o600))

	_, _, err := openChunkReader(name)
	require.Error(t, err)
}

// requireSameFloats compares bit patterns rather than values, so a NaN that round-trips unchanged
// is equal to itself.
func requireSameFloats(tb testing.TB, want, got []float64) {
	tb.Helper()

	require.Len(tb, got, len(want))
	for i := range want {
		require.Equal(tb, math.Float64bits(want[i]), math.Float64bits(got[i]), "value %d", i)
	}
}

func FuzzDecodeChunk(f *testing.F) {
	for _, c := range testChunks() {
		f.Add(appendChunk(nil, &c))
	}
	// The pieces an oversized batch is split into are ordinary chunks, so the split's boundary is
	// in the corpus like any other shape.
	big := bigChunk(4, 8)
	for _, r := range [][2]int{{0, 0}, {0, 2}, {2, 4}} {
		part := sliceChunk(&big, r[0], r[1])
		f.Add(appendChunk(nil, &part))
	}
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := decodeChunk(data)
		if err != nil {
			return
		}
		// Anything that decodes must re-encode and decode again to the same thing, so a malformed
		// input cannot smuggle in a chunk the encoder could never have produced.
		again, err := decodeChunk(appendChunk(nil, &c))
		require.NoError(t, err)
		require.Equal(t, c.Timestamps, again.Timestamps)
		requireSameFloats(t, c.Values, again.Values)
		require.Len(t, again.Columns, len(c.Columns))
		for i := range c.Columns {
			require.Equal(t, c.Columns[i].Name, again.Columns[i].Name)
			require.Equal(t, c.Columns[i].Int64, again.Columns[i].Int64)
			require.Equal(t, c.Columns[i].Bytes, again.Columns[i].Bytes)
			requireSameFloats(t, c.Columns[i].Float64, again.Columns[i].Float64)
		}
		require.True(t, c.Series.Equal(again.Series))
	})
}

// The chunk codec writes a column by asking which of [fetch.NamedColumn]'s typed slices is
// populated, so a slice added upstream would fall through to colEmpty: the column would be backed
// up as present-but-empty, silently, with nothing failing to build and no test noticing. This is
// that notice. A failure here means the codec needs the new kind, not that this test needs the new
// count.
func TestNamedColumnKindsAreCovered(t *testing.T) {
	t.Parallel()

	want := map[string]string{
		"Name":    "string",
		"Int64":   "[]int64",
		"Float64": "[]float64",
		"Bytes":   "[][]uint8",
	}

	got := map[string]string{}
	for f := range reflect.TypeFor[fetch.NamedColumn]().Fields() {
		got[f.Name] = f.Type.String()
	}

	require.Equal(t, want, got,
		"fetch.NamedColumn changed shape: teach appendChunk and decodeChunk the new column kind, "+
			"and give it a byte in the colEmpty/colInt64/... block, before updating this test")
}
