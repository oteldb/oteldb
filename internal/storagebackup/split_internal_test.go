package storagebackup

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// bigChunk builds one batch of rows whose encoding is far larger than the limits below, standing in
// for a busy tenant's day: a single fetch batch of a real cluster's traces runs to hundreds of
// megabytes, which is what the writer must not need to hold in one chunk.
func bigChunk(rows, bodyLen int) Chunk {
	c := Chunk{
		Series: signal.Series{
			Resource: signal.Resource{
				SchemaURL:  []byte("https://opentelemetry.io/schemas/1.24.0"),
				Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))}),
			},
			Scope: signal.Scope{Name: []byte("scope"), Version: []byte("v1")},
		},
		Columns: []fetch.NamedColumn{
			{Name: "severity", Int64: make([]int64, 0, rows)},
			{Name: "body", Bytes: make([][]byte, 0, rows)},
			{Name: "ratio", Float64: make([]float64, 0, rows)},
			{Name: "absent"},
		},
	}
	for i := range rows {
		c.Timestamps = append(c.Timestamps, int64(i))
		c.Columns[0].Int64 = append(c.Columns[0].Int64, int64(i%24))
		c.Columns[1].Bytes = append(c.Columns[1].Bytes, bytes.Repeat([]byte{byte('a' + i%26)}, bodyLen))
		c.Columns[2].Float64 = append(c.Columns[2].Float64, float64(i)/3)
	}
	return c
}

// readChunks decodes every chunk of a file, copying what it keeps: the reader hands out aliases of
// one buffer it reuses.
func readChunks(tb testing.TB, name string) []Chunk {
	tb.Helper()

	r, _, err := openChunkReader(name)
	require.NoError(tb, err)
	defer func() {
		require.NoError(tb, r.Close())
	}()

	var out []Chunk
	for {
		c, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(tb, err)

		cp, err := decodeChunk(bytes.Clone(appendChunk(nil, &c)))
		require.NoError(tb, err)
		out = append(out, cp)
	}
}

// TestChunkWriterSplitsOversizedBatch pins the fix for a batch larger than the chunk limit: it is
// split over several chunks instead of failing the day, and the rows come back in order.
func TestChunkWriterSplitsOversizedBatch(t *testing.T) {
	t.Parallel()

	for _, limit := range []int{512, 4096, 1 << 16} {
		t.Run(fmt.Sprintf("Limit%d", limit), func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			rel := "log/default/2024-01-02" + fileExt
			w, err := createChunkWriter(dir, rel, FileHeader{Version: FormatVersion, Signal: "log"}, limit)
			require.NoError(t, err)

			want := bigChunk(500, 128)
			require.Greater(t, len(appendChunk(nil, &want)), limit, "the batch must not fit in one chunk")
			require.NoError(t, w.Write(&want))
			require.NoError(t, w.Close())

			require.Equal(t, 1, w.streams, "a split batch is still one stream")
			require.Greater(t, w.chunks, 1, "an oversized batch is split")
			require.Equal(t, len(want.Timestamps), w.rows)

			got := readChunks(t, filepath.Join(dir, filepath.FromSlash(rel)))
			require.Len(t, got, w.chunks)

			var merged Chunk
			for i, c := range got {
				require.LessOrEqual(t, len(appendChunk(nil, &c)), limit, "chunk %d is over the limit", i)
				require.True(t, want.Series.Equal(c.Series), "chunk %d lost the stream identity", i)
				require.NotEmpty(t, c.Timestamps, "chunk %d is empty", i)
				require.Len(t, c.Columns, len(want.Columns))
				merged = concatChunks(merged, c)
			}

			require.Equal(t, want.Timestamps, merged.Timestamps)
			for i := range want.Columns {
				require.Equal(t, want.Columns[i].Name, merged.Columns[i].Name)
				require.Equal(t, want.Columns[i].Int64, merged.Columns[i].Int64)
				require.Equal(t, want.Columns[i].Bytes, merged.Columns[i].Bytes)
				require.Equal(t, want.Columns[i].Float64, merged.Columns[i].Float64)
			}
		})
	}
}

// TestChunkWriterSplitsSamples covers the metric shape, whose rows are values rather than columns.
func TestChunkWriterSplitsSamples(t *testing.T) {
	t.Parallel()

	const limit = 1024
	dir := t.TempDir()
	rel := "metric/default/2024-01-02" + fileExt
	w, err := createChunkWriter(dir, rel, FileHeader{Version: FormatVersion, Signal: "metric"}, limit)
	require.NoError(t, err)

	want := Chunk{Series: bigChunk(0, 0).Series}
	for i := range 1000 {
		want.Timestamps = append(want.Timestamps, int64(i))
		want.Values = append(want.Values, float64(i)*1.5)
	}
	require.NoError(t, w.Write(&want))
	require.NoError(t, w.Close())
	require.Greater(t, w.chunks, 1)

	var merged Chunk
	for _, c := range readChunks(t, filepath.Join(dir, filepath.FromSlash(rel))) {
		require.LessOrEqual(t, len(appendChunk(nil, &c)), limit)
		merged = concatChunks(merged, c)
	}
	require.Equal(t, want.Timestamps, merged.Timestamps)
	require.Equal(t, want.Values, merged.Values)
}

// TestChunkWriterRejectsOversizedRow pins the one case a split cannot help: a single row that does
// not fit. It must say so rather than write a chunk no reader will accept.
func TestChunkWriterRejectsOversizedRow(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w, err := createChunkWriter(dir, "log/default/2024-01-02"+fileExt,
		FileHeader{Version: FormatVersion, Signal: "log"}, 512)
	require.NoError(t, err)
	defer w.Abort()

	c := bigChunk(2, 4096)
	err = w.Write(&c)
	require.ErrorContains(t, err, "single row")
}

// TestChunkLimitClamp pins that a writer never emits a chunk a reader would refuse.
func TestChunkLimitClamp(t *testing.T) {
	t.Parallel()

	require.Equal(t, DefaultMaxChunkBytes, clampChunkLimit(0))
	require.Equal(t, DefaultMaxChunkBytes, clampChunkLimit(-1))
	require.Equal(t, 1024, clampChunkLimit(1024))
	require.Equal(t, maxChunkBytes, clampChunkLimit(maxChunkBytes*2))
}

// concatChunks appends b's rows to a, which is how a reader reassembles a batch the writer split.
func concatChunks(a, b Chunk) Chunk {
	if a.Columns == nil && a.Timestamps == nil {
		a.Series = b.Series
		a.Columns = make([]fetch.NamedColumn, len(b.Columns))
		for i := range b.Columns {
			a.Columns[i].Name = b.Columns[i].Name
		}
	}
	a.Timestamps = append(a.Timestamps, b.Timestamps...)
	a.Values = append(a.Values, b.Values...)
	for i := range b.Columns {
		a.Columns[i].Int64 = append(a.Columns[i].Int64, b.Columns[i].Int64...)
		a.Columns[i].Float64 = append(a.Columns[i].Float64, b.Columns[i].Float64...)
		a.Columns[i].Bytes = append(a.Columns[i].Bytes, b.Columns[i].Bytes...)
	}
	return a
}
