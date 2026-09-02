package storagebackup

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"testing/iotest"

	"github.com/go-faster/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testHeader = FileHeader{Version: FormatVersion, Signal: "log", Tenant: "default", Day: "2024-01-02", Start: 1, End: 2}

// writeChunkFile writes chunks through the real writer and returns the bytes it produced, so the
// stream under test is one a backup would actually have written.
func writeChunkFile(tb testing.TB, limit int, chunks ...Chunk) []byte {
	tb.Helper()

	dir := tb.TempDir()
	rel := "log/default/2024-01-02" + fileExt

	w, err := createChunkWriter(dir, rel, testHeader, limit)
	require.NoError(tb, err)

	for i := range chunks {
		require.NoError(tb, w.Write(&chunks[i]))
	}
	require.NoError(tb, w.Close())

	raw, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(rel)))
	require.NoError(tb, err)

	return raw
}

// readAll drains a stream into the chunks it holds. Next reuses its buffer, so each chunk is
// cloned by re-encoding it.
func readAll(rd io.Reader) ([][]byte, FileHeader, error) {
	r, h, err := newChunkReader(rd)
	if err != nil {
		return nil, h, err
	}
	defer func() { _ = r.Close() }()

	var out [][]byte
	for {
		c, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out, h, nil
		}
		if err != nil {
			return out, h, err
		}
		out = append(out, appendChunk(nil, &c))
	}
}

func sampleChunks() []Chunk {
	cs := testChunks()
	return []Chunk{cs["Columns"], cs["IdentityOnly"], cs["Samples"], cs["Empty"]}
}

// The reader sits on a decompressor, which delivers whatever a block boundary happens to give it,
// so a frame arriving in pieces is the normal case rather than an edge one. These wrappers make it
// the case in every test rather than only when a block lands badly.
func TestChunkStreamShortReads(t *testing.T) {
	t.Parallel()

	chunks := sampleChunks()
	raw := writeChunkFile(t, 0, chunks...)

	want, _, err := readAll(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Len(t, want, len(chunks))

	for name, wrap := range map[string]func(io.Reader) io.Reader{
		"OneByte":     iotest.OneByteReader,
		"Half":        iotest.HalfReader,
		"DataErr":     iotest.DataErrReader,
		"OneByteData": func(r io.Reader) io.Reader { return iotest.OneByteReader(iotest.DataErrReader(r)) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, h, err := readAll(wrap(bytes.NewReader(raw)))
			require.NoError(t, err)
			require.Equal(t, testHeader, h)
			require.Equal(t, want, got)
		})
	}
}

// Split chunks are the shape a busy tenant produces, and they are the ones a short read is most
// likely to land inside, since a frame that needed splitting is the largest thing in the file.
func TestChunkStreamShortReadsSplit(t *testing.T) {
	t.Parallel()

	big := bigChunk(3, 64)
	raw := writeChunkFile(t, 512, big)

	want, _, err := readAll(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Greater(t, len(want), 1, "the batch was split, so the stream holds several frames")

	got, _, err := readAll(iotest.OneByteReader(bytes.NewReader(raw)))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// errAfter fails once n bytes have been handed over, so the failure lands mid-stream rather than
// at the open, where it would only prove that a broken file is rejected.
type errAfter struct {
	r    io.Reader
	n    int
	fail error
}

func (e *errAfter) Read(p []byte) (int, error) {
	if e.n <= 0 {
		return 0, e.fail
	}
	if len(p) > e.n {
		p = p[:e.n]
	}
	n, err := e.r.Read(p)
	e.n -= n
	return n, err
}

// A read that fails must surface as an error. The dangerous outcome is not a failed restore but a
// silent one: an [io.EOF] here would look exactly like a file that ended, and restore would report
// success over a fraction of the data.
func TestChunkStreamReadError(t *testing.T) {
	t.Parallel()

	raw := writeChunkFile(t, 0, sampleChunks()...)
	boom := errors.New("boom")

	for name, at := range map[string]int{"AtOpen": 0, "MidStream": len(raw) / 2, "BeforeEnd": len(raw) - 1} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, _, err := readAll(&errAfter{r: bytes.NewReader(raw), n: at, fail: boom})
			require.Error(t, err)
			assert.NotErrorIs(t, err, io.EOF, "a failed read must not be mistaken for the end of the file")
		})
	}

	_, _, err := readAll(iotest.ErrReader(boom))
	require.ErrorIs(t, err, boom)
}

// A file is published by rename, so a truncated one should not exist — but a backup lives on media
// that a restore has no way to vouch for, so the reader must not turn a short file into a short
// success.
func TestChunkStreamTruncated(t *testing.T) {
	t.Parallel()

	chunks := sampleChunks()
	raw := writeChunkFile(t, 0, chunks...)

	full, _, err := readAll(bytes.NewReader(raw))
	require.NoError(t, err)

	for i := range len(raw) {
		got, _, err := readAll(bytes.NewReader(raw[:i]))
		if err == nil {
			// zstd frames are self-delimiting, so a prefix that ends on a boundary can read
			// cleanly. What it must never do is invent or reorder chunks.
			require.LessOrEqual(t, len(got), len(full), "a prefix cannot hold more than the whole")
			require.Equal(t, full[:len(got)], got, "a %d-byte prefix decoded a different chunk", i)
			continue
		}
		require.Less(t, len(got), len(full)+1)
	}
}

// The frame length comes out of the decompressed stream, so a handful of bytes can announce a
// frame of any size up to the reader's bound. Allocating that up front turns a corrupt file into
// an out-of-memory kill, which is the one failure a restore cannot report.
func TestChunkStreamFrameLengthLie(t *testing.T) {
	t.Parallel()

	raw := lyingFrameFile(t, maxChunkBytes-1)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	_, _, err := readAll(bytes.NewReader(raw))
	require.Error(t, err)

	runtime.ReadMemStats(&after)
	grew := after.TotalAlloc - before.TotalAlloc
	assert.Less(t, grew, uint64(16<<20), "a %d byte claim allocated %d bytes", maxChunkBytes-1, grew)
}

func TestChunkStreamFrameTooLarge(t *testing.T) {
	t.Parallel()

	_, _, err := readAll(bytes.NewReader(lyingFrameFile(t, maxChunkBytes+1)))
	require.ErrorContains(t, err, "exceeds")
}

// FuzzChunkStream drives the whole file reader, not just one chunk's payload: the magic, the
// header, the framing and the zstd stream around them are all reachable by a corrupt backup, and
// they are the parts [FuzzDecodeChunk] never sees.
func FuzzChunkStream(f *testing.F) {
	f.Add(writeChunkFile(f, 0, sampleChunks()...))
	f.Add(writeChunkFile(f, 512, bigChunk(3, 64)))
	f.Add(writeChunkFile(f, 0))
	f.Add(lyingFrameFile(f, maxChunkBytes-1))
	f.Add([]byte{})
	f.Add([]byte(fileMagic))

	f.Fuzz(func(t *testing.T, data []byte) {
		got, _, err := readAll(bytes.NewReader(data))
		if err != nil {
			return
		}
		// Anything the reader accepted has to survive being written back out and read again, so a
		// crafted file cannot yield chunks the writer could not have produced.
		var chunks []Chunk
		for _, raw := range got {
			c, err := decodeChunk(raw)
			require.NoError(t, err)
			chunks = append(chunks, c)
		}

		again, h, err := readAll(bytes.NewReader(writeChunkFile(t, 0, chunks...)))
		require.NoError(t, err)
		require.Equal(t, testHeader, h)
		require.Equal(t, got, again)
	})
}

// lyingFrameFile is a well-formed file whose first frame after the header claims n bytes and then
// ends.
func lyingFrameFile(tb testing.TB, n uint64) []byte {
	tb.Helper()

	var buf bytes.Buffer

	enc, err := zstd.NewWriter(&buf)
	require.NoError(tb, err)

	raw, err := json.Marshal(testHeader)
	require.NoError(tb, err)

	var frame [binary.MaxVarintLen64]byte
	body := append([]byte(fileMagic), appendBlob(nil, raw)...)
	body = append(body, frame[:binary.PutUvarint(frame[:], n)]...)

	_, err = enc.Write(body)
	require.NoError(tb, err)
	require.NoError(tb, enc.Close())

	return buf.Bytes()
}
