package storagebackup

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"

	"github.com/go-faster/errors"
	"github.com/klauspost/compress/zstd"

	"github.com/oteldb/storage/signal"
)

// maxChunkBytes bounds one decoded chunk. A corrupt or hostile length prefix would otherwise ask
// for an arbitrary allocation before anything has been validated. It is the reader's hard bound,
// and the ceiling on what a writer may be configured to emit.
const maxChunkBytes = 512 << 20

// DefaultMaxChunkBytes is the default size a backup splits a fetch batch at. A batch is whatever
// the fetch seam returns for one stream over one day, which on a busy tenant runs to hundreds of
// megabytes; splitting keeps the encoder's buffer bounded rather than proportional to the busiest
// day. It is well under [maxChunkBytes] so a file stays readable by any build.
const DefaultMaxChunkBytes = 64 << 20

// frameReadStep bounds how much of a frame is allocated before any of it has been read, so the
// buffer tracks bytes actually delivered rather than the length the stream claims.
const frameReadStep = 1 << 20

// filePath returns a chunk file's path relative to the backup root. The tenant element is escaped
// because a cluster shard key contains a slash and a tenant id is otherwise unconstrained; the
// tenant recorded in the file's header stays authoritative.
func filePath(sig signal.Signal, tenant signal.TenantID, day string) string {
	return path.Join(sig.String(), url.PathEscape(string(tenant)), day+fileExt)
}

// chunkWriter writes one chunk file. It writes to a temporary name and renames on Close, so a file
// present under its final name is complete — which is what makes an interrupted backup restartable
// by skipping the days it already finished.
type chunkWriter struct {
	f     *os.File
	enc   *zstd.Encoder
	tmp   string
	final string
	buf   []byte
	limit int

	streams int
	chunks  int
	rows    int
}

func createChunkWriter(root, rel string, h FileHeader, limit int) (_ *chunkWriter, rerr error) {
	final := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(final), 0o750); err != nil {
		return nil, errors.Wrap(err, "create backup directory")
	}

	tmp := final + ".tmp"
	f, err := os.Create(filepath.Clean(tmp))
	if err != nil {
		return nil, errors.Wrap(err, "create chunk file")
	}
	defer func() {
		if rerr != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()

	enc, err := zstd.NewWriter(f)
	if err != nil {
		return nil, errors.Wrap(err, "make zstd encoder")
	}
	defer func() {
		if rerr != nil {
			_ = enc.Close()
		}
	}()

	if _, err := enc.Write([]byte(fileMagic)); err != nil {
		return nil, errors.Wrap(err, "write magic")
	}
	raw, err := json.Marshal(h)
	if err != nil {
		return nil, errors.Wrap(err, "encode header")
	}
	if _, err := enc.Write(appendBlob(nil, raw)); err != nil {
		return nil, errors.Wrap(err, "write header")
	}

	return &chunkWriter{f: f, enc: enc, tmp: tmp, final: final, limit: clampChunkLimit(limit)}, nil
}

// clampChunkLimit resolves a writer's chunk size: unset takes [DefaultMaxChunkBytes], and no
// setting may exceed [maxChunkBytes], which is what a reader will accept.
func clampChunkLimit(limit int) int {
	if limit <= 0 {
		limit = DefaultMaxChunkBytes
	}
	return min(limit, maxChunkBytes)
}

// Write appends one fetch batch, splitting it over as many chunks as its size needs.
//
// The split is by row, and every chunk repeats the batch's stream identity, so each one stays
// independently decodable — the reader needs no notion of a continuation, and restore sees a split
// batch as several writes of the same stream. That is a shape live ingest already produces, when
// one stream's records arrive in more than one export, and the per-batch approximations (span
// structural ids) are the same either way.
func (w *chunkWriter) Write(c *Chunk) error {
	w.streams++
	w.rows += len(c.Timestamps)

	rows := len(c.Timestamps)
	if rows == 0 {
		// An identity-only batch still carries its columns' shape; write it whole.
		return w.writeRange(c, 0, 0)
	}

	budget := w.limit - chunkOverhead(c)
	for i := 0; i < rows; {
		j := min(i+rowsFitting(c, i, budget), rows)
		if err := w.writeRange(c, i, j); err != nil {
			return err
		}
		i = j
	}
	return nil
}

// writeRange writes rows [i, j) of c as one chunk, halving the range if the size estimate that
// chose it turned out to be optimistic.
func (w *chunkWriter) writeRange(c *Chunk, i, j int) error {
	part := sliceChunk(c, i, j)
	w.buf = appendChunk(w.buf[:0], &part)
	if len(w.buf) > w.limit {
		if j-i <= 1 {
			return errors.Errorf("chunk of %d bytes exceeds the %d byte limit and holds a single row",
				len(w.buf), w.limit)
		}
		mid := i + (j-i)/2
		if err := w.writeRange(c, i, mid); err != nil {
			return err
		}
		return w.writeRange(c, mid, j)
	}

	var frame [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(frame[:], uint64(len(w.buf)))
	if _, err := w.enc.Write(frame[:n]); err != nil {
		return errors.Wrap(err, "write chunk length")
	}
	if _, err := w.enc.Write(w.buf); err != nil {
		return errors.Wrap(err, "write chunk")
	}
	w.chunks++
	return nil
}

// Close flushes the file and publishes it under its final name.
func (w *chunkWriter) Close() error {
	if err := w.enc.Close(); err != nil {
		_ = w.f.Close()
		return errors.Wrap(err, "close zstd encoder")
	}
	// Sync before the rename: the rename is what marks the file complete, so a crash must not be
	// able to publish a name whose contents have not reached the disk.
	if err := w.f.Sync(); err != nil {
		_ = w.f.Close()
		return errors.Wrap(err, "sync chunk file")
	}
	if err := w.f.Close(); err != nil {
		return errors.Wrap(err, "close chunk file")
	}
	if err := os.Rename(w.tmp, w.final); err != nil {
		return errors.Wrap(err, "publish chunk file")
	}
	return nil
}

// Abort discards a partially written file.
func (w *chunkWriter) Abort() {
	_ = w.enc.Close()
	_ = w.f.Close()
	_ = os.Remove(w.tmp)
}

// chunkReader reads a chunk stream written by [chunkWriter].
type chunkReader struct {
	closer io.Closer
	dec    *zstd.Decoder
	r      *bufio.Reader
	buf    []byte
}

// newChunkReader reads the magic and header off rd and returns a reader positioned at the first
// chunk. It takes an [io.Reader] rather than a name so the stream can come from somewhere other
// than a file, which is what lets the tests drive it through short and failing reads.
func newChunkReader(rd io.Reader) (_ *chunkReader, _ FileHeader, rerr error) {
	var h FileHeader

	dec, err := zstd.NewReader(rd)
	if err != nil {
		return nil, h, errors.Wrap(err, "make zstd decoder")
	}
	defer func() {
		if rerr != nil {
			dec.Close()
		}
	}()

	r := bufio.NewReader(dec)
	magic := make([]byte, len(fileMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, h, errors.Wrap(err, "read magic")
	}
	if string(magic) != fileMagic {
		return nil, h, errors.New("not an oteldb storage backup file")
	}

	cr := &chunkReader{dec: dec, r: r}
	raw, err := cr.readFrame()
	if err != nil {
		return nil, h, errors.Wrap(err, "read header")
	}
	if err := json.Unmarshal(raw, &h); err != nil {
		return nil, h, errors.Wrap(err, "decode header")
	}
	if err := h.Validate(); err != nil {
		return nil, h, err
	}
	return cr, h, nil
}

func openChunkReader(name string) (_ *chunkReader, _ FileHeader, rerr error) {
	var h FileHeader

	f, err := os.Open(filepath.Clean(name))
	if err != nil {
		return nil, h, errors.Wrap(err, "open chunk file")
	}
	defer func() {
		if rerr != nil {
			_ = f.Close()
		}
	}()

	cr, h, err := newChunkReader(f)
	if err != nil {
		return nil, h, err
	}
	cr.closer = f

	return cr, h, nil
}

// Next decodes the next chunk, returning [io.EOF] at the end of the file. The returned chunk
// aliases an internal buffer that the following call overwrites.
func (r *chunkReader) Next() (Chunk, error) {
	raw, err := r.readFrame()
	if err != nil {
		return Chunk{}, err
	}
	c, err := decodeChunk(raw)
	if err != nil {
		return Chunk{}, errors.Wrap(err, "decode chunk")
	}
	return c, nil
}

// readFrame reads one length-delimited frame into the reader's buffer.
//
// The frame is read in bounded steps rather than allocating the announced length up front: the
// length comes out of the decompressed stream, so a few bytes of a corrupt or hostile file can
// announce a frame far larger than the file could hold, and a length that lies then costs only
// what the stream actually delivers.
func (r *chunkReader) readFrame() ([]byte, error) {
	n, err := binary.ReadUvarint(r.r)
	switch {
	case errors.Is(err, io.EOF):
		return nil, io.EOF
	case err != nil:
		return nil, errors.Wrap(err, "read frame length")
	}
	if n > maxChunkBytes {
		return nil, errors.Errorf("frame of %d bytes exceeds the %d byte limit", n, maxChunkBytes)
	}

	r.buf = r.buf[:0]
	for uint64(len(r.buf)) < n {
		want := int(min(n-uint64(len(r.buf)), frameReadStep))
		r.buf = slices.Grow(r.buf, want)
		at := len(r.buf)
		r.buf = r.buf[:at+want]
		if _, err := io.ReadFull(r.r, r.buf[at:]); err != nil {
			// A frame that ends early is a truncated file, not the end of one: [io.ReadFull]
			// reports io.EOF when it read nothing at all, and passing that up would make restore
			// stop and report success over however much of the file survived.
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, errors.Wrap(err, "read frame")
		}
	}
	return r.buf, nil
}

// Close releases the underlying stream. A reader over a plain [io.Reader] has nothing to release
// beyond the decoder.
func (r *chunkReader) Close() error {
	r.dec.Close()
	if r.closer == nil {
		return nil
	}
	if err := r.closer.Close(); err != nil {
		return errors.Wrap(err, "close chunk file")
	}
	return nil
}
