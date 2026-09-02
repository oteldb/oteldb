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

	"github.com/go-faster/errors"
	"github.com/klauspost/compress/zstd"

	"github.com/oteldb/storage/signal"
)

// maxChunkBytes bounds one decoded chunk. A corrupt or hostile length prefix would otherwise ask
// for an arbitrary allocation before anything has been validated.
const maxChunkBytes = 512 << 20

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

	streams int
	rows    int
}

func createChunkWriter(root, rel string, h FileHeader) (_ *chunkWriter, rerr error) {
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

	return &chunkWriter{f: f, enc: enc, tmp: tmp, final: final}, nil
}

// Write appends one chunk.
func (w *chunkWriter) Write(c *Chunk) error {
	w.buf = appendChunk(w.buf[:0], c)
	if len(w.buf) > maxChunkBytes {
		return errors.Errorf("chunk of %d bytes exceeds the %d byte limit", len(w.buf), maxChunkBytes)
	}

	var frame [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(frame[:], uint64(len(w.buf)))
	if _, err := w.enc.Write(frame[:n]); err != nil {
		return errors.Wrap(err, "write chunk length")
	}
	if _, err := w.enc.Write(w.buf); err != nil {
		return errors.Wrap(err, "write chunk")
	}

	w.streams++
	w.rows += len(c.Timestamps)
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

// chunkReader reads a chunk file written by [chunkWriter].
type chunkReader struct {
	f   *os.File
	dec *zstd.Decoder
	r   *bufio.Reader
	buf []byte
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

	dec, err := zstd.NewReader(f)
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

	cr := &chunkReader{f: f, dec: dec, r: r}
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

	if uint64(cap(r.buf)) < n {
		r.buf = make([]byte, n)
	}
	r.buf = r.buf[:n]
	if _, err := io.ReadFull(r.r, r.buf); err != nil {
		return nil, errors.Wrap(err, "read frame")
	}
	return r.buf, nil
}

// Close releases the file.
func (r *chunkReader) Close() error {
	r.dec.Close()
	if err := r.f.Close(); err != nil {
		return errors.Wrap(err, "close chunk file")
	}
	return nil
}
