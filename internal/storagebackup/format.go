package storagebackup

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// FormatVersion is the backup format version. Restore refuses a file it does not know how to read
// rather than guessing at its layout.
const FormatVersion = 1

// fileMagic prefixes every chunk file, so a truncated or unrelated file is rejected before its
// bytes are interpreted as a length.
const fileMagic = "ODBBK1\n"

// fileExt is the extension of a chunk file. The ".zst" tail is honest: the whole file, magic
// included, is one zstd stream.
const fileExt = ".obk.zst"

// dayLayout formats the UTC day a chunk file covers, and names the file.
const dayLayout = "2006-01-02"

// FileHeader is the JSON header of a chunk file. It is authoritative for the file's identity: the
// path is only a convenience, since a tenant id has to be escaped to fit in one.
type FileHeader struct {
	Version int    `json:"version"`
	Signal  string `json:"signal"`
	Tenant  string `json:"tenant"`
	Day     string `json:"day"`
	// Start and End are the inclusive unix-nanosecond bounds the file's fetch used.
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// Validate reports whether the header is one this build can restore.
func (h FileHeader) Validate() error {
	if h.Version != FormatVersion {
		return errors.Errorf("unsupported backup format version %d (want %d)", h.Version, FormatVersion)
	}
	if _, err := signal.ParseSignal(h.Signal); err != nil {
		return errors.Wrap(err, "header signal")
	}
	return nil
}

// Manifest is the operator-facing index of a backup directory. Restore does not need it: it walks
// the tree and trusts each file's own header, so a backup that lost its manifest still restores.
type Manifest struct {
	Version   int        `json:"version"`
	CreatedAt time.Time  `json:"created_at"`
	Start     time.Time  `json:"start"`
	End       time.Time  `json:"end"`
	Files     []FileInfo `json:"files"`
}

// FileInfo describes one chunk file in a [Manifest].
type FileInfo struct {
	Path    string `json:"path"`
	Signal  string `json:"signal"`
	Tenant  string `json:"tenant"`
	Day     string `json:"day"`
	Streams int    `json:"streams"`
	// Chunks is how many chunks the file holds: one per stream, plus one for each split an
	// oversized batch needed.
	Chunks int `json:"chunks"`
	Rows   int `json:"rows"`
}

// Chunk is one fetch batch as stored: a stream (or metric series) identity plus its rows. It
// mirrors [fetch.Batch] minus the engine-internal bookkeeping, and is the unit restore converts
// back into an ingestible batch.
type Chunk struct {
	Series     signal.Series
	Timestamps []int64
	// Values holds the samples of a metric series; nil for the record signals.
	Values []float64
	// Columns are the per-record columns of a record signal; nil for metrics.
	Columns []fetch.NamedColumn
}

// Column kinds, as written into a chunk. They name which typed slice of a
// [fetch.NamedColumn] is populated.
const (
	colEmpty   byte = 0
	colInt64   byte = 1
	colFloat64 byte = 2
	colBytes   byte = 3
)

// appendChunk appends c's encoding to dst.
//
// The identity is written with [signal.Series.AppendHashInput], the engine's own reversible wire
// form, so resource/scope/attribute types survive without a second encoding to keep in step with
// the engine's.
func appendChunk(dst []byte, c *Chunk) []byte {
	dst = appendBlob(dst, c.Series.AppendHashInput(nil))
	dst = appendInt64s(dst, c.Timestamps)
	dst = appendFloat64s(dst, c.Values)

	dst = binary.AppendUvarint(dst, uint64(len(c.Columns)))
	for i := range c.Columns {
		col := &c.Columns[i]
		dst = appendBlob(dst, []byte(col.Name))
		switch {
		case col.Int64 != nil:
			dst = append(dst, colInt64)
			dst = appendInt64s(dst, col.Int64)
		case col.Float64 != nil:
			dst = append(dst, colFloat64)
			dst = appendFloat64s(dst, col.Float64)
		case col.Bytes != nil:
			dst = append(dst, colBytes)
			dst = appendBlobs(dst, col.Bytes)
		default:
			dst = append(dst, colEmpty)
		}
	}
	return dst
}

// chunkOverhead is an upper bound on what [appendChunk] writes for a chunk of c's shape before any
// row: the stream identity and the per-slice and per-column counts.
func chunkOverhead(c *Chunk) int {
	// The identity is encoded rather than estimated: it is the one part with no size bound, and
	// getting it wrong is what would let a chunk overrun the limit.
	n := binary.MaxVarintLen64 + len(c.Series.AppendHashInput(nil))
	n += 3 * binary.MaxVarintLen64
	for i := range c.Columns {
		n += binary.MaxVarintLen64 + len(c.Columns[i].Name) + 1 + binary.MaxVarintLen64
	}
	return n
}

// rowsFitting returns how many of c's rows from i on fit in budget bytes, at least one. It sums an
// upper bound per row, so the range it picks encodes to no more than the caller's limit; a caller
// that oversteps anyway (a zero or negative budget) falls back to halving the range.
func rowsFitting(c *Chunk, i, budget int) int {
	n := 0
	for j := i; j < len(c.Timestamps); j++ {
		if n > 0 && n+rowSize(c, j) > budget {
			return j - i
		}
		n += rowSize(c, j)
	}
	return len(c.Timestamps) - i
}

// rowSize bounds the bytes row i of c contributes to its chunk.
func rowSize(c *Chunk, i int) int {
	n := binary.MaxVarintLen64
	if i < len(c.Values) {
		n += 8
	}
	for k := range c.Columns {
		col := &c.Columns[k]
		switch {
		case col.Int64 != nil:
			n += binary.MaxVarintLen64
		case col.Float64 != nil:
			n += 8
		case col.Bytes != nil:
			n++
			if i < len(col.Bytes) {
				n += binary.MaxVarintLen64 + len(col.Bytes[i])
			}
		}
	}
	return n
}

// sliceChunk returns c restricted to rows [i, j), keeping the identity and the columns' kinds.
func sliceChunk(c *Chunk, i, j int) Chunk {
	out := Chunk{
		Series:     c.Series,
		Timestamps: sliceRows(c.Timestamps, i, j),
		Values:     sliceRows(c.Values, i, j),
	}
	if c.Columns == nil {
		return out
	}

	out.Columns = make([]fetch.NamedColumn, len(c.Columns))
	for k := range c.Columns {
		col := &c.Columns[k]
		out.Columns[k] = fetch.NamedColumn{
			Name:    col.Name,
			Int64:   sliceRows(col.Int64, i, j),
			Float64: sliceRows(col.Float64, i, j),
			Bytes:   sliceRows(col.Bytes, i, j),
		}
	}
	return out
}

// sliceRows takes rows [i, j) of a column, tolerating a slice shorter than the batch claims and
// keeping a nil slice nil — for a column an empty slice and an absent one encode differently.
func sliceRows[T any](vs []T, i, j int) []T {
	if vs == nil {
		return nil
	}
	return vs[min(i, len(vs)):min(j, len(vs))]
}

// decodeChunk decodes a chunk payload. The returned chunk owns no memory from src beyond what
// [signal.DecodeSeries] aliases, which the caller must copy before reusing the buffer.
func decodeChunk(src []byte) (c Chunk, _ error) {
	blob, off, err := readBlob(src)
	if err != nil {
		return c, errors.Wrap(err, "series blob")
	}
	if c.Series, _, err = signal.DecodeSeries(blob); err != nil {
		return c, errors.Wrap(err, "decode series")
	}

	rest := src[off:]
	if c.Timestamps, rest, err = readInt64s(rest); err != nil {
		return c, errors.Wrap(err, "timestamps")
	}
	if c.Values, rest, err = readFloat64s(rest); err != nil {
		return c, errors.Wrap(err, "values")
	}

	ncols, n := binary.Uvarint(rest)
	if n <= 0 {
		return c, errors.New("read column count")
	}
	rest = rest[n:]
	if ncols > uint64(len(rest)) {
		return c, errors.Errorf("column count %d exceeds remaining %d bytes", ncols, len(rest))
	}

	c.Columns = make([]fetch.NamedColumn, 0, ncols)
	for range ncols {
		name, off, err := readBlob(rest)
		if err != nil {
			return c, errors.Wrap(err, "column name")
		}
		rest = rest[off:]
		if len(rest) == 0 {
			return c, errors.New("truncated column kind")
		}
		kind := rest[0]
		rest = rest[1:]

		col := fetch.NamedColumn{Name: string(name)}
		switch kind {
		case colInt64:
			if col.Int64, rest, err = readInt64s(rest); err != nil {
				return c, errors.Wrapf(err, "column %q", col.Name)
			}
		case colFloat64:
			if col.Float64, rest, err = readFloat64s(rest); err != nil {
				return c, errors.Wrapf(err, "column %q", col.Name)
			}
		case colBytes:
			if col.Bytes, rest, err = readBlobs(rest); err != nil {
				return c, errors.Wrapf(err, "column %q", col.Name)
			}
		case colEmpty:
		default:
			return c, errors.Errorf("column %q: unknown kind %d", col.Name, kind)
		}
		c.Columns = append(c.Columns, col)
	}
	if len(rest) != 0 {
		return c, errors.Errorf("%d trailing bytes", len(rest))
	}
	return c, nil
}

func appendBlob(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func readBlob(src []byte) (blob []byte, read int, _ error) {
	n, off := binary.Uvarint(src)
	if off <= 0 {
		return nil, 0, errors.New("read length")
	}
	if n > uint64(len(src)-off) {
		return nil, 0, errors.Errorf("length %d exceeds remaining %d bytes", n, len(src)-off)
	}
	return src[off : off+int(n)], off + int(n), nil
}

func appendBlobs(dst []byte, bs [][]byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(bs)))
	for _, b := range bs {
		// A nil element is distinct from an empty one for a bytes column (an unset trace id versus
		// an empty body), so nil is tagged rather than written as a zero-length blob.
		if b == nil {
			dst = append(dst, 0)
			continue
		}
		dst = append(dst, 1)
		dst = appendBlob(dst, b)
	}
	return dst
}

func readBlobs(src []byte) (blobs [][]byte, rest []byte, _ error) {
	n, off := binary.Uvarint(src)
	if off <= 0 {
		return nil, nil, errors.New("read count")
	}
	src = src[off:]
	if n > uint64(len(src)) {
		return nil, nil, errors.Errorf("count %d exceeds remaining %d bytes", n, len(src))
	}

	out := make([][]byte, 0, n)
	for range n {
		if len(src) == 0 {
			return nil, nil, errors.New("truncated element")
		}
		present := src[0]
		src = src[1:]
		if present == 0 {
			out = append(out, nil)
			continue
		}
		b, off, err := readBlob(src)
		if err != nil {
			return nil, nil, err
		}
		src = src[off:]
		out = append(out, b)
	}
	return out, src, nil
}

func appendInt64s(dst []byte, vs []int64) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(vs)))
	for _, v := range vs {
		dst = binary.AppendVarint(dst, v)
	}
	return dst
}

func readInt64s(src []byte) (vs []int64, rest []byte, _ error) {
	n, off := binary.Uvarint(src)
	if off <= 0 {
		return nil, nil, errors.New("read count")
	}
	src = src[off:]
	if n > uint64(len(src)) {
		return nil, nil, errors.Errorf("count %d exceeds remaining %d bytes", n, len(src))
	}
	if n == 0 {
		return nil, src, nil
	}

	out := make([]int64, 0, n)
	for range n {
		v, off := binary.Varint(src)
		if off <= 0 {
			return nil, nil, errors.New("read value")
		}
		src = src[off:]
		out = append(out, v)
	}
	return out, src, nil
}

func appendFloat64s(dst []byte, vs []float64) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(vs)))
	for _, v := range vs {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(v))
	}
	return dst
}

func readFloat64s(src []byte) (vs []float64, rest []byte, _ error) {
	n, off := binary.Uvarint(src)
	if off <= 0 {
		return nil, nil, errors.New("read count")
	}
	src = src[off:]
	if n > uint64(len(src)/8) {
		return nil, nil, errors.Errorf("count %d exceeds remaining %d bytes", n, len(src))
	}
	if n == 0 {
		return nil, src, nil
	}

	out := make([]float64, 0, n)
	for range n {
		out = append(out, math.Float64frombits(binary.LittleEndian.Uint64(src)))
		src = src[8:]
	}
	return out, src, nil
}
