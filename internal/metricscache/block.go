package metricscache

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"sync"

	"github.com/go-faster/errors"
	lz4 "github.com/pierrec/lz4/v4"
)

// Block is the serialized, compressed form of a single cache entry.
// One block corresponds to one Entry (all points for one series).
//
// The Data field contains an lz4-framed payload with the following uncompressed layout:
//
//	[ 4B uint32] count
//	if count >= 1:
//	  [ 8B int64 ] ts[0]
//	if count >= 2:
//	  [ 8B int64 ] first_interval = ts[1] - ts[0]
//	  [uvarint*(count-2)] zigzag-encoded double-deltas for ts[2:]:
//	    dd = (ts[i]-ts[i-1]) - (ts[i-1]-ts[i-2])
//	[8B*count] float64 values (little-endian IEEE 754)
//
// Note: minTS/maxTS watermarks are stored in the outer Block struct (and disk
// metadata), not inside the compressed payload. They were removed from the
// header to avoid redundant ignored reads in Decode.
type Block struct {
	Data  []byte // lz4-framed compressed payload
	MinTS int64
	MaxTS int64
	Count int
}

// ByteSize returns the approximate size of the block in bytes.
func (b Block) ByteSize() int {
	return len(b.Data) + 4 // count header (minTS/maxTS live in outer Block)
}

var lz4WriterPool = sync.Pool{
	New: func() any { return lz4.NewWriter(nil) },
}

// EncodeBlock encodes timestamps and values into a compressed Block.
//
// ts and vals must have the same length and ts must be sorted.
// minTS and maxTS are the watermark bounds (may extend beyond ts range for empty-series entries).
func EncodeBlock(ts []int64, vals []float64, minTS, maxTS int64) (Block, error) {
	if len(ts) != len(vals) {
		return Block{}, errors.New("ts and vals length mismatch")
	}
	n := len(ts)

	// Build uncompressed payload.
	var payload bytes.Buffer
	// Reserve space: header (4B) + first_interval (8B) + varints (≤10B*n) + values (8B*n).
	payload.Grow(4 + 8 + 10*n + 8*n)

	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(n))
	payload.Write(hdr[:])

	if n >= 1 {
		// Write ts[0] explicitly. The watermark minTS may differ from ts[0].
		var t0 [8]byte
		binary.LittleEndian.PutUint64(t0[:], uint64(ts[0]))
		payload.Write(t0[:])
	}
	if n >= 2 {
		var fi [8]byte
		binary.LittleEndian.PutUint64(fi[:], uint64(ts[1]-ts[0])) // #nosec G602
		payload.Write(fi[:])

		// Write double-deltas as zigzag varints.
		var vbuf [binary.MaxVarintLen64]byte
		for i := 2; i < n; i++ {
			dd := (ts[i] - ts[i-1]) - (ts[i-1] - ts[i-2]) // #nosec G602
			vn := binary.PutUvarint(vbuf[:], zigzagEncode(dd))
			payload.Write(vbuf[:vn])
		}
	}

	// Write raw float64 values.
	var vb [8]byte
	for _, v := range vals {
		binary.LittleEndian.PutUint64(vb[:], math.Float64bits(v))
		payload.Write(vb[:])
	}

	// LZ4-compress.
	compressed, err := lz4Compress(payload.Bytes())
	if err != nil {
		return Block{}, err
	}

	return Block{
		Data:  compressed,
		MinTS: minTS,
		MaxTS: maxTS,
		Count: n,
	}, nil
}

// Decode decompresses and decodes the block, returning the timestamps and values.
func (b Block) Decode() (ts []int64, vals []float64, err error) {
	if b.Count == 0 {
		return nil, nil, nil
	}
	if len(b.Data) == 0 {
		return nil, nil, errors.New("empty block data")
	}

	payload, err := lz4Decompress(b.Data)
	if err != nil {
		return nil, nil, errors.Wrap(err, "lz4 decompress")
	}

	r := bytes.NewReader(payload)

	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, nil, errors.Wrap(err, "read header")
	}
	n := int(binary.LittleEndian.Uint32(hdr[0:]))
	if n == 0 {
		return nil, nil, nil
	}

	ts = make([]int64, n)

	if n >= 1 {
		var t0 [8]byte
		if _, err := io.ReadFull(r, t0[:]); err != nil {
			return nil, nil, errors.Wrap(err, "read ts[0]")
		}
		ts[0] = int64(binary.LittleEndian.Uint64(t0[:]))
	}

	if n >= 2 {
		var fi [8]byte
		if _, err := io.ReadFull(r, fi[:]); err != nil {
			return nil, nil, errors.Wrap(err, "read first interval")
		}
		firstInterval := int64(binary.LittleEndian.Uint64(fi[:]))
		ts[1] = ts[0] + firstInterval

		// Reconstruct timestamps from double-deltas.
		for i := 2; i < n; i++ {
			u, err := binary.ReadUvarint(r)
			if err != nil {
				return nil, nil, errors.Wrap(err, "read double-delta varint")
			}
			dd := zigzagDecode(u)
			prevDelta := ts[i-1] - ts[i-2]
			ts[i] = ts[i-1] + prevDelta + dd
		}
	}

	// Read float64 values.
	vals = make([]float64, n)
	var vb [8]byte
	for i := range vals {
		if _, err := io.ReadFull(r, vb[:]); err != nil {
			return nil, nil, errors.Wrap(err, "read value")
		}
		vals[i] = math.Float64frombits(binary.LittleEndian.Uint64(vb[:]))
	}

	return ts, vals, nil
}

func lz4Compress(src []byte) ([]byte, error) {
	w := lz4WriterPool.Get().(*lz4.Writer)
	defer lz4WriterPool.Put(w)

	var buf bytes.Buffer
	buf.Grow(len(src)/2 + 64)
	w.Reset(&buf)
	if _, err := w.Write(src); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func lz4Decompress(src []byte) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(src))
	return io.ReadAll(r)
}

func zigzagEncode(n int64) uint64 {
	return uint64((n << 1) ^ (n >> 63))
}

func zigzagDecode(n uint64) int64 {
	return int64((n >> 1) ^ -(n & 1))
}
