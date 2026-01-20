// Package prometheusremotewrite contains translator from Prometheus remote write format to OTLP metrics.
package prometheusremotewrite

import (
	"bytes"
	"io"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/go-faster/errors"
	"github.com/golang/snappy"
	"github.com/valyala/bytebufferpool"

	"github.com/go-faster/oteldb/internal/prompb"
	"github.com/go-faster/oteldb/internal/xsync"
)

// Settings defines translation settings.
type Settings struct {
	TimeThreshold int64
	Logger        zap.Logger
}

// DecodeRequest decodes data from reader to given dst.
func DecodeRequest(r io.Reader, settings Settings) (pmetric.Metrics, error) {
	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)

	wr := xsync.GetReset(writeRequestPool)
	defer writeRequestPool.Put(wr)

	if err := decodeRequest(r, bb, wr); err != nil {
		return pmetric.Metrics{}, errors.Wrap(err, "unmarshal data")
	}

	pms, err := FromTimeSeries(wr.Timeseries, settings)
	if err != nil {
		return pmetric.Metrics{}, errors.Wrap(err, "map timeseries")
	}
	return pms, nil
}

func decodeRequest(r io.Reader, bb *bytebufferpool.ByteBuffer, rw *prompb.WriteRequest) error {
	switch r := r.(type) {
	case *closerReader:
		// Do not make an unnecessary copy of data.
		bb.Set(r.data)
	default:
		if _, err := bb.ReadFrom(r); err != nil {
			return err
		}
	}
	return rw.Unmarshal(bb.B)
}

// SnappyDecoder provides snappy compression decoder to use as middleware.
func SnappyDecoder(body io.ReadCloser) (io.ReadCloser, error) {
	compressed := bytebufferpool.Get()
	defer bytebufferpool.Put(compressed)

	if _, err := io.Copy(compressed, body); err != nil {
		return nil, err
	}

	decompressed, err := snappy.Decode(nil, compressed.Bytes())
	if err != nil {
		return nil, err
	}

	return &closerReader{
		data:   decompressed,
		Reader: *bytes.NewReader(decompressed),
	}, nil
}

var writeRequestPool = xsync.NewPool(func() *prompb.WriteRequest {
	return &prompb.WriteRequest{}
})

type closerReader struct {
	data []byte
	bytes.Reader
}

func (c *closerReader) Close() error {
	return nil
}
