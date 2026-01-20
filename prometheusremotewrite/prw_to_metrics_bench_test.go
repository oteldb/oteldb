package prometheusremotewrite

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/otelbench"
	"github.com/go-faster/oteldb/internal/prompb"
)

func BenchmarkFromTimeSeries(b *testing.B) {
	b.ReportAllocs()

	data, err := os.ReadFile(filepath.Join("testdata", "reqs-1k-zstd.rwq"))
	require.NoError(b, err)

	reader := otelbench.NewReader(bytes.NewReader(data))
	require.True(b, reader.Decode())
	compressed := reader.Data()
	z, err := zstd.NewReader(bytes.NewReader(compressed))
	require.NoError(b, err)
	raw, err := io.ReadAll(z)
	require.NoError(b, err)

	rw := &prompb.WriteRequest{}
	require.NoError(b, rw.Unmarshal(raw))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := FromTimeSeries(rw.Timeseries, Settings{TimeThreshold: 1_000_000})
		require.NoError(b, err)
	}
}

func BenchmarkWriteRequestUnmarshal(b *testing.B) {
	// TODO(tdakkota): move to prompb package.
	b.ReportAllocs()

	data, err := os.ReadFile(filepath.Join("testdata", "reqs-1k-zstd.rwq"))
	require.NoError(b, err)

	reader := otelbench.NewReader(bytes.NewReader(data))
	require.True(b, reader.Decode())
	compressed := reader.Data()
	z, err := zstd.NewReader(bytes.NewReader(compressed))
	require.NoError(b, err)
	raw, err := io.ReadAll(z)
	require.NoError(b, err)

	// Pre-allocate request
	var req prompb.WriteRequest
	require.NoError(b, req.Unmarshal(raw))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Reset()
		if err := req.Unmarshal(raw); err != nil {
			b.Fatal(err)
		}
	}
}
