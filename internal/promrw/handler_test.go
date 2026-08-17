package promrw_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-faster/errors"
	"github.com/golang/snappy"
	"github.com/oteldb/storage/signal/metric"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/prompb"
	"github.com/oteldb/oteldb/internal/promrw"
)

// sink records what it is handed, deep-copying it: the handler recycles the batch's backing
// buffers as soon as the write returns, exactly as the storage engine's own copy-on-retain does.
type sink struct {
	mu     sync.Mutex
	dumps  []string
	failWr error
}

func (s *sink) WriteMetrics(_ context.Context, batch metric.Metrics) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failWr != nil {
		return s.failWr
	}
	s.dumps = append(s.dumps, dump(&batch))
	return nil
}

func (s *sink) last() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.dumps[len(s.dumps)-1]
}

func postRaw(t *testing.T, h http.Handler, body []byte, contentType string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func post(t *testing.T, h http.Handler, raw []byte) *httptest.ResponseRecorder {
	t.Helper()

	return postRaw(t, h, snappyEncode(raw), "application/x-protobuf")
}

func snappyEncode(raw []byte) []byte { return snappy.Encode(nil, raw) }

// errStorageDown stands in for a sink that cannot accept the write right now.
var errStorageDown = errors.New("storage is down")

func TestHandler(t *testing.T) {
	raw := readCorpus(t)

	s := &sink{}
	h := promrw.NewHandler(s, promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: wideThreshold},
	})

	// Twice, so the second request runs on recycled buffers.
	for range 2 {
		rec := post(t, h, raw)
		require.Equal(t, http.StatusNoContent, rec.Code)
	}

	var req prompb.WriteRequest
	require.NoError(t, req.Unmarshal(raw))

	require.Equal(t, dump(viaPdata(t, req.Timeseries, wideThreshold)), s.last())
	require.Equal(t, s.dumps[0], s.dumps[1])
}

func TestHandlerRejects(t *testing.T) {
	s := &sink{}
	h := promrw.NewHandler(s, promrw.HandlerConfig{})

	t.Run("Method", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/write", http.NoBody))
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	})
	t.Run("NotProtobufV2", func(t *testing.T) {
		rec := postRaw(t, h, snappy.Encode(nil, []byte{0xff, 0xff, 0xff}),
			"application/x-protobuf;proto=io.prometheus.write.v2.Request")
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})
	t.Run("NotSnappy", func(t *testing.T) {
		rec := postRaw(t, h, []byte("plain"), "application/x-protobuf")
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})
	t.Run("NotProtobuf", func(t *testing.T) {
		rec := post(t, h, []byte{0xff, 0xff, 0xff})
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})
	t.Run("BodyTooLarge", func(t *testing.T) {
		small := promrw.NewHandler(s, promrw.HandlerConfig{MaxBodyBytes: 8})
		rec := post(t, small, bytes.Repeat([]byte("x"), 1024))
		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	})
	t.Run("WrongMediaType", func(t *testing.T) {
		rec := postRaw(t, h, nil, "application/json")
		require.Equal(t, http.StatusUnsupportedMediaType, rec.Code)
	})
	t.Run("UnknownProtoParameter", func(t *testing.T) {
		rec := postRaw(t, h, nil, "application/x-protobuf;proto=some.other.Request")
		require.Equal(t, http.StatusUnsupportedMediaType, rec.Code)
	})
	t.Run("MalformedContentTypeParameter", func(t *testing.T) {
		rec := postRaw(t, h, nil, "application/x-protobuf;proto")
		require.Equal(t, http.StatusUnsupportedMediaType, rec.Code)
	})
	require.Empty(t, s.dumps)
}

// TestHandlerWriteFailure asserts a sink failure is a 5xx, so the client retries instead of
// dropping the batch as malformed.
func TestHandlerWriteFailure(t *testing.T) {
	s := &sink{failWr: errStorageDown}
	h := promrw.NewHandler(s, promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: wideThreshold},
	})

	rec := post(t, h, readCorpus(t))
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.True(t, strings.Contains(rec.Body.String(), "storage is down"))
}

func BenchmarkHandler(b *testing.B) {
	raw := readCorpus(b)
	body := snappy.Encode(nil, raw)

	h := promrw.NewHandler(nopSink{}, promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: wideThreshold},
	})

	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()

	for b.Loop() {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			b.Fatalf("code %d: %s", rec.Code, rec.Body)
		}
	}
}

type nopSink struct{}

func (nopSink) WriteMetrics(context.Context, metric.Metrics) error { return nil }

// TestHandlerDecompressionLimit asserts the decompressed size is bounded independently of the
// compressed one: snappy's decoded length is a header field the sender chooses.
func TestHandlerDecompressionLimit(t *testing.T) {
	h := promrw.NewHandler(nopSink{}, promrw.HandlerConfig{MaxDecodedBytes: 1024})

	rec := post(t, h, bytes.Repeat([]byte("x"), 4096))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "over the 1024 limit")
}
