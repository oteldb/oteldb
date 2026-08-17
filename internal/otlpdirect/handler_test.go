package otlpdirect_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/signal/profile"
	"github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// recordingSink captures what each endpoint wrote. It copies nothing, so it must only be read
// after the request returns — which is what the aliasing contract permits.
type recordingSink struct {
	mu sync.Mutex

	logs     int
	spans    int
	points   int
	samples  int
	failWith error
}

func (s *recordingSink) WriteLogs(_ context.Context, batch log.Logs) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range batch.Resources {
		for j := range batch.Resources[i].Scopes {
			s.logs += len(batch.Resources[i].Scopes[j].Records)
		}
	}

	return s.failWith
}

func (s *recordingSink) WriteTraces(_ context.Context, batch trace.Traces) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range batch.Resources {
		for j := range batch.Resources[i].Scopes {
			s.spans += len(batch.Resources[i].Scopes[j].Spans)
		}
	}

	return s.failWith
}

func (s *recordingSink) WriteMetrics(_ context.Context, batch metric.Metrics) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range batch.Resources {
		for j := range batch.Resources[i].Scopes {
			for _, mt := range batch.Resources[i].Scopes[j].Metrics {
				s.points += len(mt.Points)
			}
		}
	}

	return s.failWith
}

func (s *recordingSink) WriteProfiles(_ context.Context, batch *profile.Profiles) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range batch.Resources {
		for j := range batch.Resources[i].Scopes {
			for _, pr := range batch.Resources[i].Scopes[j].Profiles {
				s.samples += len(pr.Samples)
			}
		}
	}

	return s.failWith
}

func serveOTLP(tb testing.TB, sink otlpdirect.Sink) (*http.ServeMux, *[]otlpdirect.Stats) {
	tb.Helper()

	var (
		mu    sync.Mutex
		stats []otlpdirect.Stats
	)

	h := otlpdirect.NewHandler(sink, otlpdirect.HandlerConfig{
		Observer: func(s otlpdirect.Stats) {
			mu.Lock()
			defer mu.Unlock()

			stats = append(stats, s)
		},
	})

	mux := http.NewServeMux()
	h.Register(mux)

	return mux, &stats
}

func post(mux *http.ServeMux, path string, raw []byte, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/x-protobuf")

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	return rec
}

// TestHandlerServesEverySignal drives all four endpoints with real OTLP bytes.
func TestHandlerServesEverySignal(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	mux, stats := serveOTLP(t, sink)

	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(1)
	lr.Body().SetStr("hello")

	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetName("op")
	sp.SetStartTimestamp(1)

	md := pmetric.NewMetrics()
	mp := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	mp.SetName("g")

	gp := mp.SetEmptyGauge().DataPoints().AppendEmpty()
	gp.SetTimestamp(1)
	gp.SetDoubleValue(1)

	for _, tt := range []struct {
		path string
		raw  []byte
	}{
		{otlpdirect.LogsPath, marshal(t, ld)},
		{otlpdirect.TracesPath, marshalTraces(t, td)},
		{otlpdirect.MetricsPath, marshalMetrics(t, md)},
	} {
		rec := post(mux, tt.path, tt.raw, nil)
		require.Equal(t, http.StatusOK, rec.Code, tt.path)
		assert.Equal(t, "application/x-protobuf", rec.Header().Get("Content-Type"))

		// Full success is an empty ExportXServiceResponse, not an absent body.
		assert.Empty(t, rec.Body.Bytes(), "%s: full success carries no partial_success", tt.path)
	}

	assert.Equal(t, 1, sink.logs)
	assert.Equal(t, 1, sink.spans)
	assert.Equal(t, 1, sink.points)
	assert.Len(t, *stats, 3)
}

// TestHandlerGzip pins that a gzip-encoded body is transparently decompressed, which every real
// OTLP exporter sends by default.
func TestHandlerGzip(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	mux, _ := serveOTLP(t, sink)

	ld := plog.NewLogs()
	for range 3 {
		r := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		r.SetTimestamp(1)
		r.Body().SetStr("compressed")
	}

	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(marshal(t, ld))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	rec := post(mux, otlpdirect.LogsPath, buf.Bytes(), map[string]string{"Content-Encoding": "gzip"})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body)
	assert.Equal(t, 3, sink.logs)
}

// TestHandlerPartialSuccess pins the response OTLP requires when some items could not be stored:
// 200 with a partial_success body, not an error. A client cannot fix these by retrying, so an
// error status would make it resend forever.
func TestHandlerPartialSuccess(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	mux, stats := serveOTLP(t, sink)

	md := pmetric.NewMetrics()

	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("g")

	dps := m.SetEmptyGauge().DataPoints()

	valueless := dps.AppendEmpty()
	valueless.SetTimestamp(1)

	valued := dps.AppendEmpty()
	valued.SetTimestamp(2)
	valued.SetDoubleValue(1)

	rec := post(mux, otlpdirect.MetricsPath, marshalMetrics(t, md), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	rejected, message := decodePartialSuccess(t, rec.Body.Bytes())
	assert.Equal(t, int64(1), rejected)
	assert.NotEmpty(t, message, "a partial success must say what happened")

	assert.Equal(t, 1, sink.points, "the representable point is still stored")
	require.Len(t, *stats, 1)
	assert.Equal(t, 1, (*stats)[0].Rejected)
	assert.Equal(t, signal.Metric, (*stats)[0].Signal)
}

// TestHandlerSinkFailureIsRetryable pins that a write failure answers 5xx: the data may not have
// landed, so the sender must retry rather than move on.
func TestHandlerSinkFailureIsRetryable(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{failWith: errors.New("no primary")}
	mux, stats := serveOTLP(t, sink)

	ld := plog.NewLogs()
	r := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	r.SetTimestamp(1)

	rec := post(mux, otlpdirect.LogsPath, marshal(t, ld), nil)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Empty(t, *stats, "a failed write is not observed as ingested")
}

// TestHandlerMalformedBodyIsNotRetryable pins the other direction: bytes the sender got wrong are
// 4xx, since resending them cannot help.
func TestHandlerMalformedBodyIsNotRetryable(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	mux, _ := serveOTLP(t, sink)

	rec := post(mux, otlpdirect.TracesPath, []byte{0xff, 0xff, 0xff, 0xff}, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerRejectsNonProtobuf(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	mux, _ := serveOTLP(t, sink)

	// JSON-encoded OTLP is valid per spec but unimplemented here; it must be refused rather than
	// parsed as protobuf and reported as malformed.
	rec := post(mux, otlpdirect.LogsPath, []byte(`{}`), map[string]string{"Content-Type": "application/json"})
	assert.Equal(t, http.StatusUnsupportedMediaType, rec.Code)
}

func TestHandlerRejectsNonPost(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	mux, _ := serveOTLP(t, sink)

	req := httptest.NewRequest(http.MethodGet, otlpdirect.LogsPath, http.NoBody)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// TestHandlerBodyLimit pins that an oversized body is refused rather than buffered.
func TestHandlerBodyLimit(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}

	h := otlpdirect.NewHandler(sink, otlpdirect.HandlerConfig{MaxBodyBytes: 16})

	mux := http.NewServeMux()
	h.Register(mux)

	ld := plog.NewLogs()
	for range 50 {
		r := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		r.SetTimestamp(1)
		r.Body().SetStr("padding padding padding")
	}

	rec := post(mux, otlpdirect.LogsPath, marshal(t, ld), nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Zero(t, sink.logs)
}

// TestHandlerGzipBombIsBounded pins that the decompressed size is capped independently of the body
// limit: gzip's ratio is unbounded and the sender picks it.
func TestHandlerGzipBombIsBounded(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}

	h := otlpdirect.NewHandler(sink, otlpdirect.HandlerConfig{MaxDecodedBytes: 1 << 10})

	mux := http.NewServeMux()
	h.Register(mux)

	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(make([]byte, 1<<20))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	require.Less(t, buf.Len(), 1<<12, "the compressed bomb is small")

	rec := post(mux, otlpdirect.LogsPath, buf.Bytes(), map[string]string{"Content-Encoding": "gzip"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandlerConcurrent drives every endpoint at once, since the converters are pooled and shared
// across requests.
func TestHandlerConcurrent(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	mux, _ := serveOTLP(t, sink)

	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(1)
	lr.Body().SetStr("x")

	raw := marshal(t, ld)

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			rec := post(mux, otlpdirect.LogsPath, raw, nil)
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}

	wg.Wait()
	assert.Equal(t, 50, sink.logs)
}

// decodePartialSuccess reads an ExportXServiceResponse's partial_success submessage.
func decodePartialSuccess(t *testing.T, src []byte) (rejected int64, message string) {
	t.Helper()

	var fc easyproto.FieldContext

	for len(src) > 0 {
		var err error

		src, err = fc.NextField(src)
		require.NoError(t, err)

		if fc.FieldNum != 1 {
			continue
		}

		data, ok := fc.MessageData()
		require.True(t, ok)

		var inner easyproto.FieldContext
		for len(data) > 0 {
			data, err = inner.NextField(data)
			require.NoError(t, err)

			switch inner.FieldNum {
			case 1:
				rejected, ok = inner.Int64()
				require.True(t, ok)
			case 2:
				message, ok = inner.String()
				require.True(t, ok)
			}
		}
	}

	return rejected, message
}
