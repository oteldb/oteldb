package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"

	"github.com/prometheus/prometheus/prompb"

	"github.com/oteldb/oteldb/internal/promql"
	"github.com/oteldb/oteldb/internal/promrw"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// writeRequest encodes a remote write request with the Prometheus types, so the test drives the
// handler with the bytes a real sender produces.
func writeRequest(tb testing.TB, tss ...prompb.TimeSeries) []byte {
	tb.Helper()

	raw, err := (&prompb.WriteRequest{Timeseries: tss}).Marshal()
	require.NoError(tb, err)
	return snappy.Encode(nil, raw)
}

// TestIngestRoundtrip writes remote write requests through the handler into a real storage engine
// and queries them back with PromQL. It is the end-to-end check that the zero-copy path is sound:
// the handler recycles every buffer the batch aliased before the query runs, so anything the engine
// failed to copy would read back as garbage.
func TestIngestRoundtrip(t *testing.T) {
	ctx := t.Context()

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := storagebackend.New(store)
	h := promrw.NewHandler(b, promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: time.Hour},
	})

	at := time.Now().Truncate(time.Second)
	body := writeRequest(t,
		prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "http_requests_total"},
				{Name: "job", Value: "api"},
			},
			Samples: []prompb.Sample{{Timestamp: at.UnixMilli(), Value: 42}},
		},
		prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "go_goroutines"},
				{Name: "job", Value: "api"},
			},
			Samples: []prompb.Sample{{Timestamp: at.UnixMilli(), Value: 7}},
		},
	)

	// Twice: the second request reuses (and overwrites) every pooled buffer the first one aliased,
	// so data the engine failed to copy would be corrupt by the time it is read back.
	for range 2 {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusAccepted, rec.Code, rec.Body)
	}

	eng, err := promql.New(b, promql.EngineOpts{
		MaxSamples:    1_000_000,
		Timeout:       time.Minute,
		LookbackDelta: 5 * time.Minute,
	})
	require.NoError(t, err)

	for _, tt := range []struct {
		query string
		want  float64
	}{
		{`http_requests_total{job="api"}`, 42},
		{`go_goroutines{job="api"}`, 7},
	} {
		q, err := eng.NewInstantQuery(ctx, b, nil, tt.query, at)
		require.NoError(t, err)
		t.Cleanup(q.Close)

		res := q.Exec(ctx)
		require.NoError(t, res.Err)

		vec, err := res.Vector()
		require.NoError(t, err)
		require.Len(t, vec, 1, tt.query)
		require.Equal(t, tt.want, vec[0].F, tt.query)
		require.Equal(t, "api", vec[0].Metric.Get("job"))
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig("")
	require.NoError(t, err)

	require.Equal(t, "memory", cfg.Storage.Backend)
	require.Equal(t, ":19291", cfg.RemoteWrite.Bind)
	require.Equal(t, "/", cfg.RemoteWrite.Path)
	require.Equal(t, 15*time.Second, cfg.RemoteWrite.ShutdownTimeout)
}
