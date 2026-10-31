package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/oteldb/storage"
	prompb "github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/httpmiddleware"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestLoadRemoteWriteConfig(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "oteldb.yml")
	require.NoError(t, os.WriteFile(path, []byte(`remote_write:
  enabled: true
  time_threshold: 48h
  max_body_bytes: 64KiB
  max_decoded_bytes: 256KiB
`), 0o600))
	cfg, err := loadConfig(path)
	require.NoError(t, err)
	require.True(t, cfg.RemoteWrite.Enabled)
	require.Equal(t, 48*time.Hour, cfg.RemoteWrite.TimeThreshold)
	require.EqualValues(t, 64<<10, cfg.RemoteWrite.MaxBodyBytes)
	require.EqualValues(t, 256<<10, cfg.RemoteWrite.MaxDecodedBytes)
}

func TestRemoteWriteMiddlewareConfig(t *testing.T) {
	t.Parallel()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(t.Context()) })
	backend := storagebackend.New(store)
	for _, tt := range []struct {
		name    string
		cfg     RemoteWriteConfig
		native  bool
		backend *storagebackend.Backend
		wantErr bool
	}{
		{name: "disabled"},
		{name: "enabled", cfg: RemoteWriteConfig{Enabled: true}, native: true, backend: backend},
		{name: "clickhouse metrics", cfg: RemoteWriteConfig{Enabled: true}, backend: backend, wantErr: true},
		{name: "missing storage", cfg: RemoteWriteConfig{Enabled: true}, native: true, wantErr: true},
		{name: "negative threshold", cfg: RemoteWriteConfig{Enabled: true, TimeThreshold: -1}, native: true, backend: backend, wantErr: true},
		{name: "negative body limit", cfg: RemoteWriteConfig{Enabled: true, MaxBodyBytes: -1}, native: true, backend: backend, wantErr: true},
		{name: "negative decode limit", cfg: RemoteWriteConfig{Enabled: true, MaxDecodedBytes: -1}, native: true, backend: backend, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			app := &App{cfg: Config{RemoteWrite: tt.cfg}, storageBackend: tt.backend}
			if tt.native {
				app.cfg.MetricsBackend = MetricsBackendStorage
			}
			mw, err := app.remoteWriteMiddleware()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if !tt.cfg.Enabled {
				require.Nil(t, mw)
			} else {
				require.NotNil(t, mw)
			}
		})
	}
}

func TestRemoteWriteMiddleware(t *testing.T) {
	t.Parallel()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(t.Context()) })
	backend := storagebackend.New(store)
	app := &App{
		cfg: Config{
			MetricsBackend: MetricsBackendStorage,
			RemoteWrite:    RemoteWriteConfig{Enabled: true, TimeThreshold: 100 * 365 * 24 * time.Hour},
		},
		storageBackend: backend, lg: zap.NewNop(),
	}
	mw, err := app.remoteWriteMiddleware()
	require.NoError(t, err)
	auth, err := httpmiddleware.BearerToken([]httpmiddleware.Token{{Token: "test-token"}})
	require.NoError(t, err)
	fallback := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	handler := httpmiddleware.Wrap(fallback, httpmiddleware.Auth([]httpmiddleware.Authenticator{auth}, nil), mw)
	raw, err := (&prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "__name__", Value: "direct_metric"}, {Name: "instance", Value: "host-1"}},
		Samples: []prompb.Sample{{Timestamp: 1700000000000, Value: 7}},
	}}}).Marshal()
	require.NoError(t, err)
	body := snappy.Encode(nil, raw)
	for _, tt := range []struct {
		name, method, path, token string
		code                      int
	}{
		{"unauthenticated", http.MethodPost, "/api/v1/write", "", http.StatusUnauthorized},
		{"write", http.MethodPost, "/api/v1/write", "test-token", http.StatusNoContent},
		{"repeat write", http.MethodPost, "/api/v1/write", "test-token", http.StatusNoContent},
		{"method", http.MethodGet, "/api/v1/write", "test-token", http.StatusMethodNotAllowed},
		{"query path", http.MethodGet, "/api/v1/query", "test-token", http.StatusTeapot},
		{"similar path", http.MethodPost, "/api/v1/write/extra", "test-token", http.StatusTeapot},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/x-protobuf")
			req.Header.Set("Content-Encoding", "snappy")
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.Equal(t, tt.code, rec.Code, rec.Body.String())
		})
	}
	engine := app.newScarecrowEngine(backend, PrometheusConfig{})
	q, err := engine.NewInstantQuery(t.Context(), backend, nil, `direct_metric{instance="host-1"}`, time.UnixMilli(1700000000000))
	require.NoError(t, err)
	defer q.Close()
	res := q.Exec(t.Context())
	require.NoError(t, res.Err)
	vec, err := res.Vector()
	require.NoError(t, err)
	require.Len(t, vec, 1)
	require.Equal(t, float64(7), vec[0].F)
}
