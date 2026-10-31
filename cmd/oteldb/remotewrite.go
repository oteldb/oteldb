package main

import (
	"net/http"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/httpmiddleware"
	"github.com/oteldb/oteldb/internal/promrw"
	"github.com/oteldb/oteldb/internal/xbytes"
)

// RemoteWriteConfig configures direct native ingestion at /api/v1/write.
type RemoteWriteConfig struct {
	Enabled         bool          `json:"enabled" yaml:"enabled"`
	TimeThreshold   time.Duration `json:"time_threshold" yaml:"time_threshold"`
	MaxBodyBytes    xbytes.Bytes  `json:"max_body_bytes" yaml:"max_body_bytes"`
	MaxDecodedBytes xbytes.Bytes  `json:"max_decoded_bytes" yaml:"max_decoded_bytes"`
}

func (app *App) remoteWriteMiddleware() (httpmiddleware.Middleware, error) {
	cfg := app.cfg.RemoteWrite
	if !cfg.Enabled {
		return nil, nil
	}
	if app.cfg.MetricsBackend != MetricsBackendStorage || app.storageBackend == nil {
		return nil, errors.New("remote_write requires metrics_backend: storage")
	}
	if cfg.TimeThreshold < 0 || cfg.MaxBodyBytes < 0 || cfg.MaxDecodedBytes < 0 ||
		int64(int(cfg.MaxDecodedBytes)) != int64(cfg.MaxDecodedBytes) {
		return nil, errors.New("remote_write limits must be non-negative and fit this platform")
	}
	write := promrw.NewHandler(app.storageBackend, promrw.HandlerConfig{
		Options:         promrw.Options{TimeThreshold: cfg.TimeThreshold},
		MaxBodyBytes:    int64(cfg.MaxBodyBytes),
		MaxDecodedBytes: int(cfg.MaxDecodedBytes),
		Logger:          app.lg,
	})
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/v1/write" {
				write.ServeHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}
