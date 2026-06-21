// Package pyroproxy provides Pyroscope proxy for observability and research.
package pyroproxy

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/pyroscopeapi"
)

var _ pyroscopeapi.Handler = &Server{}

// NewServer initializes new proxy Server from openapi client.
func NewServer(api *pyroscopeapi.Client) *Server {
	return &Server{api: api}
}

// Server implement proxy server.
type Server struct {
	api *pyroscopeapi.Client
}

// GetApps implements getApps operation.
// Returns list of application metadata.
// Used by Grafana to test connection to Pyroscope.
//
// GET /api/apps
func (s *Server) GetApps(ctx context.Context) ([]pyroscopeapi.ApplicationMetadata, error) {
	return s.api.GetApps(ctx)
}

// Ingest implements ingest operation.
//
// Push data to Pyroscope.
//
// POST /ingest
func (s *Server) Ingest(ctx context.Context, req *pyroscopeapi.IngestReqWithContentType, params pyroscopeapi.IngestParams) error {
	return s.api.Ingest(ctx, req, params)
}

// LabelValues implements labelValues operation.
//
// Returns list of label values.
//
// GET /label-values
func (s *Server) LabelValues(ctx context.Context, params pyroscopeapi.LabelValuesParams) (pyroscopeapi.LabelValues, error) {
	return s.api.LabelValues(ctx, params)
}

// Labels implements labels operation.
//
// Returns list of labels.
//
// GET /labels
func (s *Server) Labels(ctx context.Context, params pyroscopeapi.LabelsParams) (pyroscopeapi.Labels, error) {
	return s.api.Labels(ctx, params)
}

// Render implements render operation.
//
// Renders given query.
// One of `query` or `key` is required.
//
// GET /render
func (s *Server) Render(ctx context.Context, params pyroscopeapi.RenderParams) (*pyroscopeapi.FlamebearerProfileV1, error) {
	return s.api.Render(ctx, params)
}

// NewError creates *ErrorStatusCode from error returned by handler.
//
// Used for common default response.
func (s *Server) NewError(ctx context.Context, err error) *pyroscopeapi.ErrorStatusCode {
	zctx.From(ctx).Error("API Error", zap.Error(err))
	if v, ok := errors.Into[*pyroscopeapi.ErrorStatusCode](err); ok {
		// Pass as-is.
		return v
	}
	msg := appendTrace(ctx, err.Error())
	return &pyroscopeapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   pyroscopeapi.Error(msg),
	}
}

func appendTrace(ctx context.Context, s string) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return s
	}
	return fmt.Sprintf("%s (trace_id=%s, span_id=%s)", s, sc.TraceID(), sc.SpanID())
}
