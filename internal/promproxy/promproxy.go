// Package promproxy provides Prometheus proxy for observability and research.
package promproxy

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/promapi"
)

var _ promapi.Handler = &Server{}

// NewServer initializes new proxy Server from openapi client.
func NewServer(api *promapi.Client, rec *Recorder) *Server {
	return &Server{
		api: api,
		rec: rec,
	}
}

// Server implement proxy server.
type Server struct {
	api *promapi.Client
	rec *Recorder
}

// GetLabelValues implements getLabelValues operation.
// GET /api/v1/label/{label}/values
func (s *Server) GetLabelValues(ctx context.Context, params promapi.GetLabelValuesParams) (*promapi.LabelValuesResponse, error) {
	return s.api.GetLabelValues(ctx, params)
}

// GetLabels implements getLabels operation.
//
// GET /api/v1/labels
func (s *Server) GetLabels(ctx context.Context, params promapi.GetLabelsParams) (*promapi.LabelsResponse, error) {
	return s.api.GetLabels(ctx, params)
}

// GetMetadata implements getMetadata operation.
//
// GET /api/v1/metadata
func (s *Server) GetMetadata(ctx context.Context, params promapi.GetMetadataParams) (*promapi.MetadataResponse, error) {
	return s.api.GetMetadata(ctx, params)
}

// GetQuery implements getQuery operation.
//
// Query Prometheus.
//
// GET /api/v1/query
func (s *Server) GetQuery(ctx context.Context, params promapi.GetQueryParams) (*promapi.QueryResponse, error) {
	return s.api.GetQuery(ctx, params)
}

// GetQueryExemplars implements getQueryExemplars operation.
//
// Query Prometheus.
//
// GET /api/v1/query_examplars
func (s *Server) GetQueryExemplars(ctx context.Context, params promapi.GetQueryExemplarsParams) (*promapi.QueryExemplarsResponse, error) {
	return s.api.GetQueryExemplars(ctx, params)
}

// GetQueryRange implements getQueryRange operation.
//
// Query Prometheus.
//
// GET /api/v1/query_range
func (s *Server) GetQueryRange(ctx context.Context, params promapi.GetQueryRangeParams) (*promapi.QueryResponse, error) {
	if err := s.rec.RecordGetQueryRange(params); err != nil {
		return nil, errors.Wrap(err, "record")
	}
	return s.api.GetQueryRange(ctx, params)
}

// GetRules implements getRules operation.
//
// GET /api/v1/rules
func (s *Server) GetRules(ctx context.Context, params promapi.GetRulesParams) (*promapi.RulesResponse, error) {
	return s.api.GetRules(ctx, params)
}

// GetSeries implements getSeries operation.
// Query Prometheus.
//
// GET /api/v1/series
func (s *Server) GetSeries(ctx context.Context, params promapi.GetSeriesParams) (*promapi.SeriesResponse, error) {
	if err := s.rec.RecordSeriesQuery(params); err != nil {
		return nil, errors.Wrap(err, "record")
	}
	return s.api.GetSeries(ctx, params)
}

// PostLabels implements postLabels operation.
//
// POST /api/v1/labels
func (s *Server) PostLabels(ctx context.Context, req *promapi.LabelsForm) (*promapi.LabelsResponse, error) {
	return s.api.PostLabels(ctx, req)
}

// PostQuery implements postQuery operation.
//
// Query Prometheus.
//
// POST /api/v1/query
func (s *Server) PostQuery(ctx context.Context, req *promapi.QueryForm) (*promapi.QueryResponse, error) {
	return s.api.PostQuery(ctx, req)
}

// PostQueryExemplars implements postQueryExemplars operation.
//
// Query Prometheus.
//
// POST /api/v1/query_examplars
func (s *Server) PostQueryExemplars(ctx context.Context, req *promapi.ExemplarsForm) (*promapi.QueryExemplarsResponse, error) {
	return s.api.PostQueryExemplars(ctx, req)
}

// PostQueryRange implements postQueryRange operation.
//
// Query Prometheus.
//
// POST /api/v1/query_range
func (s *Server) PostQueryRange(ctx context.Context, req *promapi.QueryRangeForm) (*promapi.QueryResponse, error) {
	return s.api.PostQueryRange(ctx, req)
}

// PostSeries implements postSeries operation.
//
// Query Prometheus.
//
// POST /api/v1/series
func (s *Server) PostSeries(ctx context.Context, req *promapi.SeriesForm) (*promapi.SeriesResponse, error) {
	return s.api.PostSeries(ctx, req)
}

// NewError creates *FailStatusCode from error returned by handler.
//
// Used for common default response.
func (s Server) NewError(ctx context.Context, err error) *promapi.FailStatusCode {
	zctx.From(ctx).Error("API Error", zap.Error(err))
	if v, ok := errors.Into[*promapi.FailStatusCode](err); ok {
		// Pass as-is.
		return v
	}
	msg := appendTrace(ctx, err.Error())
	return &promapi.FailStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response: promapi.Fail{
			Error:     msg,
			ErrorType: promapi.FailErrorTypeInternal,
		},
	}
}

func appendTrace(ctx context.Context, s string) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return s
	}
	return fmt.Sprintf("%s (trace_id=%s, span_id=%s)", s, sc.TraceID(), sc.SpanID())
}
