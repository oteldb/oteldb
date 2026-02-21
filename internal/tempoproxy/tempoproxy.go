// Package tempoproxy provides Tempo proxy for observability and research.
package tempoproxy

import (
	"context"
	"net/http"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/tempoapi"
)

var _ tempoapi.Handler = &Server{}

// NewServer initializes new proxy Server from openapi client.
func NewServer(api *tempoapi.Client) *Server {
	return &Server{api: api}
}

// Server implement proxy server.
type Server struct {
	api *tempoapi.Client
}

// BuildInfo implements buildInfo operation.
//
// Returns Tempo buildinfo, in the same format as Prometheus `/api/v1/status/buildinfo`.
// Used by Grafana to check Tempo API version.
//
// GET /api/status/buildinfo
func (s *Server) BuildInfo(ctx context.Context) (*tempoapi.PrometheusVersion, error) {
	return s.api.BuildInfo(ctx)
}

// Echo implements echo operation.
// Echo request for testing, issued by Grafana.
//
// GET /api/echo
func (s *Server) Echo(ctx context.Context) (tempoapi.EchoOK, error) {
	return s.api.Echo(ctx)
}

// Query implements query operation.
//
// The instant version of the Metrics API is similar to the range version, but instead returns a
// single value for the query.
//
// GET /api/metrics/query
func (s *Server) Query(ctx context.Context, params tempoapi.QueryParams) (*tempoapi.InstantMetrics, error) {
	return s.api.Query(ctx, params)
}

// QueryRange implements queryRange operation.
//
// This endpoint returns Prometheus-like time-series for a given metrics query.
//
// GET /api/metrics/query_range
func (s *Server) QueryRange(ctx context.Context, params tempoapi.QueryRangeParams) (*tempoapi.RangeMetrics, error) {
	return s.api.QueryRange(ctx, params)
}

// Search implements search operation.
//
// Execute TraceQL query.
//
// GET /api/search
func (s *Server) Search(ctx context.Context, params tempoapi.SearchParams) (*tempoapi.Traces, error) {
	return s.api.Search(ctx, params)
}

// SearchTagValues implements search_tag_values operation.
//
// This endpoint retrieves all discovered values for the given tag, which can be used in search.
//
// GET /api/search/tag/{tag_name}/values
func (s *Server) SearchTagValues(ctx context.Context, params tempoapi.SearchTagValuesParams) (*tempoapi.TagValues, error) {
	return s.api.SearchTagValues(ctx, params)
}

// SearchTagValuesV2 implements search_tag_values_v2 operation.
//
// This endpoint retrieves all discovered values and their data types for the given TraceQL
// identifier.
//
// GET /api/v2/search/tag/{tag_name}/values
func (s *Server) SearchTagValuesV2(ctx context.Context, params tempoapi.SearchTagValuesV2Params) (*tempoapi.TagValuesV2, error) {
	return s.api.SearchTagValuesV2(ctx, params)
}

// SearchTags implements search_tags operation.
//
// This endpoint retrieves all discovered tag names that can be used in search.
//
// GET /api/search/tags
func (s *Server) SearchTags(ctx context.Context, params tempoapi.SearchTagsParams) (*tempoapi.TagNames, error) {
	return s.api.SearchTags(ctx, params)
}

// SearchTagsV2 implements searchTagsV2 operation.
//
// This endpoint retrieves all discovered tag names that can be used in search.
//
// GET /api/v2/search/tags
func (s *Server) SearchTagsV2(ctx context.Context, params tempoapi.SearchTagsV2Params) (*tempoapi.TagNamesV2, error) {
	return s.api.SearchTagsV2(ctx, params)
}

// TraceByID implements traceByID operation.
//
// Querying traces by id.
//
// GET /api/traces/{traceID}
func (s *Server) TraceByID(ctx context.Context, params tempoapi.TraceByIDParams) (tempoapi.TraceByIDRes, error) {
	return s.api.TraceByID(ctx, params)
}

// TraceByIDv2 implements traceByIDv2 operation.
//
// Querying traces by id.
//
// GET /api/v2/traces/{traceID}
func (s *Server) TraceByIDv2(ctx context.Context, params tempoapi.TraceByIDv2Params) (tempoapi.TraceByIDv2Res, error) {
	return s.api.TraceByIDv2(ctx, params)
}

// NewError creates *ErrorStatusCode from error returned by handler.
//
// Used for common default response.
func (s *Server) NewError(ctx context.Context, err error) *tempoapi.ErrorStatusCode {
	zctx.From(ctx).Error("API Error", zap.Error(err))
	if v, ok := errors.Into[*tempoapi.ErrorStatusCode](err); ok {
		// Pass as-is.
		return v
	}
	return &tempoapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   tempoapi.Error(err.Error()),
	}
}
