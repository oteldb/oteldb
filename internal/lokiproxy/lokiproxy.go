// Package lokiproxy provides Loki proxy for observability and research.
package lokiproxy

import (
	"context"
	"net/http"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/lokiapi"
)

var _ lokiapi.Handler = &Server{}

// NewServer initializes new proxy Server from openapi client.
func NewServer(api *lokiapi.Client) *Server {
	return &Server{api: api}
}

// Server implement proxy server.
type Server struct {
	api *lokiapi.Client
}

// DetectedLabels implements detectedLabels operation.
//
// Get detected labels.
// Used by Grafana to test Logs Drilldown availability.
//
// GET /loki/api/v1/detected_labels
func (s *Server) DetectedLabels(ctx context.Context, params lokiapi.DetectedLabelsParams) (*lokiapi.DetectedLabels, error) {
	return s.api.DetectedLabels(ctx, params)
}

// DrilldownLimits implements drilldownLimits operation.
//
// Get drilldown limits.
// Used by Grafana to get limits from Loki.
//
// GET /loki/api/v1/drilldown-limits
func (s *Server) DrilldownLimits(ctx context.Context) (*lokiapi.DrilldownLimits, error) {
	return s.api.DrilldownLimits(ctx)
}

// IndexStats implements indexStats operation.
//
// Get index stats.
//
// GET /loki/api/v1/index/stats
func (s *Server) IndexStats(ctx context.Context, params lokiapi.IndexStatsParams) (*lokiapi.IndexStats, error) {
	return s.api.IndexStats(ctx, params)
}

// LabelValues implements labelValues operation.
// Get values of label.
//
// GET /loki/api/v1/label/{name}/values
func (s *Server) LabelValues(ctx context.Context, params lokiapi.LabelValuesParams) (*lokiapi.Values, error) {
	return s.api.LabelValues(ctx, params)
}

// Labels implements labels operation.
//
// Get labels.
// Used by Grafana to test connection to Loki.
//
// GET /loki/api/v1/labels
func (s *Server) Labels(ctx context.Context, params lokiapi.LabelsParams) (*lokiapi.Labels, error) {
	return s.api.Labels(ctx, params)
}

// Query implements query operation.
//
// Query.
//
// GET /loki/api/v1/query
func (s *Server) Query(ctx context.Context, params lokiapi.QueryParams) (*lokiapi.QueryResponse, error) {
	return s.api.Query(ctx, params)
}

// QueryRange implements queryRange operation.
//
// Query range.
//
// GET /loki/api/v1/query_range
func (s *Server) QueryRange(ctx context.Context, params lokiapi.QueryRangeParams) (*lokiapi.QueryResponse, error) {
	return s.api.QueryRange(ctx, params)
}

// QueryVolume implements queryVolume operation.
//
// Query the index for volume information about label and label-value combinations.
//
// GET /loki/api/v1/index/volume
func (s *Server) QueryVolume(ctx context.Context, params lokiapi.QueryVolumeParams) (*lokiapi.QueryResponse, error) {
	return s.api.QueryVolume(ctx, params)
}

// QueryVolumeRange implements queryVolumeRange operation.
//
// Query the index for volume information about label and label-value combinations.
//
// GET /loki/api/v1/index/volume_range
func (s *Server) QueryVolumeRange(ctx context.Context, params lokiapi.QueryVolumeRangeParams) (*lokiapi.QueryResponse, error) {
	return s.api.QueryVolumeRange(ctx, params)
}

// Series implements series operation.
//
// Get series.
//
// GET /loki/api/v1/series
func (s *Server) Series(ctx context.Context, params lokiapi.SeriesParams) (*lokiapi.Maps, error) {
	return s.api.Series(ctx, params)
}

// Push implements push operation.
//
// Push data.
//
// GET /loki/api/v1/push
func (s *Server) Push(ctx context.Context, req lokiapi.PushReq) error {
	return s.api.Push(ctx, req)
}

// NewError creates *ErrorStatusCode from error returned by handler.
//
// Used for common default response.
func (s *Server) NewError(ctx context.Context, err error) *lokiapi.ErrorStatusCode {
	zctx.From(ctx).Error("API Error", zap.Error(err))
	if v, ok := errors.Into[*lokiapi.ErrorStatusCode](err); ok {
		// Pass as-is.
		return v
	}
	return &lokiapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   lokiapi.Error(err.Error()),
	}
}
