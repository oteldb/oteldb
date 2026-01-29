// Package lokihandler provides Loki API implementation.
package lokihandler

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	ht "github.com/ogen-go/ogen/http"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlerrors"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/lokiapi"
)

// LokiAPI implements lokiapi.Handler.
type LokiAPI struct {
	q      logstorage.Querier
	engine *logqlengine.Engine
	opts   LokiAPIOptions
}

var _ lokiapi.Handler = (*LokiAPI)(nil)

// NewLokiAPI creates new LokiAPI.
func NewLokiAPI(q logstorage.Querier, engine *logqlengine.Engine, opts LokiAPIOptions) *LokiAPI {
	opts.setDefaults()

	return &LokiAPI{
		q:      q,
		engine: engine,
		opts:   opts,
	}
}

// DetectedFieldValues implements detectedFieldValues operation.
//
// Get detected field values.
//
// GET /loki/api/v1/detected_field/{field}/values
func (h *LokiAPI) DetectedFieldValues(ctx context.Context, params lokiapi.DetectedFieldValuesParams) (*lokiapi.DetectedFieldValues, error) {
	return &lokiapi.DetectedFieldValues{}, nil
}

// DetectedFields implements detectedFields operation.
//
// Get detected fields.
//
// GET /loki/api/v1/detected_fields
func (h *LokiAPI) DetectedFields(ctx context.Context, params lokiapi.DetectedFieldsParams) (*lokiapi.DetectedFields, error) {
	return &lokiapi.DetectedFields{}, nil
}

// DetectedLabels implements detectedLabels operation.
//
// Get detected labels.
// Used by Grafana to test Logs Drilldown availability.
//
// GET /loki/api/v1/detected_labels
func (h *LokiAPI) DetectedLabels(ctx context.Context, params lokiapi.DetectedLabelsParams) (*lokiapi.DetectedLabels, error) {
	if !h.opts.DrilldownEnabled {
		return &lokiapi.DetectedLabels{}, nil
	}

	start, end, err := parseTimeRange(
		time.Now(),
		params.Start,
		params.End,
		params.Since,
		h.opts.DefaultSince,
	)
	if err != nil {
		return nil, validationErr(err, "parse time range")
	}

	var sel logql.Selector
	if q := params.Query.Or(""); q != "" {
		sel, err = logql.ParseSelector(q, h.engine.ParseOptions())
		if err != nil {
			return nil, validationErr(err, "parse query")
		}
	}
	labels, err := h.q.DetectedLabels(ctx, logstorage.LabelsOptions{
		Start: start,
		End:   end,
		Query: sel,
		Limit: 100,
	})
	if err != nil {
		return nil, executionErr(err, "get detected labels")
	}

	result := make([]lokiapi.DetectedLabel, len(labels))
	for i, v := range labels {
		result[i] = lokiapi.DetectedLabel{
			Label:       v.Name,
			Cardinality: v.Cardinality,
		}
	}

	return &lokiapi.DetectedLabels{
		DetectedLabels: result,
	}, nil
}

// DrilldownLimits implements drilldownLimits operation.
//
// Get drilldown limits.
// Used by Grafana to get limits from Loki.
//
// GET /loki/api/v1/drilldown-limits
func (h *LokiAPI) DrilldownLimits(ctx context.Context) (*lokiapi.DrilldownLimits, error) {
	return &lokiapi.DrilldownLimits{
		Limits: lokiapi.DrilldownLimitsLimits{
			VolumeEnabled: lokiapi.NewOptBool(h.opts.DrilldownEnabled),
		},
		Version: "v3.6.0",
	}, nil
}

// IndexStats implements indexStats operation.
//
// Get index stats.
//
// GET /loki/api/v1/index/stats
func (h *LokiAPI) IndexStats(context.Context, lokiapi.IndexStatsParams) (*lokiapi.IndexStats, error) {
	// No stats for now.
	return &lokiapi.IndexStats{}, nil
}

// LabelValues implements labelValues operation.
// Get values of label.
//
// GET /loki/api/v1/label/{name}/values
func (h *LokiAPI) LabelValues(ctx context.Context, params lokiapi.LabelValuesParams) (*lokiapi.Values, error) {
	lg := zctx.From(ctx)

	start, end, err := parseTimeRange(
		time.Now(),
		params.Start,
		params.End,
		params.Since,
		h.opts.DefaultSince,
	)
	if err != nil {
		return nil, validationErr(err, "parse time range")
	}

	var sel logql.Selector
	if q := params.Query.Or(""); q != "" {
		sel, err = logql.ParseSelector(q, h.engine.ParseOptions())
		if err != nil {
			return nil, validationErr(err, "parse query")
		}
	}

	iter, err := h.q.LabelValues(ctx, params.Name, logstorage.LabelsOptions{
		Start: start,
		End:   end,
		Query: sel,
	})
	if err != nil {
		return nil, executionErr(err, "get label values")
	}
	defer func() {
		_ = iter.Close()
	}()

	var values []string
	if err := iterators.ForEach(iter, func(tag logstorage.Label) error {
		values = append(values, tag.Value)
		return nil
	}); err != nil {
		return nil, executionErr(err, "read tags")
	}
	lg.Debug("Got tag values",
		zap.String("label_name", params.Name),
		zap.Int("count", len(values)),
	)

	return &lokiapi.Values{
		Status: "success",
		Data:   values,
	}, nil
}

// Labels implements labels operation.
//
// Get labels.
// Used by Grafana to test connection to Loki.
//
// GET /loki/api/v1/labels
func (h *LokiAPI) Labels(ctx context.Context, params lokiapi.LabelsParams) (*lokiapi.Labels, error) {
	lg := zctx.From(ctx)

	start, end, err := parseTimeRange(
		time.Now(),
		params.Start,
		params.End,
		params.Since,
		h.opts.DefaultSince,
	)
	if err != nil {
		return nil, validationErr(err, "parse time range")
	}

	names, err := h.q.LabelNames(ctx, logstorage.LabelsOptions{
		Start: start,
		End:   end,
	})
	if err != nil {
		return nil, executionErr(err, "get label names")
	}
	lg.Debug("Got label names", zap.Int("count", len(names)))

	return &lokiapi.Labels{
		Status: "success",
		Data:   names,
	}, nil
}

// Query implements query operation.
//
// Query.
//
// GET /loki/api/v1/query
func (h *LokiAPI) Query(ctx context.Context, params lokiapi.QueryParams) (*lokiapi.QueryResponse, error) {
	ts, err := ParseTimestamp(params.Time.Value, time.Now())
	if err != nil {
		return nil, validationErr(err, "parse time")
	}

	direction, err := parseDirection(params.Direction)
	if err != nil {
		return nil, validationErr(err, "parse direction")
	}

	data, err := h.eval(ctx, params.Query, logqlengine.EvalParams{
		Start:     ts,
		End:       ts,
		Step:      0,
		Direction: direction,
		Limit:     params.Limit.Or(100),
	})
	if err != nil {
		return nil, evalErr(err, "instant query")
	}

	return &lokiapi.QueryResponse{
		Status: "success",
		Data:   data,
	}, nil
}

// QueryRange implements queryRange operation.
//
// Query range.
//
// GET /loki/api/v1/query_range
func (h *LokiAPI) QueryRange(ctx context.Context, params lokiapi.QueryRangeParams) (*lokiapi.QueryResponse, error) {
	start, end, err := parseTimeRange(
		time.Now(),
		params.Start,
		params.End,
		params.Since,
		h.opts.DefaultSince,
	)
	if err != nil {
		return nil, validationErr(err, "parse time range")
	}

	step, err := parseStep(params.Step, start, end)
	if err != nil {
		return nil, validationErr(err, "parse step")
	}

	direction, err := parseDirection(params.Direction)
	if err != nil {
		return nil, validationErr(err, "parse direction")
	}

	data, err := h.eval(ctx, params.Query, logqlengine.EvalParams{
		Start:     start,
		End:       end,
		Step:      step,
		Direction: direction,
		Limit:     params.Limit.Or(100),
	})
	if err != nil {
		return nil, evalErr(err, "range query")
	}

	return &lokiapi.QueryResponse{
		Status: "success",
		Data:   data,
	}, nil
}

// QueryVolume implements queryVolume operation.
//
// Query the index for volume information about label and label-value combinations.
//
// GET /loki/api/v1/index/volume
func (h *LokiAPI) QueryVolume(ctx context.Context, params lokiapi.QueryVolumeParams) (*lokiapi.QueryResponse, error) {
	if !h.opts.DrilldownEnabled {
		return &lokiapi.QueryResponse{
			Status: "success",
			Data: lokiapi.NewVectorResultQueryResponseData(lokiapi.VectorResult{
				Result: lokiapi.Vector{},
			}),
		}, nil
	}

	start, end, err := parseTimeRange(
		time.Now(),
		params.Start,
		params.End,
		params.Since,
		h.opts.DefaultSince,
	)
	if err != nil {
		return nil, validationErr(err, "parse time range")
	}

	data, err := h.evalVolumeQuery(ctx, params.Query.Or(""), params.TargetLabels.Or(""), logqlengine.EvalParams{
		Start:     start,
		End:       end,
		Step:      0,
		Direction: logqlengine.DirectionBackward,
		Limit:     params.Limit.Or(100),
	})
	if err != nil {
		return nil, err
	}

	return &lokiapi.QueryResponse{
		Status: "success",
		Data:   data,
	}, nil
}

// QueryVolumeRange implements queryVolumeRange operation.
//
// Query the index for volume information about label and label-value combinations.
//
// GET /loki/api/v1/index/volume_range
func (h *LokiAPI) QueryVolumeRange(ctx context.Context, params lokiapi.QueryVolumeRangeParams) (*lokiapi.QueryResponse, error) {
	if !h.opts.DrilldownEnabled {
		return &lokiapi.QueryResponse{
			Status: "success",
			Data: lokiapi.NewVectorResultQueryResponseData(lokiapi.VectorResult{
				Result: lokiapi.Vector{},
			}),
		}, nil
	}

	start, end, err := parseTimeRange(
		time.Now(),
		params.Start,
		params.End,
		params.Since,
		h.opts.DefaultSince,
	)
	if err != nil {
		return nil, validationErr(err, "parse time range")
	}

	step, err := parseStep(params.Step, start, end)
	if err != nil {
		return nil, validationErr(err, "parse step")
	}

	data, err := h.evalVolumeQuery(ctx, params.Query.Or(""), params.TargetLabels.Or(""), logqlengine.EvalParams{
		Start:     start,
		End:       end,
		Step:      step,
		Direction: logqlengine.DirectionBackward,
		Limit:     params.Limit.Or(100),
	})
	if err != nil {
		return nil, err
	}

	return &lokiapi.QueryResponse{
		Status: "success",
		Data:   data,
	}, nil
}

func (h *LokiAPI) evalVolumeQuery(ctx context.Context, query, targetLabels string, params logqlengine.EvalParams) (r lokiapi.QueryResponseData, _ error) {
	var (
		err error
		sel logql.Selector
	)
	if query != "" {
		sel, err = logql.ParseSelector(query, h.engine.ParseOptions())
		if err != nil {
			return r, validationErr(err, "parse query")
		}
	}
	var agg []logql.Label
	if targetLabels != "" {
		agg = make([]logql.Label, 0, strings.Count(targetLabels, ",")+1)
		for v := range strings.SplitSeq(targetLabels, ",") {
			agg = append(agg, logql.Label(v))
		}
	} else {
		agg = make([]logql.Label, len(sel.Matchers))
		for i, m := range sel.Matchers {
			agg[i] = m.Label
		}
	}
	slices.Sort(agg)
	agg = slices.Compact(agg)

	aggRange := params.End.Sub(params.Start).Truncate(time.Second)
	if aggRange == 0 {
		aggRange = time.Hour
	}
	expr := &logql.VectorAggregationExpr{
		Op: logql.VectorOpSum,
		Expr: &logql.RangeAggregationExpr{
			Op: logql.RangeOpCount,
			Range: logql.LogRangeExpr{
				Sel:   sel,
				Range: aggRange,
			},
		},
		Grouping: &logql.Grouping{
			Labels:  agg,
			Without: false,
		},
	}
	// We need an instant.
	params.Start = params.End

	q, err := h.engine.NewQueryFromExpr(ctx, expr)
	if err != nil {
		return r, errors.Wrap(err, "compile query")
	}
	r, err = q.Eval(ctx, params)
	if err != nil {
		return r, err
	}
	return r, nil
}

// Series implements series operation.
//
// Get series.
//
// GET /loki/api/v1/series
func (h *LokiAPI) Series(ctx context.Context, params lokiapi.SeriesParams) (*lokiapi.Maps, error) {
	start, end, err := parseTimeRange(
		time.Now(),
		params.Start,
		params.End,
		params.Since,
		h.opts.DefaultSince,
	)
	if err != nil {
		return nil, validationErr(err, "parse time range")
	}

	selectors := make([]logql.Selector, len(params.Match))
	for i, m := range params.Match {
		selectors[i], err = logql.ParseSelector(m, h.engine.ParseOptions())
		if err != nil {
			return nil, validationErr(err, fmt.Sprintf("invalid match[%d]", i))
		}
	}

	series, err := h.q.Series(ctx, logstorage.SeriesOptions{
		Start:     start,
		End:       end,
		Selectors: selectors,
	})
	if err != nil {
		return nil, executionErr(err, "get series")
	}

	// FIXME(tdakkota): copying slice only because generated type is named.
	result := make([]lokiapi.MapsDataItem, len(series))
	for i, s := range series {
		result[i] = s
	}

	return &lokiapi.Maps{
		Status: "success",
		Data:   result,
	}, nil
}

// Patterns implements patterns operation.
//
// Endpoint can be used to query loki for patterns detected in the logs.
// This helps understand the structure of the logs Loki has ingested.
//
// GET /loki/api/v1/patterns
func (h *LokiAPI) Patterns(ctx context.Context, params lokiapi.PatternsParams) (*lokiapi.Patterns, error) {
	return &lokiapi.Patterns{}, nil
}

// Push implements push operation.
//
// Push data.
//
// POST /loki/api/v1/push
func (h *LokiAPI) Push(context.Context, lokiapi.PushReq) error {
	return ht.ErrNotImplemented
}

func (h *LokiAPI) eval(ctx context.Context, query string, params logqlengine.EvalParams) (r lokiapi.QueryResponseData, _ error) {
	q, err := h.engine.NewQuery(ctx, query)
	if err != nil {
		return r, errors.Wrap(err, "compile query")
	}
	r, err = q.Eval(ctx, params)
	if err != nil {
		return r, err
	}
	return r, nil
}

// NewError creates *ErrorStatusCode from error returned by handler.
//
// Used for common default response.
func (h *LokiAPI) NewError(_ context.Context, err error) *lokiapi.ErrorStatusCode {
	code := http.StatusBadRequest
	if _, ok := errors.Into[*logqlerrors.UnsupportedError](err); ok {
		code = http.StatusNotImplemented
	}
	return &lokiapi.ErrorStatusCode{
		StatusCode: code,
		Response:   lokiapi.Error(err.Error()),
	}
}
