// Package promhandler provides Prometheus API implementation.
package promhandler

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/go-faster/errors"
	ht "github.com/ogen-go/ogen/http"
	"github.com/ogen-go/ogen/middleware"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/metricstorage"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/xattribute"
)

// Engine is a Prometheus engine interface.
type Engine interface {
	NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error)
	NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error)
}

// PromAPI implements promapi.Handler.
type PromAPI struct {
	eng       Engine
	store     storage.Queryable
	exemplars storage.ExemplarQueryable

	lookbackDelta time.Duration
}

var _ promapi.Handler = (*PromAPI)(nil)

// NewPromAPI creates new PromAPI.
func NewPromAPI(
	eng Engine,
	store storage.Queryable,
	exemplars storage.ExemplarQueryable,
	opts PromAPIOptions,
) *PromAPI {
	opts.setDefaults()
	return &PromAPI{
		eng:           eng,
		store:         store,
		exemplars:     exemplars,
		lookbackDelta: opts.LookbackDelta,
	}
}

var errResultTruncated = errors.New("results truncated due to limit")

// GetLabelValues implements getLabelValues operation.
// GET /api/v1/label/{label}/values
func (h *PromAPI) GetLabelValues(ctx context.Context, params promapi.GetLabelValuesParams) (*promapi.LabelValuesResponse, error) {
	mint, err := parseOptTimestamp(params.Start, promapi.MinTime)
	if err != nil {
		return nil, validationErr("parse start", err)
	}
	maxt, err := parseOptTimestamp(params.End, promapi.MaxTime)
	if err != nil {
		return nil, validationErr("parse end", err)
	}
	sets, err := parseLabelMatchers(params.Match)
	if err != nil {
		return nil, validationErr("parse match", err)
	}
	hints := &storage.LabelHints{Limit: params.Limit.Or(0)}

	q, err := h.querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = q.Close()
	}()

	// Fast path for cases when match[] is not set.
	if len(sets) < 2 {
		var matchers []*labels.Matcher
		if len(sets) > 0 {
			matchers = sets[0]
		}

		values, annots, err := q.LabelValues(ctx, params.Label, hints, matchers...)
		if err != nil {
			return nil, executionErr("get label values", err)
		}
		if l := params.Limit.Or(-1); l > 0 && len(values) >= l {
			values = values[:l]
			annots.Add(errResultTruncated)
		}

		warnings, infos := annots.AsStrings("", 0, 0)
		return &promapi.LabelValuesResponse{
			Status:   "success",
			Warnings: warnings,
			Infos:    infos,
			Data:     values,
		}, nil
	}

	var (
		dedup  = map[string]struct{}{}
		annots annotations.Annotations
		mux    sync.Mutex
	)
	grp, grpCtx := errgroup.WithContext(ctx)
	for _, set := range sets {
		set := set

		grp.Go(func() error {
			ctx := grpCtx

			vals, w, err := q.LabelValues(ctx, params.Label, hints, set...)
			if err != nil {
				return err
			}

			mux.Lock()
			defer mux.Unlock()

			for _, val := range vals {
				dedup[val] = struct{}{}
			}
			annots = annots.Merge(w)
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return nil, executionErr("get labels", err)
	}

	data := maps.Keys(dedup)
	slices.Sort(data)
	// Truncating data AFTER reading whole data into memory is suboptimal, yet
	// it makes output consistent.
	if l := params.Limit.Or(-1); l > 0 && len(data) >= l {
		data = data[:l]
		annots.Add(errResultTruncated)
	}

	warnings, infos := annots.AsStrings("", 0, 0)
	return &promapi.LabelValuesResponse{
		Status:   "success",
		Warnings: warnings,
		Infos:    infos,
		Data:     data,
	}, nil
}

// GetLabels implements getLabels operation.
//
// GET /api/v1/labels
func (h *PromAPI) GetLabels(ctx context.Context, params promapi.GetLabelsParams) (*promapi.LabelsResponse, error) {
	mint, err := parseOptTimestamp(params.Start, promapi.MinTime)
	if err != nil {
		return nil, validationErr("parse start", err)
	}
	maxt, err := parseOptTimestamp(params.End, promapi.MaxTime)
	if err != nil {
		return nil, validationErr("parse end", err)
	}
	sets, err := parseLabelMatchers(params.Match)
	if err != nil {
		return nil, validationErr("parse match", err)
	}
	hints := &storage.LabelHints{Limit: params.Limit.Or(0)}

	q, err := h.querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = q.Close()
	}()

	// Fast path for cases when match[] is not set.
	if len(sets) < 2 {
		var matchers []*labels.Matcher
		if len(sets) > 0 {
			matchers = sets[0]
		}

		values, annots, err := q.LabelNames(ctx, hints, matchers...)
		if err != nil {
			return nil, executionErr("label names", err)
		}
		if l := params.Limit.Or(-1); l > 0 && len(values) >= l {
			values = values[:l]
			annots.Add(errResultTruncated)
		}

		warnings, infos := annots.AsStrings("", 0, 0)
		return &promapi.LabelsResponse{
			Status:   "success",
			Warnings: warnings,
			Infos:    infos,
			Data:     values,
		}, nil
	}

	var (
		dedup  = map[string]struct{}{}
		annots annotations.Annotations
		mux    sync.Mutex
	)
	grp, grpCtx := errgroup.WithContext(ctx)
	for _, set := range sets {
		set := set

		grp.Go(func() error {
			ctx := grpCtx

			vals, w, err := q.LabelNames(ctx, hints, set...)
			if err != nil {
				return err
			}

			mux.Lock()
			defer mux.Unlock()

			for _, val := range vals {
				dedup[val] = struct{}{}
			}
			annots = annots.Merge(w)
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return nil, executionErr("get labels", err)
	}

	data := maps.Keys(dedup)
	slices.Sort(data)
	// Truncating data AFTER reading whole data into memory is suboptimal, yet
	// it makes output consistent.
	if l := params.Limit.Or(-1); l > 0 && len(data) >= l {
		data = data[:l]
		annots.Add(errResultTruncated)
	}

	warnings, infos := annots.AsStrings("", 0, 0)
	return &promapi.LabelsResponse{
		Status:   "success",
		Warnings: warnings,
		Infos:    infos,
		Data:     data,
	}, nil
}

// PostLabels implements postLabels operation.
//
// POST /api/v1/labels
func (h *PromAPI) PostLabels(ctx context.Context, req *promapi.LabelsForm) (*promapi.LabelsResponse, error) {
	return h.GetLabels(ctx, promapi.GetLabelsParams{
		Start: req.Start,
		End:   req.End,
		Match: req.Match,
	})
}

// GetQuery implements getQuery operation.
//
// Query Prometheus.
//
// GET /api/v1/query
func (h *PromAPI) GetQuery(ctx context.Context, params promapi.GetQueryParams) (*promapi.QueryResponse, error) {
	t, err := parseOptTimestamp(params.Time, time.Now())
	if err != nil {
		return nil, validationErr("parse time", err)
	}
	opts, err := parseQueryOpts(params.LookbackDelta, params.Stats, h.lookbackDelta)
	if err != nil {
		return nil, err
	}

	rawQuery := params.Query
	q, err := h.eng.NewInstantQuery(ctx, h.store, opts, rawQuery, t)
	if err != nil {
		return nil, executionErr("make instant query", err)
	}
	defer q.Close()

	span := trace.SpanFromContext(ctx)
	span.AddEvent("executing_query", trace.WithAttributes(
		xattribute.UnixNano("promapi.time", t),
		attribute.String("promapi.query", q.String()),
	))
	r := q.Exec(ctx)

	span.AddEvent("mapping_result")
	return mapResult(rawQuery, r)
}

// PostQuery implements postQuery operation.
//
// Query Prometheus.
//
// POST /api/v1/query
func (h *PromAPI) PostQuery(ctx context.Context, req *promapi.QueryForm) (*promapi.QueryResponse, error) {
	return h.GetQuery(ctx, promapi.GetQueryParams{
		Query:         req.Query,
		Time:          req.Time,
		LookbackDelta: req.LookbackDelta,
		Stats:         req.Stats,
	})
}

// GetQueryRange implements getQueryRange operation.
//
// Query Prometheus.
//
// GET /api/v1/query_range
func (h *PromAPI) GetQueryRange(ctx context.Context, params promapi.GetQueryRangeParams) (*promapi.QueryResponse, error) {
	start, err := parseTimestamp(params.Start)
	if err != nil {
		return nil, validationErr("parse start", err)
	}
	end, err := parseTimestamp(params.End)
	if err != nil {
		return nil, validationErr("parse end", err)
	}
	step, err := parseStep(params.Step)
	if err != nil {
		return nil, validationErr("parse step", err)
	}
	opts, err := parseQueryOpts(params.LookbackDelta, params.Stats, h.lookbackDelta)
	if err != nil {
		return nil, err
	}

	if end.Before(start) {
		err := errors.New("end timestamp must not be before start time")
		return nil, validationErr("check range", err)
	}

	rawQuery := params.Query
	q, err := h.eng.NewRangeQuery(ctx, h.store, opts, rawQuery, start, end, step)
	if err != nil {
		return nil, executionErr("make range query", err)
	}
	defer q.Close()

	span := trace.SpanFromContext(ctx)
	span.AddEvent("executing_query", trace.WithAttributes(
		xattribute.UnixNano("promapi.start", start),
		xattribute.UnixNano("promapi.end", end),
		attribute.Stringer("promapi.step", step),
		attribute.String("promapi.query", q.String()),
	))
	r := q.Exec(ctx)

	span.AddEvent("mapping_result")
	return mapResult(rawQuery, r)
}

// PostQueryRange implements postQueryRange operation.
//
// Query Prometheus.
//
// POST /api/v1/query_range
func (h *PromAPI) PostQueryRange(ctx context.Context, req *promapi.QueryRangeForm) (*promapi.QueryResponse, error) {
	return h.GetQueryRange(ctx, promapi.GetQueryRangeParams{
		Query:         req.Query,
		Start:         req.Start,
		End:           req.End,
		Step:          req.Step,
		LookbackDelta: req.LookbackDelta,
		Stats:         req.Stats,
	})
}

// GetQueryExemplars implements getQueryExemplars operation.
//
// Query Prometheus.
//
// GET /api/v1/query_exemplars
func (h *PromAPI) GetQueryExemplars(ctx context.Context, params promapi.GetQueryExemplarsParams) (*promapi.QueryExemplarsResponse, error) {
	if h.exemplars == nil {
		return nil, ht.ErrNotImplemented
	}
	start, err := parseTimestamp(params.Start)
	if err != nil {
		return nil, validationErr("parse start", err)
	}
	end, err := parseTimestamp(params.End)
	if err != nil {
		return nil, validationErr("parse end", err)
	}
	if end.Before(start) {
		err := errors.New("end timestamp must not be before start time")
		return nil, validationErr("check range", err)
	}
	expr, err := parser.ParseExpr(params.Query)
	if err != nil {
		return nil, validationErr("parse query", err)
	}
	matcherSets := parser.ExtractSelectors(expr)

	q, err := h.exemplars.ExemplarQuerier(ctx)
	if err != nil {
		return nil, executionErr("get exemplar querier", err)
	}
	queryResults, err := q.Select(start.UnixMilli(), end.UnixMilli(), matcherSets...)
	if err != nil {
		return nil, executionErr("select", err)
	}
	result := make(promapi.Exemplars, len(queryResults))
	for i, r := range queryResults {
		exemplars := make([]promapi.Exemplar, len(r.Exemplars))
		for i, e := range r.Exemplars {
			exemplars[i] = promapi.Exemplar{
				Labels: e.Labels.Map(),
				Value:  e.Value,
				Timestamp: promapi.OptFloat64{
					Value: apiTimestamp(e.Ts),
					Set:   e.HasTs,
				},
			}
		}
		result[i] = promapi.ExemplarsSet{
			SeriesLabels: promapi.NewOptLabelSet(r.SeriesLabels.Map()),
			Exemplars:    exemplars,
		}
	}

	return &promapi.QueryExemplarsResponse{
		Status: "success",
		Data:   result,
	}, nil
}

// PostQueryExemplars implements postQueryExemplars operation.
//
// Query Prometheus.
//
// POST /api/v1/query_exemplars
func (h *PromAPI) PostQueryExemplars(ctx context.Context, params *promapi.ExemplarsForm) (*promapi.QueryExemplarsResponse, error) {
	return h.GetQueryExemplars(ctx, promapi.GetQueryExemplarsParams{
		Query: params.Query,
		Start: params.Start,
		End:   params.End,
	})
}

// GetMetadata implements getMetadata operation.
//
// GET /api/v1/metadata
func (h *PromAPI) GetMetadata(context.Context, promapi.GetMetadataParams) (*promapi.MetadataResponse, error) {
	return nil, ht.ErrNotImplemented
}

// GetRules implements getRules operation.
//
// GET /api/v1/rules
func (h *PromAPI) GetRules(context.Context, promapi.GetRulesParams) (*promapi.RulesResponse, error) {
	return nil, ht.ErrNotImplemented
}

// GetSeries implements getSeries operation.
//
// Query Prometheus.
//
// GET /api/v1/series
func (h *PromAPI) GetSeries(ctx context.Context, params promapi.GetSeriesParams) (*promapi.SeriesResponse, error) {
	mint, err := parseOptTimestamp(params.Start, promapi.MinTime)
	if err != nil {
		return nil, validationErr("parse start", err)
	}
	maxt, err := parseOptTimestamp(params.End, promapi.MaxTime)
	if err != nil {
		return nil, validationErr("parse end", err)
	}
	matchers, err := parseLabelMatchers(params.Match)
	if err != nil {
		return nil, validationErr("parse match", err)
	}
	if len(matchers) == 0 {
		err := errors.New("at least one matcher is required")
		return nil, validationErr("validate match", err)
	}

	q, err := h.querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = q.Close()
	}()

	var result storage.SeriesSet
	if osq, ok := q.(metricstorage.OptimizedSeriesQuerier); ok {
		// TODO(tdakkota): pass limit.
		result = osq.OnlySeries(ctx, false, mint.UnixMilli(), maxt.UnixMilli(), matchers...)
	} else {
		result, err = h.querySeries(ctx, q, mint, maxt, matchers, params)
		if err != nil {
			return nil, err
		}
	}

	var (
		data   []promapi.LabelSet
		annots = result.Warnings()

		limit = params.Limit.Or(-1)
	)
	for result.Next() {
		if limit > 0 && len(data) >= limit {
			data = data[:limit]
			annots.Add(errResultTruncated)
			break
		}

		series := result.At()
		data = append(data, series.Labels().Map())
	}
	if err := result.Err(); err != nil {
		return nil, executionErr("select", err)
	}

	warnings, infos := result.Warnings().AsStrings("", 0, 0)
	return &promapi.SeriesResponse{
		Status:   "success",
		Warnings: warnings,
		Infos:    infos,
		Data:     data,
	}, nil
}

func (h *PromAPI) querySeries(
	ctx context.Context,
	q storage.Querier,
	mint, maxt time.Time,
	matchers [][]*labels.Matcher,
	params promapi.GetSeriesParams,
) (storage.SeriesSet, error) {
	var (
		hints = &storage.SelectHints{
			Start: mint.UnixMilli(),
			End:   maxt.UnixMilli(),
			Limit: params.Limit.Or(0),
			Func:  "series",
		}
		result storage.SeriesSet
	)
	if len(matchers) > 1 {
		var (
			sets        = make([]storage.SeriesSet, len(matchers))
			grp, grpCtx = errgroup.WithContext(ctx)
		)
		for i, mset := range matchers {
			i, mset := i, mset
			grp.Go(func() error {
				ctx := grpCtx

				set := q.Select(ctx, true, hints, mset...)
				if err := set.Err(); err != nil {
					sel := "<match>"
					if m := params.Match; i < len(m) {
						sel = m[i]
					}
					return errors.Wrapf(err, "select %s", sel)
				}
				sets[i] = set
				return nil
			})
		}
		if err := grp.Wait(); err != nil {
			return nil, executionErr("select", err)
		}

		result = storage.NewMergeSeriesSet(sets, 0, storage.ChainedSeriesMerge)
	} else {
		result = q.Select(ctx, false, hints, matchers[0]...)
	}
	return result, nil
}

// PostSeries implements postSeries operation.
//
// Query Prometheus.
//
// POST /api/v1/series
func (h *PromAPI) PostSeries(ctx context.Context, req *promapi.SeriesForm) (*promapi.SeriesResponse, error) {
	return h.GetSeries(ctx, promapi.GetSeriesParams{
		Start: req.Start,
		End:   req.End,
		Match: req.Match,
	})
}

func (h *PromAPI) querier(ctx context.Context, mint, maxt time.Time) (storage.Querier, error) {
	q, err := h.store.Querier(mint.UnixMilli(), maxt.UnixMilli())
	if err != nil {
		return nil, executionErr("get querier", err)
	}
	trace.SpanFromContext(ctx).AddEvent("querier_created", trace.WithAttributes(
		attribute.Int64("promapi.mint", mint.UnixMilli()),
		attribute.Int64("promapi.maxt", maxt.UnixMilli()),
	))
	return q, nil
}

// NewError creates *FailStatusCode from error returned by handler.
//
// Used for common default response.
func (h *PromAPI) NewError(_ context.Context, err error) *promapi.FailStatusCode {
	if _, ok := errors.Into[promql.ErrQueryCanceled](err); ok || errors.Is(err, context.Canceled) {
		return fail(promapi.FailErrorTypeCanceled, err)
	}
	if _, ok := errors.Into[promql.ErrQueryTimeout](err); ok || errors.Is(err, context.DeadlineExceeded) {
		return fail(promapi.FailErrorTypeTimeout, err)
	}
	if _, ok := errors.Into[promql.ErrStorage](err); ok {
		return fail(promapi.FailErrorTypeInternal, err)
	}

	if pe, ok := errors.Into[*PromError](err); ok {
		return fail(pe.Kind, err)
	}

	return fail(promapi.FailErrorTypeInternal, err)
}

func fail(kind promapi.FailErrorType, err error) *promapi.FailStatusCode {
	return &promapi.FailStatusCode{
		StatusCode: promapi.FailToCode(kind),
		Response: promapi.Fail{
			Status:    "error",
			Error:     err.Error(),
			ErrorType: kind,
		},
	}
}

// TimeoutMiddleware sets request timeout by given parameter, if set.
func TimeoutMiddleware() promapi.Middleware {
	return func(req middleware.Request, next middleware.Next) (middleware.Response, error) {
		q := req.Raw.URL.Query()
		if q.Has("timeout") {
			timeout, err := parseDuration(q.Get("timeout"))
			if err != nil {
				return middleware.Response{}, validationErr("parse timeout", err)
			}

			var cancel context.CancelFunc
			req.Context, cancel = context.WithTimeout(req.Context, timeout)
			defer cancel()
		}
		resp, err := next(req)
		return resp, err
	}
}
