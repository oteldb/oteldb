// Package profilehandler provides a Pyroscope-compatible API implementation
// backed by the ProfileQL engine.
package profilehandler

import (
	"context"
	"net/http"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	ht "github.com/ogen-go/ogen/http"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/profileql"
	"github.com/oteldb/oteldb/internal/profileql/profileqlengine"
	"github.com/oteldb/oteldb/internal/profilestorage"
	"github.com/oteldb/oteldb/internal/pyroscopeapi"
)

// PyroscopeAPI implements [pyroscopeapi.Handler].
type PyroscopeAPI struct {
	q      profilestorage.Querier
	engine *profileqlengine.Engine

	defaultSince time.Duration
}

var _ pyroscopeapi.Handler = (*PyroscopeAPI)(nil)

// NewPyroscopeAPI creates a new [PyroscopeAPI].
func NewPyroscopeAPI(
	q profilestorage.Querier,
	engine *profileqlengine.Engine,
	opts PyroscopeAPIOptions,
) *PyroscopeAPI {
	opts.setDefaults()

	return &PyroscopeAPI{
		q:            q,
		engine:       engine,
		defaultSince: opts.DefaultSince,
	}
}

// GetApps implements getApps operation.
//
// Returns the list of application metadata. Used by Grafana to test the
// connection to Pyroscope.
//
// GET /api/apps
func (h *PyroscopeAPI) GetApps(ctx context.Context) ([]pyroscopeapi.ApplicationMetadata, error) {
	types, err := h.q.ProfileTypes(ctx, profilestorage.ProfileTypesOptions{})
	if err != nil {
		return nil, executionErr(ctx, err, "get profile types")
	}

	apps := make([]pyroscopeapi.ApplicationMetadata, 0, len(types))
	for _, t := range types {
		units := profilestorage.UnitsForProfileType(t)
		apps = append(apps, pyroscopeapi.ApplicationMetadata{
			Name:       pyroscopeapi.NewOptString(t.Name),
			SpyName:    pyroscopeapi.NewOptString("oteldb"),
			SampleRate: pyroscopeapi.NewOptUint32(100),
			Units:      pyroscopeapi.NewOptApplicationMetadataUnits(pyroscopeapi.ApplicationMetadataUnits(units)),
		})
	}
	return apps, nil
}

// Ingest implements ingest operation.
//
// Push data to Pyroscope.
//
// POST /ingest
func (h *PyroscopeAPI) Ingest(ctx context.Context, _ *pyroscopeapi.IngestReqWithContentType, _ pyroscopeapi.IngestParams) error {
	// Ingestion is handled by the OTLP receiver; the storage backend is
	// implemented by oteldb/storage. Not implemented here yet.
	return ht.ErrNotImplemented
}

// LabelValues implements labelValues operation.
//
// Returns the list of label values.
//
// GET /label-values
func (h *PyroscopeAPI) LabelValues(ctx context.Context, params pyroscopeapi.LabelValuesParams) (pyroscopeapi.LabelValues, error) {
	if params.Label == "" {
		return nil, badRequest(ctx, `"label" is required`)
	}

	start, end, err := parseTimeRange(time.Now(), params.From, params.Until, h.defaultSince)
	if err != nil {
		return nil, validationErr(ctx, err, "parse time range")
	}

	typ, matchers, err := h.parseQuery(params.Query)
	if err != nil {
		return nil, validationErr(ctx, err, "parse query")
	}

	// Pyroscope returns profile types when the special profile-type label is
	// requested.
	if params.Label == profileql.LabelProfileType || params.Label == profileql.LabelName {
		types, err := h.q.ProfileTypes(ctx, profilestorage.ProfileTypesOptions{Start: start, End: end})
		if err != nil {
			return nil, executionErr(ctx, err, "get profile types")
		}
		values := make(pyroscopeapi.LabelValues, 0, len(types))
		for _, t := range types {
			values = append(values, t.ID())
		}
		return values, nil
	}

	values, err := h.q.LabelValues(ctx, params.Label, profilestorage.LabelValuesOptions{
		Type:     typ,
		Matchers: matchers,
		Start:    start,
		End:      end,
	})
	if err != nil {
		return nil, executionErr(ctx, err, "get label values")
	}
	return values, nil
}

// Labels implements labels operation.
//
// Returns the list of labels.
//
// GET /labels
func (h *PyroscopeAPI) Labels(ctx context.Context, params pyroscopeapi.LabelsParams) (pyroscopeapi.Labels, error) {
	start, end, err := parseTimeRange(time.Now(), params.From, params.Until, h.defaultSince)
	if err != nil {
		return nil, validationErr(ctx, err, "parse time range")
	}

	typ, matchers, err := h.parseQuery(params.Query)
	if err != nil {
		return nil, validationErr(ctx, err, "parse query")
	}

	names, err := h.q.LabelNames(ctx, profilestorage.LabelNamesOptions{
		Type:     typ,
		Matchers: matchers,
		Start:    start,
		End:      end,
	})
	if err != nil {
		return nil, executionErr(ctx, err, "get label names")
	}
	return names, nil
}

// Render implements render operation.
//
// Renders the given query as a flamebearer profile.
//
// GET /render
func (h *PyroscopeAPI) Render(ctx context.Context, params pyroscopeapi.RenderParams) (*pyroscopeapi.FlamebearerProfileV1, error) {
	if h.engine == nil {
		return nil, &pyroscopeapi.ErrorStatusCode{
			StatusCode: http.StatusInternalServerError,
			Response:   pyroscopeapi.Error(appendTrace(ctx, "ProfileQL engine is disabled")),
		}
	}

	switch params.Format {
	case "", pyroscopeapi.RenderFormatJSON:
		// Only the flamebearer JSON format is supported.
	default:
		return nil, badRequest(ctx, "only JSON render format is supported")
	}

	query, ok := params.Query.Get()
	if !ok || query == "" {
		return nil, badRequest(ctx, `"query" is required`)
	}

	start, end, err := parseTimeRange(time.Now(), params.From, params.Until, h.defaultSince)
	if err != nil {
		return nil, validationErr(ctx, err, "parse time range")
	}

	profile, err := h.engine.Eval(ctx, query, profileqlengine.EvalParams{
		Start:    start,
		End:      end,
		MaxNodes: params.MaxNodes.Or(0),
	})
	if err != nil {
		return nil, executionErr(ctx, err, "eval")
	}
	return profile, nil
}

// parseQuery parses an optional ProfileQL query parameter into a profile type
// and label matchers. A missing or empty query yields nil values (match all).
func (h *PyroscopeAPI) parseQuery(query pyroscopeapi.OptString) (*profileql.ProfileType, []profileql.LabelMatcher, error) {
	q, ok := query.Get()
	if !ok || q == "" {
		return nil, nil, nil
	}
	expr, err := profileql.Parse(q)
	if err != nil {
		return nil, nil, err
	}
	typ := expr.Type
	return &typ, expr.Matchers, nil
}

// NewError creates *ErrorStatusCode from an error returned by the handler.
//
// Used for the common default response.
func (h *PyroscopeAPI) NewError(ctx context.Context, err error) *pyroscopeapi.ErrorStatusCode {
	if v, ok := errors.Into[*pyroscopeapi.ErrorStatusCode](err); ok {
		// Pass as-is.
		return v
	}
	zctx.From(ctx).Error("API error", zap.Error(err))
	return &pyroscopeapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   pyroscopeapi.Error(appendTrace(ctx, err.Error())),
	}
}
