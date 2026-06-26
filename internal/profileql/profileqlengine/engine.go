// Package profileqlengine implements the ProfileQL evaluation engine.
//
// It parses ProfileQL queries, selects matching profiles from a
// [profilestorage.Querier], and renders them into the Pyroscope-compatible
// flamebearer format.
package profileqlengine

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/profileql"
	"github.com/oteldb/oteldb/internal/profilestorage"
	"github.com/oteldb/oteldb/internal/pyroscopeapi"
	"github.com/oteldb/oteldb/internal/xattribute"
)

// Engine is a ProfileQL evaluation engine.
type Engine struct {
	querier profilestorage.Querier

	tracer trace.Tracer
}

// Options sets [Engine] options.
type Options struct {
	// TracerProvider provides the OpenTelemetry tracer for this engine.
	TracerProvider trace.TracerProvider
}

func (o *Options) setDefaults() {
	if o.TracerProvider == nil {
		o.TracerProvider = otel.GetTracerProvider()
	}
}

// NewEngine creates a new [Engine].
func NewEngine(querier profilestorage.Querier, opts Options) *Engine {
	opts.setDefaults()

	return &Engine{
		querier: querier,
		tracer:  opts.TracerProvider.Tracer("profileql.Engine"),
	}
}

// EvalParams sets evaluation parameters.
type EvalParams struct {
	// Time range to select, optional.
	Start time.Time
	End   time.Time
	// MaxNodes, when positive, limits the number of nodes in the resulting
	// flamegraph; smaller nodes are folded into an "other" node.
	MaxNodes int
}

// Eval parses and evaluates the query, returning a flamebearer profile.
func (e *Engine) Eval(ctx context.Context, query string, params EvalParams) (profile *pyroscopeapi.FlamebearerProfileV1, rerr error) {
	ctx, span := e.tracer.Start(ctx, "Eval",
		trace.WithAttributes(
			attribute.String("profileql.query", query),
			xattribute.UnixNano("profileql.params.start", params.Start),
			xattribute.UnixNano("profileql.params.end", params.End),
			attribute.Int("profileql.params.max_nodes", params.MaxNodes),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	expr, err := profileql.Parse(query)
	if err != nil {
		return nil, errors.Wrap(err, "parse")
	}

	tree, err := e.querier.SelectMergeProfile(ctx, profilestorage.SelectProfileParams{
		Type:     expr.Type,
		Matchers: expr.Matchers,
		Start:    params.Start,
		End:      params.End,
		MaxNodes: params.MaxNodes,
	})
	if err != nil {
		return nil, errors.Wrap(err, "select merge profile")
	}

	profile = buildFlamebearerProfile(tree, expr.Type, params.MaxNodes)
	span.AddEvent("return_result", trace.WithAttributes(
		attribute.Int("profileql.names", len(profile.Flamebearer.Names)),
		attribute.Int("profileql.levels", len(profile.Flamebearer.Levels)),
		attribute.Int("profileql.num_ticks", profile.Flamebearer.NumTicks),
	))
	return profile, nil
}
