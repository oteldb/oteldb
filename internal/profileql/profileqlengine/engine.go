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

// Result is the outcome of a ProfileQL query: a merged flamegraph tree together
// with the profile type it was selected for. It can be rendered into the various
// Pyroscope output formats.
type Result struct {
	// Tree is the merged, symbol-resolved flamegraph tree. It may be nil for an
	// empty result.
	Tree *profilestorage.FlameTree
	// Type is the profile type that was selected.
	Type profileql.ProfileType
	// MaxNodes is the node limit requested for the query, if any.
	MaxNodes int
}

// Select parses and evaluates the query, returning the merged result.
func (e *Engine) Select(ctx context.Context, query string, params EvalParams) (result *Result, rerr error) {
	ctx, span := e.tracer.Start(ctx, "Select",
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

	span.AddEvent("return_result", trace.WithAttributes(
		attribute.Int64("profileql.total", tree.Total()),
	))
	return &Result{
		Tree:     tree,
		Type:     expr.Type,
		MaxNodes: params.MaxNodes,
	}, nil
}

// Eval parses and evaluates the query, returning a flamebearer profile.
func (e *Engine) Eval(ctx context.Context, query string, params EvalParams) (*pyroscopeapi.FlamebearerProfileV1, error) {
	result, err := e.Select(ctx, query, params)
	if err != nil {
		return nil, err
	}
	return result.Flamebearer(), nil
}

// Flamebearer renders the result into the Pyroscope flamebearer JSON format.
func (r *Result) Flamebearer() *pyroscopeapi.FlamebearerProfileV1 {
	return buildFlamebearerProfile(r.Tree, r.Type, r.MaxNodes)
}
