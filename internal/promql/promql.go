// Package promql constructs the Scarecrow PromQL engine.
package promql

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// Engine evaluates PromQL queries.
type Engine interface {
	NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error)
	NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error)
}

// Querier supplies metric series and exemplars.
type Querier interface {
	storage.Queryable
	storage.ExemplarQueryable
}

// EngineOpts configures query limits and PromQL modifiers.
type EngineOpts = promql.EngineOpts

// New creates a Scarecrow engine, using a native Scanner when q provides one.
func New(q Querier, opts EngineOpts) (Engine, error) {
	engineOpts := scarecrow.Opts{
		LookbackDelta:        opts.LookbackDelta,
		EnableAtModifier:     opts.EnableAtModifier,
		EnableNegativeOffset: opts.EnableNegativeOffset,
		MaxSamples:           opts.MaxSamples,
		Timeout:              opts.Timeout,
	}
	if q, ok := q.(interface{ ScarecrowScanner() scarecrow.Scanner }); ok {
		engineOpts.NewScanner = func(storage.Queryable) scarecrow.Scanner { return q.ScarecrowScanner() }
	}
	return scarecrow.NewEngine(engineOpts), nil
}
