// Package promql provides a PromQL engine implementation.
package promql

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	promqlengine "github.com/oteldb/promql-engine/engine"
	enginestorage "github.com/oteldb/promql-engine/storage"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

// Engine is a Prometheus engine interface.
type Engine interface {
	NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error)
	NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error)
}

// Querier is Prometheus storage interface.
type Querier interface {
	storage.Queryable
	storage.ExemplarQueryable
	MetricsScanners() (enginestorage.Scanners, error)
}

// EngineOpts defines PromQL engine options.
type EngineOpts = promql.EngineOpts

// New creates new [Engine].
func New(q Querier, opts promql.EngineOpts) (Engine, error) {
	scanners, err := q.MetricsScanners()
	if err != nil {
		return nil, errors.Wrap(err, "create scanners")
	}
	return promqlengine.NewWithScanners(promqlengine.Opts{EngineOpts: opts}, scanners), nil
}
