package queryapi

import (
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/metricstorage"
	"github.com/oteldb/oteldb/internal/promql"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// MetricQuerier serves the Prometheus API.
type MetricQuerier interface {
	promql.Querier
	metricstorage.MetadataQuerier
}

// LogQuerier serves the Loki API.
type LogQuerier interface {
	logstorage.Querier
	logqlengine.Querier
}

// TraceQuerier serves the Tempo API.
type TraceQuerier interface {
	tracestorage.Querier
	traceqlengine.Querier
}
