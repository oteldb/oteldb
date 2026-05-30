package logqlengine

import (
	"context"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/xattribute"
)

// LiteralQuery is simple literal expression query.
type LiteralQuery struct {
	Value float64

	stats  engineStats
	tracer trace.Tracer
}

var _ Query = (*LiteralQuery)(nil)

func (e *Engine) buildLiteralQuery(_ context.Context, expr *logql.LiteralExpr) (Query, error) {
	return &LiteralQuery{
		Value: expr.Value,

		stats:  e.stats,
		tracer: e.tracer,
	}, nil
}

// Eval implements [Query].
func (q *LiteralQuery) Eval(ctx context.Context, params EvalParams) (data lokiapi.QueryResponseData, rerr error) {
	start := time.Now()
	ctx, span := q.tracer.Start(ctx, "logql.LiteralQuery", trace.WithAttributes(
		xattribute.UnixNano("logql.params.start", params.Start),
		xattribute.UnixNano("logql.params.end", params.End),
		xattribute.Duration("logql.params.step", params.Step),
		attribute.Stringer("logql.params.direction", params.Direction),
		attribute.Int("logql.params.limit", params.Limit),
	))
	defer func() {
		q.stats.QueryDuration.Record(ctx, time.Since(start).Seconds())
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	if params.IsInstant() {
		data.SetScalarResult(lokiapi.ScalarResult{
			Result: lokiapi.FPoint{
				T: getPrometheusTimestamp(params.Start),
				V: strconv.FormatFloat(q.Value, 'f', -1, 64),
			},
		})
		return data, nil
	}
	data.SetMatrixResult(lokiapi.MatrixResult{
		Result: generateLiteralMatrix(q.Value, params),
	})
	return data, nil
}

func generateLiteralMatrix(value float64, params EvalParams) lokiapi.Matrix {
	var (
		start = params.Start
		end   = params.End

		series = lokiapi.Series{
			Metric: lokiapi.NewOptLabelSet(lokiapi.LabelSet{}),
		}
	)

	var (
		until    = end.Add(params.Step)
		strValue = strconv.FormatFloat(value, 'f', -1, 64)
	)
	for ts := start; !ts.After(until); ts = ts.Add(params.Step) {
		series.Values = append(series.Values, lokiapi.FPoint{
			T: getPrometheusTimestamp(ts),
			V: strValue,
		})
	}

	return lokiapi.Matrix{series}
}

func getPrometheusTimestamp(t time.Time) float64 {
	// Pass milliseconds as fraction part.
	return float64(t.UnixMilli()) / 1000
}
