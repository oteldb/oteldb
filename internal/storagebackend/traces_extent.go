package storagebackend

import (
	"time"

	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

type traceExtent struct {
	start, end time.Time
}

func needsTraceExtents(params traceqlengine.SelectSpansetsParams) bool {
	if !params.Start.IsZero() || !params.End.IsZero() || params.MinDuration != 0 || params.MaxDuration != 0 {
		return true
	}
	for _, matcher := range params.TraceDurationMatchers {
		if matcher.Attribute.Prop == traceql.TraceDuration && matcher.Op != 0 {
			return true
		}
	}
	return false
}

func traceExtentPredicate(matchers []traceql.SpanMatcher) func(time.Duration) bool {
	var preds []func(traceql.Static) bool
	for _, m := range matchers {
		if m.Attribute.Prop != traceql.TraceDuration || m.Attribute.Parent || m.Op == 0 {
			continue
		}
		if pred, ok := staticPredicate(m.Op, m.Static); ok {
			preds = append(preds, pred)
		}
	}
	return func(duration time.Duration) bool {
		var value traceql.Static
		value.SetDuration(duration)
		for _, pred := range preds {
			if !pred(value) {
				return false
			}
		}
		return true
	}
}
