package traceqlengine

import (
	"fmt"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

func mergeSpansetsBy[L, R Processor, Op SpansetOp](sets []Spanset, left L, op Op, right R) (output []Spanset, _ error) {
	for _, set := range sets {
		leftSet, err := left.Process([]Spanset{set})
		if err != nil {
			return nil, errors.Wrap(err, "evaluate left")
		}

		rightSet, err := right.Process([]Spanset{set})
		if err != nil {
			return nil, errors.Wrap(err, "evaluate right")
		}

		spans, err := op(leftSet, rightSet)
		if err != nil {
			return nil, errors.Wrap(err, "evaluate spanset op")
		}
		if len(spans) == 0 {
			continue
		}

		set.Spans = spans
		output = append(output, set)
	}
	return output, nil
}

// SpansetOp merges two spansets.
type SpansetOp func(a, b []Spanset) ([]tracestorage.Span, error)

func buildSpansetOp(op traceql.SpansetOp) (SpansetOp, error) {
	switch op {
	case traceql.SpansetOpAnd:
		return func(a, b []Spanset) ([]tracestorage.Span, error) {
			if len(a) == 0 || len(b) == 0 {
				return nil, nil
			}
			return mergeSpans(a, b), nil
		}, nil
	case traceql.SpansetOpChild:
		return func(a, b []Spanset) (result []tracestorage.Span, _ error) {
			if len(a) == 0 && len(b) == 0 {
				return nil, nil
			}
			if len(a) != 1 && len(b) != 1 {
				return nil, errors.New("can't find children spans of multiple spansets at once, try to use colaesce()")
			}
			return childSpans(a[0], b[0]), nil
		}, nil
	case traceql.SpansetOpUnion:
		return func(a, b []Spanset) ([]tracestorage.Span, error) {
			if len(a) == 0 && len(b) == 0 {
				return nil, nil
			}
			return mergeSpans(a, b), nil
		}, nil
	case traceql.SpansetOpSibling:
		return func(a, b []Spanset) (result []tracestorage.Span, _ error) {
			if len(a) == 0 && len(b) == 0 {
				return nil, nil
			}
			if len(a) != 1 && len(b) != 1 {
				return nil, errors.New("can't find siblings of multiple spansets at once, try to use colaesce()")
			}
			return siblingSpans(a[0], b[0]), nil
		}, nil
	case traceql.SpansetOpDescendant:
		return nil, &UnsupportedError{Msg: fmt.Sprintf("unsupported spanset op %q", op)}
	default:
		return nil, errors.Errorf("unexpected spanset op %q", op)
	}
}

type spanMerger struct {
	seen   map[otelstorage.SpanID]struct{}
	result []tracestorage.Span
}

func (s *spanMerger) Add(span tracestorage.Span) {
	spanID := span.SpanID
	if _, ok := s.seen[spanID]; ok {
		// Span already in result.
		return
	}

	if s.seen == nil {
		s.seen = map[otelstorage.SpanID]struct{}{}
	}
	s.seen[spanID] = struct{}{}
	s.result = append(s.result, span)
}

func (s *spanMerger) Result() []tracestorage.Span {
	return s.result
}

func mergeSpans(left, right []Spanset) []tracestorage.Span {
	// Left and right spasnets are guaranteed to have the same Trace ID.
	smaller := left
	bigger := right
	if len(right) < len(left) {
		smaller, bigger = right, left
	}

	m := spanMerger{}
	for _, ss := range smaller {
		for _, span := range ss.Spans {
			m.Add(span)
		}
	}

	for _, ss := range bigger {
		for _, span := range ss.Spans {
			m.Add(span)
		}
	}

	return m.result
}

func childSpans(left, right Spanset) []tracestorage.Span {
	leftIDs := map[otelstorage.SpanID]struct{}{}
	for _, span := range left.Spans {
		leftIDs[span.SpanID] = struct{}{}
	}

	m := spanMerger{}
	for _, span := range right.Spans {
		parentID := span.ParentSpanID
		if parentID.IsEmpty() {
			continue
		}

		if _, ok := leftIDs[parentID]; ok {
			m.Add(span)
		}
	}

	return m.Result()
}

func siblingSpans(left, right Spanset) []tracestorage.Span {
	leftParentIDs := map[otelstorage.SpanID]struct{}{}
	for _, span := range left.Spans {
		leftParentIDs[span.ParentSpanID] = struct{}{}
	}

	m := spanMerger{}
	for _, span := range right.Spans {
		if _, ok := leftParentIDs[span.ParentSpanID]; !ok {
			continue
		}
		m.Add(span)
	}

	return m.Result()
}
