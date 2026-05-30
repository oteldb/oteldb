package logqlengine

import (
	"fmt"
	"net/netip"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

func buildLabelFilter(stage *logql.LabelFilter) (Processor, error) {
	return buildLabelPredicate(stage.Pred)
}

func buildLabelPredicate(pred logql.LabelPredicate) (Processor, error) {
	pred = logql.UnparenLabelPredicate(pred)

	switch pred := pred.(type) {
	case *logql.LabelPredicateBinOp:
		left, err := buildLabelPredicate(pred.Left)
		if err != nil {
			return nil, errors.Wrap(err, "build left")
		}

		right, err := buildLabelPredicate(pred.Right)
		if err != nil {
			return nil, errors.Wrap(err, "build right")
		}

		switch pred.Op {
		case logql.OpAnd:
			return &AndLabelMatcher{
				Left:  left,
				Right: right,
			}, nil
		case logql.OpOr:
			return &OrLabelMatcher{
				Left:  left,
				Right: right,
			}, nil
		default:
			return nil, errors.Errorf("unexpected operation %q", pred.Op)
		}
	case *logql.LabelMatcher:
		return buildLabelMatcher(*pred)
	case *logql.DurationFilter:
		return buildDurationLabelFilter(pred)
	case *logql.BytesFilter:
		return buildBytesLabelFilter(pred)
	case *logql.NumberFilter:
		return buildNumberLabelFilter(pred)
	case *logql.IPFilter:
		return buildIPLabelFilter(pred)
	default:
		panic(fmt.Sprintf("unknown predicate %T", pred))
	}
}

// AndLabelMatcher is a AND logical operation for two label matchers.
type AndLabelMatcher struct {
	Left  Processor
	Right Processor
}

// Process implements Processor.
func (m *AndLabelMatcher) Process(ts otelstorage.Timestamp, line string, set logqlabels.LabelSet) (_ string, keep bool) {
	line, keep = m.Left.Process(ts, line, set)
	if !keep {
		return line, keep
	}
	return m.Right.Process(ts, line, set)
}

// OrLabelMatcher is a OR logical operation for two label matchers.
type OrLabelMatcher struct {
	Left  Processor
	Right Processor
}

// Process implements Processor.
func (m *OrLabelMatcher) Process(ts otelstorage.Timestamp, line string, set logqlabels.LabelSet) (_ string, keep bool) {
	line, keep = m.Left.Process(ts, line, set)
	if keep {
		return line, keep
	}
	return m.Right.Process(ts, line, set)
}

// LabelMatcher is a label filter Processor.
type LabelMatcher struct {
	name    logql.Label
	matcher StringMatcher
}

func buildLabelMatcher(pred logql.LabelMatcher) (Processor, error) {
	matcher, err := buildStringMatcher(pred.Op, pred.Value, pred.Re, true)
	if err != nil {
		return nil, err
	}

	return &LabelMatcher{
		name:    pred.Label,
		matcher: matcher,
	}, nil
}

// Process implements Processor.
func (lf *LabelMatcher) Process(_ otelstorage.Timestamp, line string, set logqlabels.LabelSet) (_ string, keep bool) {
	// NOTE(tdakkota): unlike other label matchers, string matcher does not
	// 	return false in case if label not found. Instead, a zero value is matched.
	//
	// See https://github.com/grafana/loki/blob/b4f7181c7aa9484e66976e8a933111a9b85ea8c2/pkg/logql/log/label_filter.go#L377
	labelValue, _ := set.GetString(lf.name)
	keep = lf.matcher.Match(labelValue)
	return line, keep
}

// DurationLabelFilter is a label filter Processor.
type DurationLabelFilter[C Comparator[time.Duration]] struct {
	name  logql.Label
	value time.Duration
	cmp   C
}

func buildDurationLabelFilter(pred *logql.DurationFilter) (Processor, error) {
	switch pred.Op {
	case logql.OpEq:
		return &DurationLabelFilter[EqComparator[time.Duration]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpNotEq:
		return &DurationLabelFilter[NotEqComparator[time.Duration]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpGt:
		return &DurationLabelFilter[GtComparator[time.Duration]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpGte:
		return &DurationLabelFilter[GteComparator[time.Duration]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpLt:
		return &DurationLabelFilter[LtComparator[time.Duration]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpLte:
		return &DurationLabelFilter[LteComparator[time.Duration]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	default:
		return nil, errors.Errorf("unexpected operation %q", pred.Op)
	}
}

// Process implements Processor.
func (lf *DurationLabelFilter[C]) Process(_ otelstorage.Timestamp, line string, set logqlabels.LabelSet) (_ string, keep bool) {
	v, ok := set.GetString(lf.name)
	if !ok {
		return "", false
	}

	labelValue, err := time.ParseDuration(v)
	if err != nil {
		// Keep the line, but set error label.
		set.SetError("duration parsing error", err)
		return line, true
	}

	keep = lf.cmp.Compare(labelValue, lf.value)
	return line, keep
}

// BytesLabelFilter is a label filter Processor.
type BytesLabelFilter[C Comparator[uint64]] struct {
	name  logql.Label
	value uint64
	cmp   C
}

func buildBytesLabelFilter(pred *logql.BytesFilter) (Processor, error) {
	switch pred.Op {
	case logql.OpEq:
		return &BytesLabelFilter[EqComparator[uint64]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpNotEq:
		return &BytesLabelFilter[NotEqComparator[uint64]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpGt:
		return &BytesLabelFilter[GtComparator[uint64]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpGte:
		return &BytesLabelFilter[GteComparator[uint64]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpLt:
		return &BytesLabelFilter[LtComparator[uint64]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpLte:
		return &BytesLabelFilter[LteComparator[uint64]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	default:
		return nil, errors.Errorf("unexpected operation %q", pred.Op)
	}
}

// Process implements Processor.
func (lf *BytesLabelFilter[C]) Process(_ otelstorage.Timestamp, line string, set logqlabels.LabelSet) (_ string, keep bool) {
	v, ok := set.GetString(lf.name)
	if !ok {
		return "", false
	}

	labelValue, err := humanize.ParseBytes(v)
	if err != nil {
		// Keep the line, but set error label.
		set.SetError("bytes parsing error", err)
		return line, true
	}

	keep = lf.cmp.Compare(labelValue, lf.value)
	return line, keep
}

// NumberLabelFilter is a label filter Processor.
type NumberLabelFilter[C Comparator[float64]] struct {
	name  logql.Label
	value float64
	cmp   C
}

func buildNumberLabelFilter(pred *logql.NumberFilter) (Processor, error) {
	switch pred.Op {
	case logql.OpEq:
		return &NumberLabelFilter[EqComparator[float64]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpNotEq:
		return &NumberLabelFilter[NotEqComparator[float64]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpGt:
		return &NumberLabelFilter[GtComparator[float64]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpGte:
		return &NumberLabelFilter[GteComparator[float64]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpLt:
		return &NumberLabelFilter[LtComparator[float64]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	case logql.OpLte:
		return &NumberLabelFilter[LteComparator[float64]]{
			name:  pred.Label,
			value: pred.Value,
		}, nil
	default:
		return nil, errors.Errorf("unexpected operation %q", pred.Op)
	}
}

// Process implements Processor.
func (lf *NumberLabelFilter[C]) Process(_ otelstorage.Timestamp, line string, set logqlabels.LabelSet) (_ string, keep bool) {
	switch val, ok, err := set.GetFloat(lf.name); {
	case err != nil:
		// Keep the line, but set error label.
		set.SetError("number parsing error", err)
		return line, true
	case !ok:
		// No such label, skip the line.
		return "", false
	default:
		keep = lf.cmp.Compare(val, lf.value)
		return line, keep
	}
}

// IPLabelFilter is a label filter Processor.
type IPLabelFilter struct {
	name    logql.Label
	matcher IPMatcher
}

func buildIPLabelFilter(pred *logql.IPFilter) (Processor, error) {
	matcher, err := buildIPMatcher(pred.Value)
	if err != nil {
		return nil, err
	}
	if pred.Op == logql.OpNotEq {
		matcher = NotMatcher[netip.Addr, IPMatcher]{
			Next: matcher,
		}
	}
	return &IPLabelFilter{name: pred.Label, matcher: matcher}, nil
}

// Process implements Processor.
func (lf *IPLabelFilter) Process(_ otelstorage.Timestamp, line string, set logqlabels.LabelSet) (_ string, keep bool) {
	v, ok := set.GetString(lf.name)
	if !ok {
		return "", false
	}

	labelValue, err := netip.ParseAddr(v)
	if err != nil {
		// Keep the line, but set error label.
		set.SetError("ip parsing error", err)
		return line, true
	}

	keep = lf.matcher.Match(labelValue)
	return line, keep
}
