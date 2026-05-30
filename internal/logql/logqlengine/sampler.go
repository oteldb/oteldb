package logqlengine

import (
	"context"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlmetric"
)

// SamplingNode implements entry sampling.
type SamplingNode struct {
	Input PipelineNode
	Expr  *logql.RangeAggregationExpr
}

var _ SampleNode = (*SamplingNode)(nil)

func (e *Engine) buildSampleNode(ctx context.Context, expr *logql.RangeAggregationExpr) (SampleNode, error) {
	root, err := e.buildPipelineNode(ctx, expr.Range.Sel, expr.Range.Pipeline)
	if err != nil {
		return nil, err
	}

	return &SamplingNode{
		Input: root,
		Expr:  expr,
	}, nil
}

// Traverse implements [Node].
func (n *SamplingNode) Traverse(cb NodeVisitor) error {
	if err := cb(n); err != nil {
		return err
	}
	return n.Input.Traverse(cb)
}

// EvalSample implements [SampleNode].
func (n *SamplingNode) EvalSample(ctx context.Context, params EvalParams) (SampleIterator, error) {
	iter, err := n.Input.EvalPipeline(ctx, params)
	if err != nil {
		return nil, err
	}
	return newSampleIterator(iter, n.Expr)
}

type sampleIterator struct {
	iter    EntryIterator
	sampler sampleExtractor

	// grouping parameters.
	by      map[string]struct{}
	without map[string]struct{}
}

func newSampleIterator(iter EntryIterator, expr *logql.RangeAggregationExpr) (*sampleIterator, error) {
	sampler, err := buildSampleExtractor(expr)
	if err != nil {
		return nil, errors.Wrap(err, "build sample extractor")
	}

	var (
		by      []logql.Label
		without []logql.Label

		g = expr.Grouping
	)
	if g != nil {
		if g.Without {
			without = g.Labels
		} else {
			by = g.Labels
		}
	}
	if unwrap := expr.Range.Unwrap; unwrap != nil && (g == nil || g.Without) {
		// NOTE(tdakkota): Loki removes label that used to sample entry (convert to metric)
		// 	unless grouping by is explicitly specified.
		without = append(without, unwrap.Label)
	}

	return &sampleIterator{
		iter:    iter,
		sampler: sampler,
		by:      buildSet(nil, by...),
		without: buildSet(nil, without...),
	}, nil
}

func buildSet[K ~string](r map[string]struct{}, input ...K) map[string]struct{} {
	if len(input) == 0 {
		return r
	}

	if r == nil {
		r = make(map[string]struct{}, len(input))
	}
	for _, k := range input {
		r[string(k)] = struct{}{}
	}
	return r
}

func (i *sampleIterator) Next(s *logqlmetric.SampledEntry) bool {
	var e Entry
	for {
		if !i.iter.Next(&e) {
			return false
		}

		v, ok := i.sampler.Extract(e)
		if !ok {
			continue
		}

		s.Timestamp = e.Timestamp
		s.Sample = v
		s.Set = logqlabels.AggregatedLabelsFromSet(e.Set, i.by, i.without)
		return true
	}
}

func (i *sampleIterator) Err() error {
	return i.iter.Err()
}

func (i *sampleIterator) Close() error {
	return i.iter.Close()
}

// sampleExtractor extracts samples from log records.
type sampleExtractor interface {
	Extract(e Entry) (float64, bool)
}

func buildSampleExtractor(expr *logql.RangeAggregationExpr) (sampleExtractor, error) {
	qrange := expr.Range
	switch expr.Op {
	case logql.RangeOpCount, logql.RangeOpRate, logql.RangeOpAbsent:
		return &lineCounterExtractor{}, nil
	case logql.RangeOpBytes, logql.RangeOpBytesRate:
		return &bytesCounterExtractor{}, nil
	case logql.RangeOpRateCounter,
		logql.RangeOpAvg,
		logql.RangeOpSum,
		logql.RangeOpMin,
		logql.RangeOpMax,
		logql.RangeOpStdvar,
		logql.RangeOpStddev,
		logql.RangeOpQuantile,
		logql.RangeOpFirst,
		logql.RangeOpLast:
		unwrap := qrange.Unwrap
		if unwrap == nil {
			return nil, errors.Errorf("operation %q require unwrap expression", expr.Op)
		}

		le := &labelsExtractor{
			label: unwrap.Label,
		}

		switch unwrap.Op {
		case "":
			le.converter = func(s string) (float64, error) {
				return strconv.ParseFloat(s, 64)
			}
		case "bytes":
			le.converter = convertBytes
		case "duration", "duration_seconds":
			le.converter = convertDuration
		default:
			return nil, errors.Errorf("unknown conversion operation %q", unwrap.Op)
		}

		switch len(unwrap.Filters) {
		case 0:
			le.postfilter = NopProcessor
		case 1:
			proc, err := buildLabelMatcher(unwrap.Filters[0])
			if err != nil {
				return nil, err
			}
			le.postfilter = proc
		default:
			procs := make([]Processor, len(unwrap.Filters))
			var err error
			for i, f := range unwrap.Filters {
				procs[i], err = buildLabelMatcher(f)
				if err != nil {
					return nil, err
				}
			}
			le.postfilter = &Pipeline{Stages: procs}
		}

		return le, nil
	default:
		return nil, errors.Errorf("unknown range operation %q", expr.Op)
	}
}

func convertBytes(s string) (float64, error) {
	v, err := humanize.ParseBytes(s)
	if err != nil {
		return 0, err
	}
	return float64(v), nil
}

func convertDuration(s string) (float64, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	return d.Seconds(), nil
}

type lineCounterExtractor struct{}

func (*lineCounterExtractor) Extract(Entry) (float64, bool) {
	return 1., true
}

type bytesCounterExtractor struct{}

func (*bytesCounterExtractor) Extract(e Entry) (float64, bool) {
	return float64(len(e.Line)), true
}

type labelsExtractor struct {
	label      logql.Label
	converter  func(string) (float64, error)
	postfilter Processor
}

func (l *labelsExtractor) Extract(e Entry) (p float64, _ bool) {
	v, ok := e.Set.GetString(l.label)
	if !ok {
		return p, false
	}

	p, _ = l.converter(v)
	// TODO(tdakkota): save error

	_, ok = l.postfilter.Process(e.Timestamp, e.Line, e.Set)
	return p, ok
}
