package storagebackend

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"

	siglog "github.com/oteldb/storage/signal/log"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlmetric"
	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// sampleOp is the per-line sample extracted for a range-aggregation offload.
type sampleOp int

const (
	countSampling sampleOp = iota + 1 // count_over_time / rate: 1 per line
	bytesSampling                     // bytes_over_time / bytes_rate: len(line)
)

// bucketSamplingNode pushes count_over_time/bytes_over_time step-bucketing into the columnar log
// scan (the [logqlengine.BucketedSampleNode] capability), grouped by `by` labels. It buckets per
// (step, group) directly from the severity/timestamp columns instead of materializing a full label
// set per record — the win for `sum by (level) (count_over_time(...))` style queries.
//
// It is installed by [LogQLOptimizer] only for offload-safe shapes (see optimizeSampling): a bare
// storage selector, count/bytes sampling, and grouping limited to severity-derived labels (or none).
type bucketSamplingNode struct {
	// inner is the generic engine sampling node; it provides the streaming EvalSample fallback and
	// Traverse, used whenever the bucketed path is not taken.
	inner *logqlengine.SamplingNode
	src   *logStreamNode
	op    sampleOp
	by    []logql.Label
}

var (
	_ logqlengine.SampleNode         = (*bucketSamplingNode)(nil)
	_ logqlengine.BucketedSampleNode = (*bucketSamplingNode)(nil)
)

// Traverse implements [logqlengine.Node].
func (n *bucketSamplingNode) Traverse(cb logqlengine.NodeVisitor) error { return n.inner.Traverse(cb) }

// EvalSample implements [logqlengine.SampleNode] by delegating to the generic streaming path; it is
// only reached when the caller does not use the bucketed capability.
func (n *bucketSamplingNode) EvalSample(ctx context.Context, params logqlengine.EvalParams) (logqlengine.SampleIterator, error) {
	return n.inner.EvalSample(ctx, params)
}

// EvalBucketedSample implements [logqlengine.BucketedSampleNode]: it returns one [logqlmetric.Step]
// per output step in [params.Start, params.End], each holding the per-group sampled total over the
// trailing window (step-window, step].
func (n *bucketSamplingNode) EvalBucketedSample(ctx context.Context, params logqlengine.EvalParams, window time.Duration) (logqlengine.StepIterator, error) {
	step := params.Step
	if step <= 0 {
		step = window
	}
	startNs, endNs := params.Start.UnixNano(), params.End.UnixNano()
	stepNs, windowNs := step.Nanoseconds(), window.Nanoseconds()
	if stepNs <= 0 || windowNs <= 0 || endNs < startNs {
		return iterators.Empty[logqlmetric.Step](), nil
	}
	numSteps := int((endNs-startNs)/stepNs) + 1

	// Fetch the trailing window too, so a record before Start still feeds step 0. Project only the
	// columns the bucketing reads — severity (the level group key) and, for bytes sampling, the body
	// — so the storage skips decoding the attributes column entirely.
	projection := []string{siglog.ColSeverity, siglog.ColSeverityText}
	if n.op == bytesSampling {
		projection = append(projection, siglog.ColBody)
	}
	batches, fullyPushed, err := n.src.fetchBatches(ctx, startNs-windowNs, endNs, projection...)
	if err != nil {
		return nil, err
	}
	// The fast bucketing skips the in-engine selector re-check, so it is only sound when the fetch
	// applied the whole selector. Otherwise fall back to the generic streaming path (which filters).
	if !fullyPushed {
		return n.streamingFallback(ctx, params, window)
	}

	// counts[k] maps a group key to its running total at output step k. groupMaps caches the label
	// map per group key so AggregatedLabels is built once per distinct group, not per record.
	counts := make([]map[string]float64, numSteps)
	groupMaps := map[string]map[string]string{}

	for _, batch := range batches {
		cols := newLogColumns(batch)
		for i, ts := range batch.Timestamps {
			gk := n.groupKey(cols, i)
			var val float64
			switch n.op {
			case bytesSampling:
				if i < len(cols.body) {
					val = float64(len(cols.body[i]))
				}
			default:
				val = 1
			}

			// Steps s_k = Start + k*step with ts <= s_k < ts+window contain this record.
			d := ts - startNs
			kLo := ceilDiv(d, stepNs)
			if kLo < 0 {
				kLo = 0
			}
			kHi := floorDiv(d+windowNs-1, stepNs) // strict s_k < ts+window
			if kHi >= int64(numSteps) {
				kHi = int64(numSteps) - 1
			}
			for k := kLo; k <= kHi; k++ {
				m := counts[k]
				if m == nil {
					m = map[string]float64{}
					counts[k] = m
				}
				m[gk] += val
			}
			// Build the label map once per distinct group, not per record.
			if _, ok := groupMaps[gk]; !ok {
				groupMaps[gk] = n.groupLabels(gk)
			}
		}
	}

	steps := make([]logqlmetric.Step, 0, numSteps)
	for k := 0; k < numSteps; k++ {
		m := counts[k]
		if len(m) == 0 {
			continue
		}
		samples := make([]logqlmetric.Sample, 0, len(m))
		for gk, total := range m {
			samples = append(samples, logqlmetric.Sample{
				Data: total,
				Set:  logqlabels.AggregatedLabelsFromMap(groupMaps[gk]),
			})
		}
		steps = append(steps, logqlmetric.Step{
			Timestamp: otelstorage.Timestamp(startNs + int64(k)*stepNs),
			Samples:   samples,
		})
	}
	return iterators.Slice(steps), nil
}

// streamingFallback evaluates the range aggregation the generic way (stream entries, apply the
// selector + pipeline, bucket in Go) when the bucketed fast path is not sound. It returns the
// undivided count/bytes total per (step, stream) — the dispatch divides by the range for rate ops
// and the outer aggregation regroups — matching the fast path's contract.
func (n *bucketSamplingNode) streamingFallback(ctx context.Context, params logqlengine.EvalParams, window time.Duration) (logqlengine.StepIterator, error) {
	iter, err := n.inner.EvalSample(ctx, logqlengine.EvalParams{
		Start:     params.Start.Add(-window),
		End:       params.End,
		Step:      params.Step,
		Direction: logqlengine.DirectionForward,
		Limit:     -1,
	})
	if err != nil {
		return nil, err
	}
	// Aggregate as count/bytes_over_time (undivided), regardless of the rate variant, so the
	// dispatch's DivideStep applies exactly once.
	expr := *n.inner.Expr
	if n.op == bytesSampling {
		expr.Op = logql.RangeOpBytes
	} else {
		expr.Op = logql.RangeOpCount
	}
	return logqlmetric.RangeAggregation(iter, &expr, params.Start, params.End, params.Step)
}

// groupKey returns the grouping key for record i — allocation-free on the hot path. Only
// severity-derived grouping labels are supported (the optimizer guarantees this), so the key is the
// record's level value (every grouping label shares it); the empty key is the single ungrouped
// bucket.
func (n *bucketSamplingNode) groupKey(cols logColumns, i int) string {
	if len(n.by) == 0 {
		return ""
	}
	return levelValue(cols.severity, cols.severityText, i)
}

// groupLabels builds the label map for a group key. Called once per distinct group, not per record.
func (n *bucketSamplingNode) groupLabels(key string) map[string]string {
	if len(n.by) == 0 {
		return nil
	}
	m := make(map[string]string, len(n.by))
	for _, l := range n.by {
		m[string(l)] = key
	}
	return m
}

// levelValue mirrors logqlabels.SetFromRecord's level label: the severity number's name, or the raw
// severity text when the number is unspecified.
func levelValue(severity []int64, text [][]byte, i int) string {
	if i < len(severity) {
		if s := severity[i]; s != int64(plog.SeverityNumberUnspecified) {
			return plog.SeverityNumber(s).String()
		}
	}
	if i < len(text) {
		return string(text[i])
	}
	return ""
}

// isLevelLabel reports whether a grouping label is one of the severity-derived labels the bucketed
// path can extract from the severity column.
func isLevelLabel(l logql.Label) bool {
	return string(l) == logstorage.LabelSeverity || string(l) == logstorage.LabelDetectedLevel
}

func ceilDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a > 0) == (b > 0) {
		q++
	}
	return q
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a > 0) != (b > 0) {
		q--
	}
	return q
}
