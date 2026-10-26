package storagebackend

import (
	"context"
	"math"
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
	ctx = n.src.q.b.queryContext(ctx)

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
	// No limit: a range aggregation must see every record in the window.
	batches, fullyPushed, err := n.src.fetchBatches(ctx, startNs-windowNs, endNs, fetchOptions{projection: projection})
	if err != nil {
		return nil, err
	}
	// The fast bucketing skips the in-engine selector re-check, so it is only sound when the fetch
	// applied the whole selector. Otherwise fall back to the generic streaming path (which filters).
	if !fullyPushed {
		return n.streamingFallback(ctx, params, window)
	}

	groups := map[string]*logWindowGroup{}
	var denseBudget int64
	for _, batch := range batches {
		denseBudget += min(int64(len(batch.Timestamps)), math.MaxInt64-denseBudget)
	}

	for _, batch := range batches {
		cols := newLogColumns(batch)
		for i, ts := range batch.Timestamps {
			if i&4095 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}

			// Steps s_k = Start + k*step with ts <= s_k < ts+window contain this record.
			d := ts - startNs
			kLo := max(ceilDiv(d, stepNs), 0)
			kHi := floorDiv(d+windowNs-1, stepNs) // strict s_k < ts+window
			if kHi >= int64(numSteps) {
				kHi = int64(numSteps) - 1
			}
			if kLo > kHi {
				continue
			}
			gk := n.groupKey(cols, i)
			group := groups[gk]
			if group == nil {
				group = newLogWindowGroup(numSteps, &denseBudget)
				groups[gk] = group
			}
			var bytes int64
			if n.op == bytesSampling && i < len(cols.body) {
				bytes = int64(len(cols.body[i]))
			}
			group.add(int(kLo), int(kHi), bytes)
		}
	}

	steps := make([]logqlmetric.Step, numSteps)
	for key, group := range groups {
		labels := n.groupLabels(key)
		err := group.visit(func(first, end int, rows, bytes int64) error {
			for k := first; k < end; k++ {
				if k&255 == 0 || k == first {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				total := float64(rows)
				if n.op == bytesSampling {
					total = float64(bytes)
					if group.direct != nil {
						total = group.direct[k]
					}
				}
				steps[k].Samples = append(steps[k].Samples, logqlmetric.Sample{
					Data: total,
					Set:  logqlabels.AggregatedLabelsFromMap(labels),
				})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	populated := steps[:0]
	for k := range steps {
		if len(steps[k].Samples) > 0 {
			steps[k].Timestamp = otelstorage.Timestamp(startNs + int64(k)*stepNs)
			populated = append(populated, steps[k])
		}
	}
	return iterators.Slice(populated), nil
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

// levelValue mirrors logqlabels.SetFromRecord's level label: the lower-cased severity number's name,
// or the lower-cased raw severity text when the number is unspecified. The casing must match
// SetFromRecord (which lower-cases both) so the bucketed and generic paths agree on the series key.
func levelValue(severity []int64, text [][]byte, i int) string {
	var number plog.SeverityNumber
	if i < len(severity) {
		number = plog.SeverityNumber(severity[i])
	}
	var raw string
	if i < len(text) {
		raw = string(text[i])
	}

	return logqlabels.Level(number, raw)
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
