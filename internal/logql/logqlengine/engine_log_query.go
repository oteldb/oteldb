package logqlengine

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/xattribute"
)

// LogQuery represents a log query.
type LogQuery struct {
	Root             PipelineNode
	LookbackDuration time.Duration

	stats  engineStats
	tracer trace.Tracer
}

var _ Query = (*LogQuery)(nil)

func (e *Engine) buildLogQuery(ctx context.Context, expr *logql.LogExpr) (Query, error) {
	root, err := e.buildPipelineNode(ctx, expr.Sel, expr.Pipeline)
	if err != nil {
		return nil, err
	}

	return &LogQuery{
		Root:             root,
		LookbackDuration: e.lookbackDuration,

		stats:  e.stats,
		tracer: e.tracer,
	}, nil
}

// Eval implements [Query].
func (q *LogQuery) Eval(ctx context.Context, params EvalParams) (data lokiapi.QueryResponseData, _ error) {
	// Instant query, sub lookback duration from Start.
	if params.IsInstant() {
		params.Start = params.Start.Add(q.LookbackDuration)
	}
	if params.Direction == "" {
		params.Direction = DirectionForward
	}

	streams, err := q.eval(ctx, params)
	if err != nil {
		return data, errors.Wrap(err, "evaluate log query")
	}

	data.SetStreamsResult(lokiapi.StreamsResult{
		Result: streams,
	})
	return data, nil
}

func (q *LogQuery) eval(ctx context.Context, params EvalParams) (data lokiapi.Streams, rerr error) {
	start := time.Now()
	ctx, span := q.tracer.Start(ctx, "logql.LogQuery", trace.WithAttributes(
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

	iter, err := q.Root.EvalPipeline(ctx, params)
	if err != nil {
		return data, err
	}
	defer func() {
		_ = iter.Close()
	}()

	span.AddEvent("read_result")
	streams, total, err := groupEntries(iter)
	if err != nil {
		return data, err
	}
	span.AddEvent("return_result", trace.WithAttributes(
		attribute.Int("logql.total_streams", len(streams)),
		attribute.Int("logql.total_entries", total),
	))

	return streams, nil
}

func groupEntries(iter EntryIterator) (s lokiapi.Streams, total int, _ error) {
	var (
		e   Entry
		buf = make([]byte, 0, 1024)

		set     = map[string]int{}
		streams []lokiapi.Stream
	)
	for iter.Next(&e) {
		buf = e.Set.AppendString(buf[:0])

		idx, ok := set[string(buf)]
		if !ok {
			streams = append(streams, lokiapi.Stream{
				Stream: lokiapi.NewOptLabelSet(e.Set.AsLokiAPI()),
			})
			idx = len(streams) - 1
			set[string(buf)] = idx
		}
		stream := &streams[idx]

		stream.Values = append(stream.Values, lokiapi.LogEntry{T: uint64(e.Timestamp), V: e.Line})
		total++
	}
	if err := iter.Err(); err != nil {
		return s, 0, err
	}

	for _, stream := range streams {
		slices.SortFunc(stream.Values, func(a, b lokiapi.LogEntry) int {
			return cmp.Compare(a.T, b.T)
		})
	}
	return streams, total, nil
}

// ProcessorNode implements [PipelineNode].
type ProcessorNode struct {
	Input             PipelineNode
	Prefilter         Processor
	Selector          logql.Selector
	Pipeline          []logql.PipelineStage
	EnableOTELAdapter bool
}

var _ PipelineNode = (*ProcessorNode)(nil)

func (e *Engine) buildPipelineNode(ctx context.Context, sel logql.Selector, stages []logql.PipelineStage) (PipelineNode, error) {
	cond, err := extractQueryConditions(e.querierCaps, sel)
	if err != nil {
		return nil, errors.Wrap(err, "extract preconditions")
	}

	input, err := e.querier.Query(ctx, sel.Matchers)
	if err != nil {
		return nil, errors.Wrap(err, "create input node")
	}

	if p := cond.prefilter; (p == nil || p == NopProcessor) && len(stages) == 0 && !e.otelAdapter {
		// Empty processing pipeline, get data directly from storage.
		return input, nil
	}

	return &ProcessorNode{
		Input:             input,
		Prefilter:         cond.prefilter,
		Selector:          sel,
		Pipeline:          stages,
		EnableOTELAdapter: e.otelAdapter,
	}, nil
}

// Traverse implements [Node].
func (n *ProcessorNode) Traverse(cb NodeVisitor) error {
	if err := cb(n); err != nil {
		return err
	}
	return n.Input.Traverse(cb)
}

// EvalPipeline implements [PipelineNode].
func (n *ProcessorNode) EvalPipeline(ctx context.Context, params EvalParams) (_ EntryIterator, rerr error) {
	pipeline, err := BuildPipeline(n.Pipeline...)
	if err != nil {
		return nil, errors.Wrap(err, "build pipeline")
	}

	// Do not limit storage query.
	qparams := params
	qparams.Limit = -1
	iter, err := n.Input.EvalPipeline(ctx, qparams)
	if err != nil {
		return nil, err
	}
	defer closeOnError(iter, &rerr)

	return &entryIterator{
		iter:        iter,
		prefilter:   n.Prefilter,
		pipeline:    pipeline,
		entries:     0,
		limit:       params.Limit,
		otelAdapter: n.EnableOTELAdapter,
	}, nil
}

type entryIterator struct {
	iter EntryIterator

	prefilter Processor
	pipeline  Processor

	entries int
	limit   int
	// TODO(tdakkota): what?
	otelAdapter bool
}

func (i *entryIterator) Next(e *Entry) bool {
	for {
		if !i.iter.Next(e) || (i.limit > 0 && i.entries >= i.limit) {
			return false
		}

		var (
			ts   = e.Timestamp
			keep bool
		)
		if i.otelAdapter {
			e.Line = LineFromEntry(*e)
		}

		e.Line, keep = i.prefilter.Process(ts, e.Line, e.Set)
		if !keep {
			continue
		}

		e.Line, keep = i.pipeline.Process(ts, e.Line, e.Set)
		if !keep {
			continue
		}

		i.entries++
		return true
	}
}

func (i *entryIterator) Err() error {
	return i.iter.Err()
}

func (i *entryIterator) Close() error {
	return i.iter.Close()
}
