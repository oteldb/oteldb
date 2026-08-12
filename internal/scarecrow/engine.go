package scarecrow

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Opts configures an [Engine].
type Opts struct {
	// LookbackDelta bounds how far back a vector selector reaches for a sample. Zero selects
	// the Prometheus default of 5m.
	LookbackDelta time.Duration
	// EnableNegativeOffset allows negative offset modifiers.
	EnableNegativeOffset bool
	// EnableAtModifier allows the @ modifier.
	EnableAtModifier bool
	// NoStepSubqueryInterval is the inner step used by a subquery written without one
	// (`foo[5m:]`). Zero selects the Prometheus default of 1m.
	NoStepSubqueryInterval time.Duration
	// Parser configures the upstream parser, notably the experimental-feature gates the
	// compliance corpus needs.
	Parser parser.Options
	// NewScanner builds the columnar seam for a query. When nil, the engine adapts the
	// storage.Queryable it is handed via [NewQueryableScanner] — the path the upstream test
	// corpus and any non-columnar backend take.
	NewScanner func(storage.Queryable) Scanner
	// TracerProvider provides the OpenTelemetry tracer for this engine. Nil selects the global
	// provider, matching internal/logql/logqlengine.
	TracerProvider trace.TracerProvider
	// ChunkSteps bounds how many steps a range query plans and executes at once (§4.4's
	// time-chunking). A query with more steps than this runs as several sequential chunks
	// whose results are concatenated, so accumulator memory stays bounded in the query's step
	// count rather than growing with the range. Zero selects the default; negative disables
	// chunking (one chunk covers the whole range, the pre-M16 behavior).
	ChunkSteps int
	// MaxSamples caps how many samples one query may read before it fails with
	// [promql.ErrTooManySamples]. It mirrors promql.EngineOpts.MaxSamples so switching engines
	// does not silently drop the limit. Zero disables it — unlike the upstream engine, where a
	// zero value fails every query.
	//
	// The count is cumulative over the query rather than a live peak: the columnar model holds
	// one series' raw samples at a time, so a peak gauge would not trip on a scan touching
	// millions of series, which is the shape worth stopping. See [sampleBudget].
	MaxSamples int
	// Timeout bounds a single query's wall time, after which it fails with
	// [promql.ErrQueryTimeout]. Zero disables it — again unlike the upstream engine.
	Timeout time.Duration
}

const (
	defaultLookbackDelta          = 5 * time.Minute
	defaultNoStepSubqueryInterval = time.Minute
	// defaultChunkSteps is conservative rather than tuned: bounding resident memory in range
	// length matters far more than picking the largest safe chunk. See [Opts.ChunkSteps].
	defaultChunkSteps = 10_000
)

func (o *Opts) setDefaults() {
	if o.LookbackDelta == 0 {
		o.LookbackDelta = defaultLookbackDelta
	}

	if o.NoStepSubqueryInterval == 0 {
		o.NoStepSubqueryInterval = defaultNoStepSubqueryInterval
	}

	if o.NewScanner == nil {
		o.NewScanner = NewQueryableScanner
	}

	if o.ChunkSteps == 0 {
		o.ChunkSteps = defaultChunkSteps
	}

	if o.TracerProvider == nil {
		o.TracerProvider = otel.GetTracerProvider()
	}
}

// Engine evaluates PromQL queries using the series-major columnar execution model.
//
// It implements promql.QueryEngine, so it is a drop-in for the existing engine seam.
type Engine struct {
	opts   Opts
	tracer trace.Tracer
}

var _ promql.QueryEngine = (*Engine)(nil)

// NewEngine returns an [Engine].
func NewEngine(opts Opts) *Engine {
	opts.setDefaults()

	return &Engine{
		opts:   opts,
		tracer: opts.TracerProvider.Tracer("scarecrow.Engine"),
	}
}

// NewInstantQuery builds a query evaluating expr at a single timestamp.
func (e *Engine) NewInstantQuery(
	_ context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time,
) (promql.Query, error) {
	expr, err := e.parse(qs)
	if err != nil {
		return nil, err
	}

	return &query{
		engine:    e,
		queryable: q,
		text:      qs,
		expr:      expr,
		start:     ts,
		end:       ts,
		lookback:  e.lookback(opts),
	}, nil
}

// NewRangeQuery builds a query evaluating expr at every step of [start, end].
func (e *Engine) NewRangeQuery(
	_ context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration,
) (promql.Query, error) {
	expr, err := e.parse(qs)
	if err != nil {
		return nil, err
	}

	if expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
		return nil, errors.Errorf(
			"invalid expression type %q for range query, must be scalar or instant vector",
			parser.DocumentedType(expr.Type()),
		)
	}

	return &query{
		engine:    e,
		queryable: q,
		text:      qs,
		expr:      expr,
		start:     start,
		end:       end,
		interval:  interval,
		lookback:  e.lookback(opts),
	}, nil
}

func (e *Engine) parse(qs string) (parser.Expr, error) {
	expr, err := parser.NewParser(e.opts.Parser).ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	if err := e.validateModifiers(expr); err != nil {
		return nil, err
	}

	return expr, nil
}

// validateModifiers rejects modifiers the engine was not configured to allow, matching the
// upstream engine's behavior so the compliance corpus sees the same errors.
func (e *Engine) validateModifiers(expr parser.Expr) error {
	var err error

	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		switch n := node.(type) {
		case *parser.VectorSelector:
			if n.Timestamp != nil && !e.opts.EnableAtModifier {
				err = errors.New("@ modifier is disabled")
			}
			if n.OriginalOffset < 0 && !e.opts.EnableNegativeOffset {
				err = errors.New("negative offset is disabled")
			}
		case *parser.SubqueryExpr:
			if n.Timestamp != nil && !e.opts.EnableAtModifier {
				err = errors.New("@ modifier is disabled")
			}
			if n.OriginalOffset < 0 && !e.opts.EnableNegativeOffset {
				err = errors.New("negative offset is disabled")
			}
		}

		return err
	})

	return err
}

func (e *Engine) lookback(opts promql.QueryOpts) time.Duration {
	if opts != nil && opts.LookbackDelta() > 0 {
		return opts.LookbackDelta()
	}

	return e.opts.LookbackDelta
}

// query is one planned-and-executable PromQL query.
type query struct {
	engine    *Engine
	queryable storage.Queryable
	text      string
	expr      parser.Expr

	start, end time.Time
	interval   time.Duration
	lookback   time.Duration

	cancel func()
	stats  *stats.Statistics
	// budget is the query's sample allowance, created in Exec and shared by every EvalContext
	// the query builds.
	budget *sampleBudget
}

var _ promql.Query = (*query)(nil)

func (q *query) String() string { return q.text }

func (q *query) Statement() parser.Statement { return nil }

func (q *query) Stats() *stats.Statistics { return q.stats }

func (q *query) Close() {}

func (q *query) Cancel() {
	if q.cancel != nil {
		q.cancel()
	}
}

// Exec plans and evaluates the query.
func (q *query) Exec(ctx context.Context) *promql.Result {
	ctx, cancel := q.engine.withTimeout(ctx)
	q.cancel = cancel

	defer cancel()

	steps := len(stepGrid(q.start, q.end, q.interval))

	ctx, span := q.engine.tracer.Start(ctx, "scarecrow.Exec", trace.WithAttributes(
		attribute.String("promql.query", q.text),
		attribute.String("promql.start", q.start.Format(time.RFC3339Nano)),
		attribute.String("promql.end", q.end.Format(time.RFC3339Nano)),
		attribute.Stringer("promql.step", q.interval),
		attribute.Int("promql.steps", steps),
		attribute.Bool("promql.instant", q.instant()),
	))
	defer span.End()

	// One budget for the whole query, so a range query's chunks and any subquery share it and
	// time-chunking cannot be used to read past the limit.
	q.budget = newSampleBudget(q.engine.opts.MaxSamples)

	v, err := q.exec(ctx)

	span.SetAttributes(attribute.Int64("promql.samples_read", q.budget.Used()))

	if err != nil {
		// A canceled or expired context surfaces as a storage/iteration error deep in the tree;
		// map it to the error the upstream engine returns so callers (and the HTTP layer's status
		// mapping) cannot tell the two engines apart.
		err = queryContextErr(ctx, err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return &promql.Result{Err: err}
	}

	span.SetStatus(codes.Ok, "")

	return &promql.Result{Value: v}
}

// withTimeout applies the engine's query timeout, falling back to a plain cancel context when
// none is configured. The returned cancel is always non-nil.
func (e *Engine) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if e.opts.Timeout > 0 {
		return context.WithTimeout(ctx, e.opts.Timeout)
	}

	return context.WithCancel(ctx)
}

// queryContextErr replaces err with the upstream engine's timeout/cancellation error when the
// query's context is what actually ended it. Errors unrelated to the context pass through.
func queryContextErr(ctx context.Context, err error) error {
	switch ctx.Err() {
	case context.DeadlineExceeded:
		return promql.ErrQueryTimeout("query execution")
	case context.Canceled:
		return promql.ErrQueryCanceled("query execution")
	default:
		return err
	}
}

func (q *query) exec(ctx context.Context) (parser.Value, error) {
	// Resolve start()/end() into concrete timestamps and mark step-invariant subtrees. Using
	// the upstream pass keeps @ semantics identical rather than reimplemented. This runs once
	// against the query's own start/end regardless of chunking below — start()/end() name the
	// whole query's range, not a chunk's.
	expr, err := promql.PreprocessExpr(q.expr, q.start, q.end, q.interval)
	if err != nil {
		return nil, err
	}

	scanner := q.engine.opts.NewScanner(q.queryable)
	defer func() { _ = scanner.Close() }()

	// A string literal is not a series at all, so it bypasses the operator tree entirely.
	if lit, ok := unwrapStringLiteral(expr); ok {
		return promql.String{T: q.start.UnixMilli(), V: lit.Val}, nil
	}

	// A bare range selector or subquery as an instant query returns the raw samples as a matrix.
	// It is the one result shape no operator produces (nothing in this engine emits a range
	// vector), so it is materialized at the result boundary rather than planned.
	if q.instant() {
		ec := &EvalContext{
			Steps:         stepGrid(q.start, q.end, q.interval),
			LookbackDelta: q.lookback,
			Tracer:        q.engine.tracer,
			Budget:        q.budget,
		}
		p := &planner{scanner: scanner, ec: ec, noStepSubqueryInterval: q.engine.opts.NoStepSubqueryInterval}

		if ms, ok := unwrapMatrixSelector(expr); ok {
			return collectRawMatrix(ctx, scanner, ms, ec)
		}

		if sq, ok := unwrapSubquery(expr); ok {
			return collectSubqueryMatrix(ctx, p, sq)
		}

		root, schema, err := q.buildRoot(ctx, p, expr)
		if err != nil {
			return nil, err
		}
		defer func() { _ = root.Close() }()

		return collectInstant(ctx, root, schema, expr.Type(), ec)
	}

	return q.execRange(ctx, scanner, expr)
}

// buildRoot plans expr against ec and resolves its schema, ready for Next.
func (q *query) buildRoot(ctx context.Context, p *planner, expr parser.Expr) (Operator, *Schema, error) {
	// This span covers schema resolution as well as planning, which is deliberate: schemas
	// resolve eagerly (§3.3), so every data-dependent operator — the pushdowns, quantile, topk,
	// count_values — does all of its storage work inside here rather than during Next.
	ctx, span := p.ec.span(ctx, "scarecrow.Plan")
	defer span.End()

	root, err := p.plan(ctx, expr)
	if err != nil {
		return nil, nil, err
	}

	schema, err := root.Schema(ctx)
	if err != nil {
		_ = root.Close()

		return nil, nil, err
	}

	span.SetAttributes(
		attribute.String("promql.plan", root.String()),
		attribute.Int("promql.series", schema.Len()),
	)

	return root, schema, nil
}

// execRange evaluates a range query, splitting it into sequential time chunks when it has more
// steps than the engine's chunk budget (§4.4's time-chunking, M16). Each chunk plans and runs
// its own operator tree against a chunk-scoped [EvalContext], so no accumulator ever sees more
// than one chunk's steps; results are concatenated by series identity. A query within budget
// runs as the single chunk it always used to be, so this changes nothing for the common case.
func (q *query) execRange(ctx context.Context, scanner Scanner, expr parser.Expr) (parser.Value, error) {
	steps := stepGrid(q.start, q.end, q.interval)

	chunkSteps := q.engine.opts.ChunkSteps
	if chunkSteps <= 0 || len(steps) <= chunkSteps {
		chunkSteps = len(steps)
	}

	merged := rangeMerger{totalSteps: len(steps)}
	chunks := (len(steps) + chunkSteps - 1) / chunkSteps

	for start := 0; start < len(steps); start += chunkSteps {
		end := min(start+chunkSteps, len(steps))

		v, err := q.execChunk(ctx, scanner, expr, steps[start:end], start/chunkSteps, chunks)
		if err != nil {
			return nil, err
		}

		if err := merged.add(v); err != nil {
			return nil, err
		}
	}

	return merged.result(), nil
}

// execChunk plans and evaluates one chunk of a range query. It is a method rather than the body
// of the loop above so its span closes when the chunk finishes: a deferred End inside the loop
// would hold every chunk's span open until the whole query returned, reporting each chunk as
// lasting until the end of the query.
func (q *query) execChunk(
	ctx context.Context, scanner Scanner, expr parser.Expr, steps []int64, index, chunks int,
) (parser.Value, error) {
	ec := &EvalContext{
		Steps:         steps,
		Interval:      q.interval,
		LookbackDelta: q.lookback,
		Tracer:        q.engine.tracer,
		Budget:        q.budget,
	}

	// Only span the chunks when there is more than one: an unchunked query would otherwise get a
	// redundant span wrapping the whole of its only chunk.
	if chunks > 1 {
		var span trace.Span

		ctx, span = ec.span(ctx, "scarecrow.Chunk",
			attribute.Int("promql.chunk", index),
			attribute.Int("promql.chunks", chunks),
			attribute.Int("promql.steps", len(steps)),
		)
		defer span.End()
	}

	p := &planner{scanner: scanner, ec: ec, noStepSubqueryInterval: q.engine.opts.NoStepSubqueryInterval}

	root, schema, err := q.buildRoot(ctx, p, expr)
	if err != nil {
		return nil, err
	}

	defer func() { _ = root.Close() }()

	return collectRange(ctx, root, schema, ec)
}

func (q *query) instant() bool {
	return q.interval == 0 && q.start.Equal(q.end)
}

// stepGrid returns the evaluation timestamps in unix milliseconds. An instant query yields
// exactly one step, which makes it the degenerate case of the range grid rather than a separate
// code path.
func stepGrid(start, end time.Time, interval time.Duration) []int64 {
	startMs, endMs := start.UnixMilli(), end.UnixMilli()
	if interval <= 0 {
		return []int64{startMs}
	}

	stepMs := interval.Milliseconds()

	n := int((endMs-startMs)/stepMs) + 1
	steps := make([]int64, 0, n)

	for t := startMs; t <= endMs; t += stepMs {
		steps = append(steps, t)
	}

	return steps
}
