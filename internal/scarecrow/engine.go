package scarecrow

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"
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
	// Parser configures the upstream parser, notably the experimental-feature gates the
	// compliance corpus needs.
	Parser parser.Options
	// NewScanner builds the columnar seam for a query. When nil, the engine adapts the
	// storage.Queryable it is handed via [NewQueryableScanner] — the path the upstream test
	// corpus and any non-columnar backend take.
	NewScanner func(storage.Queryable) Scanner
}

const defaultLookbackDelta = 5 * time.Minute

func (o *Opts) setDefaults() {
	if o.LookbackDelta == 0 {
		o.LookbackDelta = defaultLookbackDelta
	}

	if o.NewScanner == nil {
		o.NewScanner = NewQueryableScanner
	}
}

// Engine evaluates PromQL queries using the series-major columnar execution model.
//
// It implements promql.QueryEngine, so it is a drop-in for the existing engine seam.
type Engine struct {
	opts Opts
}

var _ promql.QueryEngine = (*Engine)(nil)

// NewEngine returns an [Engine].
func NewEngine(opts Opts) *Engine {
	opts.setDefaults()

	return &Engine{opts: opts}
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
	ctx, cancel := context.WithCancel(ctx)
	q.cancel = cancel

	defer cancel()

	v, err := q.exec(ctx)
	if err != nil {
		return &promql.Result{Err: err}
	}

	return &promql.Result{Value: v}
}

func (q *query) exec(ctx context.Context) (parser.Value, error) {
	// Resolve start()/end() into concrete timestamps and mark step-invariant subtrees. Using
	// the upstream pass keeps @ semantics identical rather than reimplemented.
	expr, err := promql.PreprocessExpr(q.expr, q.start, q.end, q.interval)
	if err != nil {
		return nil, err
	}

	ec := &EvalContext{
		Steps:         stepGrid(q.start, q.end, q.interval),
		Interval:      q.interval,
		LookbackDelta: q.lookback,
	}

	scanner := q.engine.opts.NewScanner(q.queryable)
	defer func() { _ = scanner.Close() }()

	// A string literal is not a series at all, so it bypasses the operator tree entirely.
	if lit, ok := unwrapStringLiteral(expr); ok {
		return promql.String{T: ec.Steps[0], V: lit.Val}, nil
	}

	// A bare range selector as an instant query returns the raw samples as a matrix. It is the
	// one result shape no operator produces (nothing in this engine emits a range vector), so
	// it is materialized directly from the scanner rather than planned.
	if q.instant() {
		if ms, ok := unwrapMatrixSelector(expr); ok {
			return collectRawMatrix(ctx, scanner, ms, ec)
		}
	}

	p := &planner{scanner: scanner, ec: ec}

	root, err := p.plan(ctx, expr)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()

	schema, err := root.Schema(ctx)
	if err != nil {
		return nil, err
	}

	if q.instant() {
		return collectInstant(ctx, root, schema, expr.Type(), ec)
	}

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
