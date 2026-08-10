package scarecrow

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// EvalContext is the per-chunk evaluation grid, shared by every operator in a tree. Step
// timestamps live here rather than on each [Column] so that a column is a bare float64 run.
type EvalContext struct {
	// Steps are the grid timestamps in unix milliseconds, ascending. An instant query has
	// exactly one step.
	Steps []int64
	// Interval is the step spacing. Zero for an instant query.
	Interval time.Duration
	// LookbackDelta bounds how far back a vector selector may reach for a sample.
	LookbackDelta time.Duration
	// Tracer instruments the operators in this chunk. It lives here rather than being threaded
	// through every constructor because it is exactly what an EvalContext already is: the state
	// every operator in one evaluation shares. Nil is legal and means no spans, so a test or an
	// embedder building an EvalContext by hand need not care.
	Tracer trace.Tracer
}

// span starts a span on the context's tracer, returning a no-op end function when tracing is off.
// Operators call it rather than touching Tracer directly, so the nil case lives in one place.
func (e *EvalContext) span(
	ctx context.Context, name string, attrs ...attribute.KeyValue,
) (context.Context, trace.Span) {
	if e == nil || e.Tracer == nil {
		return ctx, tracenoop.Span{}
	}

	return e.Tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

// NumSteps returns the number of steps in this chunk.
func (e *EvalContext) NumSteps() int { return len(e.Steps) }

// Instant reports whether this is a single-step (instant) evaluation.
func (e *EvalContext) Instant() bool { return len(e.Steps) == 1 && e.Interval == 0 }

// Operator is a node of the physical plan and its evaluation logic.
//
// Execution is a lockstep cascade: one Next on the root pulls at most one Next from each child.
// An operator is called once per output series, so an operator emitting n series is called n+1
// times per chunk, the last returning nil.
//
// Operators come in two shapes. A *streaming* operator maps one input column to one output
// column (unary functions, scalar binops). An *accumulating* operator drains its child fully on
// the first Next, building an accumulator, then emits from it (aggregations, binops, topk).
// Nothing else is needed: no operator ever sees more than one input series at a time, and what
// varies is only how much it chooses to remember.
type Operator interface {
	fmt.Stringer

	// Schema returns this operator's output series set. It is called once, at plan time,
	// bottom-up, and must be deterministic and independent of execution.
	Schema(ctx context.Context) (*Schema, error)

	// Next returns the next output series column for the current chunk, or nil at end of
	// stream. The returned column is owned by this operator and is valid only until the next
	// Next or Close call; callers must not retain it.
	Next(ctx context.Context) (*Column, error)

	// Children returns the input operators, for EXPLAIN and for tree walks.
	Children() []Operator

	// Close releases resources. It is safe to call more than once.
	Close() error
}

// numberLiteral emits a scalar whose value is constant across every step.
type numberLiteral struct {
	value float64
	ec    *EvalContext

	out  Column
	done bool
}

// newNumberLiteral returns an operator for a PromQL scalar literal.
func newNumberLiteral(v float64, ec *EvalContext) *numberLiteral {
	return &numberLiteral{value: v, ec: ec}
}

func (o *numberLiteral) String() string { return fmt.Sprintf("NumberLiteral(%g)", o.value) }

func (o *numberLiteral) Children() []Operator { return nil }

func (o *numberLiteral) Close() error { return nil }

func (o *numberLiteral) Schema(context.Context) (*Schema, error) { return ScalarSchema(), nil }

func (o *numberLiteral) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if o.done {
		return nil, nil
	}
	o.done = true

	o.out.Resize(0, o.ec.NumSteps())
	for i := range o.out.V {
		o.out.Set(i, o.value)
	}

	return &o.out, nil
}
