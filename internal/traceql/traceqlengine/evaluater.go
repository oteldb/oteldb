package traceqlengine

import (
	"fmt"
	"math"
	"regexp"
	"slices"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

func filterBy[E Evaluater](eval E, sets []Spanset) (result []Spanset) {
	var buf []tracestorage.Span
	for _, set := range sets {
		buf = buf[:0]
		ectx := set.evaluateCtx()
		for _, span := range set.Spans {
			if v := eval.Eval(span, ectx); v.Type == traceql.TypeBool && v.AsBool() {
				buf = append(buf, span)
			}
		}

		if len(buf) == 0 {
			continue
		}
		set.Spans = slices.Clone(buf)
		result = append(result, set)
	}
	return result
}

// EvaluateCtx is evaluation context.
type EvaluateCtx struct {
	Set Spanset
}

// Evaluater evaluates TraceQL expression.
type Evaluater interface {
	Eval(span tracestorage.Span, ctx EvaluateCtx) traceql.Static
}

// BinaryOp is a binary operation.
type BinaryOp func(a, b traceql.Static) traceql.Static

func buildEvaluater(expr traceql.TypedExpr) (Evaluater, error) {
	expr = ReduceExpr(expr)

	switch expr := expr.(type) {
	case *traceql.BinaryFieldExpr:
		return buildBinaryEvaluater(expr.Left, expr.Op, expr.Right)
	case *traceql.UnaryFieldExpr:
		return buildUnaryEvaluater(expr.Op, expr.Expr)
	case *traceql.BinaryScalarExpr:
		return buildBinaryEvaluater(expr.Left, expr.Op, expr.Right)
	case *traceql.AggregateScalarExpr:
		return buildAggregator(expr)
	case *traceql.Static:
		return buildStaticEvaluater(expr)
	case *traceql.Attribute:
		return buildAttributeEvaluater(expr)
	default:
		return nil, errors.Errorf("unexpected expression %T", expr)
	}
}

func buildBinaryEvaluater(
	left traceql.TypedExpr,
	op traceql.BinaryOp,
	right traceql.TypedExpr,
) (Evaluater, error) {
	opEval, err := buildBinaryOp(op, right)
	if err != nil {
		return nil, err
	}

	leftEval, err := buildEvaluater(left)
	if err != nil {
		return nil, err
	}

	rightEval, err := buildEvaluater(right)
	if err != nil {
		return nil, err
	}

	return &BinaryEvaluater{
		Left:  leftEval,
		Op:    opEval,
		Right: rightEval,
	}, nil
}

// BinaryEvaluater is a binary operation [Evaluater].
type BinaryEvaluater struct {
	Left  Evaluater
	Op    BinaryOp
	Right Evaluater
}

// Eval implemenets [Evaluater].
func (e *BinaryEvaluater) Eval(span tracestorage.Span, ctx EvaluateCtx) traceql.Static {
	left := e.Left.Eval(span, ctx)
	right := e.Right.Eval(span, ctx)
	return e.Op(left, right)
}

func buildBinaryOp(op traceql.BinaryOp, right traceql.TypedExpr) (BinaryOp, error) {
	switch op {
	case traceql.OpAnd:
		return func(a, b traceql.Static) (r traceql.Static) {
			if a.Type != traceql.TypeBool || b.Type != traceql.TypeBool {
				r.SetBool(false)
			} else {
				r.SetBool(a.AsBool() && b.AsBool())
			}
			return r
		}, nil
	case traceql.OpOr:
		return func(a, b traceql.Static) (r traceql.Static) {
			if a.Type != traceql.TypeBool || b.Type != traceql.TypeBool {
				r.SetBool(false)
			} else {
				r.SetBool(a.AsBool() || b.AsBool())
			}
			return r
		}, nil
	case traceql.OpAdd:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetNumber(a.ToFloat() + b.ToFloat())
			return r
		}, nil
	case traceql.OpSub:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetNumber(a.ToFloat() - b.ToFloat())
			return r
		}, nil
	case traceql.OpMul:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetNumber(a.ToFloat() * b.ToFloat())
			return r
		}, nil
	case traceql.OpDiv:
		return func(a, b traceql.Static) (r traceql.Static) {
			dividend := a.ToFloat()
			// Checked division.
			if dividend == 0 {
				r.SetNumber(math.NaN())
			} else {
				r.SetNumber(dividend / b.ToFloat())
			}
			return r
		}, nil
	case traceql.OpMod:
		return func(a, b traceql.Static) (r traceql.Static) {
			dividend := a.ToFloat()
			// Checked modular division.
			if dividend == 0 {
				r.SetNumber(math.NaN())
			} else {
				r.SetNumber(math.Mod(dividend, b.ToFloat()))
			}
			return r
		}, nil
	case traceql.OpPow:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetNumber(math.Pow(a.ToFloat(), b.ToFloat()))
			return r
		}, nil
	case traceql.OpEq:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) == 0)
			return r
		}, nil
	case traceql.OpNotEq:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) != 0)
			return r
		}, nil
	case traceql.OpRe, traceql.OpNotRe:
		static, ok := right.(*traceql.Static)
		if !ok {
			return nil, errors.Errorf("unexpected pattern expression %T", right)
		}
		if static.Type != traceql.TypeString {
			return nil, errors.Errorf("expected string pattern, got %q", static.Type)
		}
		pattern := static.AsString()

		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, errors.Wrapf(err, "compile regexp %q", pattern)
		}

		if op == traceql.OpRe {
			return func(a, _ traceql.Static) (r traceql.Static) {
				if a.Type != traceql.TypeString {
					r.SetBool(false)
				} else {
					r.SetBool(re.MatchString(a.AsString()))
				}
				return r
			}, nil
		}
		return func(a, _ traceql.Static) (r traceql.Static) {
			if a.Type != traceql.TypeString {
				r.SetBool(false)
			} else {
				r.SetBool(!re.MatchString(a.AsString()))
			}
			return r
		}, nil
	case traceql.OpGt:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) > 0)
			return r
		}, nil
	case traceql.OpGte:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) >= 0)
			return r
		}, nil
	case traceql.OpLt:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) < 0)
			return r
		}, nil
	case traceql.OpLte:
		return func(a, b traceql.Static) (r traceql.Static) {
			r.SetBool(a.Compare(b) <= 0)
			return r
		}, nil
	default:
		return nil, errors.Errorf("unexpected binary op %q", op)
	}
}

func buildUnaryEvaluater(op traceql.UnaryOp, expr traceql.FieldExpr) (Evaluater, error) {
	sub, err := buildEvaluater(expr)
	if err != nil {
		return nil, err
	}
	switch op {
	case traceql.OpNeg:
		return &NegEvaluater{Sub: sub}, nil
	case traceql.OpNot:
		return &NotEvaluater{Sub: sub}, nil
	default:
		return nil, errors.Errorf("unexpected unary op %q", op)
	}
}

// NegEvaluater is a unary negation operation [Evaluater].
type NegEvaluater struct {
	Sub Evaluater
}

// Eval implemenets [Evaluater].
func (e *NegEvaluater) Eval(span tracestorage.Span, ctx EvaluateCtx) (r traceql.Static) {
	val := e.Sub.Eval(span, ctx)
	if !val.Type.IsNumeric() {
		r.SetNil()
	} else {
		r.SetNumber(-val.ToFloat())
	}
	return r
}

// NotEvaluater is a unary NOT operation [Evaluater].
type NotEvaluater struct {
	Sub Evaluater
}

// Eval implemenets [Evaluater].
func (e *NotEvaluater) Eval(span tracestorage.Span, ctx EvaluateCtx) (r traceql.Static) {
	val := e.Sub.Eval(span, ctx)
	if val.Type != traceql.TypeBool {
		r.SetNil()
	} else {
		r.SetBool(!val.AsBool())
	}
	return r
}

func buildStaticEvaluater(val *traceql.Static) (Evaluater, error) {
	return &StaticEvaluater{Val: *val}, nil
}

// StaticEvaluater is a [Evaluater] returning a static value.
type StaticEvaluater struct {
	Val traceql.Static
}

// Eval implemenets [Evaluater].
func (e *StaticEvaluater) Eval(tracestorage.Span, EvaluateCtx) traceql.Static {
	return e.Val
}

func buildAttributeEvaluater(attr *traceql.Attribute) (Evaluater, error) {
	switch attr.Prop {
	case traceql.SpanDuration:
		return &SpanDurationEvalauter{}, nil
	case traceql.SpanChildCount:
		// TODO(tdakkota): span child count
	case traceql.SpanName:
		return &SpanNameEvaluater{}, nil
	case traceql.SpanStatus:
		return &SpanStatusEvaluater{}, nil
	case traceql.SpanKind:
		return &SpanKindEvaluater{}, nil
	case traceql.SpanParent:
		return &ParentEvaluater{}, nil
	case traceql.RootSpanName:
		return &RootSpanNameEvaluater{}, nil
	case traceql.RootServiceName:
		return &RootServiceNameEvaluater{}, nil
	case traceql.TraceDuration:
		return &TraceDurationEvaluater{}, nil
	case traceql.SpanStatusMessage:
		return &SpanStatusMessageEvaluater{}, nil
	case traceql.SpanID:
		return &SpanIDEvaluater{}, nil
	case traceql.ParentID:
		return &ParentIDEvaluater{}, nil
	case traceql.TraceID:
		return &TraceIDEvaluater{}, nil
	case traceql.InstrumentationName:
		return &InstrumentationNameEvaluater{}, nil
	case traceql.InstrumentationVersion:
		return &InstrumentationVersionEvaluater{}, nil
	case traceql.EventName:
		return &EventNameEvaluater{}, nil
	case traceql.EventTimeSinceStart:
		return &EventTimeSinceStartEvaluater{}, nil
	case traceql.LinkTraceID:
		return &LinkTraceIDEvaluater{}, nil
	case traceql.LinkSpanID:
		return &LinkSpanIDEvaluater{}, nil
	default:
		// SpanAttribute.
		if attr.Parent {
			// TODO(tdakkota): parent span attributes
			break
		}

		switch attr.Scope {
		case traceql.ScopeResource:
			return &ResourceAttributeEvaluater{Name: attr.Name}, nil
		case traceql.ScopeSpan:
			return &SpanAttributeEvaluater{Name: attr.Name}, nil
		case traceql.ScopeInstrumentation:
			return &InstrumentationAttributeEvaluater{Name: attr.Name}, nil
		case traceql.ScopeEvent:
			return &EventAttributeEvaluater{Name: attr.Name}, nil
		case traceql.ScopeLink:
			return &LinkAttributeEvaluater{Name: attr.Name}, nil
		default:
			return &AttributeEvaluater{Name: attr.Name}, nil
		}
	}
	return nil, &UnsupportedError{Msg: fmt.Sprintf("unsupported attribute %q", attr)}
}

// SpanDurationEvalauter evaluates `duration` property.
type SpanDurationEvalauter struct{}

// Eval implemenets [Evaluater].
func (*SpanDurationEvalauter) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	end := span.End.AsTime()
	start := span.Start.AsTime()
	r.SetDuration(end.Sub(start))
	return r
}

// SpanNameEvaluater evaluates `name` property.
type SpanNameEvaluater struct{}

// Eval implemenets [Evaluater].
func (*SpanNameEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	r.SetString(span.Name)
	return r
}

// SpanStatusEvaluater evaluates `status` property.
type SpanStatusEvaluater struct{}

// Eval implemenets [Evaluater].
func (*SpanStatusEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	r.SetSpanStatus(ptrace.StatusCode(span.StatusCode))
	return r
}

// SpanKindEvaluater evaluates `kind` property.
type SpanKindEvaluater struct{}

// Eval implemenets [Evaluater].
func (*SpanKindEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	r.SetSpanKind(ptrace.SpanKind(span.Kind))
	return r
}

// ParentEvaluater evaluates `parent` property.
type ParentEvaluater struct{}

// Eval implemenets [Evaluater].
func (*ParentEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	if span.ParentSpanID.IsEmpty() {
		r.SetNil()
	} else {
		// Just set some non-nil value.
		r.SetBool(true)
	}
	return r
}

// RootSpanNameEvaluater evaluates `rootName` property.
type RootSpanNameEvaluater struct{}

// Eval implemenets [Evaluater].
func (*RootSpanNameEvaluater) Eval(_ tracestorage.Span, ctx EvaluateCtx) (r traceql.Static) {
	r.SetString(ctx.Set.RootSpanName)
	return r
}

// RootServiceNameEvaluater evaluates `rootServiceName` property.
type RootServiceNameEvaluater struct{}

// Eval implemenets [Evaluater].
func (*RootServiceNameEvaluater) Eval(_ tracestorage.Span, ctx EvaluateCtx) (r traceql.Static) {
	r.SetString(ctx.Set.RootServiceName)
	return r
}

// TraceDurationEvaluater evaluates `traceDuration“ property.
type TraceDurationEvaluater struct{}

// Eval implemenets [Evaluater].
func (*TraceDurationEvaluater) Eval(_ tracestorage.Span, ctx EvaluateCtx) (r traceql.Static) {
	r.SetDuration(ctx.Set.TraceDuration)
	return r
}

// ResourceAttributeEvaluater evaluates resource attribute selector.
type ResourceAttributeEvaluater struct {
	Name string
}

// Eval implemenets [Evaluater].
func (e *ResourceAttributeEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) traceql.Static {
	return evaluateAttr(
		e.Name,
		span.ScopeAttrs,
		span.ResourceAttrs,
	)
}

// SpanAttributeEvaluater evaluates Span attribute selector.
type SpanAttributeEvaluater struct {
	Name string
}

// Eval implemenets [Evaluater].
func (e *SpanAttributeEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) traceql.Static {
	return evaluateAttr(
		e.Name,
		span.Attrs,
	)
}

// AttributeEvaluater evaluates attribute selector.
type AttributeEvaluater struct {
	Name string
}

// Eval implemenets [Evaluater].
func (e *AttributeEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) traceql.Static {
	return evaluateAttr(
		e.Name,
		span.Attrs,
		span.ScopeAttrs,
		span.ResourceAttrs,
	)
}

func evaluateAttr(name string, attrs ...otelstorage.Attrs) (r traceql.Static) {
	for _, m := range attrs {
		if m.IsZero() {
			continue
		}
		if v, ok := m.AsMap().Get(name); ok && r.SetOTELValue(v) {
			return r
		}
	}
	r.SetNil()
	return r
}

// SpanStatusMessageEvaluater evaluates `statusMessage` property.
type SpanStatusMessageEvaluater struct{}

// Eval implemenets [Evaluater].
func (*SpanStatusMessageEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	r.SetString(span.StatusMessage)
	return r
}

// SpanIDEvaluater evaluates `span:id` property.
type SpanIDEvaluater struct{}

// Eval implemenets [Evaluater].
func (*SpanIDEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	r.SetString(span.SpanID.Hex())
	return r
}

// ParentIDEvaluater evaluates `span:parentId` property.
type ParentIDEvaluater struct{}

// Eval implemenets [Evaluater].
func (*ParentIDEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	if span.ParentSpanID.IsEmpty() {
		r.SetNil()
	} else {
		r.SetString(span.ParentSpanID.Hex())
	}
	return r
}

// TraceIDEvaluater evaluates `trace:id` property.
type TraceIDEvaluater struct{}

// Eval implemenets [Evaluater].
func (*TraceIDEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	r.SetString(span.TraceID.Hex())
	return r
}

// InstrumentationNameEvaluater evaluates `instrumentation:name` property.
type InstrumentationNameEvaluater struct{}

// Eval implemenets [Evaluater].
func (*InstrumentationNameEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	r.SetString(span.ScopeName)
	return r
}

// InstrumentationVersionEvaluater evaluates `instrumentation:version` property.
type InstrumentationVersionEvaluater struct{}

// Eval implemenets [Evaluater].
func (*InstrumentationVersionEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	r.SetString(span.ScopeVersion)
	return r
}

// EventNameEvaluater evaluates `event:name` property.
// Returns the name of the first span event, or nil if the span has no events.
type EventNameEvaluater struct{}

// Eval implemenets [Evaluater].
func (*EventNameEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	if len(span.Events) == 0 {
		r.SetNil()
		return r
	}
	r.SetString(span.Events[0].Name)
	return r
}

// EventTimeSinceStartEvaluater evaluates `event:timeSinceStart` property.
// Returns the elapsed time from span start to the first event, or nil if the span has no events.
type EventTimeSinceStartEvaluater struct{}

// Eval implemenets [Evaluater].
func (*EventTimeSinceStartEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	if len(span.Events) == 0 {
		r.SetNil()
		return r
	}
	start := span.Start.AsTime()
	eventTime := span.Events[0].Timestamp.AsTime()
	r.SetDuration(eventTime.Sub(start))
	return r
}

// LinkTraceIDEvaluater evaluates `link:traceId` property.
// Returns the trace ID of the first span link, or nil if the span has no links.
type LinkTraceIDEvaluater struct{}

// Eval implemenets [Evaluater].
func (*LinkTraceIDEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	if len(span.Links) == 0 {
		r.SetNil()
		return r
	}
	r.SetString(span.Links[0].TraceID.Hex())
	return r
}

// LinkSpanIDEvaluater evaluates `link:spanId` property.
// Returns the span ID of the first span link, or nil if the span has no links.
type LinkSpanIDEvaluater struct{}

// Eval implemenets [Evaluater].
func (*LinkSpanIDEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	if len(span.Links) == 0 {
		r.SetNil()
		return r
	}
	r.SetString(span.Links[0].SpanID.Hex())
	return r
}

// InstrumentationAttributeEvaluater evaluates instrumentation scope attribute selector.
type InstrumentationAttributeEvaluater struct {
	Name string
}

// Eval implemenets [Evaluater].
func (e *InstrumentationAttributeEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) traceql.Static {
	return evaluateAttr(e.Name, span.ScopeAttrs)
}

// EventAttributeEvaluater evaluates event scope attribute selector.
// Returns the attribute value from the first event that has it.
type EventAttributeEvaluater struct {
	Name string
}

// Eval implemenets [Evaluater].
func (e *EventAttributeEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	for _, ev := range span.Events {
		if r = evaluateAttr(e.Name, ev.Attrs); r.Type != traceql.TypeNil {
			return r
		}
	}
	r.SetNil()
	return r
}

// LinkAttributeEvaluater evaluates link scope attribute selector.
// Returns the attribute value from the first link that has it.
type LinkAttributeEvaluater struct {
	Name string
}

// Eval implemenets [Evaluater].
func (e *LinkAttributeEvaluater) Eval(span tracestorage.Span, _ EvaluateCtx) (r traceql.Static) {
	for _, lnk := range span.Links {
		if r = evaluateAttr(e.Name, lnk.Attrs); r.Type != traceql.TypeNil {
			return r
		}
	}
	r.SetNil()
	return r
}
