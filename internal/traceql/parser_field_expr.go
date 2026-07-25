package traceql

import (
	"fmt"
	"math"
	"strings"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/oteldb/internal/traceql/lexer"
)

func (p *parser) parseFieldExpr() (FieldExpr, error) {
	expr, err := p.parseFieldExpr1()
	if err != nil {
		return nil, err
	}
	return p.parseBinaryFieldExpr(expr, 0)
}

func (p *parser) parseFieldExpr1() (FieldExpr, error) {
	switch t := p.peek(); t.Type {
	case lexer.OpenParen:
		p.next()

		expr, err := p.parseFieldExpr()
		if err != nil {
			return nil, err
		}

		if err := p.consume(lexer.CloseParen); err != nil {
			return nil, err
		}
		return expr, nil
	case lexer.Not, lexer.Sub:
		p.next()

		pos := p.peek().Pos
		expr, err := p.parseFieldExpr1()
		if err != nil {
			return nil, err
		}

		var op UnaryOp
		switch t.Type {
		case lexer.Not:
			op = OpNot
		case lexer.Sub:
			op = OpNeg
		}

		if t := expr.ValueType(); !op.CheckType(t) {
			return nil, &TypeError{
				Msg: fmt.Sprintf("unary operator %q not defined on %q", op, t),
				Pos: pos,
			}
		}
		return &UnaryFieldExpr{
			Expr: expr,
			Op:   op,
		}, nil
	default:
		switch s, ok, err := p.tryStatic(); {
		case err != nil:
			return nil, err
		case ok:
			return s, nil
		}

		switch a, ok, err := p.tryAttribute(); {
		case err != nil:
			return nil, err
		case ok:
			return &a, nil
		}
		return nil, p.unexpectedToken(t)
	}
}

func (p *parser) parseBinaryFieldExpr(left FieldExpr, minPrecedence int) (FieldExpr, error) {
	for {
		op, ok := p.peekBinaryOp()
		if !ok || op.Precedence() < minPrecedence {
			return left, nil
		}
		// Consume op and get op token position.
		opPos := p.next().Pos

		// Get right expression position.
		rightPos := p.peek().Pos
		right, err := p.parseFieldExpr1()
		if err != nil {
			return nil, err
		}

		if op.IsRegex() {
			if _, ok := right.(*Static); !ok {
				return nil, &TypeError{
					Msg: fmt.Sprintf("regexp pattern should be a static string, got %T", right),
					Pos: rightPos,
				}
			}
		}

		for {
			rightOp, ok := p.peekBinaryOp()
			if !ok || rightOp.Precedence() < op.Precedence() {
				break
			}

			nextPrecedence := op.Precedence()
			if rightOp.Precedence() > op.Precedence() {
				nextPrecedence++
			}

			right, err = p.parseBinaryFieldExpr(right, nextPrecedence)
			if err != nil {
				return nil, err
			}
		}

		if err := p.checkBinaryExpr(left, op, opPos, right, rightPos); err != nil {
			return nil, err
		}
		left = &BinaryFieldExpr{Left: left, Op: op, Right: right}
	}
}

func (p *parser) peekBinaryOp() (op BinaryOp, _ bool) {
	switch t := p.peek(); t.Type {
	case lexer.Eq:
		return OpEq, true
	case lexer.NotEq:
		return OpNotEq, true
	case lexer.Re:
		return OpRe, true
	case lexer.NotRe:
		return OpNotRe, true
	case lexer.Gt:
		return OpGt, true
	case lexer.Gte:
		return OpGte, true
	case lexer.Lt:
		return OpLt, true
	case lexer.Lte:
		return OpLte, true
	case lexer.Add:
		return OpAdd, true
	case lexer.Sub:
		return OpSub, true
	case lexer.Div:
		return OpDiv, true
	case lexer.Mod:
		return OpMod, true
	case lexer.Mul:
		return OpMul, true
	case lexer.Pow:
		return OpPow, true
	case lexer.And:
		return OpAnd, true
	case lexer.Or:
		return OpOr, true
	default:
		return op, false
	}
}

func (p *parser) parseStatic() (*Static, error) {
	switch s, ok, err := p.tryStatic(); {
	case err != nil:
		return nil, err
	case ok:
		return s, nil
	default:
		return nil, p.unexpectedToken(p.peek())
	}
}

func (p *parser) tryStatic() (s *Static, ok bool, _ error) {
	s = new(Static)
	switch t := p.peek(); t.Type {
	case lexer.String:
		p.next()
		s.SetString(t.Text)
	case lexer.Integer:
		v, err := p.parseInteger()
		if err != nil {
			return s, false, err
		}
		s.SetInt(v)
	case lexer.Number:
		v, err := p.parseNumber()
		if err != nil {
			return s, false, err
		}
		s.SetNumber(v)
	case lexer.True:
		p.next()
		s.SetBool(true)
	case lexer.False:
		p.next()
		s.SetBool(false)
	case lexer.Nil:
		p.next()
		s.SetNil()
	case lexer.Duration:
		v, err := p.parseDuration()
		if err != nil {
			return s, false, err
		}
		s.SetDuration(v)
	case lexer.StatusOk:
		p.next()
		s.SetSpanStatus(ptrace.StatusCodeOk)
	case lexer.StatusError:
		p.next()
		s.SetSpanStatus(ptrace.StatusCodeError)
	case lexer.StatusUnset:
		p.next()
		s.SetSpanStatus(ptrace.StatusCodeUnset)
	case lexer.KindUnspecified:
		p.next()
		s.SetSpanKind(ptrace.SpanKindUnspecified)
	case lexer.KindInternal:
		p.next()
		s.SetSpanKind(ptrace.SpanKindInternal)
	case lexer.KindServer:
		p.next()
		s.SetSpanKind(ptrace.SpanKindServer)
	case lexer.KindClient:
		p.next()
		s.SetSpanKind(ptrace.SpanKindClient)
	case lexer.KindProducer:
		p.next()
		s.SetSpanKind(ptrace.SpanKindProducer)
	case lexer.KindConsumer:
		p.next()
		s.SetSpanKind(ptrace.SpanKindConsumer)
	case lexer.Ident:
		// The only bare identifiers TraceQL defines are these two int constants.
		switch t.Text {
		case "minInt":
			p.next()
			s.SetInt(math.MinInt64)
		case "maxInt":
			p.next()
			s.SetInt(math.MaxInt64)
		default:
			return s, false, nil
		}
	default:
		return s, false, nil
	}
	return s, true, nil
}

func (p *parser) tryAttribute() (a Attribute, _ bool, _ error) {
	switch t := p.peek(); t.Type {
	case lexer.SpanDuration:
		a.Prop = SpanDuration
	case lexer.ChildCount:
		a.Prop = SpanChildCount
	case lexer.Name:
		a.Prop = SpanName
	case lexer.Status:
		a.Prop = SpanStatus
	case lexer.StatusMessage:
		a.Prop = SpanStatusMessage
	case lexer.Kind:
		a.Prop = SpanKind
	case lexer.Parent:
		a.Prop = SpanParent
	case lexer.RootName:
		a.Prop = RootSpanName
	case lexer.RootServiceName:
		a.Prop = RootServiceName
	case lexer.TraceDuration:
		a.Prop = TraceDuration
	case lexer.NestedSetLeft:
		a.Prop = NestedSetLeft
	case lexer.NestedSetRight:
		a.Prop = NestedSetRight
	case lexer.NestedSetParent:
		a.Prop = NestedSetParent
	case lexer.TraceColon:
		p.next()
		a, ok := p.parseScopedTraceIntrinsic()
		return a, ok, nil
	case lexer.SpanColon:
		p.next()
		a, ok := p.parseScopedSpanIntrinsic()
		return a, ok, nil
	case lexer.EventColon:
		p.next()
		a, ok := p.parseScopedEventIntrinsic()
		return a, ok, nil
	case lexer.LinkColon:
		p.next()
		a, ok := p.parseScopedLinkIntrinsic()
		return a, ok, nil
	case lexer.InstrumentationColon:
		p.next()
		a, ok := p.parseScopedInstrumentationIntrinsic()
		return a, ok, nil
	case lexer.Ident:
		if err := parseAttributeSelector(t.Text, &a); err != nil {
			return a, false, &SyntaxError{Msg: err.Error(), Pos: t.Pos}
		}
	default:
		return a, false, nil
	}
	p.next()

	return a, true, nil
}

func (p *parser) parseScopedTraceIntrinsic() (a Attribute, _ bool) {
	switch p.peek().Type {
	case lexer.SpanDuration:
		a.Prop = TraceDuration
	case lexer.RootName:
		a.Prop = RootSpanName
	case lexer.RootService:
		a.Prop = RootServiceName
	case lexer.ID:
		a.Prop = TraceID
	default:
		p.unread() // unread trace:
		return a, false
	}
	p.next()
	return a, true
}

func (p *parser) parseScopedSpanIntrinsic() (a Attribute, _ bool) {
	switch p.peek().Type {
	case lexer.SpanDuration:
		a.Prop = SpanDuration
	case lexer.Name:
		a.Prop = SpanName
	case lexer.Kind:
		a.Prop = SpanKind
	case lexer.Status:
		a.Prop = SpanStatus
	case lexer.StatusMessage:
		a.Prop = SpanStatusMessage
	case lexer.ID:
		a.Prop = SpanID
	case lexer.ParentID:
		a.Prop = ParentID
	case lexer.ChildCount:
		a.Prop = SpanChildCount
	default:
		p.unread() // unread span:
		return a, false
	}
	p.next()
	return a, true
}

func (p *parser) parseScopedEventIntrinsic() (a Attribute, _ bool) {
	switch p.peek().Type {
	case lexer.Name:
		a.Prop = EventName
	case lexer.TimeSinceStart:
		a.Prop = EventTimeSinceStart
	default:
		p.unread() // unread event:
		return a, false
	}
	p.next()
	return a, true
}

func (p *parser) parseScopedLinkIntrinsic() (a Attribute, _ bool) {
	switch p.peek().Type {
	case lexer.TraceID:
		a.Prop = LinkTraceID
	case lexer.SpanID:
		a.Prop = LinkSpanID
	default:
		p.unread() // unread link:
		return a, false
	}
	p.next()
	return a, true
}

func (p *parser) parseScopedInstrumentationIntrinsic() (a Attribute, _ bool) {
	switch p.peek().Type {
	case lexer.Name:
		a.Prop = InstrumentationName
	case lexer.Version:
		a.Prop = InstrumentationVersion
	default:
		p.unread() // unread instrumentation:
		return a, false
	}
	p.next()
	return a, true
}

func parseAttributeSelector(attr string, a *Attribute) error {
	attr, a.Parent = strings.CutPrefix(attr, "parent.")

	uncut := attr
	scope, attr, ok := strings.Cut(attr, ".")
	if !ok {
		if !a.Parent {
			// A bare word is not an attribute selector: a scope prefix
			// ("span.", "resource.", ...) or a leading dot is required.
			return errors.Errorf("unknown identifier %q", uncut)
		}
		a.Name = uncut
		return checkAttributeName(a.Name)
	}

	switch scope {
	case "resource":
		a.Name = attr
		a.Scope = ScopeResource
	case "span":
		a.Name = attr
		a.Scope = ScopeSpan
	case "instrumentation":
		a.Name = attr
		a.Scope = ScopeInstrumentation
	case "event":
		a.Name = attr
		a.Scope = ScopeEvent
	case "link":
		a.Name = attr
		a.Scope = ScopeLink
	case "":
		a.Name = attr
		a.Scope = ScopeNone
	default:
		a.Name = uncut
		a.Scope = ScopeNone
	}
	return checkAttributeName(a.Name)
}

func checkAttributeName(name string) error {
	if name == "" {
		return errors.New("attribute name is empty")
	}
	return nil
}
