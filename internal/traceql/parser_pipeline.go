package traceql

import (
	"github.com/oteldb/oteldb/internal/traceql/lexer"
)

func (p *parser) parsePipeline() (stages []PipelineStage, rerr error) {
	p.first = true
	p.parens = 0
	defer func() {
		if rerr != nil {
			return
		}
		for i := 0; i < p.parens; i++ {
			if err := p.consume(lexer.CloseParen); err != nil {
				rerr = err
			}
		}
	}()

	for {
		t := p.lookaheadParen()
		switch t.Type {
		case lexer.OpenBrace:
			expr, err := p.parseSpansetExpr()
			if err != nil {
				return stages, err
			}
			stages = append(stages, expr)
		case lexer.By:
			p.next()

			op, err := p.parseGroupOperation()
			if err != nil {
				return stages, err
			}
			stages = append(stages, op)
		case lexer.Coalesce:
			if len(stages) < 1 {
				return stages, &SyntaxError{
					Msg: "coalesce cannot be first operation",
					Pos: t.Pos,
				}
			}
			p.next()

			op, err := p.parseCoalesceOperation()
			if err != nil {
				return stages, err
			}
			stages = append(stages, op)
		case lexer.Select:
			p.next()

			op, err := p.parseSelectOperation()
			if err != nil {
				return stages, err
			}
			stages = append(stages, op)
		case lexer.Integer,
			lexer.Number,
			lexer.Duration,
			lexer.Count,
			lexer.Max,
			lexer.Min,
			lexer.Avg,
			lexer.Sum:
			expr, err := p.parseScalarFilter()
			if err != nil {
				return stages, err
			}
			stages = append(stages, expr)
		default:
			return stages, p.unexpectedToken(t)
		}

		if t := p.peek(); t.Type != lexer.Pipe {
			return stages, nil
		}
		// Consume "|".
		p.next()
		p.first = false
	}
}

func (p *parser) tryReadCloseParen() error {
	if !p.first {
		p.parens--
		return p.consume(lexer.CloseParen)
	}
	t := p.peek()
	if t.Type != lexer.CloseParen {
		return nil
	}
	if p.parens <= 0 {
		return p.unexpectedToken(t)
	}
	// Consume.
	p.next()
	p.parens--
	return nil
}

func (p *parser) lookaheadParen() lexer.Token {
	var n int
	for p.peek().Type == lexer.OpenParen {
		p.next()
		n++
	}
	t := p.peek()
	for i := 0; i < n; i++ {
		p.unread()
	}
	return t
}

func (p *parser) parseSpansetExpr() (SpansetExpr, error) {
	expr, err := p.parseSpansetExpr1()
	if err != nil {
		return nil, err
	}
	return p.parseBinarySpansetExpr(expr, 0)
}

func (p *parser) parseSpansetExpr1() (SpansetExpr, error) {
	switch t := p.next(); t.Type {
	case lexer.OpenParen:
		p.parens++
		expr, err := p.parseSpansetExpr()
		if err != nil {
			return nil, err
		}

		if err := p.tryReadCloseParen(); err != nil {
			return nil, err
		}
		return expr, nil
	case lexer.OpenBrace:
		var filter SpansetFilter
		if t2 := p.peek(); t2.Type != lexer.CloseBrace {
			fieldExpr, err := p.parseFieldExpr()
			if err != nil {
				return nil, err
			}
			switch fieldExpr.ValueType() {
			case TypeBool, TypeAttribute:
			default:
				return nil, &TypeError{
					Msg: "filter expression must evaluate to boolean",
					Pos: t2.Pos,
				}
			}
			filter.Expr = fieldExpr
		} else {
			s := &Static{}
			s.SetBool(true)
			filter.Expr = s
		}

		if err := p.consume(lexer.CloseBrace); err != nil {
			return nil, err
		}

		return &filter, nil
	default:
		return nil, p.unexpectedToken(t)
	}
}

func (p *parser) parseBinarySpansetExpr(left SpansetExpr, minPrecedence int) (SpansetExpr, error) {
	for {
		op, ok := p.peekSpansetOp()
		if !ok || op.Precedence() < minPrecedence {
			return left, nil
		}
		// Consume op.
		p.next()

		right, err := p.parseSpansetExpr1()
		if err != nil {
			return nil, err
		}

		for {
			rightOp, ok := p.peekSpansetOp()
			if !ok || rightOp.Precedence() < op.Precedence() {
				break
			}

			nextPrecedence := op.Precedence()
			if rightOp.Precedence() > op.Precedence() {
				nextPrecedence++
			}

			right, err = p.parseBinarySpansetExpr(right, nextPrecedence)
			if err != nil {
				return nil, err
			}
		}

		left = &BinarySpansetExpr{Left: left, Op: op, Right: right}
	}
}

func (p *parser) peekSpansetOp() (op SpansetOp, _ bool) {
	switch t := p.peek(); t.Type {
	case lexer.And:
		return SpansetOpAnd, true
	case lexer.Gt:
		return SpansetOpChild, true
	case lexer.Desc:
		return SpansetOpDescendant, true
	case lexer.Or:
		return SpansetOpUnion, true
	case lexer.Tilde:
		return SpansetOpSibling, true
	default:
		return op, false
	}
}

func (p *parser) parseScalarFilter() (*ScalarFilter, error) {
	left, err := p.parseScalarExpr()
	if err != nil {
		return nil, err
	}

	var op BinaryOp
	switch t := p.next(); t.Type {
	case lexer.Eq:
		op = OpEq
	case lexer.NotEq:
		op = OpNotEq
	case lexer.Gt:
		op = OpGt
	case lexer.Gte:
		op = OpGte
	case lexer.Lt:
		op = OpLt
	case lexer.Lte:
		op = OpLte
	default:
		return nil, p.unexpectedToken(t)
	}

	right, err := p.parseScalarExpr()
	if err != nil {
		return nil, err
	}

	return &ScalarFilter{Left: left, Op: op, Right: right}, nil
}

func (p *parser) parseGroupOperation() (*GroupOperation, error) {
	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	field, err := p.parseFieldExpr()
	if err != nil {
		return nil, err
	}

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}

	return &GroupOperation{By: field}, nil
}

func (p *parser) parseCoalesceOperation() (*CoalesceOperation, error) {
	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}

	return &CoalesceOperation{}, nil
}

func (p *parser) parseSelectOperation() (s *SelectOperation, _ error) {
	s = new(SelectOperation)

	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	for {
		field, err := p.parseFieldExpr()
		if err != nil {
			return nil, err
		}
		s.Args = append(s.Args, field)

		if t := p.peek(); t.Type != lexer.Comma {
			break
		}
		// Consume comma.
		p.next()
	}

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}

	return s, nil
}
