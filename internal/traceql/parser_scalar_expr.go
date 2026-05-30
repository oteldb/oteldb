package traceql

import (
	"github.com/oteldb/oteldb/internal/traceql/lexer"
)

func (p *parser) parseScalarExpr() (ScalarExpr, error) {
	expr, err := p.parseScalarExpr1()
	if err != nil {
		return nil, err
	}
	return p.parseBinaryScalarExpr(expr, 0)
}

func (p *parser) parseScalarExpr1() (ScalarExpr, error) {
	switch t := p.peek(); t.Type {
	case lexer.OpenParen:
		p.next()
		p.parens++

		expr, err := p.parseScalarExpr()
		if err != nil {
			return nil, err
		}

		if err := p.tryReadCloseParen(); err != nil {
			return nil, err
		}
		return expr, nil
	case lexer.Integer,
		lexer.Number,
		lexer.Duration:
		return p.parseStatic()
	case lexer.Count,
		lexer.Max,
		lexer.Min,
		lexer.Avg,
		lexer.Sum:
		return p.parseAggregateScalarExpr()
	default:
		return nil, p.unexpectedToken(t)
	}
}

func (p *parser) parseAggregateScalarExpr() (expr *AggregateScalarExpr, _ error) {
	expr = new(AggregateScalarExpr)
	switch t := p.next(); t.Type {
	case lexer.Count:
		expr.Op = AggregateOpCount
	case lexer.Max:
		expr.Op = AggregateOpMax
	case lexer.Min:
		expr.Op = AggregateOpMin
	case lexer.Avg:
		expr.Op = AggregateOpAvg
	case lexer.Sum:
		expr.Op = AggregateOpSum
	default:
		return nil, p.unexpectedToken(t)
	}

	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	if expr.Op != AggregateOpCount {
		pos := p.peek().Pos

		field, err := p.parseFieldExpr()
		if err != nil {
			return nil, err
		}
		expr.Field = field

		if t := field.ValueType(); t != TypeAttribute && !t.IsNumeric() {
			return nil, &TypeError{
				Msg: "aggregate expression must evaluate to numeric",
				Pos: pos,
			}
		}
	}

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}

	return expr, nil
}

func (p *parser) parseBinaryScalarExpr(left ScalarExpr, minPrecedence int) (ScalarExpr, error) {
	for {
		op, ok := p.peekBinaryOp()
		if !ok || !op.IsArithmetic() || op.Precedence() < minPrecedence {
			return left, nil
		}
		// Consume op and get op token position.
		opPos := p.next().Pos

		// Get right expression position.
		rightPos := p.peek().Pos
		right, err := p.parseScalarExpr1()
		if err != nil {
			return nil, err
		}

		for {
			rightOp, ok := p.peekBinaryOp()
			if !ok || !op.IsArithmetic() || rightOp.Precedence() < op.Precedence() {
				break
			}

			nextPrecedence := op.Precedence()
			if rightOp.Precedence() > op.Precedence() {
				nextPrecedence++
			}

			right, err = p.parseBinaryScalarExpr(right, nextPrecedence)
			if err != nil {
				return nil, err
			}
		}

		if err := p.checkBinaryExpr(left, op, opPos, right, rightPos); err != nil {
			return nil, err
		}
		left = &BinaryScalarExpr{Left: left, Op: op, Right: right}
	}
}
