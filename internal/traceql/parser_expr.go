package traceql

import (
	"github.com/oteldb/oteldb/internal/traceql/lexer"
)

func (p *parser) parseExpr() (Expr, error) {
	switch p.peek().Type {
	case lexer.OpenParen, lexer.Integer, lexer.Number:
		switch expr, ok, err := p.tryMetricsMath(); {
		case err != nil:
			return nil, err
		case ok:
			return expr, nil
		}
	}

	expr, err := p.parseExpr1()
	if err != nil {
		return nil, err
	}
	expr, err = p.parseBinaryExpr(expr, 0)
	if err != nil {
		return nil, err
	}
	return p.parseMetricsExpr(expr)
}

func (p *parser) parseExpr1() (Expr, error) {
	pipeline, err := p.parsePipeline()
	if err != nil {
		return nil, err
	}
	return &SpansetPipeline{Pipeline: pipeline}, nil
}

func (p *parser) parseBinaryExpr(left Expr, minPrecedence int) (Expr, error) {
	for {
		op, ok := p.peekSpansetOp()
		if !ok || op.Precedence() < minPrecedence {
			return left, nil
		}
		// Consume op.
		p.next()

		right, err := p.parseExpr1()
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

			right, err = p.parseBinaryExpr(right, nextPrecedence)
			if err != nil {
				return nil, err
			}
		}

		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}
}
