package traceql

import (
	"fmt"

	"github.com/oteldb/oteldb/internal/traceql/lexer"
)

// parseMetricsExpr parses a metrics aggregation applied to the given spanset
// pipeline, if there is any.
//
// The pipe is left unconsumed by [parser.parsePipeline], since a metrics
// aggregation terminates the pipeline rather than being a stage of it.
func (p *parser) parseMetricsExpr(spanset Expr) (Expr, error) {
	if p.peek().Type != lexer.Pipe {
		return spanset, nil
	}
	// Consume "|".
	p.next()

	e, err := p.parseMetricsAggregation()
	if err != nil {
		return nil, err
	}
	e.Spanset = spanset

	e.Stages, err = p.parseMetricsStages()
	if err != nil {
		return nil, err
	}
	return e, nil
}

// parseMetricsStages parses a chain of second stage operations.
//
// Unlike a filter, a `topk()`/`bottomk()` operation is preceded by a pipe.
func (p *parser) parseMetricsStages() (stages []MetricsStage, _ error) {
	for {
		switch t := p.peek(); t.Type {
		case lexer.Pipe:
			// Consume "|".
			p.next()

			stage, err := p.parseTopKOperation()
			if err != nil {
				return nil, err
			}
			stages = append(stages, stage)
		case lexer.Eq, lexer.NotEq, lexer.Gt, lexer.Gte, lexer.Lt, lexer.Lte:
			stage, err := p.parseMetricsFilter()
			if err != nil {
				return nil, err
			}
			stages = append(stages, stage)
		default:
			return stages, nil
		}
	}
}

func (p *parser) parseTopKOperation() (e *TopKOperation, _ error) {
	e = new(TopKOperation)

	opTok := p.next()
	switch opTok.Type {
	case lexer.TopK:
		e.Op = MetricsStageOpTopK
	case lexer.BottomK:
		e.Op = MetricsStageOpBottomK
	default:
		return nil, p.unexpectedToken(opTok)
	}

	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	limitPos := p.peek().Pos
	limit, err := p.parseInteger()
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, &SyntaxError{
			Msg: fmt.Sprintf("%s limit must be greater than 0, got %d", e.Op, limit),
			Pos: limitPos,
		}
	}
	e.Limit = int(limit)

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}
	return e, nil
}

func (p *parser) parseMetricsFilter() (e *MetricsFilter, _ error) {
	e = new(MetricsFilter)

	switch t := p.next(); t.Type {
	case lexer.Eq:
		e.Op = OpEq
	case lexer.NotEq:
		e.Op = OpNotEq
	case lexer.Gt:
		e.Op = OpGt
	case lexer.Gte:
		e.Op = OpGte
	case lexer.Lt:
		e.Op = OpLt
	case lexer.Lte:
		e.Op = OpLte
	default:
		return nil, p.unexpectedToken(t)
	}

	valuePos := p.peek().Pos
	value, err := p.parseStatic()
	if err != nil {
		return nil, err
	}
	if !value.Type.IsNumeric() {
		return nil, &TypeError{
			Msg: "metrics filter value must be numeric",
			Pos: valuePos,
		}
	}
	e.Value = value

	return e, nil
}

func (p *parser) parseMetricsAggregation() (e *MetricsAggregation, _ error) {
	e = new(MetricsAggregation)

	opTok := p.next()
	op, ok := metricsOp(opTok.Type)
	if !ok {
		return nil, p.unexpectedToken(opTok)
	}
	e.Op = op

	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}
	if t := p.peek(); t.Type != lexer.CloseParen {
		attr, ok, err := p.tryAttribute()
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, p.unexpectedToken(t)
		}
		e.Field = &attr

		for p.peek().Type == lexer.Comma {
			// Consume comma.
			p.next()

			param, err := p.parseFloat()
			if err != nil {
				return nil, err
			}
			e.Parameters = append(e.Parameters, param)
		}
	}
	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}

	if p.peek().Type == lexer.By {
		// Consume "by".
		p.next()

		by, err := p.parseAttributeList()
		if err != nil {
			return nil, err
		}
		e.By = by
	}

	if err := e.validate(); err != nil {
		return nil, &SyntaxError{Msg: err.Error(), Pos: opTok.Pos}
	}
	return e, nil
}

// parseAttributeList parses a parenthesized list of comma-separated attributes.
func (p *parser) parseAttributeList() (attrs []Attribute, _ error) {
	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	for {
		t := p.peek()
		attr, ok, err := p.tryAttribute()
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, p.unexpectedToken(t)
		}
		attrs = append(attrs, attr)

		if p.peek().Type != lexer.Comma {
			break
		}
		// Consume comma.
		p.next()
	}

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}
	return attrs, nil
}

func metricsOp(tt lexer.TokenType) (op MetricsOp, _ bool) {
	switch tt {
	case lexer.Rate:
		return MetricsOpRate, true
	case lexer.CountOverTime:
		return MetricsOpCountOverTime, true
	case lexer.MinOverTime:
		return MetricsOpMinOverTime, true
	case lexer.MaxOverTime:
		return MetricsOpMaxOverTime, true
	case lexer.SumOverTime:
		return MetricsOpSumOverTime, true
	case lexer.AvgOverTime:
		return MetricsOpAvgOverTime, true
	case lexer.QuantileOverTime:
		return MetricsOpQuantileOverTime, true
	case lexer.HistogramOverTime:
		return MetricsOpHistogramOverTime, true
	default:
		return op, false
	}
}
