package traceql

import (
	"text/scanner"

	"github.com/oteldb/oteldb/internal/traceql/lexer"
)

// mathOperand is an operand of a metrics arithmetic expression.
//
// Exactly one of the fields is set: an operand is either a metrics expression
// or a constant.
type mathOperand struct {
	expr   MetricsExpr
	scalar float64
}

// tryMetricsMath tries to parse a metrics arithmetic expression.
//
// A metrics arithmetic expression starts either with a parenthesized sub-query
// or with a constant, and so does a spanset query like `({a}) && {b}` or a
// scalar filter pipeline like `1 > count()`. They are told apart by parsing:
// unless a metrics sub-query is reached, the parser state is restored and ok
// is false, so that the caller may parse a spanset query instead.
func (p *parser) tryMetricsMath() (_ Expr, ok bool, _ error) {
	save := *p

	expr, err := p.parseMetricsMath()
	if err != nil {
		if !p.metricsSubQuery {
			*p = save
			return nil, false, nil
		}
		return nil, true, err
	}
	return expr, true, nil
}

func (p *parser) parseMetricsMath() (Expr, error) {
	operand, err := p.parseMetricsMathOperand()
	if err != nil {
		return nil, err
	}

	operand, err = p.parseMetricsMathExpr(operand, 0)
	if err != nil {
		return nil, err
	}
	if operand.expr == nil {
		// A bare constant is not a metrics expression.
		return nil, p.unexpectedToken(p.peek())
	}

	stagesPos := p.peek().Pos
	stages, err := p.parseMetricsStages()
	if err != nil {
		return nil, err
	}
	if len(stages) == 0 {
		return operand.expr, nil
	}
	if err := checkNotCompare(operand.expr, stagesPos); err != nil {
		return nil, err
	}
	return &MetricsPipeline{Expr: operand.expr, Stages: stages}, nil
}

// parseMetricsMathExpr parses arithmetic operations applied to left, if any.
func (p *parser) parseMetricsMathExpr(left mathOperand, minPrecedence int) (mathOperand, error) {
	for {
		op, ok := p.peekMetricsMathOp()
		if !ok || op.Precedence() < minPrecedence {
			return left, nil
		}
		// Consume op.
		opPos := p.next().Pos

		right, err := p.parseMetricsMathOperand()
		if err != nil {
			return left, err
		}

		for {
			rightOp, ok := p.peekMetricsMathOp()
			if !ok || rightOp.Precedence() <= op.Precedence() {
				break
			}

			right, err = p.parseMetricsMathExpr(right, op.Precedence()+1)
			if err != nil {
				return left, err
			}
		}

		left, err = combineMetricsMath(left, op, right, opPos)
		if err != nil {
			return left, err
		}
	}
}

// parseMetricsMathOperand parses a single operand of an arithmetic expression.
func (p *parser) parseMetricsMathOperand() (o mathOperand, _ error) {
	switch t := p.peek(); t.Type {
	case lexer.Integer, lexer.Number:
		scalar, err := p.parseFloat()
		if err != nil {
			return o, err
		}
		o.scalar = scalar
		return o, nil
	case lexer.OpenParen:
		// Consume "(".
		p.next()

		// A sub-query must start with a spanset filter, anything else is
		// a parenthesized arithmetic expression.
		if p.peek().Type == lexer.OpenBrace {
			expr, err := p.parseWrappedMetricsPipeline()
			if err != nil {
				return o, err
			}
			// Past this point the query is certainly an arithmetic expression,
			// see [parser.tryMetricsMath].
			p.metricsSubQuery = true
			o.expr = expr
		} else {
			inner, err := p.parseMetricsMathOperand()
			if err != nil {
				return o, err
			}
			if o, err = p.parseMetricsMathExpr(inner, 0); err != nil {
				return o, err
			}
		}

		if err := p.consume(lexer.CloseParen); err != nil {
			return o, err
		}
		return o, nil
	default:
		return o, p.unexpectedToken(t)
	}
}

// parseWrappedMetricsPipeline parses a `<spanset pipeline> | <metrics function>`
// sub-query, with the opening parenthesis already consumed.
func (p *parser) parseWrappedMetricsPipeline() (MetricsExpr, error) {
	pipeline, err := p.parsePipeline()
	if err != nil {
		return nil, err
	}

	if err := p.consume(lexer.Pipe); err != nil {
		return nil, err
	}
	return p.parseMetricsFirstStage(&SpansetPipeline{Pipeline: pipeline})
}

func combineMetricsMath(left mathOperand, op BinaryOp, right mathOperand, opPos scanner.Position) (o mathOperand, _ error) {
	for _, e := range []MetricsExpr{left.expr, right.expr} {
		if e == nil {
			continue
		}
		if err := checkNotCompare(e, opPos); err != nil {
			return o, err
		}
	}

	switch {
	case left.expr != nil && right.expr != nil:
		o.expr = &MetricsBinaryExpr{Left: left.expr, Op: op, Right: right.expr}
	case left.expr != nil:
		o.expr = appendMetricsStage(left.expr, &MetricsScalarOp{Op: op, Value: right.scalar})
	case right.expr != nil:
		o.expr = appendMetricsStage(right.expr, &MetricsScalarOp{Op: op, Value: left.scalar, ScalarLeft: true})
	default:
		// Unlike Tempo, constant arithmetic is not folded at parse time.
		return o, &SyntaxError{
			Msg: "arithmetic between two constants is not supported",
			Pos: opPos,
		}
	}
	return o, nil
}

func appendMetricsStage(e MetricsExpr, stage MetricsStage) *MetricsPipeline {
	if pipeline, ok := e.(*MetricsPipeline); ok {
		pipeline.Stages = append(pipeline.Stages, stage)
		return pipeline
	}
	return &MetricsPipeline{Expr: e, Stages: []MetricsStage{stage}}
}

func (p *parser) peekMetricsMathOp() (op BinaryOp, _ bool) {
	switch p.peek().Type {
	case lexer.Add:
		return OpAdd, true
	case lexer.Sub:
		return OpSub, true
	case lexer.Mul:
		return OpMul, true
	case lexer.Div:
		return OpDiv, true
	default:
		return op, false
	}
}
