package logql

import (
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/logql/lexer"
)

func (p *parser) parseMetricExpr() (MetricExpr, error) {
	expr, err := p.parseMetricExpr1()
	if err != nil {
		return nil, err
	}
	return p.parseBinOp(expr, 0)
}

func (p *parser) parseMetricExpr1() (MetricExpr, error) {
	switch t := p.peek(); t.Type {
	case lexer.OpenParen:
		p.next()

		subExpr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		if err := p.consume(lexer.CloseParen); err != nil {
			return nil, err
		}

		return &ParenExpr{X: subExpr}, nil
	case lexer.CountOverTime,
		lexer.Rate,
		lexer.RateCounter,
		lexer.BytesOverTime,
		lexer.BytesRate,
		lexer.AvgOverTime,
		lexer.SumOverTime,
		lexer.MinOverTime,
		lexer.MaxOverTime,
		lexer.StdvarOverTime,
		lexer.StddevOverTime,
		lexer.QuantileOverTime,
		lexer.FirstOverTime,
		lexer.LastOverTime,
		lexer.AbsentOverTime:
		return p.parseRangeAggregationExpr()
	case lexer.Sum,
		lexer.Avg,
		lexer.Count,
		lexer.Max,
		lexer.Min,
		lexer.Stddev,
		lexer.Stdvar,
		lexer.Bottomk,
		lexer.Topk,
		lexer.Sort,
		lexer.SortDesc:
		return p.parseVectorAggregationExpr()
	case lexer.Number, lexer.Add, lexer.Sub:
		return p.parseLiteralExpr()
	case lexer.LabelReplace:
		return p.parseLabelReplace()
	case lexer.Vector:
		return p.parseVectorExpr()
	default:
		return nil, p.unexpectedToken(t)
	}
}

func (p *parser) parseBinOp(left MetricExpr, minPrecedence int) (MetricExpr, error) {
	for {
		op, ok := p.peekBinOp()
		if !ok || op.Precedence() < minPrecedence {
			return left, nil
		}
		// Consume op.
		opTok := p.next()

		modifier, err := p.parseBinOpModifier()
		if err != nil {
			return nil, err
		}

		right, err := p.parseMetricExpr1()
		if err != nil {
			return nil, err
		}

		if op.IsLogic() {
			if v, ok := left.(*LiteralExpr); ok {
				return nil, &ParseError{
					Pos: opTok.Pos,
					Err: errors.Errorf("unexpected left scalar %v in a logical operation %s", v.Value, op),
				}
			}
			if v, ok := right.(*LiteralExpr); ok {
				return nil, &ParseError{
					Pos: opTok.Pos,
					Err: errors.Errorf("unexpected right scalar %v in a logical operation %s", v.Value, op),
				}
			}
		}

		for {
			rightOp, ok := p.peekBinOp()
			if !ok {
				break
			}
			if !(rightOp.Precedence() > op.Precedence() ||
				(rightOp.IsRightAssoc() && rightOp.Precedence() == op.Precedence())) {
				break
			}

			nextPrecedence := op.Precedence()
			if rightOp.Precedence() > op.Precedence() {
				nextPrecedence++
			}

			right, err = p.parseBinOp(right, nextPrecedence)
			if err != nil {
				return nil, err
			}
		}

		left = &BinOpExpr{Left: left, Op: op, Modifier: modifier, Right: right}
	}
}

func (p *parser) peekBinOp() (op BinOp, ok bool) {
	switch t := p.peek(); t.Type {
	case lexer.Or:
		return OpOr, true
	case lexer.And:
		return OpAnd, true
	case lexer.Unless:
		return OpUnless, true
	case lexer.Add:
		return OpAdd, true
	case lexer.Sub:
		return OpSub, true
	case lexer.Mul:
		return OpMul, true
	case lexer.Div:
		return OpDiv, true
	case lexer.Mod:
		return OpMod, true
	case lexer.Pow:
		return OpPow, true
	case lexer.CmpEq:
		return OpEq, true
	case lexer.NotEq:
		return OpNotEq, true
	case lexer.Gt:
		return OpGt, true
	case lexer.Gte:
		return OpGte, true
	case lexer.Lt:
		return OpLt, true
	case lexer.Lte:
		return OpLte, true
	default:
		return op, false
	}
}

func (p *parser) parseRangeAggregationExpr() (e *RangeAggregationExpr, _ error) {
	e = new(RangeAggregationExpr)

	opTok := p.next()
	switch opTok.Type {
	case lexer.CountOverTime:
		e.Op = RangeOpCount
	case lexer.Rate:
		e.Op = RangeOpRate
	case lexer.RateCounter:
		e.Op = RangeOpRateCounter
	case lexer.BytesOverTime:
		e.Op = RangeOpBytes
	case lexer.BytesRate:
		e.Op = RangeOpBytesRate
	case lexer.AvgOverTime:
		e.Op = RangeOpAvg
	case lexer.SumOverTime:
		e.Op = RangeOpSum
	case lexer.MinOverTime:
		e.Op = RangeOpMin
	case lexer.MaxOverTime:
		e.Op = RangeOpMax
	case lexer.StdvarOverTime:
		e.Op = RangeOpStdvar
	case lexer.StddevOverTime:
		e.Op = RangeOpStddev
	case lexer.QuantileOverTime:
		e.Op = RangeOpQuantile
	case lexer.FirstOverTime:
		e.Op = RangeOpFirst
	case lexer.LastOverTime:
		e.Op = RangeOpLast
	case lexer.AbsentOverTime:
		e.Op = RangeOpAbsent
	}

	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	if t := p.peek(); t.Type == lexer.Number {
		param, err := p.parseNumber()
		if err != nil {
			return nil, err
		}
		e.Parameter = &param

		if err := p.consume(lexer.Comma); err != nil {
			return nil, err
		}
	}

	expr, err := p.parseRangeExpr()
	if err != nil {
		return nil, err
	}
	e.Range = expr

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}

	switch t := p.peek(); t.Type {
	case lexer.By, lexer.Without:
		e.Grouping, err = p.parseGrouping()
		if err != nil {
			return nil, err
		}
	}

	if err := e.validate(); err != nil {
		return nil, &ParseError{
			Pos: opTok.Pos,
			Err: err,
		}
	}
	return e, nil
}

func (p *parser) parseVectorAggregationExpr() (e *VectorAggregationExpr, err error) {
	e = new(VectorAggregationExpr)
	opTok := p.next()
	switch opTok.Type {
	case lexer.Sum:
		e.Op = VectorOpSum
	case lexer.Avg:
		e.Op = VectorOpAvg
	case lexer.Count:
		e.Op = VectorOpCount
	case lexer.Max:
		e.Op = VectorOpMax
	case lexer.Min:
		e.Op = VectorOpMin
	case lexer.Stddev:
		e.Op = VectorOpStddev
	case lexer.Stdvar:
		e.Op = VectorOpStdvar
	case lexer.Bottomk:
		e.Op = VectorOpBottomk
	case lexer.Topk:
		e.Op = VectorOpTopk
	case lexer.Sort:
		e.Op = VectorOpSort
	case lexer.SortDesc:
		e.Op = VectorOpSortDesc
	}

	parseMetricExpr := func() error {
		if err := p.consume(lexer.OpenParen); err != nil {
			return err
		}

		if t := p.peek(); t.Type == lexer.Number {
			param, err := p.parseInt()
			if err != nil {
				return err
			}
			e.Parameter = &param

			if err := p.consume(lexer.Comma); err != nil {
				return err
			}
		}

		expr, err := p.parseMetricExpr()
		if err != nil {
			return err
		}
		e.Expr = expr

		return p.consume(lexer.CloseParen)
	}

	switch t := p.peek(); t.Type {
	case lexer.By, lexer.Without:
		e.Grouping, err = p.parseGrouping()
		if err != nil {
			return nil, err
		}

		if err := parseMetricExpr(); err != nil {
			return nil, err
		}
	case lexer.OpenParen:
		if err := parseMetricExpr(); err != nil {
			return nil, err
		}

		switch gt := p.peek(); gt.Type {
		case lexer.By, lexer.Without:
			e.Grouping, err = p.parseGrouping()
			if err != nil {
				return nil, err
			}
		}
	default:
		return nil, p.unexpectedToken(t)
	}

	if err := e.validate(); err != nil {
		return nil, &ParseError{
			Pos: opTok.Pos,
			Err: err,
		}
	}
	return e, nil
}

func (p *parser) parseLiteralExpr() (*LiteralExpr, error) {
	sign := float64(1)
	switch t := p.next(); t.Type {
	case lexer.Add:
		sign = 1
	case lexer.Sub:
		sign = -1
	case lexer.Number:
		p.unread()
	default:
		return nil, p.unexpectedToken(t)
	}

	f, err := p.parseNumber()
	if err != nil {
		return nil, err
	}
	return &LiteralExpr{Value: math.Copysign(f, sign)}, nil
}

// Parses PromQL function.
//
//	label_replace(v instant-vector, dst_label string, replacement string, src_label string, regex string)
func (p *parser) parseLabelReplace() (lr *LabelReplaceExpr, err error) {
	lr = new(LabelReplaceExpr)
	if err := p.consume(lexer.LabelReplace); err != nil {
		return nil, err
	}

	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	lr.Expr, err = p.parseMetricExpr()
	if err != nil {
		return nil, err
	}

	readParam := func(to *string) error {
		if err := p.consume(lexer.Comma); err != nil {
			return err
		}

		val, err := p.parseString()
		if err != nil {
			return err
		}
		*to = val
		return nil
	}

	if err := readParam(&lr.DstLabel); err != nil {
		return nil, err
	}
	if err := readParam(&lr.Replacement); err != nil {
		return nil, err
	}
	if err := readParam(&lr.SrcLabel); err != nil {
		return nil, err
	}
	regexTok := p.peek()
	if err := readParam(&lr.Regex); err != nil {
		return nil, err
	}
	lr.Re, err = compileLabelRegex(lr.Regex)
	if err != nil {
		return nil, &ParseError{
			Pos: regexTok.Pos,
			Err: err,
		}
	}

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}
	return lr, nil
}

func (p *parser) parseVectorExpr() (ve *VectorExpr, err error) {
	ve = new(VectorExpr)
	if err := p.consume(lexer.Vector); err != nil {
		return nil, err
	}

	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}
	ve.Value, err = p.parseNumber()
	if err != nil {
		return nil, err
	}
	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}
	return ve, nil
}

func (p *parser) parseGrouping() (g *Grouping, err error) {
	g = new(Grouping)
	switch t := p.next(); t.Type {
	case lexer.By:
	case lexer.Without:
		g.Without = true
	default:
		return nil, p.unexpectedToken(t)
	}

	g.Labels, err = p.parseLabels()
	return g, err
}

// parseLabels parses a possibly empty parenthesized list of comma-separated labels.
func (p *parser) parseLabels() (labels []Label, _ error) {
	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	// Empty grouping.
	if t := p.peek(); t.Type == lexer.CloseParen {
		p.next()
		return labels, nil
	}

	for {
		label, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		labels = append(labels, label)

		switch t := p.next(); t.Type {
		case lexer.CloseParen:
			return labels, nil
		case lexer.Comma:
		default:
			return nil, p.unexpectedToken(t)
		}
	}
}
