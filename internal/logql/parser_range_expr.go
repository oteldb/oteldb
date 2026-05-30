package logql

import "github.com/oteldb/oteldb/internal/logql/lexer"

func (p *parser) parseRangeExpr() (e LogRangeExpr, err error) {
	e.Sel, err = p.parseSelector()
	if err != nil {
		return e, err
	}

	parseRangeOffset := func() (err error) {
		if err := p.consume(lexer.OpenBracket); err != nil {
			return err
		}

		e.Range, err = p.parseDuration()
		if err != nil {
			return err
		}

		if err := p.consume(lexer.CloseBracket); err != nil {
			return err
		}

		if t := p.peek(); t.Type == lexer.Offset {
			// Consume "offset" token.
			p.next()

			offset, err := p.parseDuration()
			if err != nil {
				return err
			}
			e.Offset = &OffsetExpr{Duration: offset}
		}
		return nil
	}
	parsePipeline := func() error {
		e.Pipeline, err = p.parsePipeline(true)
		if err != nil {
			return err
		}
		if t := p.peek(); t.Type == lexer.Unwrap {
			e.Unwrap, err = p.parseUnwrapExpr()
			if err != nil {
				return err
			}
		}
		return nil
	}

	switch t := p.peek(); t.Type {
	case lexer.OpenBracket: // selector RANGE offsetExpr? pipeline...
		if err := parseRangeOffset(); err != nil {
			return e, err
		}
		if err := parsePipeline(); err != nil {
			return e, err
		}
	case lexer.Pipe, lexer.PipeExact, lexer.PipeMatch, lexer.NotEq, lexer.NotRe: // selector pipeline... RANGE offsetExpr?
		if err := parsePipeline(); err != nil {
			return e, err
		}
		if err := parseRangeOffset(); err != nil {
			return e, err
		}
	default:
		return e, p.unexpectedToken(t)
	}
	return e, nil
}

func (p *parser) parseUnwrapExpr() (ue *UnwrapExpr, err error) {
	ue = new(UnwrapExpr)
	if err := p.consume(lexer.Unwrap); err != nil {
		return nil, err
	}

	switch t := p.peek(); t.Type {
	case lexer.Ident:
		ue.Label, err = p.parseIdent()
		if err != nil {
			return nil, err
		}
	case lexer.BytesConv, lexer.DurationConv, lexer.DurationSecondsConv:
		p.next()

		if err := p.consume(lexer.OpenParen); err != nil {
			return nil, err
		}

		// TODO(tdakkota): parse it properly
		ue.Op = t.Text
		ue.Label, err = p.parseIdent()
		if err != nil {
			return nil, err
		}

		if err := p.consume(lexer.CloseParen); err != nil {
			return nil, err
		}
	default:
		return nil, p.unexpectedToken(t)
	}

	for {
		if t := p.peek(); t.Type != lexer.Pipe {
			return ue, nil
		}
		// Consume "|".
		p.next()

		lm, err := p.parseLabelMatcher()
		if err != nil {
			return nil, err
		}
		ue.Filters = append(ue.Filters, lm)
	}
}
