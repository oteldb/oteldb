package logql

import (
	"regexp"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/logql/lexer"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlpattern"
)

func (p *parser) parsePipeline(allowUnwrap bool) (stages []PipelineStage, err error) {
	for {
		switch t := p.peek(); t.Type {
		case lexer.PipeExact, lexer.PipeMatch, lexer.NotEq, lexer.NotRe, lexer.PipePattern, lexer.NotPipePattern: // ( "|=" | "|~" | "!=" | "!~" | "|>" | "!>")
			lf, err := p.parseLineFilters()
			if err != nil {
				return stages, err
			}
			stages = append(stages, lf)
		case lexer.Pipe: // "|"
			p.next()

			switch t := p.next(); t.Type {
			case lexer.JSON:
				labels, exprs, err := p.parseLabelExtraction()
				if err != nil {
					return stages, err
				}
				stages = append(stages, &JSONExpressionParser{Labels: labels, Exprs: exprs})
			case lexer.Logfmt:
				flags, err := p.parseLogfmtFlags()
				if err != nil {
					return stages, err
				}

				labels, exprs, err := p.parseLabelExtraction()
				if err != nil {
					return stages, err
				}
				stages = append(stages, &LogfmtExpressionParser{
					Labels: labels,
					Exprs:  exprs,
					Flags:  flags,
				})
			case lexer.Regexp:
				p, err := p.parseRegexpLabelParser()
				if err != nil {
					return stages, err
				}
				stages = append(stages, p)
			case lexer.Pattern:
				pattern, patternTok, err := p.consumeText(lexer.String)
				if err != nil {
					return stages, err
				}
				compiled, err := logqlpattern.Parse(pattern, logqlpattern.ExtractorFlags)
				if err != nil {
					return nil, &ParseError{
						Pos: patternTok.Pos,
						Err: errors.Wrap(err, "pattern"),
					}
				}
				stages = append(stages, &PatternLabelParser{Pattern: compiled})
			case lexer.Unpack:
				stages = append(stages, &UnpackLabelParser{})
			case lexer.LineFormat:
				tmpl, err := p.parseString()
				if err != nil {
					return stages, err
				}
				// FIXME(tdakkota): parse template?
				stages = append(stages, &LineFormat{Template: tmpl})
			case lexer.Decolorize:
				stages = append(stages, &DecolorizeExpr{})
			case lexer.Ident, lexer.OpenParen:
				p.unread()

				pred, err := p.parseLabelPredicate()
				if err != nil {
					return stages, err
				}
				stages = append(stages, &LabelFilter{Pred: pred})
			case lexer.LabelFormat:
				lf, err := p.parseLabelFormatExpr()
				if err != nil {
					return stages, err
				}
				stages = append(stages, lf)
			case lexer.Keep:
				keep, err := p.parseKeepLabelsExpr()
				if err != nil {
					return stages, err
				}
				stages = append(stages, keep)
			case lexer.Drop:
				drop, err := p.parseDropLabelsExpr()
				if err != nil {
					return stages, err
				}
				stages = append(stages, drop)
			case lexer.Distinct:
				distinct, err := p.parseDistinctFilter()
				if err != nil {
					return stages, err
				}
				stages = append(stages, distinct)
			case lexer.Unwrap:
				// Only for metricExpr.
				// Allow caller parse it afterwards.
				if allowUnwrap {
					p.unread()
					return stages, nil
				}
				// Fail otherwise.
				fallthrough
			default:
				// TODO(tdakkota): parse other filters
				return stages, p.unexpectedToken(t)
			}
		default:
			return stages, nil
		}
	}
}

func (p *parser) parseLineFilters() (f *LineFilter, err error) {
	t := p.next()

	f = new(LineFilter)
	switch t.Type {
	case lexer.PipeExact: // "|="
		f.Op = OpEq
	case lexer.PipeMatch: // "|~"
		f.Op = OpRe
	case lexer.NotEq: // "!="
		f.Op = OpNotEq
	case lexer.NotRe: // "!~"
		f.Op = OpNotRe
	case lexer.PipePattern: // "|>"
		f.Op = OpPattern
	case lexer.NotPipePattern: // "!>"
		f.Op = OpNotPattern
	default:
		return f, p.unexpectedToken(t)
	}

	f.By, err = p.parseLineFilterValue(f.Op)
	if err != nil {
		return nil, err
	}

	for {
		if t := p.peek(); t.Type != lexer.Or {
			return f, nil
		}
		p.next()

		sub, err := p.parseLineFilterValue(f.Op)
		if err != nil {
			return nil, err
		}
		f.Or = append(f.Or, sub)
	}
}

func (p *parser) parseLineFilterValue(op BinOp) (f LineFilterValue, err error) {
	switch t := p.peek(); t.Type {
	case lexer.String:
		f.Value, err = p.parseString()
		if err != nil {
			return f, err
		}

		// TODO(tdakkota): validate pattern too?
		// 	pattern parser is a part of engine for now
		switch op {
		case OpRe, OpNotRe:
			f.Re, err = regexp.Compile(f.Value)
			if err != nil {
				return f, &ParseError{
					Pos: t.Pos,
					Err: err,
				}
			}
		}
	case lexer.IP:
		p.next()

		switch op {
		case OpEq, OpNotEq:
		default:
			return f, &ParseError{
				Pos: t.Pos,
				Err: errors.Errorf("invalid IP line filter operation %q", op),
			}
		}

		if err := p.consume(lexer.OpenParen); err != nil {
			return f, err
		}

		f.Value, err = p.parseString()
		if err != nil {
			return f, err
		}
		f.IP = true

		if err := p.consume(lexer.CloseParen); err != nil {
			return f, err
		}
	default:
		return f, p.unexpectedToken(t)
	}
	return f, nil
}

func (p *parser) parseLogfmtFlags() (flags LogfmtFlags, _ error) {
	for {
		t := p.peek()
		if t.Type != lexer.ParserFlag {
			return flags, nil
		}
		switch t.Text {
		case "--strict":
			flags.Set(LogfmtFlagStrict)
		case "--keep-empty":
			flags.Set(LogfmtFlagKeepEmpty)
		default:
			return flags, &ParseError{
				Pos: t.Pos,
				Err: errors.Errorf("unknown parser flag %q", t.Text),
			}
		}
		p.next()
	}
}

func (p *parser) parseLabelExtraction() (labels []Label, exprs []LabelExtractionExpr, err error) {
	for {
		if t := p.peek(); t.Type != lexer.Ident {
			return labels, exprs, nil
		}

		label, err := p.parseIdent()
		if err != nil {
			return labels, exprs, err
		}

		switch t := p.peek(); t.Type {
		case lexer.Comma:
			labels = append(labels, label)
		case lexer.Eq:
			p.next()

			expr, err := p.parseString()
			if err != nil {
				return labels, exprs, err
			}
			exprs = append(exprs, LabelExtractionExpr{
				Label: label,
				Expr:  expr,
			})

			if t := p.peek(); t.Type != lexer.Comma {
				continue
			}
		default:
			labels = append(labels, label)
			continue
		}

		// Consume comma.
		p.next()
		// Expect a label after that.
		if t := p.peek(); t.Type != lexer.Ident {
			return labels, exprs, p.unexpectedToken(t)
		}
	}
}

func (p *parser) parseRegexpLabelParser() (*RegexpLabelParser, error) {
	pattern, patternTok, err := p.consumeText(lexer.String)
	if err != nil {
		return nil, err
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, &ParseError{
			Pos: patternTok.Pos,
			Err: err,
		}
	}
	if re.NumSubexp() == 0 {
		return nil, &ParseError{
			Pos: patternTok.Pos,
			Err: errors.New("at least one capture expected"),
		}
	}

	var (
		captures = re.SubexpNames()
		mapping  = make(map[int]Label, len(captures))
		unique   = make(map[string]struct{}, len(captures))
	)

	for i, name := range captures {
		// Not capturing.
		if name == "" {
			continue
		}

		if _, ok := unique[name]; ok {
			return nil, &ParseError{
				Pos: patternTok.Pos,
				Err: errors.Errorf("duplicate capture %q", name),
			}
		}
		unique[name] = struct{}{}

		if err := IsValidLabel(name, p.allowDots); err != nil {
			return nil, &ParseError{
				Pos: patternTok.Pos,
				Err: errors.Errorf("invalid label name %q", name),
			}
		}
		mapping[i] = Label(name)
	}
	if len(mapping) == 0 {
		return nil, &ParseError{
			Pos: patternTok.Pos,
			Err: errors.New("at least one capture expected"),
		}
	}

	return &RegexpLabelParser{
		Regexp:  re,
		Mapping: mapping,
	}, nil
}

func (p *parser) parseLabelPredicate() (pred LabelPredicate, _ error) {
	expr, err := p.parseLabelPredicate1()
	if err != nil {
		return nil, err
	}
	return p.parseLabelPredicateBinOp(expr)
}

func (p *parser) parseLabelPredicate1() (LabelPredicate, error) {
	switch t := p.next(); t.Type {
	case lexer.OpenParen:
		lp, err := p.parseLabelPredicate()
		if err != nil {
			return nil, err
		}
		if err := p.consume(lexer.CloseParen); err != nil {
			return nil, err
		}

		return &LabelPredicateParen{X: lp}, nil
	case lexer.Ident:
		var op BinOp

		opTok := p.next()
		switch opTok.Type {
		case lexer.Eq:
			op = OpEq
		case lexer.CmpEq:
			op = OpEq
		case lexer.NotEq:
			op = OpNotEq
		case lexer.Re:
			op = OpRe
		case lexer.NotRe:
			op = OpNotRe
		case lexer.Gt:
			op = OpGt
		case lexer.Gte:
			op = OpGte
		case lexer.Lt:
			op = OpLt
		case lexer.Lte:
			op = OpLte
		default:
			return nil, p.unexpectedToken(opTok)
		}

		switch literalTok := p.peek(); literalTok.Type {
		case lexer.String:
			switch opTok.Type {
			case lexer.Eq, lexer.NotEq, lexer.Re, lexer.NotRe:
			default:
				return nil, &ParseError{
					Pos: opTok.Pos,
					Err: errors.Errorf("invalid string operator %q", opTok.Type),
				}
			}

			v, err := p.parseString()
			if err != nil {
				return nil, err
			}

			var re *regexp.Regexp
			switch op {
			case OpRe, OpNotRe:
				re, err = compileLabelRegex(v)
				if err != nil {
					return nil, &ParseError{
						Pos: literalTok.Pos,
						Err: err,
					}
				}
			}
			return &LabelMatcher{Label: Label(t.Text), Op: op, Value: v, Re: re}, nil
		case lexer.Number:
			switch opTok.Type {
			case lexer.CmpEq, lexer.NotEq, lexer.Lt, lexer.Lte, lexer.Gt, lexer.Gte:
			default:
				return nil, &ParseError{
					Pos: opTok.Pos,
					Err: errors.Errorf("invalid number operator %q", opTok.Type),
				}
			}

			v, err := p.parseNumber()
			if err != nil {
				return nil, err
			}
			return &NumberFilter{Label: Label(t.Text), Op: op, Value: v}, nil
		case lexer.Duration:
			switch opTok.Type {
			case lexer.CmpEq, lexer.NotEq, lexer.Lt, lexer.Lte, lexer.Gt, lexer.Gte:
			default:
				return nil, &ParseError{
					Pos: opTok.Pos,
					Err: errors.Errorf("invalid duration operator %q", opTok.Type),
				}
			}

			d, err := p.parseDuration()
			if err != nil {
				return nil, err
			}
			return &DurationFilter{Label: Label(t.Text), Op: op, Value: d}, nil
		case lexer.Bytes:
			switch opTok.Type {
			case lexer.CmpEq, lexer.NotEq, lexer.Lt, lexer.Lte, lexer.Gt, lexer.Gte:
			default:
				return nil, &ParseError{
					Pos: opTok.Pos,
					Err: errors.Errorf("invalid bytes operator %q", opTok.Type),
				}
			}

			b, err := p.parseBytes()
			if err != nil {
				return nil, err
			}
			return &BytesFilter{Label: Label(t.Text), Op: op, Value: b}, nil
		case lexer.IP:
			switch opTok.Type {
			case lexer.Eq, lexer.NotEq:
			default:
				return nil, &ParseError{
					Pos: opTok.Pos,
					Err: errors.Errorf("invalid IP operator %q", opTok.Type),
				}
			}
			// Read "ip" token.
			p.next()

			if err := p.consume(lexer.OpenParen); err != nil {
				return nil, err
			}

			ipPattern, err := p.parseString()
			if err != nil {
				return nil, err
			}

			if err := p.consume(lexer.CloseParen); err != nil {
				return nil, err
			}

			return &IPFilter{Label: Label(t.Text), Op: op, Value: ipPattern}, nil
		default:
			return nil, p.unexpectedToken(literalTok)
		}
	default:
		return nil, p.unexpectedToken(t)
	}
}

func (p *parser) parseLabelPredicateBinOp(left LabelPredicate) (LabelPredicate, error) {
	for {
		var op BinOp
		switch tok := p.peek(); tok.Type {
		case lexer.Ident:
			op = OpAnd
		case lexer.Comma, lexer.And:
			p.next()
			op = OpAnd
		case lexer.Or:
			p.next()
			op = OpOr
		case lexer.EOF:
			p.next()
			return left, nil
		default:
			return left, nil
		}
		// Note: OpAnd and OpOr have equal precedence, unlike metric binary operations.

		right, err := p.parseLabelPredicate1()
		if err != nil {
			return nil, err
		}

		left = &LabelPredicateBinOp{Left: left, Op: op, Right: right}
	}
}

func (p *parser) parseLabelFormatExpr() (lf *LabelFormatExpr, err error) {
	lf = new(LabelFormatExpr)

	labels := map[Label]struct{}{}
	for {
		value, token, err := p.consumeText(lexer.Ident)
		if err != nil {
			return nil, err
		}
		label := Label(value)

		if _, ok := labels[label]; ok {
			return nil, &ParseError{
				Pos: token.Pos,
				Err: errors.Errorf("label %q can be formatted only once per stage", label),
			}
		}
		labels[label] = struct{}{}

		if err := p.consume(lexer.Eq); err != nil {
			return nil, err
		}

		switch t := p.peek(); t.Type {
		case lexer.Ident:
			value, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			lf.Labels = append(lf.Labels, RenameLabel{To: label, From: value})
		case lexer.String:
			value, err := p.parseString()
			if err != nil {
				return nil, err
			}
			lf.Values = append(lf.Values, LabelTemplate{Label: label, Template: value})
		default:
			return nil, p.unexpectedToken(t)
		}

		if t := p.peek(); t.Type != lexer.Comma {
			return lf, nil
		}
		p.next()
	}
}

func (p *parser) parseKeepLabelsExpr() (*KeepLabelsExpr, error) {
	labels, matchers, err := p.parseLabelsAndMatchers()
	if err != nil {
		return nil, err
	}
	return &KeepLabelsExpr{Labels: labels, Matchers: matchers}, nil
}

func (p *parser) parseDropLabelsExpr() (*DropLabelsExpr, error) {
	labels, matchers, err := p.parseLabelsAndMatchers()
	if err != nil {
		return nil, err
	}
	return &DropLabelsExpr{Labels: labels, Matchers: matchers}, nil
}

func (p *parser) parseLabelsAndMatchers() (labels []Label, matchers []LabelMatcher, _ error) {
	for {
		if err := p.consume(lexer.Ident); err != nil {
			return labels, matchers, err
		}

		switch t := p.peek(); t.Type {
		case lexer.Eq, lexer.NotEq, lexer.Re, lexer.NotRe:
			p.unread()

			m, err := p.parseLabelMatcher()
			if err != nil {
				return labels, matchers, err
			}
			matchers = append(matchers, m)
		default:
			p.unread()

			label, err := p.parseIdent()
			if err != nil {
				return labels, matchers, err
			}
			labels = append(labels, label)
		}

		if t := p.peek(); t.Type != lexer.Comma {
			return labels, matchers, nil
		}
		// Consume comma.
		p.next()
	}
}

func (p *parser) parseDistinctFilter() (df *DistinctFilter, _ error) {
	df = new(DistinctFilter)
	for {
		label, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		df.Labels = append(df.Labels, label)

		if t := p.peek(); t.Type != lexer.Comma {
			return df, nil
		}
		// Consume comma.
		p.next()
	}
}
