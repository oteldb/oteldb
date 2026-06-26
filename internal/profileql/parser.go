package profileql

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/profileql/lexer"
)

// Parse parses a ProfileQL query from string.
func Parse(input string) (Expr, error) {
	p, err := newParser(input)
	if err != nil {
		return Expr{}, err
	}
	expr, err := p.parseExpr()
	if err != nil {
		return Expr{}, err
	}
	if t := p.next(); t.Type != lexer.EOF {
		return Expr{}, p.tailToken(t)
	}
	return expr, nil
}

func newParser(input string) (parser, error) {
	tokens, err := lexer.Tokenize(input, lexer.TokenizeOptions{})
	if err != nil {
		return parser{}, errors.Wrap(err, "tokenize")
	}
	return parser{tokens: tokens}, nil
}

type parser struct {
	tokens []lexer.Token
	pos    int
}

func (p *parser) next() lexer.Token {
	t := p.peek()
	if t.Type != lexer.EOF {
		p.pos++
	}
	return t
}

func (p *parser) peek() lexer.Token {
	if len(p.tokens) <= p.pos {
		return lexer.Token{Type: lexer.EOF}
	}
	return p.tokens[p.pos]
}

func (p *parser) consumeText(tt lexer.TokenType) (string, lexer.Token, error) {
	t := p.next()
	if t.Type != tt {
		return "", t, &ParseError{
			Pos: t.Pos,
			Err: errors.Errorf("expected %q, got %q", tt, t.Type),
		}
	}
	return t.Text, t, nil
}

// parseExpr parses a full ProfileQL expression: an optional leading profile-type
// selector followed by an optional brace-enclosed matcher list. The profile type
// may be given either as the leading bare token or via a __name__ matcher.
func (p *parser) parseExpr() (Expr, error) {
	var (
		e         Expr
		nameValue string
		nameTok   lexer.Token
		haveName  bool
	)

	// Optional leading profile-type selector (the metric name).
	if t := p.peek(); t.Type == lexer.Ident {
		nameValue = t.Text
		nameTok = t
		haveName = true
		p.next()
	}

	// Optional matcher block.
	if t := p.peek(); t.Type == lexer.OpenBrace {
		matchers, err := p.parseMatchers()
		if err != nil {
			return Expr{}, err
		}
		for _, m := range matchers {
			if m.Label == LabelName {
				if m.Op != OpEq {
					return Expr{}, &ParseError{
						Pos: nameTok.Pos,
						Err: errors.Errorf("profile-type matcher %q must use %q", LabelName, OpEq),
					}
				}
				nameValue = m.Value
				haveName = true
				continue
			}
			e.Matchers = append(e.Matchers, m)
		}
	}

	if !haveName {
		return Expr{}, &ParseError{
			Pos: p.peek().Pos,
			Err: errors.New("query must contain a profile-type selection"),
		}
	}

	pt, err := ParseProfileType(nameValue)
	if err != nil {
		return Expr{}, &ParseError{
			Pos: nameTok.Pos,
			Err: err,
		}
	}
	e.Type = pt
	return e, nil
}

// parseMatchers parses a brace-enclosed, comma-separated list of label matchers.
func (p *parser) parseMatchers() (matchers []LabelMatcher, _ error) {
	if t := p.next(); t.Type != lexer.OpenBrace {
		return nil, p.unexpectedToken(t)
	}

	// Empty matcher list.
	if t := p.peek(); t.Type == lexer.CloseBrace {
		p.next()
		return matchers, nil
	}

	for {
		m, err := p.parseLabelMatcher()
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, m)

		switch t := p.next(); t.Type {
		case lexer.CloseBrace:
			return matchers, nil
		case lexer.Comma:
			// Allow a trailing comma before the closing brace.
			if t := p.peek(); t.Type == lexer.CloseBrace {
				p.next()
				return matchers, nil
			}
		default:
			return nil, p.unexpectedToken(t)
		}
	}
}

func (p *parser) parseLabelMatcher() (m LabelMatcher, err error) {
	var labelTok lexer.Token
	switch t := p.peek(); t.Type {
	case lexer.Ident:
		s, tok, err := p.consumeText(lexer.Ident)
		if err != nil {
			return m, err
		}
		m.Label, labelTok = Label(s), tok
	case lexer.String:
		s, tok, err := p.consumeText(lexer.String)
		if err != nil {
			return m, err
		}
		m.Label, labelTok = Label(s), tok
	default:
		return m, p.unexpectedToken(t)
	}
	if err := IsValidLabel(m.Label); err != nil {
		return m, &ParseError{Pos: labelTok.Pos, Err: err}
	}

	switch t := p.next(); t.Type {
	case lexer.Eq:
		m.Op = OpEq
	case lexer.NotEq:
		m.Op = OpNotEq
	case lexer.Re:
		m.Op = OpRe
	case lexer.NotRe:
		m.Op = OpNotRe
	default:
		return m, p.unexpectedToken(t)
	}

	value, valueTok, err := p.consumeText(lexer.String)
	if err != nil {
		return m, err
	}
	m.Value = value

	if m.Op.IsRegex() {
		m.Re, err = compileLabelRegex(m.Value)
		if err != nil {
			return m, &ParseError{
				Pos: valueTok.Pos,
				Err: err,
			}
		}
	}
	return m, nil
}
