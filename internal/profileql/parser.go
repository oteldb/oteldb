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

// Selector is the result of parsing a label selector with an optional profile
// type, as produced by [ParseSelector].
type Selector struct {
	// Type is the selected profile type, or nil if the selector did not name one.
	Type *ProfileType
	// Matchers are the label matchers, excluding the profile-type selection.
	Matchers []LabelMatcher
}

// ParseSelector parses an optional leading profile-type selector followed by an
// optional brace-enclosed matcher list, like [Parse], but the profile type is
// optional. It is used where the profile type and label selector are supplied
// separately (for example by the connect QuerierService), so a bare matcher list
// such as `{service.name="frontend"}` is valid.
func ParseSelector(input string) (Selector, error) {
	p, err := newParser(input)
	if err != nil {
		return Selector{}, err
	}
	matchers, nameValue, nameTok, haveName, err := p.parseSelector()
	if err != nil {
		return Selector{}, err
	}
	if t := p.next(); t.Type != lexer.EOF {
		return Selector{}, p.tailToken(t)
	}
	sel := Selector{Matchers: matchers}
	if haveName {
		pt, err := ParseProfileType(nameValue)
		if err != nil {
			return Selector{}, &ParseError{Pos: nameTok.Pos, Err: err}
		}
		sel.Type = &pt
	}
	return sel, nil
}

// parseExpr parses a full ProfileQL expression: an optional leading profile-type
// selector followed by an optional brace-enclosed matcher list. The profile type
// may be given either as the leading bare token or via a __name__ matcher, and is
// required.
func (p *parser) parseExpr() (Expr, error) {
	matchers, nameValue, nameTok, haveName, err := p.parseSelector()
	if err != nil {
		return Expr{}, err
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
	return Expr{Type: pt, Matchers: matchers}, nil
}

// parseSelector parses the shared shape of [Parse] and [ParseSelector]: an
// optional leading profile-type token and an optional matcher block, returning
// the non-name matchers and the profile-type name (from the leading token or a
// __name__ matcher) if one was present.
func (p *parser) parseSelector() (matchers []LabelMatcher, nameValue string, nameTok lexer.Token, haveName bool, _ error) {
	// Optional leading profile-type selector (the metric name).
	if t := p.peek(); t.Type == lexer.Ident {
		nameValue = t.Text
		nameTok = t
		haveName = true
		p.next()
	}

	// Optional matcher block.
	if t := p.peek(); t.Type == lexer.OpenBrace {
		ms, err := p.parseMatchers()
		if err != nil {
			return nil, "", lexer.Token{}, false, err
		}
		for _, m := range ms {
			if m.Label == LabelName {
				if m.Op != OpEq {
					return nil, "", lexer.Token{}, false, &ParseError{
						Pos: nameTok.Pos,
						Err: errors.Errorf("profile-type matcher %q must use %q", LabelName, OpEq),
					}
				}
				nameValue = m.Value
				haveName = true
				continue
			}
			matchers = append(matchers, m)
		}
	}
	return matchers, nameValue, nameTok, haveName, nil
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
