package traceql

import (
	"fmt"
	"strconv"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/lexerql"
	"github.com/oteldb/oteldb/internal/traceql/lexer"
)

// Parse parses TraceQL query from string.
func Parse(input string) (Expr, error) {
	p, err := newParser(input)
	if err != nil {
		return nil, err
	}
	return p.parseExpr()
}

func newParser(input string) (parser, error) {
	tokens, err := lexer.Tokenize(input, lexer.TokenizeOptions{})
	if err != nil {
		return parser{}, errors.Wrap(err, "tokenize")
	}
	return parser{
		tokens: tokens,
	}, nil
}

type parser struct {
	tokens []lexer.Token
	pos    int

	first  bool
	parens int
}

func (p *parser) consume(tt lexer.TokenType) error {
	if t := p.next(); t.Type != tt {
		return errors.Wrapf(p.unexpectedToken(t), "expected %q", tt)
	}
	return nil
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

func (p *parser) unread() {
	if p.pos > 0 {
		p.pos--
	}
}

func (p *parser) unexpectedToken(t lexer.Token) error {
	if t.Type == lexer.EOF {
		return &SyntaxError{
			Msg: "unexpected EOF",
			Pos: t.Pos,
		}
	}
	return &SyntaxError{
		Msg: fmt.Sprintf("unexpected token %q", t.Type),
		Pos: t.Pos,
	}
}

func (p *parser) consumeText(expect lexer.TokenType) (string, error) {
	t := p.next()
	if t.Type != expect {
		return "", &SyntaxError{
			Msg: fmt.Sprintf("expected %q, got %q", expect, t.Type),
			Pos: t.Pos,
		}
	}
	return t.Text, nil
}

func (p *parser) parseInteger() (int64, error) {
	text, err := p.consumeText(lexer.Integer)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(text, 0, 64)
}

func (p *parser) parseNumber() (float64, error) {
	text, err := p.consumeText(lexer.Number)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(text, 64)
}

func (p *parser) parseDuration() (time.Duration, error) {
	text, err := p.consumeText(lexer.Duration)
	if err != nil {
		return 0, err
	}
	return lexerql.ParseDuration(text)
}
