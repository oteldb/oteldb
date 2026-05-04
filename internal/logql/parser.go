package logql

import (
	"fmt"
	"iter"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/lexerql"
	"github.com/go-faster/oteldb/internal/logql/lexer"
)

// ParseOptions is LogQL parser options.
type ParseOptions struct {
	// AllowDots allows dots in identifiers.
	AllowDots bool
}

// Parse parses LogQL query from string.
func Parse(s string, opts ParseOptions) (Expr, error) {
	tokens, err := lexer.Tokenize(s, lexer.TokenizeOptions{AllowDots: opts.AllowDots})
	if err != nil {
		return nil, err
	}
	p := parser{
		tokens:    tokens,
		allowDots: opts.AllowDots,
	}

	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if t := p.next(); t.Type != lexer.EOF {
		return nil, p.tailToken(t)
	}
	return expr, nil
}

// ExtractSelectors returns the selectors extracted from the given expression.
func ExtractSelectors(s string, opts ParseOptions) (iter.Seq[Selector], error) {
	expr, err := Parse(s, opts)
	if err != nil {
		return nil, err
	}

	seq := func(yield func(Selector) bool) {
		var walk func(Expr) bool
		walk = func(e Expr) bool {
			switch e := e.(type) {
			case *ParenExpr:
				return walk(e.X)
			case *ExplainExpr:
				return walk(e.X)
			case *LogExpr:
				return yield(e.Sel)
			case *RangeAggregationExpr:
				return yield(e.Range.Sel)
			case *VectorAggregationExpr:
				return walk(e.Expr)
			case *LiteralExpr:
				return true
			case *LabelReplaceExpr:
				return walk(e.Expr)
			case *VectorExpr:
				return true
			case *BinOpExpr:
				if !walk(e.Left) {
					return false
				}
				return walk(e.Right)
			default:
				panic(fmt.Sprintf("unexpected type %T", e))
			}
		}
		walk(expr)
	}
	return seq, nil
}

// ParseSelector parses label selector from string.
func ParseSelector(s string, opts ParseOptions) (sel Selector, _ error) {
	tokens, err := lexer.Tokenize(s, lexer.TokenizeOptions{AllowDots: opts.AllowDots})
	if err != nil {
		return sel, err
	}
	p := parser{
		tokens:    tokens,
		allowDots: opts.AllowDots,
	}

	sel, err = p.parseSelector()
	if err != nil {
		return sel, err
	}
	if t := p.next(); t.Type != lexer.EOF {
		return sel, p.tailToken(t)
	}
	return sel, nil
}

type parser struct {
	tokens []lexer.Token
	pos    int

	allowDots bool
}

func (p *parser) consume(tt lexer.TokenType) error {
	if t := p.next(); t.Type != tt {
		return p.unexpectedToken(t)
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

func (p *parser) parseIdent() (Label, error) {
	s, _, err := p.consumeText(lexer.Ident)
	return Label(s), err
}

func (p *parser) parseString() (string, error) {
	s, _, err := p.consumeText(lexer.String)
	return s, err
}

func (p *parser) parseNumber() (float64, error) {
	text, _, err := p.consumeText(lexer.Number)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(text, 64)
}

func (p *parser) parseInt() (int, error) {
	text, _, err := p.consumeText(lexer.Number)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(text)
}

func (p *parser) parseDuration() (time.Duration, error) {
	text, _, err := p.consumeText(lexer.Duration)
	if err != nil {
		return 0, err
	}
	return lexerql.ParseDuration(text)
}

func (p *parser) parseBytes() (uint64, error) {
	text, _, err := p.consumeText(lexer.Bytes)
	if err != nil {
		return 0, err
	}
	return humanize.ParseBytes(text)
}
