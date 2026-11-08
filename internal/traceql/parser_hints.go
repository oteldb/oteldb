package traceql

import (
	"fmt"
	"unicode"

	"github.com/oteldb/oteldb/internal/traceql/lexer"
)

func (p *parser) parseQueryHints(expr Expr) (Expr, error) {
	var hints []Hint
	for p.peek().Type == lexer.With {
		p.next()
		parsed, err := p.parseHints()
		if err != nil {
			return nil, err
		}
		// Tempo replaces earlier hint blocks with the last block.
		hints = parsed
	}
	if len(hints) == 0 {
		return expr, nil
	}
	return &HintedExpr{Expr: expr, Hints: hints}, nil
}

func (p *parser) parseHints() ([]Hint, error) {
	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}
	var hints []Hint
	for {
		name := p.next()
		if name.Type != lexer.Ident || !isHintName(name.Text) {
			return nil, &SyntaxError{
				Msg: fmt.Sprintf("expected query hint name, got %q", name.Text),
				Pos: name.Pos,
			}
		}
		if err := p.consume(lexer.Eq); err != nil {
			return nil, err
		}
		value, err := p.parseStatic()
		if err != nil {
			return nil, err
		}
		hints = append(hints, Hint{Name: name.Text, Value: *value})
		if p.peek().Type != lexer.Comma {
			break
		}
		p.next()
	}
	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}
	return hints, nil
}

func isHintName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if r != '_' && !unicode.IsLetter(r) && !(i > 0 && unicode.IsDigit(r)) {
			return false
		}
	}
	return true
}
