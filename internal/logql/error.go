package logql

import (
	"fmt"
	"text/scanner"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/logql/lexer"
)

// ParseError is a LogQL query parsing error.
type ParseError struct {
	Pos scanner.Position
	Err error
}

// Error implements error.
func (e *ParseError) Error() string {
	return fmt.Sprintf("at %s: %s", e.Pos, e.Err)
}

// Unwrap implements [errors.Unwrap] interface.
func (e *ParseError) Unwrap() error {
	return e.Err
}

// FormatError implements [errors.Formatter].
func (e *ParseError) FormatError(p errors.Printer) error {
	p.Printf("at %s", e.Pos)
	return e.Err
}

func (p *parser) tailToken(t lexer.Token) error {
	return &ParseError{
		Pos: t.Pos,
		Err: errors.Errorf("unexpected tail token %q", t.Type),
	}
}

func (p *parser) unexpectedToken(t lexer.Token) error {
	if t.Type == lexer.EOF {
		return &ParseError{
			Pos: t.Pos,
			Err: errors.New("unexpected EOF token"),
		}
	}
	return &ParseError{
		Pos: t.Pos,
		Err: errors.Errorf("unexpected token %q", t.Type),
	}
}
