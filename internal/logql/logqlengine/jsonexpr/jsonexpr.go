// Package jsonexpr provides JSON extractor expression parser.
package jsonexpr

import (
	"io"
	"strconv"
	"text/scanner"
	"unicode/utf8"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/lexerql"
)

// Path is a list of selectors.
type Path []Selector

// SelectorType is a [Selector] type.
type SelectorType int

const (
	Index = iota + 1
	Key
)

// Selector is a JSON field/element selector.
type Selector struct {
	Type  SelectorType
	Index int
	Key   string
}

// IndexSel creates new index Selector.
func IndexSel(i int) Selector {
	return Selector{Type: Index, Index: i}
}

// KeySel creates new key Selector.
func KeySel(k string) Selector {
	return Selector{Type: Key, Key: k}
}

// Parse parses selector expression.
func Parse(input string) (sel Path, _ error) {
	r := &reader{
		input: input,
	}

	for {
		switch ch := r.Peek(); {
		case ch == '.':
			r.Read()
			fallthrough
		case lexerql.IsIdentStartRune(ch):
			field, err := r.scanField()
			if err != nil {
				return sel, errors.Wrap(err, "scan field")
			}
			sel = append(sel, KeySel(field))
		case ch == '[':
			r.Read()

			switch indexCh := r.Peek(); {
			case lexerql.IsDigit(indexCh):
				index, err := r.scanInteger()
				if err != nil {
					return sel, errors.Wrap(err, "scan index")
				}
				sel = append(sel, IndexSel(index))
			case indexCh == '"':
				key, err := r.scanString()
				if err != nil {
					return sel, errors.Wrap(err, "scan key")
				}
				sel = append(sel, KeySel(key))
			case indexCh == scanner.EOF:
				return sel, io.ErrUnexpectedEOF
			default:
				return sel, errors.Errorf("expected number or string, got %q", indexCh)
			}

			if br := r.Read(); br != ']' {
				return sel, errors.Errorf("expected ']', got %q", br)
			}
		case ch == scanner.EOF:
			return sel, io.ErrUnexpectedEOF
		default:
			return sel, errors.Errorf("expected identifier or '[', got %q", ch)
		}

		if ch := r.Peek(); ch == scanner.EOF {
			return sel, nil
		}
	}
}

type reader struct {
	input string
	pos   int
}

func (r *reader) scanField() (string, error) {
	input := r.input[r.pos:]

	for i, c := range []byte(input) {
		if !lexerql.IsIdentRune(c) {
			input = input[:i]
			break
		}
	}

	r.pos += len(input)
	if r, _ := utf8.DecodeRuneInString(input); !lexerql.IsIdentStartRune(r) {
		return "", errors.Errorf("field must start with letter or underscore, got %q", r)
	}
	return input, nil
}

func (r *reader) scanInteger() (int, error) {
	input := r.input[r.pos:]

	for i, c := range []byte(input) {
		if !lexerql.IsDigit(c) {
			input = input[:i]
			break
		}
	}

	r.pos += len(input)
	return strconv.Atoi(input)
}

func (r *reader) scanString() (string, error) {
	input := r.input[r.pos:]

scanLoop:
	for i := 1; i < len(input); {
		c := input[i]
		switch c {
		case '\\':
			// Check if there is a character after \.
			if i+1 >= len(input) {
				// If there is not, just let Unquote generate an error.
				break scanLoop
			}
			if input[i+1] == '"' {
				// Skip escaped quote.
				i += 2
			} else {
				i++
			}
		case '"':
			// We are done, unquote it.
			input = input[:i+1] // +1 to capture ending quote too
			break scanLoop
		default:
			i++
		}
	}

	r.pos += len(input)
	return strconv.Unquote(input)
}

func (r *reader) next() (ch rune, size int) {
	if r.pos >= len(r.input) {
		return scanner.EOF, 0
	}
	return utf8.DecodeRuneInString(r.input[r.pos:])
}

func (r *reader) Peek() rune {
	ch, _ := r.next()
	return ch
}

func (r *reader) Read() rune {
	ch, size := r.next()
	r.pos += size
	return ch
}
