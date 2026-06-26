// Package lexer contains ProfileQL lexer.
package lexer

import (
	"fmt"
	"strconv"
	"strings"
	"text/scanner"
	"unicode"

	"github.com/oteldb/oteldb/internal/lexerql"
)

type lexer struct {
	scanner scanner.Scanner
	tokens  []Token
	err     error
}

// TokenizeOptions is a Tokenize options structure.
type TokenizeOptions struct {
	// Filename sets filename for the scanner.
	Filename string
}

// Tokenize scans given string to ProfileQL tokens.
func Tokenize(s string, opts TokenizeOptions) ([]Token, error) {
	l := lexer{}
	l.scanner.Init(strings.NewReader(s))
	l.scanner.Filename = opts.Filename
	// Profile type identifiers contain colons
	// (e.g. "process_cpu:cpu:nanoseconds:cpu:nanoseconds"), and OTel label
	// names may contain dots (e.g. "service.name"). Allow both, but only
	// after the first character.
	l.scanner.IsIdentRune = func(ch rune, i int) bool {
		return ch == '_' || unicode.IsLetter(ch) ||
			(unicode.IsDigit(ch) && i > 0) ||
			((ch == '.' || ch == ':') && i > 0)
	}
	l.scanner.Error = func(s *scanner.Scanner, msg string) {
		l.setError(msg, s.Position)
	}

	for {
		r := l.scanner.Scan()
		switch r {
		case scanner.EOF:
			return l.tokens, l.err
		case '#':
			lexerql.ScanComment(&l.scanner)
			continue
		}

		tok, ok := l.nextToken(r, l.scanner.TokenText())
		if !ok {
			return l.tokens, l.err
		}
		l.tokens = append(l.tokens, tok)
	}
}

func (l *lexer) setError(msg string, pos scanner.Position) {
	l.err = &Error{
		Msg: msg,
		Pos: pos,
	}
}

func (l *lexer) nextToken(r rune, text string) (tok Token, _ bool) {
	tok.Pos = l.scanner.Position
	tok.Text = text

	switch r {
	case scanner.String, scanner.RawString:
		unquoted, err := strconv.Unquote(text)
		if err != nil {
			l.setError(fmt.Sprintf("unquote string: %s", err), tok.Pos)
			return tok, false
		}
		tok.Type = String
		tok.Text = unquoted
		return tok, true
	}

	peekCh := l.scanner.Peek()
	peeked := text + string(peekCh)
	if tt, ok := tokens[peeked]; ok {
		l.scanner.Next()
		tok.Type = tt
		tok.Text = peeked
		return tok, true
	}

	if tt, ok := tokens[text]; ok {
		tok.Type = tt
		return tok, true
	}

	tok.Type = Ident
	return tok, true
}
