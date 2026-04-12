// Package lexer contains TraceQL lexer.
package lexer

import (
	"fmt"
	"strconv"
	"strings"
	"text/scanner"
	"unicode"

	"github.com/go-faster/oteldb/internal/lexerql"
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

// Tokenize scans given string to TraceQL tokens.
func Tokenize(s string, opts TokenizeOptions) ([]Token, error) {
	l := lexer{}
	l.scanner.Init(strings.NewReader(s))
	l.scanner.Filename = opts.Filename
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
	if r == '-' {
		if peekCh := l.scanner.Peek(); lexerql.IsDigit(peekCh) || peekCh == '.' {
			r = l.scanner.Scan()
			text = "-" + l.scanner.TokenText()
		}
	}
	tok.Text = text

	switch r {
	case scanner.Float:
		switch r := l.scanner.Peek(); {
		case lexerql.IsDurationRune(r):
			duration, err := lexerql.ScanDuration(&l.scanner, text)
			if err != nil {
				l.setError(err.Error(), tok.Pos)
				return tok, false
			}
			tok.Type = Duration
			tok.Text = duration
		default:
			tok.Type = Number
		}
		return tok, true
	case scanner.Int:
		switch r := l.scanner.Peek(); {
		case lexerql.IsDurationRune(r):
			duration, err := lexerql.ScanDuration(&l.scanner, text)
			if err != nil {
				l.setError(err.Error(), tok.Pos)
				return tok, false
			}
			tok.Type = Duration
			tok.Text = duration
		default:
			tok.Type = Integer
		}
		return tok, true
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
	switch text {
	case "parent":
		if peekCh != '.' {
			// Just "parent".
			break
		}
		// "parent" followed by dot, it's attribute selector.
		fallthrough
	case ".":
		tok.Type = Ident
		tok.Text = l.readAttributeSelector(text, peekCh)
		return tok, true
	case "resource":
		if peekCh == '.' {
			tok.Type = Ident
			tok.Text = l.readAttributeSelector(text, peekCh)
			return tok, true
		}
	case "span":
		switch peekCh {
		case '.':
			tok.Type = Ident
			tok.Text = l.readAttributeSelector(text, peekCh)
			return tok, true
		case ':':
			l.scanner.Next()
			tok.Type = SpanColon
			tok.Text = "span:"
			return tok, true
		}
	case "trace":
		if peekCh == ':' {
			l.scanner.Next()
			tok.Type = TraceColon
			tok.Text = "trace:"
			return tok, true
		}
	case "event":
		switch peekCh {
		case '.':
			tok.Type = Ident
			tok.Text = l.readAttributeSelector(text, peekCh)
			return tok, true
		case ':':
			l.scanner.Next()
			tok.Type = EventColon
			tok.Text = "event:"
			return tok, true
		}
	case "link":
		switch peekCh {
		case '.':
			tok.Type = Ident
			tok.Text = l.readAttributeSelector(text, peekCh)
			return tok, true
		case ':':
			l.scanner.Next()
			tok.Type = LinkColon
			tok.Text = "link:"
			return tok, true
		}
	case "instrumentation":
		switch peekCh {
		case '.':
			tok.Type = Ident
			tok.Text = l.readAttributeSelector(text, peekCh)
			return tok, true
		case ':':
			l.scanner.Next()
			tok.Type = InstrumentationColon
			tok.Text = "instrumentation:"
			return tok, true
		}
	}
	peeked := text + string(peekCh)

	tt, ok := tokens[peeked]
	if ok {
		tok.Type = tt
		tok.Text = peeked
		l.scanner.Next()
		return tok, true
	}

	tt, ok = tokens[text]
	if ok {
		tok.Type = tt
		return tok, true
	}

	tok.Type = Ident
	return tok, true
}

func (l *lexer) readAttributeSelector(prefix string, peekCh rune) string {
	var sb strings.Builder
	sb.WriteString(prefix)
	ch := peekCh
	for isAttributeRune(ch) {
		sb.WriteRune(l.scanner.Next())
		ch = l.scanner.Peek()
	}
	return sb.String()
}

func isAttributeRune(r rune) bool {
	if unicode.IsSpace(r) {
		return false
	}

	switch r {
	case scanner.EOF, '{', '}', '(', ')', '=', '~', '!', '<', '>', '&', '|', '^', ',':
		return false
	default:
		return true
	}
}
