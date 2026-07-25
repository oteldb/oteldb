// Package lexer contains TraceQL lexer.
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
		// NOTE: do not peek '.' here: "-.a" is a negated attribute selector,
		// not a number. Fractions like "-.5" are lexed as [Sub, Number].
		if peekCh := l.scanner.Peek(); lexerql.IsDigit(peekCh) {
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
		return l.attributeToken(tok, text, peekCh)
	case "resource":
		if peekCh == '.' {
			return l.attributeToken(tok, text, peekCh)
		}
	case "span":
		switch peekCh {
		case '.':
			return l.attributeToken(tok, text, peekCh)
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
			return l.attributeToken(tok, text, peekCh)
		case ':':
			l.scanner.Next()
			tok.Type = EventColon
			tok.Text = "event:"
			return tok, true
		}
	case "link":
		switch peekCh {
		case '.':
			return l.attributeToken(tok, text, peekCh)
		case ':':
			l.scanner.Next()
			tok.Type = LinkColon
			tok.Text = "link:"
			return tok, true
		}
	case "instrumentation":
		switch peekCh {
		case '.':
			return l.attributeToken(tok, text, peekCh)
		case ':':
			l.scanner.Next()
			tok.Type = InstrumentationColon
			tok.Text = "instrumentation:"
			return tok, true
		}
	}
	// Greedily consume the longest known operator, e.g. "!" -> "!>" -> "!>>".
	//
	// Only extend while the result is still a known token, so a partial match
	// never consumes runes belonging to the next token.
	longest := text
	tt, ok := tokens[text]
	for {
		candidate := longest + string(l.scanner.Peek())
		ct, cok := tokens[candidate]
		if !cok {
			break
		}
		l.scanner.Next()
		longest, tt, ok = candidate, ct, true
	}
	if ok {
		tok.Type = tt
		tok.Text = longest
		return tok, true
	}

	tok.Type = Ident
	return tok, true
}

// attributeToken reads an attribute selector token starting with prefix.
func (l *lexer) attributeToken(tok Token, prefix string, peekCh rune) (Token, bool) {
	text, ok := l.readAttributeSelector(prefix, peekCh)
	if !ok {
		return tok, false
	}
	tok.Type = Ident
	tok.Text = text
	return tok, true
}

// readAttributeSelector reads an attribute selector.
//
// Quoted parts are kept verbatim, quotes and all: the scope prefix must be cut
// off before they can be decoded, which the parser does.
func (l *lexer) readAttributeSelector(prefix string, peekCh rune) (string, bool) {
	var sb strings.Builder
	sb.WriteString(prefix)
	for ch := peekCh; ; ch = l.scanner.Peek() {
		switch {
		case ch == '"':
			if !l.readQuotedAttributePart(&sb) {
				return "", false
			}
		case isAttributeRune(ch):
			sb.WriteRune(l.scanner.Next())
		default:
			return sb.String(), true
		}
	}
}

// readQuotedAttributePart reads a quoted part of an attribute selector, so that
// a name may contain runes that would otherwise end the selector.
func (l *lexer) readQuotedAttributePart(sb *strings.Builder) bool {
	// Consume the opening quote.
	sb.WriteRune(l.scanner.Next())
	for {
		switch ch := l.scanner.Peek(); ch {
		case scanner.EOF:
			l.setError(`unexpected EOF, expecting '"'`, l.scanner.Pos())
			return false
		case '"':
			sb.WriteRune(l.scanner.Next())
			return true
		case '\\':
			sb.WriteRune(l.scanner.Next())
			if esc := l.scanner.Peek(); esc != '\\' && esc != '"' {
				l.setError("invalid escape sequence", l.scanner.Pos())
				return false
			}
			sb.WriteRune(l.scanner.Next())
		default:
			sb.WriteRune(l.scanner.Next())
		}
	}
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
