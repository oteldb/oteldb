package lexer

import "text/scanner"

// Token is a ProfileQL token.
type Token struct {
	Type TokenType
	Text string
	Pos  scanner.Position
}

// TokenType defines ProfileQL token type.
type TokenType int

//go:generate go tool stringer -type=TokenType

const (
	Invalid TokenType = iota
	EOF
	Ident
	// String is a quoted string literal.
	String

	Comma
	OpenBrace
	CloseBrace
	Eq
	NotEq
	Re
	NotRe
)

var tokens = map[string]TokenType{
	",":  Comma,
	"{":  OpenBrace,
	"}":  CloseBrace,
	"=":  Eq,
	"!=": NotEq,
	"=~": Re,
	"!~": NotRe,
}
