package lexer

import (
	"fmt"
	"text/scanner"
)

// Error is a lexing error.
type Error struct {
	Msg string
	Pos scanner.Position
}

// Error implements error.
func (e *Error) Error() string {
	return fmt.Sprintf("at %s: %s", e.Pos, e.Msg)
}
