package logql

import (
	"regexp"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/lexerql"
)

// Label is a LogQL identifier.
type Label string

const (
	// ErrorLabel is a specific label for LogQL errors.
	ErrorLabel = "__error__"
	// ErrorDetailsLabel is a specific label for LogQL error details.
	ErrorDetailsLabel = "__error_details__"
)

// String implements [fmt.Stringer].
func (l Label) String() string {
	return string(l)
}

// IsValidLabel validates label name.
func IsValidLabel[S ~string | ~[]byte](s S, allowDot bool) error {
	if len(s) == 0 {
		return errors.New("label name cannot be empty")
	}
	if r := s[0]; !lexerql.IsIdentStartRune(r) {
		return errors.Errorf("invalid label name character %[1]q (%[1]U) at 0", r)
	}
	for i, r := range string(s) {
		if lexerql.IsIdentRune(r) || (allowDot && r == '.') {
			continue
		}
		return errors.Errorf("invalid label name character %[1]q (%[1]U) at %[2]d", r, i)
	}
	return nil
}

func compileLabelRegex(re string) (*regexp.Regexp, error) {
	return regexp.Compile("^(?:" + re + ")$")
}
