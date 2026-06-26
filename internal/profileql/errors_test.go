package profileql

import (
	"fmt"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
)

func TestParseErrorType(t *testing.T) {
	_, err := Parse(`process_cpu:cpu:nanoseconds:cpu:nanoseconds{region=~"("}`)
	require.Error(t, err)

	var perr *ParseError
	require.ErrorAs(t, err, &perr)
	require.NotNil(t, perr.Err)

	// Error message carries the position.
	require.Contains(t, perr.Error(), "at ")
	// Unwrap exposes the underlying cause.
	require.Equal(t, perr.Err, errors.Unwrap(perr))
	// FormatError path (verbose formatting) does not panic and mentions position.
	require.Contains(t, fmt.Sprintf("%+v", perr), "at ")
}

func TestTokenizeErrorPropagates(t *testing.T) {
	// Unterminated string surfaces a lexer error wrapped by newParser.
	_, err := Parse(`process_cpu:cpu:nanoseconds:cpu:nanoseconds{a="x}`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tokenize")
}
