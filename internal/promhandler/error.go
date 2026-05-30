package promhandler

import (
	"fmt"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/promapi"
)

// PromError is a wrapper for API errors.
type PromError struct {
	Kind promapi.FailErrorType
	Msg  string
	Err  error
}

func validationErr(msg string, err error) error {
	return &PromError{
		Kind: promapi.FailErrorTypeBadData,
		Msg:  msg,
		Err:  err,
	}
}

func executionErr(msg string, err error) error {
	return &PromError{
		Kind: promapi.FailErrorTypeExecution,
		Msg:  msg,
		Err:  err,
	}
}

// Error implements [error].
func (e *PromError) Error() string {
	return fmt.Sprintf("%s: %s", e.Msg, e.Err)
}

// Unwrap returns the next error in the error chain.
// If there is no next error, Unwrap returns nil.
func (e *PromError) Unwrap() error {
	return e.Err
}

// FormatError prints the receiver's first error and returns the next error in
// the error chain, if any.
func (e *PromError) FormatError(p errors.Printer) error {
	p.Print(e.Msg)
	return e.Err
}
