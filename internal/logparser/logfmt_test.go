package logparser

import (
	"testing"
)

func TestLogFmtParser(t *testing.T) {
	testParser("logfmt")(t)
}
