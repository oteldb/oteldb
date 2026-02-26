package logparser

import (
	"testing"
)

func TestLogFmtParser(t *testing.T) {
	testParser("logfmt")(t)
}

func FuzzLogFmtParser(f *testing.F) {
	fuzzParser(f, "logfmt")
}
